/*
Copyright 2025 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/felixge/httpsnoop"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/llm-d/llm-d-router/pkg/common/observability/tracing"
	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

var (
	sglangBootstrapPort           int
	sglangInjectPrefillDPRankHint bool
)

const sglangInjectPrefillDPRankHintEnv = "SGLANG_INJECT_PREFILL_DP_RANK_HINT"
const requestFieldDisaggPrefillDPRank = "disagg_prefill_dp_rank"

func newFirstWriteResponseWriter(w http.ResponseWriter) (http.ResponseWriter, <-chan time.Time) {
	firstWrite := make(chan time.Time, 1)
	var once sync.Once
	record := func() {
		once.Do(func() { firstWrite <- time.Now() })
	}
	return httpsnoop.Wrap(w, httpsnoop.Hooks{
		Write: func(next httpsnoop.WriteFunc) httpsnoop.WriteFunc {
			return func(body []byte) (int, error) {
				record()
				return next(body)
			}
		},
		ReadFrom: func(next httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
			return func(src io.Reader) (int64, error) {
				record()
				return next(src)
			}
		},
	}), firstWrite
}

func init() {
	// Default SGLang bootstrap port
	sglangBootstrapPort = 8998

	// Override from environment variable if set
	if portStr := os.Getenv("SGLANG_BOOTSTRAP_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			sglangBootstrapPort = port
		}
	}
	if enabled, err := strconv.ParseBool(os.Getenv(sglangInjectPrefillDPRankHintEnv)); err == nil {
		sglangInjectPrefillDPRankHint = enabled
	}
}

func dataParallelRankFromEndpoint(endpoint string, basePort, dpSize int) (int, bool) {
	_, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return 0, false
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < basePort || port >= basePort+dpSize {
		return 0, false
	}
	return port - basePort, true
}

func (s *Server) handleSGLang(w http.ResponseWriter, r *http.Request, prefillPodHostPort string) {
	s.logger.V(4).Info("running SGLang protocol", "url", prefillPodHostPort)

	// Make Request
	requestData, err := s.parseSGLangRequest(r)

	if err != nil {
		if err := errorJSONInvalid(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}

	roomID := s.generateSGLangRoomID()

	// Inject bootstrap info for both prefill and decode
	bootstrapInfo := s.addSGLangBootstrapInfo(requestData, prefillPodHostPort, roomID)

	body, err := json.Marshal(bootstrapInfo)
	if err != nil {
		if err := errorJSONInvalid(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}

	// Send concurrent prefill and decode requests
	s.handleSGLangConcurrentRequests(w, r, body, prefillPodHostPort, roomID)
}

func (s *Server) handleSGLangConcurrentRequests(w http.ResponseWriter, r *http.Request, body []byte, prefillHost string, roomID int64) {
	tracer := tracing.Tracer(tracerScope)
	ctx := r.Context()
	requestStart, _ := ctx.Value(requestStartTimeKey).(time.Time)
	requestID := r.Header.Get(requestHeaderRequestID)
	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Prefill Stage - async
	prefillCtx, prefillSpan := tracer.Start(dispatchCtx, "prefill",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	prefillSpan.SetAttributes(
		attribute.String("llm_d.pd_proxy.prefill_target", prefillHost),
		attribute.String("llm_d.pd_proxy.connector", KVConnectorSGLang),
		attribute.Bool("llm_d.pd_proxy.prefill.async", true),
	)
	prefillStart := time.Now()

	// Both legs share a cancelable context. Decode output is held until prefill
	// succeeds, so a failed prefill can cancel decode without exposing a bogus
	// success response to the client.
	prefillReq := cloneRequestWithBody(prefillCtx, r, body)
	// The incoming data-parallel endpoint selects the decode rank. The prefill
	// sidecar must instead see its own selected virtual endpoint; otherwise rank
	// 0's primary listener tries to find the decode pod in its local proxy map
	// and returns 400. The decode request retains the original header below.
	prefillReq.Header.Set(routing.DataParallelEndpointHeader, prefillHost)

	prefillHandler, err := s.prefillerProxyHandler(prefillHost)
	if err != nil {
		prefillSpan.SetStatus(codes.Error, "failed to create prefill handler")
		prefillSpan.End()
		if err := errorBadGateway(err, w); err != nil {
			s.logger.Error(err, "failed to send error response to client")
		}
		return
	}

	type prefillResult struct {
		response       *bufferedResponseWriter
		duration       time.Duration
		transportError string
	}
	prefillDone := make(chan prefillResult, 1)

	// Clone the cached reverse proxy before installing a request-scoped error
	// handler. Mutating the shared proxy would race concurrent requests.
	var transportError string
	if proxy, ok := prefillHandler.(*httputil.ReverseProxy); ok {
		proxyClone := *proxy
		originalErrorHandler := proxy.ErrorHandler
		proxyClone.ErrorHandler = func(w http.ResponseWriter, req *http.Request, err error) {
			transportError = err.Error()
			if originalErrorHandler != nil {
				originalErrorHandler(w, req, err)
				return
			}
			w.WriteHeader(http.StatusBadGateway)
		}
		prefillHandler = &proxyClone
	}

	// Send prefill request asynchronously.
	go func() {
		defer prefillSpan.End()
		defer func() {
			if rec := recover(); rec != nil && rec != http.ErrAbortHandler {
				s.logger.Error(fmt.Errorf("panic: %v", rec), "panic in prefill request")
			}
		}()
		pw := &bufferedResponseWriter{}
		prefillHandler.ServeHTTP(pw, prefillReq)
		prefillDuration := time.Since(prefillStart)
		prefillSpan.SetAttributes(
			attribute.Int("llm_d.pd_proxy.prefill.status_code", pw.statusCode),
			attribute.Float64("llm_d.pd_proxy.prefill.duration_ms", float64(prefillDuration.Milliseconds())),
		)
		if pw.statusCode < 200 || pw.statusCode >= 300 {
			prefillSpan.SetStatus(codes.Error, "prefill request failed")
		}
		s.logger.V(5).Info("prefill request completed", "status", pw.statusCode)
		prefillDone <- prefillResult{
			response:       pw,
			duration:       prefillDuration,
			transportError: transportError,
		}
	}()

	// Decode Stage - concurrent, with its response deferred until prefill wins
	// the commit decision.
	decodeCtx, decodeSpan := tracer.Start(dispatchCtx, "decode",
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer decodeSpan.End()

	decodeSpan.SetAttributes(
		attribute.String("llm_d.pd_proxy.connector", KVConnectorSGLang),
		attribute.Bool("llm_d.pd_proxy.decode.concurrent_with_prefill", true),
	)
	decodeStart := time.Now()

	decodeBody := body
	if sglangInjectPrefillDPRankHint {
		basePort := s.dpBasePort
		if basePort != 0 {
			if prefillRank, ok := dataParallelRankFromEndpoint(
				prefillHost, basePort, s.config.DataParallelSize,
			); ok {
				decodeRequestData := make(map[string]interface{})
				decoder := json.NewDecoder(bytes.NewReader(body))
				decoder.UseNumber()
				if err := decoder.Decode(&decodeRequestData); err != nil {
					if err := errorJSONInvalid(err, w); err != nil {
						s.logger.Error(err, "failed to send error response to client")
					}
					return
				}
				decodeRequestData[requestFieldDisaggPrefillDPRank] = prefillRank
				var err error
				decodeBody, err = json.Marshal(decodeRequestData)
				if err != nil {
					if err := errorJSONInvalid(err, w); err != nil {
						s.logger.Error(err, "failed to send error response to client")
					}
					return
				}
				s.logger.V(4).Info("Added selected prefill DP rank hint to decode request",
					"prefillEndpoint", prefillHost,
					"prefillDPRank", prefillRank,
					"decodeEndpoint", r.Header.Get(routing.DataParallelEndpointHeader),
				)
			}
		}
	}
	decodeReq := cloneRequestWithBody(decodeCtx, r, decodeBody)
	deferredWriter := newDeferredCommitWriter(w)
	decodeWriter, firstWriteCh := newFirstWriteResponseWriter(deferredWriter)
	type decodeResult struct {
		duration   time.Duration
		panicValue any
	}
	decodeDone := make(chan decodeResult, 1)
	go func() {
		defer func() {
			decodeDone <- decodeResult{
				duration:   time.Since(decodeStart),
				panicValue: recover(),
			}
		}()

		// Virtual-port servers bypass dataParallelHandler because their listener
		// already identifies the independently EPP-selected decode rank.
		dataParallelUsed := false
		if s.forwardDataParallel {
			dataParallelUsed = s.dataParallelHandler(decodeWriter, decodeReq)
		}
		if !dataParallelUsed {
			s.decoderProxy.ServeHTTP(decodeWriter, decodeReq)
		}
	}()

	prefill := <-prefillDone
	var decode decodeResult
	if isHTTPError(prefill.response.statusCode) {
		cancel()
		deferredWriter.abort()
		decode = <-decodeDone
		if decode.panicValue != nil && decode.panicValue != http.ErrAbortHandler {
			panic(decode.panicValue)
		}
		for key, values := range prefill.response.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(prefill.response.statusCode)
		if _, err := w.Write(prefill.response.bodyBytes()); err != nil {
			s.logger.Error(err, "failed to send prefill error response to client")
		}
	} else {
		deferredWriter.commit()
		decode = <-decodeDone
		if decode.panicValue != nil {
			panic(decode.panicValue)
		}
	}
	decodeDuration := decode.duration

	firstWriteDuration := time.Duration(-1)
	select {
	case firstWrite := <-firstWriteCh:
		firstWriteDuration = firstWrite.Sub(decodeStart)
	default:
	}
	prefillDuration := prefill.duration
	setupDuration := time.Duration(-1)
	totalDuration := time.Duration(-1)
	requestStartText := ""
	if !requestStart.IsZero() {
		setupDuration = decodeStart.Sub(requestStart)
		totalDuration = time.Since(requestStart)
		requestStartText = requestStart.UTC().Format(time.RFC3339Nano)
	}
	s.logger.Info("SGLang P/D request timing",
		"prefillTarget", prefillHost,
		"prefillStatusCode", prefill.response.statusCode,
		"prefillTransportError", prefill.transportError,
		"bootstrapRoom", roomID,
		"requestID", requestID,
		"requestStart", requestStartText,
		"setupDurationMs", float64(setupDuration)/float64(time.Millisecond),
		"prefillUpstreamDurationMs", float64(prefillDuration)/float64(time.Millisecond),
		"decodeFirstWriteDurationMs", float64(firstWriteDuration)/float64(time.Millisecond),
		"decodeUpstreamDurationMs", float64(decodeDuration)/float64(time.Millisecond),
		"totalDurationMs", float64(totalDuration)/float64(time.Millisecond),
	)
	decodeSpan.SetAttributes(
		attribute.Float64("llm_d.pd_proxy.decode.duration_ms", float64(decodeDuration.Milliseconds())),
		attribute.String("llm_d.pd_proxy.decode.target", s.config.DecoderURL.Host),
	)

	// Calculate end-to-end P/D timing metrics for concurrent P/D.
	// True TTFT captures time from gateway request start to decode start.
	// In SGLang's concurrent mode, prefill duration is tracked in the async prefill span.
	if currentSpan := trace.SpanFromContext(decodeCtx); currentSpan.SpanContext().IsValid() {
		var totalDuration time.Duration
		var trueTTFT time.Duration
		if requestStartValue := ctx.Value(requestStartTimeKey); requestStartValue != nil {
			if requestStart, ok := requestStartValue.(time.Time); ok {
				totalDuration = time.Since(requestStart)
				trueTTFT = decodeStart.Sub(requestStart)
			}
		}

		currentSpan.SetAttributes(
			attribute.Float64("llm_d.pd_proxy.total_duration_ms", float64(totalDuration.Milliseconds())),
			attribute.Float64("llm_d.pd_proxy.true_ttft_ms", float64(trueTTFT.Milliseconds())),
			attribute.Float64("llm_d.pd_proxy.decode_duration_ms", float64(decodeDuration.Milliseconds())),
			attribute.Bool("llm_d.pd_proxy.concurrent_pd", true),
		)
	}
}

func (s *Server) addSGLangBootstrapInfo(requestData map[string]interface{}, prefillHostPort string, roomID int64) map[string]interface{} {
	modifiedRequest := make(map[string]interface{})
	for k, v := range requestData {
		modifiedRequest[k] = v
	}

	// Generate bootstrap host from prefill host
	bootstrapHost := extractHost(prefillHostPort)

	// Add bootstrap information
	modifiedRequest[requestFieldBootstrapHost] = bootstrapHost
	modifiedRequest[requestFieldBootstrapPort] = sglangBootstrapPort
	modifiedRequest[requestFieldBootstrapRoom] = roomID

	s.logger.V(5).Info("bootstrap info added",
		"bootstrap_host", bootstrapHost,
		"bootstrap_port", sglangBootstrapPort,
		"bootstrap_room", roomID)

	return modifiedRequest
}

func (s *Server) parseSGLangRequest(r *http.Request) (map[string]interface{}, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	var requestData map[string]interface{}
	if err := json.Unmarshal(body, &requestData); err != nil {
		return nil, fmt.Errorf("failed to parse request body: %w", err)
	}

	return requestData, nil
}

func (s *Server) generateSGLangRoomID() int64 {
	return time.Now().UnixNano() + int64(rand.IntN(1000))
}
