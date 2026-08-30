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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

var _ = Describe("SGLang Connector", func() {

	var testInfo *sidecarTestInfo

	BeforeEach(func() {
		// Mock testing setup using the SGLang connector mode
		testInfo = sidecarConnectionTestSetup(KVConnectorSGLang)
	})

	It("should successfully send concurrent requests to prefill and decode with bootstrap info", func() {
		By("starting the proxy")
		go func() {
			defer GinkgoRecover()

			testInfo.proxy.allowlistValidator = &AllowlistValidator{enabled: false}
			err := testInfo.proxy.Start(testInfo.ctx)
			Expect(err).ToNot(HaveOccurred())

			testInfo.stoppedCh <- struct{}{}
		}()

		<-testInfo.proxy.readyCh
		proxyBaseAddr := "http://" + testInfo.proxy.addr.String()

		By("sending a /v1/chat/completions request with prefill header")
		body := `{
				"model": "Qwen/Qwen2-0.5B",
				"messages": [
				  {"role": "user", "content": "Hello"}
				],
				"max_tokens": 50
			}`

		req, err := http.NewRequest(http.MethodPost, proxyBaseAddr+ChatCompletionsPath, bytes.NewReader([]byte(body)))
		Expect(err).ToNot(HaveOccurred())

		prefillHostPort := testInfo.prefillBackend.URL[len("http://"):]
		req.Header.Add(routing.PrefillEndpointHeader, prefillHostPort)

		rp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())

		if rp.StatusCode != 200 {
			bp, _ := io.ReadAll(rp.Body) //nolint:errcheck
			Fail(string(bp))
		}

		// Because SGLang connector sends requests concurrently (prefill in goroutine),
		// wait until the prefill handler has finished processing before reading its state.
		Eventually(testInfo.prefillHandler.RequestCount.Load).Should(Equal(int32(1)))

		// Validate prefill request
		prefillReqs := testInfo.prefillHandler.GetCompletionRequests()
		Expect(prefillReqs).To(HaveLen(1))
		prq1 := prefillReqs[0]

		// Validate decode request
		Expect(testInfo.decodeHandler.RequestCount.Load()).To(BeNumerically("==", 1))
		decodeReqs := testInfo.decodeHandler.GetCompletionRequests()
		Expect(decodeReqs).To(HaveLen(1))
		drq1 := decodeReqs[0]

		// Bootstrap validations for prefill
		Expect(prq1).To(HaveKey(requestFieldBootstrapHost))
		Expect(prq1).To(HaveKey(requestFieldBootstrapPort))
		Expect(prq1).To(HaveKey(requestFieldBootstrapRoom))

		expectedHost := extractHost(prefillHostPort)
		Expect(prq1[requestFieldBootstrapHost]).To(Equal(expectedHost))
		Expect(prq1[requestFieldBootstrapPort]).To(Equal(float64(sglangBootstrapPort)))
		Expect(prq1[requestFieldBootstrapRoom]).ToNot(BeNil())

		// Bootstrap validations for decode
		Expect(drq1).To(HaveKey(requestFieldBootstrapHost))
		Expect(drq1).To(HaveKey(requestFieldBootstrapPort))
		Expect(drq1).To(HaveKey(requestFieldBootstrapRoom))

		Expect(drq1[requestFieldBootstrapHost]).To(Equal(expectedHost))
		Expect(drq1[requestFieldBootstrapPort]).To(Equal(float64(sglangBootstrapPort)))
		Expect(drq1[requestFieldBootstrapRoom]).To(Equal(prq1[requestFieldBootstrapRoom])) // Room ID must match

		testInfo.cancelFn()
		<-testInfo.stoppedCh
	})

	It("should not panic when prefill response is slower than decode response", func() {
		// Stop previously injected servers
		testInfo.decodeBackend.Close()
		testInfo.prefillBackend.Close()

		var prefillFinished atomic.Bool

		slowPrefill := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			testInfo.prefillHandler.ServeHTTP(w, r)
			time.Sleep(300 * time.Millisecond) // Simulated load delay on KV Cache
			prefillFinished.Store(true)
		})
		testInfo.prefillBackend = httptest.NewServer(slowPrefill)

		fastDecode := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			testInfo.decodeHandler.ServeHTTP(w, r)
		})
		testInfo.decodeBackend = httptest.NewServer(fastDecode)
		testInfo.decodeURL, _ = url.Parse(testInfo.decodeBackend.URL)

		// Re-initialize proxy to fetch the new mock addresses
		cfg := Config{
			Port:        "0",
			DecoderURL:  testInfo.decodeURL,
			KVConnector: KVConnectorSGLang,
		}
		testInfo.proxy = NewProxy(cfg)

		go func() {
			defer GinkgoRecover()
			testInfo.proxy.allowlistValidator = &AllowlistValidator{enabled: false}
			err := testInfo.proxy.Start(testInfo.ctx)
			Expect(err).ToNot(HaveOccurred())
			testInfo.stoppedCh <- struct{}{}
		}()

		<-testInfo.proxy.readyCh
		proxyBaseAddr := "http://" + testInfo.proxy.addr.String()

		body := `{"model": "Qwen", "messages": [{"role": "user", "content": "Hello"}], "max_tokens": 50}`
		req, err := http.NewRequest(http.MethodPost, proxyBaseAddr+ChatCompletionsPath, bytes.NewReader([]byte(body)))
		Expect(err).ToNot(HaveOccurred())

		prefillHostPort := testInfo.prefillBackend.URL[len("http://"):]
		req.Header.Add(routing.PrefillEndpointHeader, prefillHostPort)

		// Submit request. This will complete as soon as fastDecode completes.
		rp, err := http.DefaultClient.Do(req)
		Expect(err).ToNot(HaveOccurred())
		Expect(rp.StatusCode).To(Equal(200))

		// The original panicking goroutine takes 300ms total. Give it time to attempt finishing up!
		time.Sleep(500 * time.Millisecond)

		Expect(prefillFinished.Load()).To(BeTrue())
		Expect(testInfo.prefillHandler.RequestCount.Load()).To(BeNumerically("==", 1))
		Expect(testInfo.decodeHandler.RequestCount.Load()).To(BeNumerically("==", 1))

		testInfo.cancelFn()
		<-testInfo.stoppedCh
	})

	It("should dispatch decode to the data-parallel endpoint selected by EPP", func() {
		var defaultDecodeRequests atomic.Int32
		var selectedDecodeRequests atomic.Int32
		prefillDataParallelTarget := make(chan string, 1)
		prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			prefillDataParallelTarget <- r.Header.Get(routing.DataParallelEndpointHeader)
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(prefill.Close)
		prefillTarget := strings.TrimPrefix(prefill.URL, "http://")

		testInfo.proxy.decoderProxy = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			defaultDecodeRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		})
		selectedHostPort := "10.0.0.1:8005"
		testInfo.proxy.forwardDataParallel = true
		testInfo.proxy.dataParallelProxies[selectedHostPort] = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			selectedDecodeRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath,
			bytes.NewBufferString(`{"model":"Qwen","messages":[{"role":"user","content":"Hello"}]}`))
		req.Header.Set(routing.DataParallelEndpointHeader, selectedHostPort)
		res := httptest.NewRecorder()
		body := []byte(`{"model":"Qwen","messages":[{"role":"user","content":"Hello"}]}`)

		testInfo.proxy.handleSGLangConcurrentRequests(res, req, body, prefillTarget, 1234)

		Expect(selectedDecodeRequests.Load()).To(Equal(int32(1)))
		Expect(defaultDecodeRequests.Load()).To(Equal(int32(0)))
		Expect(<-prefillDataParallelTarget).To(Equal(prefillTarget))
	})

	It("should keep the EPP-selected decode rank while hinting the selected prefill rank", func() {
		originalSetting := sglangInjectPrefillDPRankHint
		sglangInjectPrefillDPRankHint = true
		DeferCleanup(func() { sglangInjectPrefillDPRankHint = originalSetting })

		var selectedDecodeRequests atomic.Int32
		var coupledDecodeRequests atomic.Int32
		prefillRequest := make(chan map[string]any, 1)
		prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			decoder := json.NewDecoder(r.Body)
			decoder.UseNumber()
			Expect(decoder.Decode(&body)).To(Succeed())
			prefillRequest <- body
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(prefill.Close)
		prefillURL, err := url.Parse(prefill.URL)
		Expect(err).NotTo(HaveOccurred())
		prefillPort, err := strconv.Atoi(prefillURL.Port())
		Expect(err).NotTo(HaveOccurred())

		basePort := prefillPort - 7
		originalDecodeEndpoint := "10.0.0.20:" + strconv.Itoa(basePort+1)
		coupledDecodeEndpoint := "10.0.0.20:" + strconv.Itoa(prefillPort)
		// Rank listeners override config.Port, but retain the rank-0 base port.
		testInfo.proxy.config.Port = strconv.Itoa(prefillPort)
		testInfo.proxy.dpBasePort = basePort
		testInfo.proxy.config.DataParallelSize = 8
		// Requests selected through virtual ports 8001-8007 run on a cloned
		// server with forwardDataParallel disabled. The selected listener must
		// continue to own decode routing even when the prefill-rank hint differs.
		testInfo.proxy.forwardDataParallel = false
		decodeRequest := make(chan map[string]any, 1)
		testInfo.proxy.decoderProxy = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body map[string]any
			decoder := json.NewDecoder(r.Body)
			decoder.UseNumber()
			Expect(decoder.Decode(&body)).To(Succeed())
			Expect(r.Header.Get(routing.DataParallelEndpointHeader)).To(Equal(originalDecodeEndpoint))
			decodeRequest <- body
			selectedDecodeRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		})
		testInfo.proxy.dataParallelProxies[coupledDecodeEndpoint] = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			coupledDecodeRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath,
			bytes.NewBufferString(`{"model":"Qwen","messages":[{"role":"user","content":"Hello"}]}`))
		req.Header.Set(routing.DataParallelEndpointHeader, originalDecodeEndpoint)
		res := httptest.NewRecorder()
		testInfo.proxy.handleSGLangConcurrentRequests(
			res, req, []byte(`{"model":"Qwen","messages":[{"role":"user","content":"Hello"}],"bootstrap_room":1787598460057474549}`),
			strings.TrimPrefix(prefill.URL, "http://"), 1234,
		)

		Expect(res.Code).To(Equal(http.StatusOK))
		Expect(selectedDecodeRequests.Load()).To(Equal(int32(1)))
		Expect(coupledDecodeRequests.Load()).To(Equal(int32(0)))
		prefillBody := <-prefillRequest
		decodeBody := <-decodeRequest
		Expect(prefillBody).NotTo(HaveKey("disagg_prefill_dp_rank"))
		Expect(decodeBody).To(HaveKeyWithValue("disagg_prefill_dp_rank", json.Number("7")))
		Expect(prefillBody).To(HaveKeyWithValue("bootstrap_room", json.Number("1787598460057474549")))
		Expect(decodeBody).To(HaveKeyWithValue("bootstrap_room", json.Number("1787598460057474549")))
	})

	It("cancels decode and returns the prefill HTTP error", func() {
		prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"prefill failed"}`))
		}))
		DeferCleanup(prefill.Close)

		var decodeCanceled atomic.Bool
		testInfo.proxy.decoderProxy = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			decodeCanceled.Store(true)
		})

		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath,
			bytes.NewBufferString(`{"model":"Qwen"}`)).WithContext(ctx)
		req.Header.Set("x-request-id", "prefill-http-error")
		res := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			testInfo.proxy.handleSGLangConcurrentRequests(
				res, req, []byte(`{"model":"Qwen"}`),
				strings.TrimPrefix(prefill.URL, "http://"), 1234,
			)
		}()

		Eventually(done, time.Second).Should(BeClosed())
		Expect(res.Code).To(Equal(http.StatusInternalServerError))
		Expect(res.Body.String()).To(Equal(`{"error":"prefill failed"}`))
		Expect(decodeCanceled.Load()).To(BeTrue())
	})

	It("cancels decode and returns bad gateway on prefill transport failure", func() {
		prefill := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		prefillTarget := strings.TrimPrefix(prefill.URL, "http://")
		prefill.Close()

		var decodeCanceled atomic.Bool
		testInfo.proxy.decoderProxy = http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			decodeCanceled.Store(true)
		})

		ctx, cancel := context.WithCancel(context.Background())
		DeferCleanup(cancel)
		req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath,
			bytes.NewBufferString(`{"model":"Qwen"}`)).WithContext(ctx)
		req.Header.Set("x-request-id", "prefill-transport-error")
		res := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(done)
			testInfo.proxy.handleSGLangConcurrentRequests(
				res, req, []byte(`{"model":"Qwen"}`), prefillTarget, 5678,
			)
		}()

		Eventually(done, time.Second).Should(BeClosed())
		Expect(res.Code).To(Equal(http.StatusBadGateway))
		Expect(decodeCanceled.Load()).To(BeTrue())
	})

	It("propagates an abort-handler panic to the serving goroutine", func() {
		prefill := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		DeferCleanup(prefill.Close)

		testInfo.proxy.decoderProxy = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic(http.ErrAbortHandler)
		})

		req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath,
			bytes.NewBufferString(`{"model":"Qwen"}`))
		res := httptest.NewRecorder()
		Expect(func() {
			testInfo.proxy.handleSGLangConcurrentRequests(
				res, req, []byte(`{"model":"Qwen"}`),
				strings.TrimPrefix(prefill.URL, "http://"), 1234,
			)
		}).To(PanicWith(http.ErrAbortHandler))
	})

	It("records the first decode response write once", func() {
		res := httptest.NewRecorder()
		writer, firstWrite := newFirstWriteResponseWriter(res)

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte("first"))
		Expect(err).NotTo(HaveOccurred())
		first := <-firstWrite

		_, err = writer.Write([]byte("second"))
		Expect(err).NotTo(HaveOccurred())
		Consistently(firstWrite).ShouldNot(Receive())
		Expect(first).NotTo(BeZero())
	})
})
