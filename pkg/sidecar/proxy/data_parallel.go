package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

const (
	DataParallelModeExternalLB = "external-lb"
	DataParallelModeInternalLB = "internal-lb"
)

// newInternalLBProxyHandler pins inference requests to rank while presenting a
// rank-local metrics view to the EPP. Only /metrics is buffered; normal model
// responses retain the reverse proxy's streaming behavior.
func newInternalLBProxyHandler(backend http.Handler, rank int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/metrics" {
			proxyRankMetrics(w, r, backend, rank)
			return
		}

		r.Header.Set(requestHeaderDataParallelRank, strconv.Itoa(rank))
		backend.ServeHTTP(w, r)
	})
}

func proxyRankMetrics(w http.ResponseWriter, r *http.Request, backend http.Handler, rank int) {
	req := r.Clone(r.Context())
	req.Header = r.Header.Clone()
	req.Header.Del("Accept-Encoding")

	buffered := &bufferedResponseWriter{}
	backend.ServeHTTP(buffered, req)
	for key, values := range buffered.Header() {
		if strings.EqualFold(key, "Content-Length") {
			continue
		}
		w.Header()[key] = append([]string(nil), values...)
	}

	body := buffered.bodyBytes()
	if buffered.statusCode >= 200 && buffered.statusCode < 300 {
		var err error
		body, err = filterMetricsForRank(body, rank)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to filter model-server metrics: %v", err), http.StatusBadGateway)
			return
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(buffered.statusCode)
	_, _ = w.Write(body)
}

func filterMetricsForRank(input []byte, rank int) ([]byte, error) {
	wanted := strconv.Itoa(rank)
	var output bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(input))
	// Prometheus exposition lines can be large when they contain many labels.
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if metricRank, found := prometheusLabelValue(line, "dp_rank"); found && metricRank != wanted {
			continue
		}
		_, _ = io.WriteString(&output, line)
		_ = output.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func prometheusLabelValue(line, label string) (string, bool) {
	open := strings.IndexByte(line, '{')
	close := strings.LastIndexByte(line, '}')
	if open < 0 || close <= open {
		return "", false
	}
	needle := label + "=\""
	labels := line[open+1 : close]
	for _, field := range strings.Split(labels, ",") {
		field = strings.TrimSpace(field)
		if strings.HasPrefix(field, needle) && strings.HasSuffix(field, "\"") {
			return strings.TrimSuffix(strings.TrimPrefix(field, needle), "\""), true
		}
	}
	return "", false
}

// dataParallelHandler checks if Data Parallel handling is needed.
// Returns true if Data Parallel processing was needed
func (s *Server) dataParallelHandler(w http.ResponseWriter, r *http.Request) bool {
	dataParallelPodHostPort := r.Header.Get(routing.DataParallelEndpointHeader)
	if dataParallelPodHostPort != "" {
		s.logger.Info("The use of the x-data-parallel-host-port is deprecated. Use Istio >= 1.28.1.")
		handler := s.dataParallelProxies[dataParallelPodHostPort]
		if handler != nil {
			s.logger.V(4).Info("Data parallel routing", "to", dataParallelPodHostPort)
			handler.ServeHTTP(w, r)
		} else {
			// Shouldn't happen, send to default server
			s.logger.V(4).Info("Didn't find the Data Parallel Proxy", "for", dataParallelPodHostPort)
			w.WriteHeader(http.StatusBadRequest)
		}
		return true
	}

	s.logger.V(4).Info("skip data parallel")
	return false
}

func (s *Server) startDataParallel(ctx context.Context, grp *errgroup.Group) error {
	podIP := os.Getenv("POD_IP")
	basePort, err := strconv.Atoi(s.config.Port)
	if err != nil {
		return err
	}
	baseDecoderPort, err := strconv.Atoi(s.config.DecoderURL.Port())
	if err != nil {
		return err
	}
	decoderScheme := s.config.DecoderURL.Scheme // capture before goroutines launch
	s.dataParallelProxies[net.JoinHostPort(podIP, s.config.Port)] = s.decoderProxy

	// Fill in map of proxies, thus avoiding locks
	for idx := range s.config.DataParallelSize - 1 {
		rankPort := strconv.Itoa(basePort + idx + 1)
		hostPort := net.JoinHostPort(podIP, rankPort)
		decoderPort := strconv.Itoa(baseDecoderPort + idx + 1)
		if s.config.DataParallelMode == DataParallelModeInternalLB {
			decoderPort = strconv.Itoa(baseDecoderPort)
		}
		decoderURL, err := url.Parse(decoderScheme + "://localhost:" + decoderPort)
		if err != nil {
			return err
		}
		var handler http.Handler = s.createDecoderProxyHandler(decoderURL, s.config.InsecureSkipVerifyForDecoder)
		if s.config.DataParallelMode == DataParallelModeInternalLB {
			handler = newInternalLBProxyHandler(handler, idx+1)
		}
		s.dataParallelProxies[hostPort] = handler
	}

	for idx := range s.config.DataParallelSize - 1 {
		rankPort := strconv.Itoa(basePort + idx + 1)
		decoderPort := strconv.Itoa(baseDecoderPort + idx + 1)
		if s.config.DataParallelMode == DataParallelModeInternalLB {
			decoderPort = strconv.Itoa(baseDecoderPort)
		}
		decoderURL, err := url.Parse(decoderScheme + "://localhost:" + decoderPort)
		if err != nil {
			return err
		}

		clone := s.Clone()
		clone.config.Port = rankPort
		clone.config.DecoderURL = decoderURL
		clone.forwardDataParallel = false
		clone.dataParallelRank = idx + 1

		grp.Go(func() error {
			clone.logger = log.FromContext(ctx).WithName("proxy server on port " + rankPort)
			// Configure handlers
			clone.handler = clone.createRoutes()
			clone.setKVConnector()

			return clone.startHTTP(ctx)
		})
	}
	return nil
}
