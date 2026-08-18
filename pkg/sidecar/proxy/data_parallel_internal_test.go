/*
Copyright 2026 The llm-d Authors.

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
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/llm-d/llm-d-router/pkg/common/routing"
	fwknet "github.com/llm-d/llm-d-router/test/framework/net"
)

func TestInternalLBDataParallelRoutesVirtualRanksToOneBackend(t *testing.T) {
	ranks := make(chan string, 2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ranks <- r.Header.Get(requestHeaderDataParallelRank)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	decoderURL, err := url.Parse(backend.URL)
	require.NoError(t, err)

	freePort, err := fwknet.GetFreePort()
	require.NoError(t, err)
	basePort := freePort - 1
	t.Setenv("POD_IP", testLoopbackIP)

	server := NewProxy(Config{
		Port:             strconv.Itoa(basePort),
		DecoderURL:       decoderURL,
		KVConnector:      KVConnectorNIXLV2,
		DataParallelSize: 2,
		DataParallelMode: DataParallelModeInternalLB,
	})
	server.allowlistValidator, err = NewAllowlistValidator(false, routing.InferencePoolAPIGroup, "", "")
	require.NoError(t, err)
	server.handler = server.createRoutes()

	ctx, cancel := context.WithCancel(newTestContext())
	grp, ctx := errgroup.WithContext(ctx)
	require.NoError(t, server.startDataParallel(ctx, grp))

	req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath, nil)
	req.Header.Set(routing.DataParallelEndpointHeader, testLoopbackIP+":"+strconv.Itoa(basePort+1))
	req.Header.Set(requestHeaderDataParallelRank, "7")
	resp := httptest.NewRecorder()
	server.handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusNoContent, resp.Code)
	require.Equal(t, "1", <-ranks)

	cancel()
	require.NoError(t, grp.Wait())
}

func TestInternalLBProxyHandlerPinsSelectedRank(t *testing.T) {
	var gotRank string
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRank = r.Header.Get(requestHeaderDataParallelRank)
		w.WriteHeader(http.StatusNoContent)
	})

	handler := newInternalLBProxyHandler(backend, 3)
	req := httptest.NewRequest(http.MethodPost, ChatCompletionsPath, nil)
	req.Header.Set(requestHeaderDataParallelRank, "7")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	require.Equal(t, http.StatusNoContent, resp.Code)
	require.Equal(t, "3", gotRank, "the sidecar-selected rank must override a client header")
}

func TestInternalLBProxyHandlerFiltersMetricsByRank(t *testing.T) {
	const metrics = `# HELP sglang:num_running_reqs Running requests.
# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{dp_rank="0",model="glm"} 1
sglang:num_running_reqs{dp_rank="1",model="glm"} 2
# HELP sglang:token_usage Unranked process metric.
# TYPE sglang:token_usage gauge
sglang:token_usage 0.5
`
	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, metrics)
	})

	handler := newInternalLBProxyHandler(backend, 1)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, resp.Code)
	body := resp.Body.String()
	require.Contains(t, body, "# HELP sglang:num_running_reqs")
	require.Contains(t, body, `sglang:num_running_reqs{dp_rank="1",model="glm"} 2`)
	require.NotContains(t, body, `dp_rank="0"`)
	require.Contains(t, body, "sglang:token_usage 0.5")
}

func TestInternalLBProxyHandlerDoesNotBufferStreamingResponses(t *testing.T) {
	firstFlushed := make(chan struct{})
	releaseSecond := make(chan struct{})
	backend := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)
		_, _ = io.WriteString(w, "data: first\n\n")
		flusher.Flush()
		close(firstFlushed)
		<-releaseSecond
		_, _ = io.WriteString(w, "data: second\n\n")
	})

	server := httptest.NewServer(newInternalLBProxyHandler(backend, 2))
	defer server.Close()

	resp, err := http.Post(server.URL+ChatCompletionsPath, "application/json", strings.NewReader("{}"))
	require.NoError(t, err)
	defer resp.Body.Close()

	select {
	case <-firstFlushed:
	case <-time.After(time.Second):
		t.Fatal("backend did not flush its first event")
	}

	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "data: first\n", line)
	close(releaseSecond)
}

func TestValidateDataParallelMode(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		wantErr bool
	}{
		{mode: DataParallelModeExternalLB},
		{mode: DataParallelModeInternalLB},
		{mode: "invalid", wantErr: true},
	} {
		t.Run(fmt.Sprintf("mode=%s", tc.mode), func(t *testing.T) {
			opts := NewOptions()
			opts.DataParallelMode = tc.mode
			require.NoError(t, opts.Complete())
			err := opts.Validate()
			require.Equal(t, tc.wantErr, err != nil)
		})
	}
}

func TestDataParallelModeFlag(t *testing.T) {
	opts, flags := newTestOptions(t)
	require.NoError(t, flags.Parse([]string{"--data-parallel-mode=internal-lb"}))
	require.NoError(t, opts.Complete())
	require.Equal(t, DataParallelModeInternalLB, opts.DataParallelMode)
}
