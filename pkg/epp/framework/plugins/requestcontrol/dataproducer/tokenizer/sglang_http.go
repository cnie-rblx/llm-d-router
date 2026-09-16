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

package tokenizer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-router/pkg/kvcache/tokenization"
	tokenizerTypes "github.com/llm-d/llm-d-router/pkg/kvcache/tokenization/types"
)

const (
	defaultSGLangURL       = "http://localhost:8000"
	defaultSGLangTimeout   = 5 * time.Second
	defaultSGLangMMTimeout = 30 * time.Second
	sglangTokenizePath     = "/tokenize"
)

type sglangConfig struct {
	URL       string `json:"url,omitempty"`
	Timeout   string `json:"timeout,omitempty"`
	MMTimeout string `json:"mmTimeout,omitempty"`
}

type sglangHTTPRenderer struct {
	client    *http.Client
	baseURL   string
	timeout   time.Duration
	mmTimeout time.Duration
}

type sglangTokenizeResponse struct {
	Tokens []uint32 `json:"tokens"`
}

func newSGLangHTTPRenderer(cfg *sglangConfig) (*sglangHTTPRenderer, error) {
	baseURL := strings.TrimRight(cfg.URL, "/")
	if baseURL == "" {
		baseURL = defaultSGLangURL
	}
	timeout, err := parseHTTPDuration(cfg.Timeout, defaultSGLangTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid 'timeout': %w", err)
	}
	mmTimeout, err := parseHTTPDuration(cfg.MMTimeout, defaultSGLangMMTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid 'mmTimeout': %w", err)
	}
	return &sglangHTTPRenderer{
		client:    &http.Client{Transport: otelhttp.NewTransport(newRenderTransport())},
		baseURL:   baseURL,
		timeout:   timeout,
		mmTimeout: mmTimeout,
	}, nil
}

func (r *sglangHTTPRenderer) Render(ctx context.Context, payload fwkrh.RequestPayload) ([][]uint32, [][]tokenizerTypes.Offset, error) {
	pm, ok := payload.AsMap()
	if !ok {
		return nil, nil, errors.New("SGLang HTTP tokenizer requires a parsed PayloadMap")
	}

	prompts := []any{pm["prompt"]}
	switch prompt := pm["prompt"].(type) {
	case []string:
		prompts = make([]any, len(prompt))
		for i := range prompt {
			prompts[i] = prompt[i]
		}
	case []any:
		prompts = prompt
	}

	allTokens := make([][]uint32, len(prompts))
	for i, prompt := range prompts {
		body := maps.Clone(pm)
		body["prompt"] = prompt
		resp, err := r.tokenize(ctx, body, r.timeout)
		if err != nil {
			return nil, nil, err
		}
		allTokens[i] = resp.Tokens
	}
	return allTokens, nil, nil
}

func (r *sglangHTTPRenderer) RenderChat(ctx context.Context, payload fwkrh.RequestPayload) ([]uint32, *tokenization.MultiModalFeatures, error) {
	pm, ok := payload.AsMap()
	if !ok {
		return nil, nil, errors.New("SGLang HTTP tokenizer requires a parsed PayloadMap")
	}
	resp, err := r.tokenize(ctx, pm, r.chatTimeout(pm))
	if err != nil {
		return nil, nil, err
	}
	// SGLang's /tokenize response does not expose multimodal hashes or
	// placeholder ranges. Token IDs remain exact for text-only requests.
	return resp.Tokens, nil, nil
}

func (r *sglangHTTPRenderer) tokenize(ctx context.Context, body any, timeout time.Duration) (*sglangTokenizeResponse, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, r.baseURL+sglangTokenizePath, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SGLang tokenize request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySnippetBytes))
		return nil, fmt.Errorf("SGLang tokenize returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var result sglangTokenizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode SGLang tokenize response: %w", err)
	}
	return &result, nil
}

func (r *sglangHTTPRenderer) chatTimeout(payload fwkrh.PayloadMap) time.Duration {
	messages, ok := payload["messages"].([]any)
	if !ok {
		return r.timeout
	}
	for _, rawMessage := range messages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			continue
		}
		if parts, ok := message["content"].([]any); ok && len(parts) > 0 {
			return r.mmTimeout
		}
	}
	return r.timeout
}

func (r *sglangHTTPRenderer) produceTimeout() time.Duration {
	if r.mmTimeout > r.timeout {
		return r.mmTimeout
	}
	return r.timeout
}
