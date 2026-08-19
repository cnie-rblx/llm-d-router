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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	fwkrh "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requesthandling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSGLangHTTPRenderer_Render(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/tokenize", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		requests = append(requests, body)
		prompt := body["prompt"].(string)
		_ = json.NewEncoder(w).Encode(map[string]any{"tokens": []uint32{uint32(len(prompt)), 9}, "count": 2})
	}))
	defer server.Close()

	renderer, err := newSGLangHTTPRenderer(&sglangConfig{URL: server.URL})
	require.NoError(t, err)
	tokens, offsets, err := renderer.Render(context.Background(), fwkrh.PayloadMap{
		"prompt":      []string{"one", "three"},
		"temperature": 0,
	})

	require.NoError(t, err)
	assert.Equal(t, [][]uint32{{3, 9}, {5, 9}}, tokens)
	assert.Nil(t, offsets)
	require.Len(t, requests, 2)
	assert.Equal(t, "one", requests[0]["prompt"])
	assert.Equal(t, float64(0), requests[0]["temperature"])
}

func TestSGLangHTTPRenderer_RenderChat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Contains(t, body, "messages")
		_ = json.NewEncoder(w).Encode(map[string]any{"tokens": []uint32{1, 2, 3}, "count": 3})
	}))
	defer server.Close()

	renderer, err := newSGLangHTTPRenderer(&sglangConfig{URL: server.URL})
	require.NoError(t, err)
	tokens, features, err := renderer.RenderChat(context.Background(), fwkrh.PayloadMap{
		"messages": []any{map[string]any{"role": "user", "content": "hello"}},
	})

	require.NoError(t, err)
	assert.Equal(t, []uint32{1, 2, 3}, tokens)
	assert.Nil(t, features)
}

func TestSGLangHTTPRenderer_NonSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	renderer, err := newSGLangHTTPRenderer(&sglangConfig{URL: server.URL})
	require.NoError(t, err)
	_, _, err = renderer.RenderChat(context.Background(), fwkrh.PayloadMap{"messages": []any{}})
	require.ErrorContains(t, err, "status 400")
}
