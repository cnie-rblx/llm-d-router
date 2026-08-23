/*
Copyright 2026 The Kubernetes Authors.

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

package sessionhash

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestFactory(t *testing.T) {
	tests := []struct {
		name       string
		parameters string
		wantErr    string
	}{
		{
			name:       "valid",
			parameters: `{"rankCount":8}`,
		},
		{
			name:    "missing parameters",
			wantErr: "requires parameters",
		},
		{
			name:       "malformed parameters",
			parameters: `{"rankCount":`,
			wantErr:    "decode session-hash-scorer parameters",
		},
		{
			name:       "zero rank count",
			parameters: `{"rankCount":0}`,
			wantErr:    "rankCount must be greater than zero",
		},
		{
			name:       "negative rank count",
			parameters: `{"rankCount":-1}`,
			wantErr:    "rankCount must be greater than zero",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decoder *json.Decoder
			if test.parameters != "" {
				decoder = json.NewDecoder(strings.NewReader(test.parameters))
			}
			plugin, err := Factory("prefill-session-hash", decoder, nil)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			scorer := plugin.(*Scorer)
			assert.Equal(t, PluginType, scorer.TypedName().Type)
			assert.Equal(t, "prefill-session-hash", scorer.TypedName().Name)
			assert.Equal(t, fwksched.Affinity, scorer.Category())
			assert.Equal(t, uint64(8), scorer.rankCount)
		})
	}
}

func TestSessionHashMatchesLua(t *testing.T) {
	tests := []struct {
		sessionID string
		wantHash  uint64
		wantRank  uint64
	}{
		{sessionID: "a", wantHash: 97, wantRank: 1},
		{sessionID: "session-1", wantHash: 607842914, wantRank: 2},
		{sessionID: "room-123", wantHash: 1972950051, wantRank: 3},
		{sessionID: "你好", wantHash: 264532889, wantRank: 1},
		{sessionID: "é", wantHash: 6214, wantRank: 6},
	}

	for _, test := range tests {
		t.Run(test.sessionID, func(t *testing.T) {
			got := hashSessionID(test.sessionID)
			assert.Equal(t, test.wantHash, got)
			assert.Equal(t, test.wantRank, got%8)
		})
	}
}

func TestScore(t *testing.T) {
	endpoints := []fwksched.Endpoint{
		rankEndpoint(0),
		rankEndpoint(1),
		rankEndpoint(2),
		rankEndpoint(3),
		fwksched.NewEndpoint(nil, nil, nil),
		nil,
	}
	scorer := &Scorer{rankCount: 8}

	tests := []struct {
		name    string
		request *fwksched.InferenceRequest
		want    []float64
	}{
		{
			name:    "matching rank receives affinity",
			request: &fwksched.InferenceRequest{Headers: map[string]string{"session-id": "session-1"}},
			want:    []float64{0, 0, 1, 0, 0, 0},
		},
		{
			name:    "empty session contributes no score",
			request: &fwksched.InferenceRequest{Headers: map[string]string{"session-id": ""}},
			want:    []float64{0, 0, 0, 0, 0, 0},
		},
		{
			name:    "missing session contributes no score",
			request: &fwksched.InferenceRequest{Headers: map[string]string{}},
			want:    []float64{0, 0, 0, 0, 0, 0},
		},
		{
			name: "nil request contributes no score",
			want: []float64{0, 0, 0, 0, 0, 0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scores := scorer.Score(context.Background(), test.request, endpoints)
			for index, endpoint := range endpoints {
				assert.Equal(t, test.want[index], scores[endpoint])
			}
		})
	}
}

func rankEndpoint(rank int) fwksched.Endpoint {
	return fwksched.NewEndpoint(&fwkdl.EndpointMetadata{RankIndex: rank}, nil, nil)
}
