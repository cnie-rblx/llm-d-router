/*
Copyright 2025 The Kubernetes Authors.

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

package maxscore

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

func TestPickMaxScorePicker(t *testing.T) {
	endpoint1 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, nil, nil)
	endpoint2 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod2"}}, nil, nil)
	endpoint3 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod3"}}, nil, nil)

	tests := []struct {
		name               string
		picker             fwksched.Picker
		input              []*fwksched.ScoredEndpoint
		output             []fwksched.Endpoint
		tieBreakCandidates int // tie break is random, specify how many candidate with max score
	}{
		{
			name:   "Single max score",
			picker: NewMaxScorePicker(1),
			input: []*fwksched.ScoredEndpoint{
				{Endpoint: endpoint1, Score: 10},
				{Endpoint: endpoint2, Score: 25},
				{Endpoint: endpoint3, Score: 15},
			},
			output: []fwksched.Endpoint{
				&fwksched.ScoredEndpoint{Endpoint: endpoint2, Score: 25},
			},
		},
		{
			name:   "Multiple max scores, all are equally scored",
			picker: NewMaxScorePicker(2),
			input: []*fwksched.ScoredEndpoint{
				{Endpoint: endpoint1, Score: 50},
				{Endpoint: endpoint2, Score: 50},
				{Endpoint: endpoint3, Score: 30},
			},
			output: []fwksched.Endpoint{
				&fwksched.ScoredEndpoint{Endpoint: endpoint1, Score: 50},
				&fwksched.ScoredEndpoint{Endpoint: endpoint2, Score: 50},
			},
			tieBreakCandidates: 2,
		},
		{
			name:   "Multiple results sorted by highest score, more pods than needed",
			picker: NewMaxScorePicker(2),
			input: []*fwksched.ScoredEndpoint{
				{Endpoint: endpoint1, Score: 20},
				{Endpoint: endpoint2, Score: 25},
				{Endpoint: endpoint3, Score: 30},
			},
			output: []fwksched.Endpoint{
				&fwksched.ScoredEndpoint{Endpoint: endpoint3, Score: 30},
				&fwksched.ScoredEndpoint{Endpoint: endpoint2, Score: 25},
			},
		},
		{
			name:   "Multiple results sorted by highest score, less pods than needed",
			picker: NewMaxScorePicker(4), // picker is required to return 4 pods at most, but we have only 3.
			input: []*fwksched.ScoredEndpoint{
				{Endpoint: endpoint1, Score: 20},
				{Endpoint: endpoint2, Score: 25},
				{Endpoint: endpoint3, Score: 30},
			},
			output: []fwksched.Endpoint{
				&fwksched.ScoredEndpoint{Endpoint: endpoint3, Score: 30},
				&fwksched.ScoredEndpoint{Endpoint: endpoint2, Score: 25},
				&fwksched.ScoredEndpoint{Endpoint: endpoint1, Score: 20},
			},
		},
		{
			name:   "Multiple results sorted by highest score, num of pods exactly needed",
			picker: NewMaxScorePicker(3), // picker is required to return 3 pods at most, we have only 3.
			input: []*fwksched.ScoredEndpoint{
				{Endpoint: endpoint1, Score: 30},
				{Endpoint: endpoint2, Score: 25},
				{Endpoint: endpoint3, Score: 30},
			},
			output: []fwksched.Endpoint{
				&fwksched.ScoredEndpoint{Endpoint: endpoint1, Score: 30},
				&fwksched.ScoredEndpoint{Endpoint: endpoint3, Score: 30},
				&fwksched.ScoredEndpoint{Endpoint: endpoint2, Score: 25},
			},
			tieBreakCandidates: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.picker.Pick(context.Background(), test.input)
			got := result.TargetEndpoints

			if test.tieBreakCandidates > 0 {
				testMaxScoredEndpoints := test.output[:test.tieBreakCandidates]
				gotMaxScoredEndpoints := got[:test.tieBreakCandidates]
				diff := cmp.Diff(testMaxScoredEndpoints, gotMaxScoredEndpoints, cmpopts.SortSlices(func(a, b fwksched.Endpoint) bool {
					return a.String() < b.String() // predictable order within the endpoints with equal scores
				}), cmp.Comparer(fwksched.ScoredEndpointComparer))
				if diff != "" {
					t.Errorf("Unexpected output (-want +got): %v", diff)
				}
				test.output = test.output[test.tieBreakCandidates:]
				got = got[test.tieBreakCandidates:]
			}

			if diff := cmp.Diff(test.output, got, cmp.Comparer(fwksched.ScoredEndpointComparer)); diff != "" {
				t.Errorf("Unexpected output (-want +got): %v", diff)
			}
		})
	}
}

func TestPickRandomlyWithinTopK(t *testing.T) {
	endpoint1 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod1"}}, nil, nil)
	endpoint2 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod2"}}, nil, nil)
	endpoint3 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod3"}}, nil, nil)
	endpoint4 := fwksched.NewEndpoint(&fwkdl.EndpointMetadata{ID: k8stypes.NamespacedName{Name: "pod4"}}, nil, nil)

	tests := []struct {
		name           string
		topK           int
		allowed        map[string]bool
		wantAllSeen    bool
		iterationCount int
	}{
		{
			name:           "top one preserves highest score selection",
			topK:           1,
			allowed:        map[string]bool{"pod1": true},
			wantAllSeen:    true,
			iterationCount: 20,
		},
		{
			name:           "top two samples only the two highest scores",
			topK:           2,
			allowed:        map[string]bool{"pod1": true, "pod2": true},
			wantAllSeen:    true,
			iterationCount: 500,
		},
		{
			name:           "top three samples only the three highest scores",
			topK:           3,
			allowed:        map[string]bool{"pod1": true, "pod2": true, "pod3": true},
			wantAllSeen:    true,
			iterationCount: 500,
		},
		{
			name:           "top k larger than candidates samples all candidates",
			topK:           10,
			allowed:        map[string]bool{"pod1": true, "pod2": true, "pod3": true, "pod4": true},
			wantAllSeen:    true,
			iterationCount: 500,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := NewMaxScorePicker(1).WithTopK(test.topK)
			seen := make(map[string]bool)
			for range test.iterationCount {
				input := []*fwksched.ScoredEndpoint{
					{Endpoint: endpoint1, Score: 40},
					{Endpoint: endpoint2, Score: 30},
					{Endpoint: endpoint3, Score: 20},
					{Endpoint: endpoint4, Score: 10},
				}
				result := p.Pick(context.Background(), input)
				if len(result.TargetEndpoints) != 1 {
					t.Fatalf("expected one endpoint, got %d", len(result.TargetEndpoints))
				}
				name := result.TargetEndpoints[0].GetMetadata().ID.Name
				if !test.allowed[name] {
					t.Fatalf("selected endpoint %q outside top %d", name, test.topK)
				}
				seen[name] = true
			}

			if test.wantAllSeen && len(seen) != len(test.allowed) {
				t.Fatalf("expected to observe every allowed endpoint, saw %v", seen)
			}
		})
	}
}

func TestMaxScorePickerFactoryTopK(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		wantTopK int
	}{
		{name: "explicit top k", config: `{"maxNumOfEndpoints":1,"topK":3}`, wantTopK: 3},
		{name: "omitted top k defaults to one", config: `{"maxNumOfEndpoints":1}`, wantTopK: 1},
		{name: "invalid top k defaults to one", config: `{"maxNumOfEndpoints":1,"topK":0}`, wantTopK: 1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder := json.NewDecoder(strings.NewReader(test.config))
			plugin, err := MaxScorePickerFactory("decode-top3-picker", decoder, nil)
			if err != nil {
				t.Fatalf("factory returned error: %v", err)
			}

			p, ok := plugin.(*MaxScorePicker)
			if !ok {
				t.Fatalf("expected *MaxScorePicker, got %T", plugin)
			}
			if p.topK != test.wantTopK {
				t.Fatalf("expected topK %d, got %d", test.wantTopK, p.topK)
			}
			if p.TypedName().Name != "decode-top3-picker" {
				t.Fatalf("expected configured name, got %q", p.TypedName().Name)
			}
		})
	}
}
