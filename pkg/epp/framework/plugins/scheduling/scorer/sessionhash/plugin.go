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

// Package sessionhash provides deterministic session affinity for data-parallel ranks.
package sessionhash

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

const (
	PluginType             = "session-hash-scorer"
	sessionIDHeader        = "session-id"
	hashModulus     uint64 = 2147483647
)

type parameters struct {
	RankCount int `json:"rankCount"`
}

// Factory creates a scorer for deterministic session-to-rank affinity.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	if rawParameters == nil {
		return nil, errors.New("session-hash-scorer requires parameters")
	}
	params := parameters{}
	if err := rawParameters.Decode(&params); err != nil {
		return nil, fmt.Errorf("decode session-hash-scorer parameters: %w", err)
	}
	if params.RankCount <= 0 {
		return nil, errors.New("session-hash-scorer rankCount must be greater than zero")
	}
	if name == "" {
		name = PluginType
	}
	return &Scorer{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
		rankCount: uint64(params.RankCount),
	}, nil
}

// Scorer prefers the data-parallel rank selected by the session hash.
type Scorer struct {
	typedName fwkplugin.TypedName
	rankCount uint64
}

var _ fwksched.Scorer = (*Scorer)(nil)

func (s *Scorer) TypedName() fwkplugin.TypedName { return s.typedName }

func (s *Scorer) Category() fwksched.ScorerCategory { return fwksched.Affinity }

func (s *Scorer) Score(_ context.Context, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) map[fwksched.Endpoint]float64 {
	scores := make(map[fwksched.Endpoint]float64, len(endpoints))
	for _, endpoint := range endpoints {
		scores[endpoint] = 0
	}

	if request == nil || request.Headers[sessionIDHeader] == "" {
		return scores
	}
	targetRank := hashSessionID(request.Headers[sessionIDHeader]) % s.rankCount
	for _, endpoint := range endpoints {
		if endpoint == nil || endpoint.GetMetadata() == nil {
			continue
		}
		rankIndex := endpoint.GetMetadata().RankIndex
		if rankIndex >= 0 && uint64(rankIndex) == targetRank {
			scores[endpoint] = 1
		}
	}
	return scores
}

func hashSessionID(sessionID string) uint64 {
	var hash uint64
	for index := 0; index < len(sessionID); index++ {
		hash = (hash*31 + uint64(sessionID[index])) % hashModulus
	}
	return hash
}
