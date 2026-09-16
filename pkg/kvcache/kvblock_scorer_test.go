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

package kvcache_test

import (
	"context"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"github.com/stretchr/testify/assert"
)

const (
	testModelName = "test-model"
	podA          = "pod-a"
	podB          = "pod-b"
)

// TestLongestPrefixScorer verifies scoring based on consecutive block hits from the start.
func TestLongestPrefixScorer(t *testing.T) {
	mediumWeights := map[string]float64{
		"gpu": 1.0,
		"cpu": 0.5,
	}

	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: mediumWeights,
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{1001, 1002, 1003, 1004, 1005, 1006})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		1001: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		1002: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		1003: {
			{PodIdentifier: podA, DeviceTier: "gpu"},
			{PodIdentifier: podA, DeviceTier: "cpu"},
		},
		1004: {{PodIdentifier: podB, DeviceTier: "cpu"}},
		1005: {{PodIdentifier: podB, DeviceTier: "cpu"}},
		1006: {{PodIdentifier: podA, DeviceTier: "gpu"}},
	}

	expected := map[string]float64{
		podA: 3.0,
		podB: 0.0,
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	assert.NoError(t, err)
	for pod, score := range scored {
		assert.InDelta(t, expected[pod], score, 0.0001)
	}
}

func TestLongestPrefixScorerDifferentTiers(t *testing.T) {
	mediumWeights := map[string]float64{
		"gpu": 1.0,
		"cpu": 0.5,
	}

	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: mediumWeights,
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{1001, 1002, 1003, 1004, 1005, 1006})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		1001: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		1002: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		1003: {{PodIdentifier: podA, DeviceTier: "cpu"}},
		1004: {{PodIdentifier: podB, DeviceTier: "cpu"}},
		1005: {{PodIdentifier: podB, DeviceTier: "cpu"}},
		1006: {{PodIdentifier: podA, DeviceTier: "gpu"}},
	}

	expected := map[string]float64{
		podA: 2.5,
		podB: 0.0,
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	assert.NoError(t, err)
	for pod, score := range scored {
		assert.InDelta(t, expected[pod], score, 0.0001)
	}
}

// Blocks absent from the index for every pod carry no information: engines
// announce a block only when it is newly stored, so blocks resident since
// before the index was built are never announced. They must not decide the
// score, otherwise an unknown prompt head zeroes every pod and the routing
// signal disappears.
func TestLongestPrefixScorerSkipsUnknownLeadingBlocks(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: map[string]float64{"gpu": 1.0, "cpu": 0.5},
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{1001, 1002, 1003, 1004, 1005})

	// 1001 and 1002 are held by nobody: the prompt head is resident on every
	// engine and was never announced.
	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		1003: {{PodIdentifier: podA, DeviceTier: "gpu"}, {PodIdentifier: podB, DeviceTier: "gpu"}},
		1004: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		1005: {{PodIdentifier: podA, DeviceTier: "gpu"}},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	assert.NoError(t, err)
	// Anchoring on keys[0] would have produced an empty map here.
	assert.InDelta(t, 3.0, scored[podA], 0.0001)
	assert.InDelta(t, 1.0, scored[podB], 0.0001)
}

// A hole in the middle of the prompt must not truncate the chain either.
func TestLongestPrefixScorerSkipsUnknownInteriorBlocks(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: map[string]float64{"gpu": 1.0},
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{2001, 2002, 2003, 2004})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		2001: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		// 2002 unknown to the index.
		2003: {{PodIdentifier: podA, DeviceTier: "gpu"}},
		2004: {{PodIdentifier: podA, DeviceTier: "gpu"}},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	assert.NoError(t, err)
	assert.InDelta(t, 3.0, scored[podA], 0.0001)
}

// Discrimination between pods must be preserved: a block that some pods hold
// and this one does not is a real miss, not missing information.
func TestLongestPrefixScorerRealMissStillEndsChain(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: map[string]float64{"gpu": 1.0},
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{3001, 3002, 3003})

	hitmap := map[kvblock.BlockHash][]kvblock.PodEntry{
		3001: {{PodIdentifier: podA, DeviceTier: "gpu"}, {PodIdentifier: podB, DeviceTier: "gpu"}},
		3002: {{PodIdentifier: podA, DeviceTier: "gpu"}}, // podB genuinely misses here
		3003: {{PodIdentifier: podA, DeviceTier: "gpu"}},
	}

	scored, err := scorer.Score(context.Background(), blockKeys, hitmap)
	assert.NoError(t, err)
	assert.InDelta(t, 3.0, scored[podA], 0.0001)
	assert.InDelta(t, 1.0, scored[podB], 0.0001, "podB should stop at its real miss")
}

// With nothing known about any block there is no signal, and every pod ties.
func TestLongestPrefixScorerAllBlocksUnknown(t *testing.T) {
	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: map[string]float64{"gpu": 1.0},
	}
	blockKeys := int64KeysToKVBlockKeys([]uint64{4001, 4002})

	scored, err := scorer.Score(context.Background(), blockKeys, map[kvblock.BlockHash][]kvblock.PodEntry{})
	assert.NoError(t, err)
	assert.Empty(t, scored)
}

func int64KeysToKVBlockKeys(keys []uint64) []kvblock.BlockHash {
	kvKeys := make([]kvblock.BlockHash, len(keys))
	for i, key := range keys {
		kvKeys[i] = kvblock.BlockHash(key)
	}
	return kvKeys
}
