package kvcache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/llm-d/llm-d-router/pkg/kvcache"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// Production shape: ~1400 blocks, 16 prefill endpoints, two tiers each, and
// block 0 unknown to every pod. Score lost its early exit when block-0
// tolerance was added, so this guards what that now costs.
func BenchmarkLongestPrefixScorerProdShape(b *testing.B) {
	const (
		nKeys = 1400
		nPods = 16
	)
	scorer := &kvcache.LongestPrefixScorer{
		MediumWeights: map[string]float64{"gpu": 1.0, "cpu_pinned": 0.8},
	}
	keys := make([]kvblock.BlockHash, nKeys)
	keyToPods := make(map[kvblock.BlockHash][]kvblock.PodEntry, nKeys)
	for i := range keys {
		keys[i] = kvblock.BlockHash(i + 1)
		if i == 0 {
			continue // unknown to every pod, as in production
		}
		entries := make([]kvblock.PodEntry, 0, nPods*2)
		for p := 0; p < nPods; p++ {
			pod := fmt.Sprintf("10.0.0.%d:8000", p)
			entries = append(entries,
				kvblock.PodEntry{PodIdentifier: pod, DeviceTier: "gpu"},
				kvblock.PodEntry{PodIdentifier: pod, DeviceTier: "cpu_pinned"})
		}
		keyToPods[keys[i]] = entries
	}

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := scorer.Score(ctx, keys, keyToPods); err != nil {
			b.Fatal(err)
		}
	}
}
