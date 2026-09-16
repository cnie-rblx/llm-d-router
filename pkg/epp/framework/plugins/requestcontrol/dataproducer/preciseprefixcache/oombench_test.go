package preciseprefixcache

import (
	"fmt"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"

	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
)

// Production shape: ~1400 blocks per lookup, 16 prefill endpoints, and block 0
// unknown to every pod (the condition that used to short-circuit everything).
const (
	benchKeys      = 1400
	benchEndpoints = 16
)

func benchFixture() ([]kvblock.BlockHash, map[kvblock.BlockHash][]kvblock.PodEntry, []string) {
	pods := make([]string, benchEndpoints)
	for i := range pods {
		pods[i] = fmt.Sprintf("10.0.0.%d:8000", i)
	}
	keys := make([]kvblock.BlockHash, benchKeys)
	keyToPods := make(map[kvblock.BlockHash][]kvblock.PodEntry, benchKeys)
	for i := range keys {
		keys[i] = kvblock.BlockHash(i + 1)
		if i == 0 {
			continue // block 0 unknown to every pod, as in production
		}
		entries := make([]kvblock.PodEntry, 0, benchEndpoints)
		for _, p := range pods {
			entries = append(entries, kvblock.PodEntry{PodIdentifier: p, DeviceTier: "gpu"})
			entries = append(entries, kvblock.PodEntry{PodIdentifier: p, DeviceTier: "cpu_pinned"})
		}
		keyToPods[keys[i]] = entries
	}
	return keys, keyToPods, pods
}

// oldMatchedBlockCountByTier is the pre-fix implementation, kept here only to
// measure the cost the fix introduced.
func oldMatchedBlockCountByTier(keys []kvblock.BlockHash, keyToPods map[kvblock.BlockHash][]kvblock.PodEntry, podID string) map[string]int {
	counts := map[string]int{}
	var alive sets.Set[string]
	for _, key := range keys {
		tiersAtKey := sets.New[string]()
		for _, e := range keyToPods[key] {
			if e.PodIdentifier == podID {
				if e.Speculative {
					tiersAtKey.Insert(attrprefix.SpeculativeTierKey)
				} else {
					tiersAtKey.Insert(e.DeviceTier)
				}
			}
		}
		if alive == nil {
			alive = tiersAtKey
		} else {
			alive = alive.Intersection(tiersAtKey)
		}
		if alive.Len() == 0 {
			break
		}
		for tier := range alive {
			counts[tier]++
		}
	}
	return counts
}

func oldMatchedBlockCount(keys []kvblock.BlockHash, keyToPods map[kvblock.BlockHash][]kvblock.PodEntry, podID string) int {
	count := 0
	for _, key := range keys {
		if !slices.ContainsFunc(keyToPods[key], func(e kvblock.PodEntry) bool { return e.PodIdentifier == podID }) {
			break
		}
		count++
	}
	return count
}

// One "request" is what produceFromBlockKeys does: both counters for every
// endpoint over the whole key list.
func BenchmarkPerRequestOld(b *testing.B) {
	keys, keyToPods, pods := benchFixture()
	b.ReportAllocs()
	for b.Loop() {
		for _, p := range pods {
			_ = oldMatchedBlockCount(keys, keyToPods, p)
			_ = oldMatchedBlockCountByTier(keys, keyToPods, p)
		}
	}
}

func BenchmarkPerRequestNew(b *testing.B) {
	keys, keyToPods, pods := benchFixture()
	b.ReportAllocs()
	for b.Loop() {
		for _, p := range pods {
			_ = matchedBlockCount(keys, keyToPods, p)
			_ = matchedBlockCountByTier(keys, keyToPods, p)
		}
	}
}
