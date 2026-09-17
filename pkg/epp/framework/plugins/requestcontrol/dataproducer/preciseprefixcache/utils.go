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

package preciseprefixcache

import (
	"fmt"

	"github.com/llm-d/llm-d-router/pkg/kvcache/kvblock"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

// extractEndpointSet builds the "address:port" identifier set used to filter
// kvblock.Index lookups to candidate endpoints. Endpoints without metadata
// are skipped.
func extractEndpointSet(endpoints []scheduling.Endpoint) sets.Set[string] {
	endpointSet := sets.New[string]()
	for _, ep := range endpoints {
		if m := ep.GetMetadata(); m != nil {
			endpointSet.Insert(fmt.Sprintf("%s:%s", m.Address, m.Port))
		}
	}
	return endpointSet
}

// endpointPrefixCount holds one endpoint's contiguous cached-block counts for
// a single key list.
type endpointPrefixCount struct {
	blocks int
	byTier map[string]int
}

// tierBitLimit caps how many distinct device tiers the bitmask below can
// track. sglang reports gpu, cpu_pinned and the speculative pseudo-tier, so
// this is far beyond anything real; tiers past the limit are ignored rather
// than silently miscounted.
const tierBitLimit = 64

// matchedPrefixCounts returns, for every endpoint at once, the number of
// contiguous cached prefix blocks it holds: `blocks` counts a block held in
// any device tier, and `byTier` counts per tier, so each tier's count is at
// most `blocks`. A pod present at keys[0..n-1] yields n. Speculative entries
// count under attrprefix.SpeculativeTierKey, since PreRequest inserts them
// before vLLM has reported placement and they carry no device tier.
//
// Blocks that no pod is known to hold are skipped: they neither seed nor
// break any chain. The index is not authoritative about absence, because
// engines announce a block only when it is newly stored, so a block resident
// since before the index was built is never announced and is missing for
// every pod. A block that some pods hold and this one does not is still a
// genuine miss and ends this pod's chain.
//
// This is computed in a SINGLE pass over keys for all endpoints together.
// Doing it per endpoint is O(endpoints x keys x entries) and, with a set
// allocated per key per endpoint, cost 11.5 MB and 7.7 ms per request at
// production shape (1,400 keys, 16 endpoints) -- enough to OOM-kill the EPP.
// The per-endpoint form was only ever cheap because anchoring on keys[0] made
// it exit immediately. See oombench_test.go, which guards this.
func matchedPrefixCounts(keys []kvblock.BlockHash,
	keyToPods map[kvblock.BlockHash][]kvblock.PodEntry,
) map[string]*endpointPrefixCount {
	counts := make(map[string]*endpointPrefixCount)
	if len(keys) == 0 {
		return counts
	}

	// Tier name to bit position, built lazily as tiers are encountered.
	tierBit := make(map[string]uint, 4)
	tierName := make([]string, 0, 4)

	// Scratch, reused across keys via clear() so the pass allocates nothing
	// per key: endpoint -> bitmask of the tiers holding the current block.
	presentMask := make(map[string]uint64)

	// Two independent chains per endpoint, because the any-tier count and the
	// per-tier counts break at different points: the any-tier chain ends when
	// the endpoint does not hold the block in ANY tier, while a given tier's
	// chain ends as soon as the endpoint does not hold the block in THAT tier.
	aliveAny := make(map[string]bool)
	aliveMask := make(map[string]uint64)
	liveAny, liveMask := 0, 0
	seeded := false

	for _, key := range keys {
		entries := keyToPods[key]
		if len(entries) == 0 {
			continue // unknown to the index, not a miss
		}

		clear(presentMask)
		for _, e := range entries {
			tier := e.DeviceTier
			if e.Speculative {
				tier = attrprefix.SpeculativeTierKey
			}
			bit, ok := tierBit[tier]
			if !ok {
				if len(tierName) >= tierBitLimit {
					continue
				}
				bit = uint(len(tierName))
				tierBit[tier] = bit
				tierName = append(tierName, tier)
			}
			presentMask[e.PodIdentifier] |= 1 << bit
		}

		if !seeded {
			seeded = true
			for pod, mask := range presentMask {
				c := &endpointPrefixCount{blocks: 1, byTier: make(map[string]int, 2)}
				for b, name := range tierName {
					if mask&(1<<uint(b)) != 0 {
						c.byTier[name] = 1
					}
				}
				counts[pod] = c
				aliveAny[pod] = true
				aliveMask[pod] = mask
				liveAny++
				liveMask++
			}
			continue
		}

		for pod, c := range counts {
			mask := presentMask[pod] // absent endpoints read as 0

			if aliveAny[pod] {
				if mask != 0 {
					c.blocks++
				} else {
					aliveAny[pod] = false
					liveAny--
				}
			}

			if am := aliveMask[pod]; am != 0 {
				nm := am & mask
				aliveMask[pod] = nm
				if nm == 0 {
					liveMask--
				} else {
					for b, name := range tierName {
						if nm&(1<<uint(b)) != 0 {
							c.byTier[name]++
						}
					}
				}
			}
		}

		// Every chain has ended; remaining keys cannot change the result.
		if liveAny == 0 && liveMask == 0 {
			break
		}
	}

	return counts
}
