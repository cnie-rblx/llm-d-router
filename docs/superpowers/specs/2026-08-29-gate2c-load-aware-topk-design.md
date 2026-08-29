# Gate 2c Load-Aware Top-K Routing Design

## Goal

Prevent request herding in the GLM-5.2 Gate 2c disaggregated prefill/decode deployment while retaining useful prefix-cache affinity.

The implementation must build on the DEP/rank-aware router fork at commit `ded5f9536bf2fcd39c36b1bfa9d2c75d5966ccb0`. Upstream `main` does not contain the deployment-specific rank-preserving behavior required by this service.

## Observed Failure Mode

The existing decode profile scores generic queue depth, running requests, KV utilization, and EPP in-flight reservations. SGLang P/D requests waiting in the decode preallocation or transfer stages do not appear in `sglang:num_queue_reqs`. A rank can therefore appear idle to EPP while holding many pending handoffs.

Prefill has the analogous blind spot for `sglang:num_prefill_bootstrap_queue_reqs`. Its current prefix scorer weight of 1,000,000 also makes practically any prefix-score difference dominate all load signals.

Finally, `max-score-picker` returns the single highest-scoring endpoint. Bursty arrivals can repeatedly select the same endpoint before metrics and in-flight state catch up.

## Design

### Role-specific queue signals

Use the existing custom-metric extraction and `endpoint-attribute-scorer` facilities instead of adding metric-specific Go scorer types.

Configure the SGLang metrics extractor with these endpoint attributes:

- `decode_prealloc_queue` from `sglang:num_decode_prealloc_queue_reqs`
- `prefill_bootstrap_queue` from `sglang:num_prefill_bootstrap_queue_reqs`

Create two named `endpoint-attribute-scorer` instances using lower-is-better adaptive-range normalization:

- decode preallocation scorer, weight 2 in the decode profile
- prefill bootstrap scorer, weight 2 in the prefill profile

The existing generic queue, active-request, and KV-utilization scorers remain unchanged.

### Prefix affinity

Reduce the prefill prefix-cache scorer weight from 1,000,000 to 2. Scorer outputs are normalized to the range `[0,1]`; weight 2 preserves a material preference for cached prefixes while allowing severe combined queue, bootstrap, active-request, and KV pressure to override affinity.

### Top-K random selection

Extend `max-score-picker` with an optional `topK` parameter and preserve the existing `NewMaxScorePicker(maxNumOfEndpoints)` constructor:

1. Shuffle candidates for random tie-breaking, as today.
2. Sort candidates by descending score.
3. Restrict the candidate set to the best `topK` endpoints.
4. Shuffle that top-K set and return up to `maxNumOfEndpoints` entries.

`topK` defaults to 1, preserving existing behavior for all current configurations. Invalid non-positive values fall back to 1, matching the existing treatment of `maxNumOfEndpoints`.

Configure separate named picker instances:

- prefill picker: `topK: 2`, `maxNumOfEndpoints: 1`
- decode picker: `topK: 3`, `maxNumOfEndpoints: 1`

### Tests

Add focused tests before implementation that demonstrate:

- omitted `topK` preserves highest-score selection;
- `topK: 1` preserves highest-score selection;
- `topK: 2` selects only from the two highest-scoring endpoints and reaches both over repeated picks;
- `topK: 3` selects only from the three highest-scoring endpoints and reaches all three over repeated picks;
- `topK` larger than the candidate count remains safe;
- factory configuration decodes `topK` correctly.

Existing endpoint-attribute scorer and custom-metric extraction tests provide coverage for the queue signal plumbing; the deployment configuration will also be validated through the router config loader before rollout.

## Deployment and Verification

Build and publish an immutable EPP image from the new worktree. Update only `deploy_yamls/glm52-b200-nvfp4-gate2c-pd.yaml` with the new image and plugin configuration, preserving the prod-5 namespace, host, RBAC, and DEP/rank-aware settings.

After applying the manifest:

1. Verify EPP and serving workloads are ready.
2. Confirm EPP loads the two custom metrics and named picker instances.
3. Observe live shadow traffic for at least 15 minutes.
4. Compare per-rank selections, generic and role-specific queues, KV utilization, transfer failures, aborted requests, and prefix-cache hit rate.
5. Report both improvement and any remaining imbalance; do not declare success solely from pod readiness.
