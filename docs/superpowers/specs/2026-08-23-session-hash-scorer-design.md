# Exact Session Hash Scorer Design

## Goal

Add a small EPP scorer that reproduces the prefill-rank selection performed by
the comparison deployment's Envoy Lua filter. This lets us measure the cost of
moving the same deterministic selection from Envoy into the EPP and the
rank-aware sidecar path, without changing the cache-locality policy.

This is an experiment-focused plugin. It is not a general consistent-hashing
framework, does not retain session state, and does not change decode routing.

## Existing Lua Contract

For a non-empty `session-id` header, the Lua filter computes:

```text
h = 0
for each byte b in session-id:
    h = (h * 31 + b) % 2147483647
rank = h % 8
```

It then writes `x-data-parallel-rank: <rank>`. The loop operates on the UTF-8
bytes present in the HTTP header. If `session-id` is absent or empty, the Lua
filter does not select a rank.

## Plugin

Add an alpha scheduling scorer with type `session-hash-scorer` under the
existing scheduling scorer packages.

The plugin accepts one required parameter:

```yaml
- type: session-hash-scorer
  parameters:
    rankCount: 8
```

`rankCount` must be greater than zero. The request header is deliberately fixed
to `session-id` so the plugin implements the measured Lua contract rather than
introducing an unused general configuration surface.

For each request, the scorer:

1. Reads the normalized `session-id` request header.
2. Applies the byte-wise Lua hash above using integer arithmetic.
3. Computes `targetRank = hash % rankCount`.
4. Scores an endpoint `1` when `endpoint.GetMetadata().RankIndex` equals
   `targetRank`, and `0` otherwise.

The scorer category is `Affinity`. It has no data-store dependency, shared
state, response mutation, or side effects. It is registered as an alpha plugin
in the EPP runner.

If the request or header is missing, the header is empty, the endpoint is nil,
or endpoint metadata is nil, the affected endpoint receives score `0`. When
all endpoints receive `0`, `max-score-picker` uses its existing random tie
breaking. The plugin does not invent a session identifier or silently bind the
request to rank zero.

## Scheduling Configuration

The benchmark's prefill profile will contain the existing prefill-role filter,
`session-hash-scorer`, and `max-score-picker` with one selected endpoint. It
will not combine the hash score with load-aware or precise-prefix-cache scores;
therefore, a non-empty `session-id` deterministically selects the same rank as
Lua.

Decode keeps the current load-aware profile. The experiment uses one prefill
pod exposing eight rank endpoints, so each rank has exactly one candidate and
there is no cross-pod duplicate-rank tie to define.

## Tests

Implementation starts with unit tests covering:

- exact parity with independently calculated Lua golden vectors for ASCII and
  non-ASCII UTF-8 header bytes;
- matching and non-matching endpoint ranks;
- absent and empty `session-id`;
- nil request, nil endpoint, and nil endpoint metadata;
- rejection of absent parameters and `rankCount <= 0`;
- plugin type, instance name, and `Affinity` category.

The package tests must pass before registration is added. The relevant EPP
test suite and repository presubmit checks are then run without modifying the
unrelated existing worktree changes.

## Image and Live Verification

Build and push only the EPP image from the existing rank-aware branch. The
sidecar image remains the already-tested fixed image because this scorer does
not require a sidecar code change.

Before load testing, send requests with known session IDs and verify from the
EPP decision or sidecar destination logs that the selected prefill rank equals
the rank calculated locally with the Lua formula.

## Matched M3 Experiment

Run two sequential, fresh M3 arms with one EPP replica, one prefill pod, and two
decode pods:

1. **Lua baseline:** rerun the 1e sticky-routing deployment.
2. **EPP hash:** use the same SGLang image, arguments, page size, cache flags,
   resources, topology, traffic input, rate, duration, and cache-warmup state as
   the fresh Lua arm. Remove the Lua rank-selection filter and select the
   prefill virtual rank through `session-hash-scorer` instead.

The EPP-hash manifest must be derived from the fresh 1e engine configuration,
not from the older 2f manifest, because 1e and 2f have differed in page size
and cache-related settings. Precise-prefix-cache scoring and its KV-event
plumbing remain disabled in both matched arms.

Capture throughput, TTFT, E2E latency, ITL, error rate, cache-hit rate,
uncached input tokens per second, and per-rank request distribution. Also
retain the existing EPP scheduling and sidecar timing instrumentation.

The primary correctness criterion is that known session IDs and the aggregate
per-rank request distribution agree between Lua and EPP hash. Cache-hit rate
and uncached input throughput should consequently be comparable. The observed
latency and throughput delta is the cost of moving prefill rank selection into
the EPP virtual-endpoint and sidecar path. Both arms already use EPP and the
sidecar for decode, so the result is not the cost of adding those components to
an otherwise component-free serving stack.

## Deliverables

- scorer implementation and focused tests in the llm-d router worktree;
- an EPP image tag pinned in the experimental deployment;
- matched Lua and EPP-hash manifests and raw M3 artifacts in
  `/home/coder/lm-benchmark-llmd-test`;
- a short comparison section added to the existing controlled-routing report.

No upstream push or pull request is part of this work unless explicitly
requested.
