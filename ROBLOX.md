# Roblox changes

This branch is based on upstream `release-0.10` at `v0.10.0`. It carries the following Roblox-specific changes.

## Internal data-parallel routing

Adds an `internal-lb` sidecar mode that routes requests to a selected data-parallel rank while exposing rank-local metrics to the EPP. The existing external load-balancer mode remains available.

## SGLang tokenizer and KV events

Adds SGLang HTTP tokenization support to the EPP and decodes SGLang bigram KV-event keys. This keeps prefix-cache signals consistent with the tokens used by SGLang workers.

## Rank-aware disaggregated routing

Preserves the selected decode data-parallel rank when routing through the sidecar's primary listener. It also passes the selected prefill rank to SGLang so prefill and decode use the intended virtual endpoints.

## Cross-replica in-flight load

Adds a Kubernetes ConfigMap-backed syncer for sharing in-flight request state across EPP replicas. Scheduling reads cached peer snapshots and does not put Kubernetes API calls on the request path.

## Reliability fixes

Retains valid metrics parsed before a malformed metric family and cancels decode work when prefill fails. These changes avoid discarding usable load signals and prevent orphaned decode requests.

## Load-aware top-K selection

Extends the max-score picker with `topK` so it can select randomly from the highest-scoring endpoints. This reduces hot-spotting while retaining score locality.

## SGLang request compatibility

Omits the unsupported `stream` field from SGLang tokenization requests. The sidecar also treats client-aborted decode streams as request cancellation instead of terminating the process.

## SGLang replay compatibility

Accepts replay frames that omit the topic and uses the subscriber's configured topic as a fallback. Both topicful and topicless terminal frames are recognized.

## Projected TTFT affinity

Includes the current request's uncached tokens when estimating prefill TTFT for prefix-affinity routing. This prevents a large incoming request from appearing artificially cheap before it enters the in-flight counters.

## Decode pipeline pressure

Adds a filter that combines ordinary waiting, decode preallocation, and transfer queues into normalized endpoint pressure. It keeps endpoints within a configurable threshold of the least-pressured decode endpoint.

## Precise prefix-cache recovery and scoring

Restores precise prefix-cache signals after cold starts by anchoring replay at the engine's oldest retained sequence and falling back to live indexing when contiguous replay is unavailable. It subscribes only to KV-event publishers, treats globally unknown blocks as missing information rather than cache misses, and counts matches for all endpoints in one pass to avoid excessive per-request allocations.
