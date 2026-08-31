# Prefix Affinity Projected TTFT Design

## Goal

Make the `prefix-cache-affinity-filter` throughput-based TTFT gate include the
current request's endpoint-specific uncached prompt work, rather than comparing
sticky and non-sticky endpoints using only work committed by earlier requests.

## Current behavior

`inflight-load-producer` publishes two separate endpoint attributes during a
scheduling cycle:

- `InFlightLoad`: committed work from previously scheduled requests.
- `UncachedRequestTokens`: the current request's projected uncached work for
  that endpoint, calculated from the selected prefix producer's match data.

`token-load-scorer` sums both attributes. The throughput mode of
`prefix-cache-affinity-filter` reads only `InFlightLoad.Tokens`, so its TTFT
comparison omits the current request.

## Design

The filter will consume `UncachedRequestTokens` from the same named
`inflight-load-producer` it already uses for `InFlightLoad`. In throughput mode,
each endpoint's predicted TTFT becomes:

```text
(inFlightTokens + uncachedRequestTokens) / peakPrefillThroughput * 1000
```

Latency-predictor mode remains unchanged. Missing or nil attributes contribute
zero tokens, preserving the filter's defensive fallback behavior.

No new configuration field is required. Existing manifests continue to select
the producer through `inFlightLoadProducerName`.

## Scope

Modify only the prefix-affinity filter implementation, its unit tests, and its
README formula. Do not merge speculative request cost into `InFlightLoad`,
recompute prefix cost inside the filter, or change token-load scoring.

## Verification

Add a regression test where committed load alone keeps stickiness but adding
the current request's per-endpoint uncached cost changes the correct gate
decision. Verify dependency declarations for throughput and latency-predictor
modes, run the focused package tests, then run the repository unit EPP suite.

For deployment, build an immutable EPP image from the resulting commit, update
the prod-5 manifests and EPP Deployment only, and verify rollout health,
endpoint health, loaded configuration, and request-time filter logs.
