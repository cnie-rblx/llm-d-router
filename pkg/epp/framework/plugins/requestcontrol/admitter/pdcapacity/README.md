# P/D Capacity Admitter

**Type:** `pd-capacity-admitter`

The P/D capacity admitter rejects a request with `ResourceExhausted` when no
feasible prefill endpoint or no feasible decode endpoint remains. It runs after
request data producers and before endpoint scheduling.

The plugin receives the combined candidate set and classifies endpoints using
the `llm-d.ai/role` label. It recognizes `prefill`, `decode`,
`encode-prefill`, `prefill-decode`, `both`, and
`encode-prefill-decode`. Unlabeled endpoints do not satisfy either required
role.

## Endpoint checks

A prefill endpoint must have a fresh ordinary waiting-queue metric below
`prefill.waitingQueueThreshold`. Admission deliberately does not use EPP-local
in-flight token state, so this check remains valid with multiple EPP replicas.

A decode endpoint must have:

- fresh ordinary-queue, preallocation-queue, and KV-utilization metrics;
- ordinary and preallocation queues below their thresholds;
- KV utilization below `kvCacheUtilizationThreshold`.

The plugin is stateless and evaluates each metrics snapshot independently.
The core metrics extractor timestamps each required signal independently and
missing metrics, missing custom attributes, zero timestamps, and stale metrics
make only the affected endpoint unavailable.

## Configuration

The plugin belongs in the top-level `plugins` list. It is an admission plugin,
not a scheduling-profile plugin.

```yaml
plugins:
- type: pd-capacity-admitter
  parameters:
    rejectAllPriorities: true
    metricsStalenessThreshold: 12s
    decode:
      waitingQueueThreshold: 4
      kvCacheUtilizationThreshold: 0.92
      prealloc:
        attributeKey: sglang.decode_prealloc_queue_reqs
        threshold: 8
    prefill:
      waitingQueueThreshold: 4
```

The custom queue attributes must be populated by the metrics extractor:

```yaml
- type: core-metrics-extractor
  parameters:
    defaultEngine: sglang
    engineConfigs:
    - name: sglang
      queuedRequestsSpec: sglang:num_queue_reqs
      runningRequestsSpec: sglang:num_running_reqs
      kvUsageSpec: sglang:token_usage
      customMetrics:
      - attributeKey: sglang.decode_prealloc_queue_reqs
        metricSpec: sglang:num_decode_prealloc_queue_reqs
```

Transfer-queue depth is intentionally not an admission condition. A populated
transfer queue represents normal pipeline occupancy and can fluctuate while
transfers continue to drain. It can still be used by scheduling filters and
scorers to steer traffic away from busier decode endpoints.

Request size and projected free KV tokens are also intentionally excluded.
Snapshot-based capacity checks do not reserve the reported headroom or bind the
request to the endpoint that passed the check. Size-aware admission therefore
belongs with endpoint selection and reservation, or in a decode-side guard.
The legacy `decode.defaultOutputTokens` and `decode.maxOutputTokens` fields are
still accepted for strict-decoding compatibility but have no effect.

`rejectAllPriorities: false` limits rejection to requests with negative
priority. Set it to `true` when overload protection must also apply to ordinary
priority-zero traffic.

## Flow-control ordering

The bounded flow-control admission controller runs before endpoint discovery,
tokenization, data producers, and this plugin. If its saturation detector
blocks dispatch, requests queue there before reaching the P/D capacity checks.
Disable that feature gate or configure its detector so this plugin owns the
resource-admission decision.
