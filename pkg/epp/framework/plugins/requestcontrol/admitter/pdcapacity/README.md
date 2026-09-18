# P/D Capacity Admitter

**Type:** `pd-capacity-admitter`

The P/D capacity admitter rejects a request with `ResourceExhausted` when no
feasible prefill endpoint or no feasible decode endpoint remains. It runs after
request data producers and before endpoint scheduling.

The plugin evaluates the full EPP InferencePool, independently of request subset
filters and screeners. Scheduling still respects those request-specific filters;
pool admission does not guarantee that a particular request subset has capacity.
Use one plugin instance per protected pool in each EPP process, not one shared
instance across independently protected deployments. It classifies endpoints using
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

## Deployment-wide circuit breaker

One open/closed state is retained per plugin instance, not per endpoint or role.
The breaker starts closed. Each evaluated request captures the pool snapshot and
updates the state under the same lock:

- Closed (admitting): open when no prefill endpoint or no decode endpoint passes
  all its trip checks. A value equal to a trip threshold fails that check.
- Open (rejecting): close only when at least one prefill and one decode endpoint
  pass all their recovery checks. Values must be strictly below the recovery
  thresholds. Different decode endpoints cannot contribute different checks to
  satisfy recovery; one endpoint must pass them all.
- Otherwise retain the state. Not every endpoint needs to recover.

Recovery thresholds must be positive and strictly lower than their corresponding
trip thresholds. When overriding trip thresholds, set compatible recovery values.

The core metrics extractor publishes each required value and its observation time
as one immutable sample. Admission does not combine legacy core values with
separately captured timestamps. Missing samples, zero or future timestamps, stale
samples, NaN/infinite values, negative queues, and KV utilization outside [0, 1]
make the affected endpoint unavailable for both admission and recovery. A missing
metric retains its last sample until it exceeds `metricsStalenessThreshold`.

Evaluation continues on requests while open; no background loop or half-open
probe is required. Each EPP replica keeps its own state, which resets to closed
on restart. There are no endpoint reservations or cross-replica state guarantees.

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
      waitingQueueRecoveryThreshold: 2
      kvCacheUtilizationThreshold: 0.92
      kvCacheUtilizationRecoveryThreshold: 0.85
      prealloc:
        attributeKey: sglang.decode_prealloc_queue_reqs
        threshold: 8
        recoveryThreshold: 4
    prefill:
      waitingQueueThreshold: 4
      waitingQueueRecoveryThreshold: 2
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
Bypassed requests do not evaluate or change the breaker state.

## Observability

`llm_d_epp_pd_capacity_breaker_open{plugin_name="..."}` is 1 while rejecting
and 0 while admitting. It reports the last evaluated state, not continuously
polled health. The plugin logs each open/close transition with its reason and
logs endpoint exclusion details at debug verbosity.

## Flow-control ordering

The bounded flow-control admission controller runs before endpoint discovery,
tokenization, data producers, and this plugin. If its saturation detector
blocks dispatch, requests queue there before reaching the P/D capacity checks.
Disable that feature gate or configure its detector so this plugin owns the
resource-admission decision.
Requests with no candidates, including an empty result after screeners, retain
the Director's existing 503 behavior and do not reach this breaker.
