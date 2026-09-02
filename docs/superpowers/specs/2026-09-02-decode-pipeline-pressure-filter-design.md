# Decode Pipeline Pressure Filter Design

## Goal

Prevent decode routing from selecting an endpoint with materially worse queue
pressure solely because that endpoint has fewer EPP-observed active requests.
Pipeline pressure takes precedence over active-request balancing. Active requests
remain the base load signal among endpoints whose pipeline pressure is close.

The implementation belongs on the DEP/rank-aware fork in the existing
`lfeng/gate2c-load-aware-topk` worktree.

## Motivation

The decode request lifecycle has three distinct waiting stages:

1. The ordinary SGLang waiting queue contains requests whose KV transfer has
   completed but which have not entered the running batch.
2. The decode preallocation queue contains requests that have not acquired the
   resources needed to begin transfer.
3. The decode transfer queue contains requests with allocated destination
   resources that are waiting for KV and metadata transfer to complete.

The EPP active-request count spans the full request lifetime observed by that
EPP. Adding three independently normalized queue scores to active requests
double-counts requests without giving queue pressure deterministic precedence.
Adaptive normalization also makes small differences, such as zero versus one
queued request, consume an entire scorer weight.

## Considered Approaches

### Increase the existing scorer weights

Keep the independent queue scorers and assign them larger profile weights.
This is configuration-only, but it does not guarantee precedence because the
final result remains a sum of unrelated normalized scores.

### Add one combined effective-load scorer

Calculate active requests plus weighted queue pressure in one scorer. This is
compact, but strict precedence requires an arbitrary conversion between queue
pressure and active-request units.

### Filter by combined pipeline pressure

Calculate one pressure value per endpoint, retain endpoints sufficiently close
to the minimum pressure, and let the existing active-request scorer rank the
survivors. This directly represents the required priority and does not require
encoding lexicographic ordering in scorer weights.

This is the selected approach.

## Plugin

Add an alpha scheduling filter with type
`decode-pipeline-pressure-filter`. The filter consumes:

- ordinary waiting queue depth from the standard endpoint metrics field;
- decode preallocation queue depth from a configured scalar endpoint attribute;
- decode transfer queue depth from a configured scalar endpoint attribute.

The plugin configuration is:

```yaml
- type: decode-pipeline-pressure-filter
  name: decode-pipeline-pressure-filter
  parameters:
    threshold: 0.75
    ordinaryWaiting:
      weight: 2
      fixedRange:
        min: 0
        max: 4
    prealloc:
      attributeKey: sglang.decode_prealloc_queue_reqs
      weight: 2
      fixedRange:
        min: 1
        max: 8
    transfer:
      attributeKey: sglang.decode_transfer_queue_reqs
      weight: 1
      fixedRange:
        min: 2
        max: 12
```

All signal weights, fixed-range bounds, attribute keys, and the pressure
threshold are configurable. Weights must be non-negative, at least one weight
must be positive, every fixed range must have `min < max`, and the threshold
must be non-negative.

## Pressure Calculation

For a signal value `x` and fixed range `[min, max]`, calculate normalized
pressure as:

```text
normalized(x) = clamp((x - min) / (max - min), 0, 1)
```

The endpoint pressure is:

```text
pressure =
    ordinaryWaiting.weight * normalized(ordinaryWaiting)
  + prealloc.weight        * normalized(prealloc)
  + transfer.weight        * normalized(transfer)
```

The filter finds the minimum pressure across endpoints with complete metrics
and retains every endpoint satisfying:

```text
endpointPressure <= minimumPressure + threshold
```

With the proposed defaults, a pressure difference of `0.75` tolerates:

- one additional ordinary-waiting request;
- two additional preallocation requests;
- seven additional transfer requests.

A larger combined difference filters the endpoint before active-request
scoring. This gives long queues precedence while avoiding hard decisions from
one small or transient metric difference.

## Missing Metrics

An endpoint missing either configured scalar attribute has incomplete pressure
data. If at least one endpoint has complete pressure data, incomplete endpoints
are excluded. If no endpoint has complete data, the filter fails open and
returns all original endpoints.

Empty and single-endpoint candidate sets are returned unchanged.

## Scheduling Profile

Place the filter after `decode-filter`. Remove the independent generic queue,
decode preallocation, and decode transfer scorers from the decode profile. Keep
the active-request scorer as the base load signal for the retained endpoints.

The KV-cache-utilization scorer remains an independent capacity signal among
the retained endpoints. The decode picker remains deterministic top-1.

```yaml
- name: decode
  plugins:
  - pluginRef: decode-filter
  - pluginRef: decode-pipeline-pressure-filter
  - pluginRef: kv-cache-utilization-scorer
    weight: 1
  - pluginRef: active-request-scorer
    weight: 1
  - pluginRef: decode-top1-picker
```

## Observability

At debug level, log the minimum pressure, configured threshold, number of input
endpoints, and number of retained endpoints. At trace level, log each
endpoint's raw queue values, normalized components, and combined pressure.

## Tests

Add focused unit tests before implementation for:

- configuration decoding and validation;
- fixed-range normalization, clamping, and zero-weight signals;
- combined weighted pressure;
- retention within the configured pressure threshold;
- hard precedence over active-request scoring through profile composition;
- incomplete metric handling and all-metrics-missing fallback;
- empty and single-endpoint inputs;
- plugin registration and configuration loading.

## Scope

The change adds the filter and its tests to the existing fork. It does not
change SGLang metrics, active-request accounting, the picker implementation, or
deployment state. Manifest rollout is a separate step after the image is built
and the behavior is verified.
