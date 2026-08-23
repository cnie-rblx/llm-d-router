# Session Hash Scorer

**Type:** `session-hash-scorer`
**Interface:** `scheduling.Scorer`

Adds deterministic affinity to a data-parallel rank using the request's
`session-id` header. The hash matches the Envoy Lua session-routing policy:

```text
h = 0
for each byte b in session-id:
    h = (h * 31 + b) % 2147483647
rank = h % rankCount
```

The endpoint whose `RankIndex` equals `rank` receives a score of `1`; all other
endpoints receive `0`. A missing or empty `session-id` gives every endpoint a
score of `0`, leaving tie-breaking to the configured picker.

## Parameters

| Name | Type | Required | Description |
|---|---|---|---|
| `rankCount` | integer | Yes | Number of data-parallel ranks. Must be greater than zero. |

## Configuration

```yaml
plugins:
- type: session-hash-scorer
  name: prefill-session-hash
  parameters:
    rankCount: 8
- type: max-score-picker
  name: picker

schedulingProfiles:
- name: prefill
  plugins:
  - pluginRef: prefill-session-hash
  - pluginRef: picker
```

Configure this scorer only when endpoint `RankIndex` values cover the range
`0` through `rankCount - 1`.
