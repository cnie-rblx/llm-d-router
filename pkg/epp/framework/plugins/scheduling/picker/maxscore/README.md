# Max Score Picker

**Type:** `max-score-picker`

Selects the endpoint(s) with the highest score calculated during the scoring phase.

> [!NOTE]
> This plugin is enabled by default if no other picker is specified. You do not need to explicitly declare it in your configuration.

## What it does

1.  Receives a list of `ScoredEndpoint` candidates.
2.  Shuffles the list in-place to ensure random tie-breaking when multiple endpoints share the same maximum score.
3.  Sorts the candidates by score in descending order.
4.  Restricts randomized selection to the highest-scoring `topK` candidates.
5.  Returns up to `maxNumOfEndpoints` candidates from that set.

## Behavioral Intent

This picker maximizes the adherence to scoring objectives (e.g., cache affinity, lowest load). However, it is susceptible to **hot-spotting** if many concurrent requests produce identical scores for the same endpoint (e.g., identical prompts targeting a specific cache hit).

## Inputs consumed

- Consumes the list of `ScoredEndpoint` results from the scoring phase.

## Configuration

The plugin config supports:

- `maxNumOfEndpoints` (default 1)
  - The maximum number of endpoints to pick and return. Must be > 0. If more candidates are available than this limit, only the top subset is returned.
- `topK` (default 1)
  - Randomly selects from the highest-scoring K candidates. Values less than or equal to zero fall back to 1. If `maxNumOfEndpoints` is greater than or equal to `topK`, the legacy sorted multi-endpoint behavior is preserved.

> [!TIP]
> In most production scenarios, `maxNumOfEndpoints` is left at its default value of `1` to select a single target endpoint for the request.

To reduce hot-spotting while retaining score locality, select one endpoint randomly from the top three:

```yaml
- type: max-score-picker
  name: top-three-picker
  parameters:
    maxNumOfEndpoints: 1
    topK: 3
```
