# Decode Pipeline Pressure Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a configurable decode pipeline pressure filter that gives ordinary waiting, preallocation, and transfer queue pressure precedence over active-request scoring.

**Architecture:** A new scheduling filter reads the standard waiting-queue metric and two configured scalar endpoint attributes. It normalizes each signal against its own fixed range, computes a weighted pressure sum, and retains endpoints whose pressure is within a configurable threshold of the least-pressured complete endpoint. Existing active-request and KV-utilization scorers rank the retained endpoints.

**Tech Stack:** Go, llm-d EPP plugin framework, testify, go-logr

## Global Constraints

- Work only in the existing `lfeng/gate2c-load-aware-topk` fork worktree.
- Plugin type is `decode-pipeline-pressure-filter` and stability is alpha.
- Signal weights, fixed ranges, scalar attribute keys, and pressure threshold are configurable.
- Weights are non-negative and at least one weight is positive.
- Every fixed range has `min < max`; threshold is non-negative.
- Missing custom metrics exclude an endpoint when any endpoint has complete data; all missing fails open.
- Do not change SGLang, active-request accounting, picker behavior, or deployment state.

---

### Task 1: Implement pressure calculation and filtering with tests

**Files:**
- Create: `pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure/filter.go`
- Create: `pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure/filter_test.go`

**Interfaces:**
- Consumes: `scheduling.Endpoint.GetMetrics().WaitingQueueSize`, `attrmetrics.ReadScalarMetricValue`, and the two configured scalar attribute keys.
- Produces: `Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error)`, `New(name string, config Config) (*Filter, error)`, and `Filter.Filter(context.Context, *scheduling.InferenceRequest, []scheduling.Endpoint) []scheduling.Endpoint`.

- [ ] **Step 1: Write failing factory validation tests**

Cover valid configuration plus negative threshold, negative weight, all-zero weights, missing attribute keys, and invalid fixed ranges. Use the public JSON factory path so field names are tested.

```go
const validConfig = `{
  "threshold": 0.75,
  "ordinaryWaiting": {"weight": 2, "fixedRange": {"min": 0, "max": 4}},
  "prealloc": {"attributeKey": "sglang.decode_prealloc_queue_reqs", "weight": 2,
               "fixedRange": {"min": 1, "max": 8}},
  "transfer": {"attributeKey": "sglang.decode_transfer_queue_reqs", "weight": 1,
               "fixedRange": {"min": 2, "max": 12}}
}`
```

- [ ] **Step 2: Run the package test and verify RED**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure
```

Expected: fail because the package and factory do not exist.

- [ ] **Step 3: Write failing behavior tests**

Construct endpoints with `fwkdl.Metrics.WaitingQueueSize` and scalar attributes. Cover:

```text
normalize below min -> 0
normalize at max -> 1
normalize above max -> 1
pressure difference equal to threshold -> retained
pressure difference greater than threshold -> filtered
combined moderate signals -> filtered when their weighted sum crosses threshold
one incomplete endpoint plus complete endpoints -> incomplete endpoint excluded
all endpoints incomplete -> original endpoints returned
empty and singleton candidate sets -> unchanged
zero-weight signal -> ignored and its attribute is not required
```

- [ ] **Step 4: Implement the minimal filter**

Define focused configuration types:

```go
type FixedRange struct {
    Min float64 `json:"min"`
    Max float64 `json:"max"`
}

type SignalConfig struct {
    AttributeKey string     `json:"attributeKey,omitempty"`
    Weight       float64    `json:"weight"`
    FixedRange   FixedRange `json:"fixedRange"`
}

type Config struct {
    Threshold       float64      `json:"threshold"`
    OrdinaryWaiting SignalConfig `json:"ordinaryWaiting"`
    Prealloc        SignalConfig `json:"prealloc"`
    Transfer        SignalConfig `json:"transfer"`
}
```

Use one helper for normalization:

```go
func normalize(value float64, r FixedRange) float64 {
    return math.Max(0, math.Min(1, (value-r.Min)/(r.Max-r.Min)))
}
```

For each endpoint with complete enabled signals, calculate:

```go
pressure := ordinaryWeight*normalize(waiting, ordinaryRange) +
    preallocWeight*normalize(prealloc, preallocRange) +
    transferWeight*normalize(transfer, transferRange)
```

Find `minPressure`, then preserve input order while retaining endpoints with
`pressure <= minPressure + threshold`. Exclude incomplete endpoints if any
complete endpoint exists; return the original slice if none is complete.

- [ ] **Step 5: Run package tests and verify GREEN**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure
```

Expected: PASS.

- [ ] **Step 6: Format and commit Task 1**

Run:

```bash
gofmt -w pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure/*.go
git add pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure
git commit -s -m "Add decode pipeline pressure filter"
```

### Task 2: Register the plugin and verify configuration loading

**Files:**
- Modify: `cmd/epp/runner/runner.go`
- Modify: `pkg/epp/config/loader/configloader_test.go`

**Interfaces:**
- Consumes: `decodepipelinepressure.PluginType` and `decodepipelinepressure.Factory` from Task 1.
- Produces: runner registration that permits `type: decode-pipeline-pressure-filter` in EPP configuration.

- [ ] **Step 1: Write a failing configuration-loader test**

Add a minimal plugin declaration and decode scheduling profile using:

```yaml
- type: decode-pipeline-pressure-filter
  name: decode-pipeline-pressure-filter
  parameters:
    threshold: 0.75
    ordinaryWaiting:
      weight: 2
      fixedRange: {min: 0, max: 4}
    prealloc:
      attributeKey: sglang.decode_prealloc_queue_reqs
      weight: 2
      fixedRange: {min: 1, max: 8}
    transfer:
      attributeKey: sglang.decode_transfer_queue_reqs
      weight: 1
      fixedRange: {min: 2, max: 12}
```

Assert configuration loading succeeds and the filter appears before the active-request scorer in the decode profile.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test ./pkg/epp/config/loader -run TestDecodePipelinePressureFilterConfiguration -count=1
```

Expected: fail because the plugin type is not registered.

- [ ] **Step 3: Register the filter**

Import the package in `cmd/epp/runner/runner.go` and add:

```go
fwkplugin.Register(
    decodepipelinepressure.PluginType,
    fwkplugin.StabilityAlpha,
    decodepipelinepressure.Factory,
)
```

Place it with the other alpha scheduling filters.

- [ ] **Step 4: Run focused and adjacent tests**

Run:

```bash
go test ./pkg/epp/config/loader -run 'TestDecodePipelinePressureFilterConfiguration|TestFilterExecutionOrderFromYAML' -count=1
go test ./cmd/epp/runner ./pkg/epp/framework/plugins/scheduling/filter/...
```

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

Run:

```bash
git add cmd/epp/runner/runner.go pkg/epp/config/loader/configloader_test.go
git commit -s -m "Register decode pipeline pressure filter"
```

### Task 3: Final verification

**Files:**
- Verify only; no planned code changes.

**Interfaces:**
- Consumes: completed filter implementation and registration.
- Produces: evidence that formatting, tests, lint, and repository presubmit pass.

- [ ] **Step 1: Run formatting and focused tests**

```bash
make format
go test ./pkg/epp/framework/plugins/scheduling/filter/decodepipelinepressure ./pkg/epp/config/loader ./cmd/epp/runner
```

Expected: PASS with no formatting diff outside touched files.

- [ ] **Step 2: Run the repository presubmit gate**

```bash
make presubmit
```

Expected: PASS.

- [ ] **Step 3: Inspect the final diff and worktree state**

```bash
git diff --check HEAD~2..HEAD
git status --short
git log -3 --oneline
```

Expected: no whitespace errors, a clean worktree, and only the design plus filter implementation and registration commits.
