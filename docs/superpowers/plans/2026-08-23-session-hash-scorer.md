# Exact Session Hash Scorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an EPP scorer that selects the same prefill DP rank as the Phase 1e Envoy Lua session hash, then measure the EPP virtual-rank routing overhead with a matched M3 comparison.

**Architecture:** A stateless alpha scorer reads `session-id`, applies the exact byte-wise Lua polynomial hash, and scores only the endpoint whose `RankIndex` matches `hash % rankCount`. The live EPP-hash arm keeps the 1e SGLang configuration and decode routing but replaces Envoy's prefill-rank Lua selection with EPP virtual-rank selection.

**Tech Stack:** Go, llm-d EPP plugin framework, Kubernetes/Envoy Gateway, SGLang, Python benchmark scripts.

## Global Constraints

- Use the existing `/home/coder/llm-d-router-glm52-rank-aware-epp` worktree and preserve its unrelated dirty changes.
- Use the exact modulus `2147483647`, multiplier `31`, UTF-8 header bytes, and `session-id` header.
- Configure `rankCount: 8` for the experiment.
- Keep decode load-aware and use one EPP replica, one prefill pod, and two decode pods.
- Do not add generalized hashing configuration, state, or unrelated refactors.
- Do not push or open a pull request.

---

### Task 1: Add the session hash scorer with TDD

**Files:**
- Create: `pkg/epp/framework/plugins/scheduling/scorer/sessionhash/plugin_test.go`
- Create: `pkg/epp/framework/plugins/scheduling/scorer/sessionhash/plugin.go`
- Create: `pkg/epp/framework/plugins/scheduling/scorer/sessionhash/README.md`

**Interfaces:**
- Consumes: `scheduling.InferenceRequest.Headers["session-id"]` and `Endpoint.GetMetadata().RankIndex`.
- Produces: `Factory(name string, rawParameters *json.Decoder, handle plugin.Handle) (plugin.Plugin, error)` and a `scheduling.Scorer` registered as `session-hash-scorer`.

- [ ] **Step 1: Write failing tests**

  Add table tests that construct rank endpoints and assert factory validation,
  plugin identity/category, exact ASCII and UTF-8 hash vectors, matching-rank
  score `1`, all other scores `0`, and zero scores for absent/empty headers or
  nil inputs.

- [ ] **Step 2: Verify RED**

  Run:

  ```bash
  go test ./pkg/epp/framework/plugins/scheduling/scorer/sessionhash
  ```

  Expected: compilation fails because `Factory`, `Scorer`, and `PluginType` do
  not exist.

- [ ] **Step 3: Implement the minimal scorer**

  Implement these constants and types:

  ```go
  const (
      PluginType = "session-hash-scorer"
      sessionIDHeader = "session-id"
      hashModulus uint64 = 2147483647
  )

  type parameters struct {
      RankCount int `json:"rankCount"`
  }

  type Scorer struct {
      typedName fwkplugin.TypedName
      rankCount uint64
  }
  ```

  `Factory` rejects nil parameters, malformed JSON, and `rankCount <= 0`.
  `Score` initializes every candidate to zero, returns early for an absent or
  empty session header, computes `h = (h*31 + uint64(b)) % hashModulus` for
  each byte, and assigns `1` only to valid metadata with the target rank.

- [ ] **Step 4: Verify GREEN and document configuration**

  Run the package test again and add a concise README containing the parameter,
  formula, missing-header behavior, and one EPP configuration example.

- [ ] **Step 5: Commit only Task 1 files**

  ```bash
  git add pkg/epp/framework/plugins/scheduling/scorer/sessionhash
  git commit -s -m "Add exact session hash scorer"
  ```

### Task 2: Register and validate the scorer

**Files:**
- Modify: `cmd/epp/runner/runner.go`
- Test: `pkg/epp/framework/plugins/scheduling/scorer/sessionhash/plugin_test.go`

**Interfaces:**
- Consumes: `sessionhash.PluginType` and `sessionhash.Factory` from Task 1.
- Produces: an alpha in-tree plugin usable from EPP configuration.

- [ ] **Step 1: Establish the registration compile gate**

  The runner has no in-tree registry enumeration test. Before adding the
  import and registration, run the focused scorer test plus
  `go test ./cmd/epp/runner` as the baseline. After registration, the same
  command is the compile and initialization regression gate.

- [ ] **Step 2: Register the plugin**

  Add:

  ```go
  fwkplugin.Register(sessionhash.PluginType, fwkplugin.StabilityAlpha, sessionhash.Factory)
  ```

  beside the other alpha scorer registrations.

- [ ] **Step 3: Verify focused and repository checks**

  Run:

  ```bash
  go test ./pkg/epp/framework/plugins/scheduling/scorer/sessionhash ./cmd/epp/runner
  make presubmit
  ```

  Expected: all commands pass. If `make presubmit` reports a pre-existing
  failure attributable to the dirty worktree, record the exact failure and run
  the narrow format, lint, and tests for the new files instead.

- [ ] **Step 4: Commit only registration files**

  ```bash
  git add cmd/epp/runner/runner.go
  git commit -s -m "Register session hash scorer"
  ```

### Task 3: Build the EPP image and create the matched manifest

**Files:**
- Create: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-dep-pd-compare-m3-epp-session-hash.yaml`

**Interfaces:**
- Consumes: the EPP binary containing `session-hash-scorer` and the Phase 1e comparison manifest.
- Produces: one deployable 1P2D/1-EPP manifest with identical engine/cache settings and EPP prefill rank selection.

- [ ] **Step 1: Build and push a uniquely tagged EPP image**

  Run:

  ```bash
  AWS_PROFILE=mlp aws ecr get-login-password --region us-east-2 \
    | docker login --username AWS --password-stdin 303743157816.dkr.ecr.us-east-2.amazonaws.com
  IMAGE_REGISTRY=303743157816.dkr.ecr.us-east-2.amazonaws.com/lfeng \
    EPP_TAG=session-hash-20260823 make image-build-epp image-push-epp
  ```

  Resolve and record the pushed image digest before editing the manifest.

- [ ] **Step 2: Derive the EPP-hash manifest from 1e**

  Copy the 1e manifest, preserve all SGLang image, argument, resource,
  topology, route, and cache settings, then make only the required routing
  changes: eight prefill target ports, no Lua rank-selection policy, EPP
  prefill role filter plus `session-hash-scorer` and `max-score-picker`, current
  load-aware decode profile, and the new EPP image.

- [ ] **Step 3: Validate the manifest**

  Run a YAML parse, `kubectl apply --dry-run=server`, and a focused diff against
  1e. Verify that all non-routing SGLang arguments match exactly.

### Task 4: Prove rank parity and run matched M3 traffic

**Files:**
- Create: `/home/coder/lm-benchmark-llmd-test/build-traffic-replay/bench-results-controlled-m3-1e-sticky-rerun/`
- Create: `/home/coder/lm-benchmark-llmd-test/build-traffic-replay/bench-results-controlled-m3-epp-session-hash/`

**Interfaces:**
- Consumes: both matched manifests and `bench_driver.py` with `m3`.
- Produces: raw replay, analysis, observability, deployment-state, and log artifacts for both arms.

- [ ] **Step 1: Record live state and replace the current deployment with fresh 1e**

  Confirm cluster context and namespace, ensure no TrafficReplay is active,
  delete the current comparison manifest, apply 1e, and wait for all 1P2D/EPP
  pods and the HTTPRoute to become healthy.

- [ ] **Step 2: Run fresh Lua M3**

  Invoke the established driver with `m3` so it waits for idle, flushes all
  prefill ranks, performs the standard two-conversation per-rank warmup, runs
  15 minutes at QPS multiplier 3 and concurrency 400, and stores the complete
  artifact set in the new rerun directory. Analyze the exact replay window.

- [ ] **Step 3: Deploy EPP hash and prove known-session parity**

  Replace 1e with the EPP-hash manifest and wait for health. For several known
  session IDs, calculate the Lua target rank locally, send one request, and
  verify the EPP/sidecar logs select that exact prefill rank.

- [ ] **Step 4: Run fresh EPP-hash M3**

  Run the identical driver procedure and save the complete artifact set in the
  EPP-hash directory. Record pod restarts and route conditions before and after.

### Task 5: Compare and report

**Files:**
- Modify: `/home/coder/lm-benchmark-llmd-test/build-traffic-replay/RESULT-glm52-controlled-m3-routing-cache-comparison.json`
- Modify: `/home/coder/lm-benchmark-llmd-test/build-traffic-replay/REPORT-glm52-controlled-m3-routing-cache-comparison.md`

**Interfaces:**
- Consumes: fresh Lua and EPP-hash analysis/observability artifacts.
- Produces: a reproducible statement of rank parity and observed prefill-routing overhead.

- [ ] **Step 1: Validate artifact completeness**

  Confirm both arms contain `raw-m3.json`, `analysis.json`,
  `pd-observability.json`, deployment state, and driver/analyzer logs.

- [ ] **Step 2: Add the matched comparison**

  Report client success/error rate, achieved and completed RPS, input/output
  token throughput, TTFT and E2E average/p50/p90/p99, ITL, cache-hit rate,
  uncached input tokens per second, queue/running load, EPP/sidecar timing, and
  per-rank distribution. Compute EPP-hash minus Lua absolute and percentage
  deltas for the principal latency and throughput metrics.

- [ ] **Step 3: State the bounded conclusion**

  Attribute the measured delta to moving prefill selection through EPP virtual
  endpoints and the sidecar while noting that both arms retain EPP/sidecar for
  decode. Report any cache or rank mismatch as a failed isolation rather than
  calling it overhead.

- [ ] **Step 4: Verify repository state**

  Confirm the lm-benchmark changes remain uncommitted on `lfeng/llmd-test`, the
  live deployment is identified, and no TrafficReplay remains active.
