# SGLang Prefill Failure Cancellation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cancel decode immediately when the concurrent SGLang prefill HTTP leg fails and emit actionable structured diagnostics.

**Architecture:** Keep prefill and decode concurrent, but route decode through the existing deferred commit writer. Prefill success commits decode streaming; prefill failure cancels decode and owns the client error response.

**Tech Stack:** Go, `net/http`, `httputil.ReverseProxy`, Ginkgo/Gomega, Kubernetes, TrafficReplay.

## Global Constraints

- Preserve response streaming after prefill succeeds.
- Never log request bodies or credentials.
- Preserve the existing uncommitted EPP metrics and SGLang timing work.
- Do not modify unrelated connector behavior.

---

### Task 1: Prefill failure cancellation

**Files:**
- Modify: `pkg/sidecar/proxy/connector_sglang.go`
- Test: `pkg/sidecar/proxy/connector_sglang_test.go`

**Interfaces:**
- Consumes: `newDeferredCommitWriter(http.ResponseWriter)`.
- Produces: concurrent prefill/decode dispatch where prefill owns the commit decision.

- [ ] Add a test whose prefill backend returns HTTP 500 while decode blocks on context cancellation; assert prompt HTTP 500 and canceled decode.
- [ ] Run the focused Ginkgo test and verify it fails because decode currently waits.
- [ ] Run decode through `deferredCommitWriter`, cancel it on prefill failure, and return the buffered prefill status/body.
- [ ] Run the focused test and the complete `pkg/sidecar/proxy` suite.

### Task 2: Transport diagnostics

**Files:**
- Modify: `pkg/sidecar/proxy/connector_sglang.go`
- Test: `pkg/sidecar/proxy/connector_sglang_test.go`

**Interfaces:**
- Produces structured fields `prefillTarget`, `prefillStatusCode`, `prefillTransportError`, `bootstrapRoom`, and `requestID`.

- [ ] Add a test with an unreachable prefill backend and assert prompt 502 plus decode cancellation.
- [ ] Verify the test fails before implementation.
- [ ] Capture a per-request reverse-proxy transport error without mutating the shared cached proxy.
- [ ] Add the five structured fields to the P/D timing log and run the focused and package test suites.

### Task 3: Build, deploy, and reproduce

**Files:**
- Modify: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-dep-pd-phase2-gate2h-load-aware-prefill-mooncake.yaml`

- [ ] Build and push the sidecar image to the existing ECR repository and record its immutable digest.
- [ ] Update only the sidecar image digest in the Gate 2h manifest and apply it.
- [ ] Wait for all prefill/decode pods and the route to become ready with stable restart counts.
- [ ] Repeat the valid-bootstrap/failed-prefill probe and verify prompt failure, decode cancellation, and all five log fields.

### Task 4: m3 verification

**Files:**
- Create: `/home/coder/lm-benchmark-llmd-test/build-traffic-replay/bench-results-pd-phase2-gate2h-prefill-failure-fix/`

- [ ] Run the existing m3 TrafficReplay for 15 minutes with one EPP replica.
- [ ] Wait for completion or explicitly clean up a stuck replay after preserving its status and logs.
- [ ] Analyze TTFT/E2E/ITL/throughput and search structured logs for every prefill non-2xx or transport failure.
- [ ] Report whether 600-second aborted requests remain and identify any observed prefill failure trigger.
