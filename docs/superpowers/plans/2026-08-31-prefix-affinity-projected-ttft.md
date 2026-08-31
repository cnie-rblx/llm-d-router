# Prefix Affinity Projected TTFT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Include the current request's endpoint-specific uncached prompt tokens in throughput-based prefix-affinity TTFT estimates.

**Architecture:** Reuse the `UncachedRequestTokens` attribute already produced by `inflight-load-producer` and consumed by `token-load-scorer`. The affinity filter will read the committed and speculative attributes separately and sum them only while estimating the current scheduling cycle's TTFT.

**Tech Stack:** Go, llm-d EPP plugin framework, testify, Kubernetes, Docker/ECR.

## Global Constraints

- Keep live `InFlightLoad` state and speculative current-request cost separate.
- Use the same `inFlightLoadProducerName` for both data keys.
- Change throughput mode only; leave latency-predictor mode unchanged.
- Add no new YAML configuration field.
- Modify only the affinity filter, its tests, its README, and deployment image pins.

---

### Task 1: Add the regression contract

**Files:**
- Modify: `pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity/plugin_test.go`

**Interfaces:**
- Consumes: `attrconcurrency.UncachedRequestTokensDataKey`
- Produces: regression coverage for projected TTFT and dependency declarations

- [ ] **Step 1: Write the failing projected-TTFT test**

Add current-request uncached-token data to the test endpoint helper and create a
case where the sticky endpoint has enough projected work to exceed
`maxTTFTPenaltyMs` only after the current request is included.

- [ ] **Step 2: Verify the new test fails for the missing behavior**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity -run 'TestFilter_ThroughputTTFTIncludesCurrentRequest|TestConsumes_ConditionalAttributes' -count=1
```

Expected: the projected-TTFT assertion fails because the filter uses only
`InFlightLoad.Tokens`.

### Task 2: Consume projected request cost in the filter

**Files:**
- Modify: `pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity/plugin.go`
- Modify: `pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity/README.md`

**Interfaces:**
- Consumes: `*attrconcurrency.InFlightLoad` and `*attrconcurrency.UncachedRequestTokens`
- Produces: throughput TTFT equal to `(committed + projected) / throughput * 1000`

- [ ] **Step 1: Add the projected-token data key**

Initialize `uncachedRequestTokensDataKey` from
`UncachedRequestTokensDataKey.WithNonEmptyProducerName(config.InFlightLoadProducerName)`.

- [ ] **Step 2: Declare the throughput dependency**

Require both concurrency attributes when the TTFT gate uses prefill throughput.

- [ ] **Step 3: Update the TTFT estimate**

Sum defensive reads of committed and current-request tokens before dividing by
`PeakPrefillThroughput`. Keep latency-predictor behavior unchanged.

- [ ] **Step 4: Update the canonical formula documentation**

Document the projected formula and both consumed attributes in the filter README.

- [ ] **Step 5: Verify focused tests pass**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity -count=1
```

Expected: PASS.

### Task 3: Verify, build, and deploy

**Files:**
- Modify: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-gate2c-pd.yaml`
- Modify: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-gate2c-pd-prod5-live-20260831.yaml`

**Interfaces:**
- Consumes: immutable EPP image built from the tested router commit
- Produces: healthy prod-5 EPP rollout using projected TTFT

- [ ] **Step 1: Run repository verification**

Run `make format`, focused tests, and `make test-unit-epp`. Report any unrelated
pre-existing presubmit blockers separately.

- [ ] **Step 2: Commit the implementation**

Commit only the filter implementation, tests, and README with DCO sign-off.

- [ ] **Step 3: Build and push the immutable EPP image**

Build from the exact commit, push to the existing prod-5 ECR repository, and
resolve its digest.

- [ ] **Step 4: Update manifests and EPP Deployment**

Replace only the EPP image pin in both manifests and the live EPP Deployment.
Do not change worker images or plugin parameters.

- [ ] **Step 5: Verify live behavior**

Confirm EPP rollout success, zero active-pod restarts, external `/v1/models`
HTTP 200, expected loaded config, and live prefix-affinity/token-load activity.
