# P/D Capacity Admission POC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a role-aware admission plugin that returns HTTP 429 when every eligible prefill or decode rank is unavailable according to fresh queue, KV, and request-cost signals.

**Architecture:** Keep the pre-tokenization flow-control queue unchanged and implement resource admission at the post-data-producer `Admitter` extension point. The plugin partitions the combined endpoint set by `llm-d.ai/role`, evaluates each rank independently, and admits only when at least one prefill and one decode rank are feasible. Raw request bytes remain a queue-memory guard rather than an inference-cost estimate.

**Tech Stack:** Go, llm-d EPP plugin framework, SGLang endpoint metrics, controller-runtime logging, testify.

## Global Constraints

- Work only in `/home/coder/llm-d-router-decode-prealloc-admission-control` on branch `lfeng/decode-prealloc-admission-control`.
- Follow test-driven development: every production behavior must first have a focused failing test.
- Do not modify existing scheduling filters or scorers.
- Evaluate explicitly labeled P/D endpoint roles from the combined candidate set.
- Admit only when at least one prefill-capable and one decode-capable endpoint are feasible.
- Missing, zero-timestamp, or stale endpoint metrics make that endpoint unavailable.
- Metrics freshness and all queue, KV, and request-cost thresholds are configurable.
- Preserve typed `ResourceExhausted` admission denials as HTTP 429; all other denial errors retain HTTP 500 behavior.
- Do not deploy, build an image, push, or perform public actions.

---

### Task 1: Preserve ResourceExhausted From Admission Plugins

**Files:**
- Modify: `pkg/epp/requestcontrol/director_test.go`
- Modify: `pkg/epp/requestcontrol/director.go`

**Interfaces:**
- Consumes: `runAdmissionPlugins(...) error`
- Produces: `HandleRequest` preserves only typed `errcommon.ResourceExhausted` admission errors.

- [ ] **Step 1: Add failing Director cases**

Add a wrapped `errcommon.Error{Code: errcommon.ResourceExhausted}` denial expecting `ResourceExhausted`, plus a typed `BadRequest` denial expecting `Internal`. Keep the existing untyped denial case expecting `Internal`.

- [ ] **Step 2: Run the focused test and confirm RED**

Run `go test ./pkg/epp/requestcontrol -run '^TestDirector_HandleRequest$' -count=1`. The ResourceExhausted case must fail because the Director returns `Internal`.

- [ ] **Step 3: Implement the minimal preservation rule**

Use `errors.As` in the admission-denial block. Return the recovered typed error only when its code is `errcommon.ResourceExhausted`; otherwise retain the existing `Internal` wrapper.

- [ ] **Step 4: Run the focused test and confirm GREEN**

Run the command from Step 2 and require exit code 0.

- [ ] **Step 5: Commit**

Run `git add pkg/epp/requestcontrol/director.go pkg/epp/requestcontrol/director_test.go` and `git commit -s -m "Preserve admission resource exhaustion errors"`.

---

### Task 2: Implement the Role-Aware P/D Capacity Admitter

**Files:**
- Create: `pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity/plugin.go`
- Create: `pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity/plugin_test.go`

**Interfaces:**
- Produces: plugin type `pd-capacity-admitter` implementing `requestcontrol.Admitter` and `plugin.ConsumerPlugin`.
- Consumes: core endpoint metrics, configured scalar custom-metric attributes, and tokenized prompt data.

- [ ] **Step 1: Write failing factory and dependency tests**

Cover defaults, malformed duration, non-positive thresholds, invalid KV fraction, invalid output-token bounds, and conditional inflight-load dependencies when prefill predicted-wait enforcement is enabled.

- [ ] **Step 2: Run package tests and confirm RED**

Run `go test ./pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity -count=1`. Compilation must fail because implementation is absent.

- [ ] **Step 3: Implement configuration and validation**

Implement this configuration shape and defaults:

```yaml
rejectAllPriorities: true
metricsStalenessThreshold: 12s
decode:
  waitingQueueThreshold: 4
  kvCacheUtilizationThreshold: 0.92
  defaultOutputTokens: 2048
  maxOutputTokens: 8192
  prealloc:
    attributeKey: sglang.decode_prealloc_queue_reqs
    threshold: 8
  transfer:
    attributeKey: sglang.decode_transfer_queue_reqs
    threshold: 12
prefill:
  waitingQueueThreshold: 4
```

Prefill admission uses engine-observed ordinary waiting only. It does not use EPP-local in-flight state.

- [ ] **Step 4: Write failing behavior tests**

Cover one feasible rank per role; all-decode prealloc, transfer, ordinary-waiting, KV-utilization, and projected-KV rejection; one healthy decode bypassing overloaded peers; all-prefill ordinary-waiting rejection; stale/zero/missing metrics; missing custom metrics; hybrid roles; unlabeled roles; priority bypass; and typed `ResourceExhausted` denials.

- [ ] **Step 5: Run tests and confirm RED**

Run the Task 2 package test and confirm the behavior tests fail for missing logic.

- [ ] **Step 6: Implement endpoint evaluation**

Decode feasibility uses:

```text
freeKVTokens = KvCacheMaxTokenCapacity * (1 - KVCacheUsagePercent)
outputReserve = min(request.MaxOutputTokens, maxOutputTokens), or defaultOutputTokens when absent
requiredKVTokens = TokenizedPrompt.TokenCount() + outputReserve
```

Require fresh metrics, queue values below thresholds, KV usage below threshold, positive KV capacity, and `requiredKVTokens <= freeKVTokens`.

Prefill feasibility requires a fresh engine-reported ordinary waiting queue below threshold. EPP-local in-flight token state is intentionally excluded so admission remains replica-independent.

Return typed `ResourceExhausted` only when no feasible endpoint exists for at least one role. Emit one operational rejection log plus debug endpoint-unavailability logs without request payloads.

- [ ] **Step 7: Run package tests and confirm GREEN**

Run the Task 2 package test and require exit code 0.

- [ ] **Step 8: Commit**

Run `git add pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity` and `git commit -s -m "Add P/D capacity admission plugin"`.

---

### Task 3: Register and Document the Plugin

**Files:**
- Modify: `cmd/epp/runner/runner.go`
- Create: `cmd/epp/runner/pd_capacity_admitter_test.go`
- Create: `pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity/README.md`

**Interfaces:**
- Consumes: `pdcapacity.Factory`
- Produces: Alpha registration under `pd-capacity-admitter` and a complete top-level YAML example.

- [ ] **Step 1: Write a failing registration test**

Follow `cmd/epp/runner/decode_pipeline_pressure_filter_test.go`: register all plugins, look up `pd-capacity-admitter`, and require a non-nil Alpha factory.

- [ ] **Step 2: Run the registration test and confirm RED**

Run `go test ./cmd/epp/runner -run '^TestPDCapacityAdmitterRegistered$' -count=1` and require a missing-registration failure.

- [ ] **Step 3: Import and register the plugin**

Register `pdcapacity.PluginType` with `fwkplugin.StabilityAlpha` beside the existing admission plugins.

- [ ] **Step 4: Document behavior and configuration**

Document that the plugin is top-level, stateless per metrics snapshot, treats missing/stale data as endpoint-unavailable, and runs after data producers. Document that an enabled earlier flow-control gate can queue requests before this plugin executes.

- [ ] **Step 5: Run registration and focused tests**

Run:

```text
go test ./cmd/epp/runner -run '^TestPDCapacityAdmitterRegistered$' -count=1
go test ./pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity -count=1
go test ./pkg/epp/requestcontrol -run '^TestDirector_HandleRequest$' -count=1
```

All commands must exit 0.

- [ ] **Step 6: Commit**

Run `git add cmd/epp/runner/runner.go cmd/epp/runner/pd_capacity_admitter_test.go pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity/README.md` and `git commit -s -m "Register P/D capacity admission plugin"`.

---

### Task 4: Verify the Branch

**Files:**
- Modify only files required by formatter output from this feature.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: fresh verification evidence for the complete branch.

- [ ] **Step 1: Run `make format`**
- [ ] **Step 2: Run `go test ./pkg/epp/framework/plugins/requestcontrol/admitter/pdcapacity ./pkg/epp/requestcontrol ./cmd/epp/runner -count=1`**
- [ ] **Step 3: Run `make presubmit`**
- [ ] **Step 4: Run `git status --short`, `git diff --check`, and `git log --oneline 8501cdb20d7fd550c103208da3fda3e30c8a5d35..HEAD` and verify scope.**
