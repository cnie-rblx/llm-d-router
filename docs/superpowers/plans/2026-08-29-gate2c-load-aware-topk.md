# Gate 2c Load-Aware Top-K Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add backward-compatible top-K random selection to the DEP-capable router fork, configure role-specific SGLang P/D queue scoring, deploy the resulting image to prod-5, and verify behavior under shadow traffic.

**Architecture:** The existing metrics extractor will expose SGLang's prefill-bootstrap and decode-preallocation gauges as custom endpoint attributes. Existing endpoint-attribute scorers will normalize those queues, while a small extension to `max-score-picker` will randomly choose returned endpoints from the highest-scoring K candidates. The production manifest will instantiate independent prefill and decode pickers with different K values.

**Tech Stack:** Go, llm-d EPP plugin framework, Kubernetes YAML, Docker, AWS ECR, kubectl, SGLang metrics.

## Global Constraints

- Build on DEP/rank-aware router commit `ded5f9536bf2fcd39c36b1bfa9d2c75d5966ccb0`, not upstream `main`.
- Preserve existing behavior when `topK` is omitted by defaulting it to 1.
- Use prefix-cache weight 2, prefill-bootstrap queue weight 2, and decode-preallocation queue weight 2.
- Configure prefill top K as 2 and decode top K as 3 while returning exactly one endpoint.
- Modify only the Gate 2c copied deployment manifest in the benchmark repository.
- Preserve prod-5 namespace, route hostname, EPP RBAC, and rank-aware DEP settings when rendering the deployment.

---

### Task 1: Add top-K random selection to max-score-picker

**Files:**
- Modify: `pkg/epp/framework/plugins/scheduling/picker/common.go`
- Modify: `pkg/epp/framework/plugins/scheduling/picker/maxscore/picker.go`
- Modify: `pkg/epp/framework/plugins/scheduling/picker/maxscore/picker_test.go`
- Modify: `pkg/epp/framework/plugins/scheduling/picker/maxscore/README.md`

**Interfaces:**
- Consumes: `picker.PickerParameters` JSON configuration and scored endpoint slices.
- Produces: `PickerParameters.TopK int` and `NewMaxScorePicker(maxNumOfEndpoints, topK int) *MaxScorePicker`.

- [ ] **Step 1: Add failing selection tests**

Add tests that repeatedly call a picker configured with top K equal to 2 or 3 and assert that only endpoints in that prefix are returned and that every eligible endpoint is observed. Add compatibility cases for omitted/default top K, top K equal to 1, and top K larger than the candidate count.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/picker/maxscore -count=1
```

Expected: compilation or assertion failure because `TopK` and top-K sampling do not exist.

- [ ] **Step 3: Implement minimal picker support**

Add `TopK int \`json:"topK"\`` to `PickerParameters`. Default it to 1 in the factory and constructor. After descending stable sort, clamp the top-K window to the candidate count, shuffle only that window, and then truncate to `maxNumOfEndpoints`.

- [ ] **Step 4: Run focused and related tests**

Run:

```bash
go test ./pkg/epp/framework/plugins/scheduling/picker/... -count=1
go test ./pkg/epp/config/loader ./cmd/epp/runner -count=1
```

Expected: all packages pass.

- [ ] **Step 5: Document and commit**

Document `topK`, its default, and its interaction with `maxNumOfEndpoints`, run `gofmt`, then commit the picker implementation and tests.

---

### Task 2: Configure Gate 2c role-specific queue scoring

**Files:**
- Modify in a dedicated benchmark worktree: `deploy_yamls/glm52-b200-nvfp4-gate2c-pd.yaml`

**Interfaces:**
- Consumes: `core-metrics-extractor.customMetrics`, `endpoint-attribute-scorer`, and `max-score-picker.topK`.
- Produces: named plugins `prefill-bootstrap-queue-scorer`, `decode-prealloc-queue-scorer`, `prefill-top2-picker`, and `decode-top3-picker`.

- [ ] **Step 1: Create an isolated benchmark worktree**

Create branch `lfeng/gate2c-load-aware-topk-deploy` from the current benchmark branch HEAD without modifying the existing checkout.

- [ ] **Step 2: Add metric extraction and plugin configuration**

Under the SGLang engine config, add:

```yaml
customMetrics:
- attributeKey: sglang.prefill_bootstrap_queue_reqs
  metricSpec: sglang:num_prefill_bootstrap_queue_reqs
- attributeKey: sglang.decode_prealloc_queue_reqs
  metricSpec: sglang:num_decode_prealloc_queue_reqs
```

Declare two lower-is-better adaptive-range endpoint-attribute scorers and two named max-score pickers:

```yaml
- type: endpoint-attribute-scorer
  name: prefill-bootstrap-queue-scorer
  parameters:
    attributeKey: sglang.prefill_bootstrap_queue_reqs
    algorithm:
      type: linear_lower_is_better
- type: endpoint-attribute-scorer
  name: decode-prealloc-queue-scorer
  parameters:
    attributeKey: sglang.decode_prealloc_queue_reqs
    algorithm:
      type: linear_lower_is_better
- type: max-score-picker
  name: prefill-top2-picker
  parameters:
    maxNumOfEndpoints: 1
    topK: 2
- type: max-score-picker
  name: decode-top3-picker
  parameters:
    maxNumOfEndpoints: 1
    topK: 3
```

Set prefix and prefill-bootstrap weights to 2, decode-preallocation weight to 2, and replace the profile picker references with their named instances.

- [ ] **Step 3: Validate the configuration**

Extract the embedded EPP config and run it through the built EPP's configuration loader or startup validation. Run YAML parsing and `kubectl apply --dry-run=server` against the prod-5-rendered manifest.

Expected: the plugin configuration loads and every Kubernetes object validates.

- [ ] **Step 4: Commit the manifest change**

Run `git diff --check`, review that only the intended config/image lines changed, and commit.

---

### Task 3: Build and publish the DEP-fork EPP image

**Files:**
- No additional source files.

**Interfaces:**
- Consumes: tested router worktree commit.
- Produces: immutable ECR image digest for the EPP deployment.

- [ ] **Step 1: Run full relevant verification**

Run focused picker tests, router config/runner tests, `go test ./pkg/epp/...`, formatting checks, and `git diff --check`.

- [ ] **Step 2: Build the image**

Use a unique tag containing the commit SHA:

```bash
EPP_IMAGE=303743157816.dkr.ecr.us-east-2.amazonaws.com/lfeng/llm-d-router-endpoint-picker:gate2c-load-aware-topk-<sha> make image-build-epp
```

- [ ] **Step 3: Push and resolve the immutable digest**

Push the image using the workspace's existing transparent registry authentication, inspect the pushed manifest, and record the `sha256:` digest. Do not print or manually configure credentials.

- [ ] **Step 4: Put the digest in the benchmark manifest**

Replace only the EPP image reference with `303743157816.dkr.ecr.us-east-2.amazonaws.com/lfeng/llm-d-router-endpoint-picker@sha256:<digest>` and rerun manifest validation.

---

### Task 4: Deploy and monitor prod-5

**Files:**
- Generate: `/tmp/glm52-b200-nvfp4-gate2c-pd-prod5-load-aware.yaml`
- Generate: benchmark evidence under `build-traffic-replay/` if a controlled replay is required to obtain a stable comparison window.

**Interfaces:**
- Consumes: immutable EPP image and validated Gate 2c manifest.
- Produces: live prod-5 deployment plus a measured post-change assessment.

- [ ] **Step 1: Render the prod-5 manifest**

Replace namespace `kubeflow-build-test` with `kubeflow-creator-code`, set the HTTPRoute hostname to `ai-inference-prod-5.prod.ml.rbx.com`, and preserve/add EPP Role access to `inferencemodelrewrites.inference.networking.x-k8s.io` with `get`, `list`, and `watch`.

- [ ] **Step 2: Capture the pre-change baseline and apply**

Save the live EPP ConfigMap/deployment and current rank-selection/queue/abort metrics. Apply the rendered manifest and verify the exact image digest and ConfigMap data now live.

- [ ] **Step 3: Wait for readiness**

Verify EPP readiness, its config-load logs, serving workload readiness, route status, and an external `/v1/models` request. Do not proceed to assessment if the new EPP is not serving.

- [ ] **Step 4: Observe traffic for at least 15 minutes**

During live shadow traffic, capture:

- prefill and decode selection counts per rank;
- generic queue plus prefill-bootstrap/decode-preallocation queues per rank;
- KV utilization per rank;
- transfer-failure and abort counts/rates;
- prefix-cache hit rate.

If live shadow volume is insufficient, run the established rank-covering warmup followed by the standard 15-minute M3 replay.

- [ ] **Step 5: Assess and report**

Compare rank concentration, maximum queue depth, cancellation rate, and cache-hit rate against the pre-change observations. Report regressions or unresolved problems explicitly and leave the deployed immutable image and manifest paths documented.
