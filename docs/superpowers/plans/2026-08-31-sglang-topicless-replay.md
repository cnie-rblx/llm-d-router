# SGLang Topicless Replay Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make EPP rebuild its KV index from upstream SGLang's topicless replay frames while retaining compatibility with topic-bearing replay frames.

**Architecture:** Keep the existing three-frame live-event parser unchanged. Add replay-only parsing that supplies the subscriber's exact `topicFilter` when an upstream two-frame event omits the topic, and recognize both upstream and topic-bearing terminal markers.

**Tech Stack:** Go, go-zeromq, Kubernetes YAML, Docker, AWS ECR, kubectl.

## Global Constraints

- Build from `/home/coder/llm-d-router-gate2c-load-aware-topk` on `lfeng/gate2c-load-aware-topk` so all existing DEP and routing fixes remain in the image.
- Use the existing `topicFilter` field; do not introduce another configuration field.
- Configure the Gate 2c EPP with the exact topic `kv@@Assistant/glm_shadow-traffic`.
- Deploy the EPP compatibility image before returning prefill to the standard unpatched SGLang image.
- Patch only intended fields in the live resources; do not reapply a stale full manifest.

---

### Task 1: Parse upstream SGLang replay frames

**Files:**
- Modify: `pkg/kvevents/zmq_subscriber.go`
- Modify: `pkg/kvevents/zmq_subscriber_test.go`

**Interfaces:**
- Consumes: `parseEventFrame(frames [][]byte)` for unchanged live traffic.
- Produces: `parseReplayEventFrame(frames [][]byte, fallbackTopic string) (string, uint64, []byte, bool)` and `isReplayTerminalFrame(frames [][]byte) bool`.

- [ ] **Step 1: Write failing parser tests**

Add table-driven tests covering topic-bearing `[topic, sequence, payload]`, upstream `[sequence, payload]`, malformed frames, `[END_SEQ, empty]`, and `[empty, END_SEQ, empty]`.

- [ ] **Step 2: Verify the new tests fail**

Run:

```bash
make test-filter TYPE=epp PATTERN='TestParseReplay|TestReplayTerminal'
```

Expected: compilation failure because the replay helpers do not exist.

- [ ] **Step 3: Implement the minimal replay helpers**

Keep `parseEventFrame` unchanged. For two-frame replay events, validate the sequence frame and return `fallbackTopic`; for three-frame events, delegate to `parseEventFrame`. Recognize only the two documented terminal layouts.

- [ ] **Step 4: Use the helpers in `requestReplay`**

After stripping the optional DEALER delimiter, check `isReplayTerminalFrame`, then call `parseReplayEventFrame(frames, z.topicFilter)`. Preserve existing sequence, retry, and invalidation behavior.

- [ ] **Step 5: Verify targeted and package tests**

Run:

```bash
make test-filter TYPE=epp PATTERN='TestParseReplay|TestReplayTerminal|TestZMQSubscriber'
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add pkg/kvevents/zmq_subscriber.go pkg/kvevents/zmq_subscriber_test.go
git commit -s -m "Support topicless SGLang replay frames"
```

### Task 2: Verify the complete router branch and build an immutable EPP image

**Files:**
- No source changes.

**Interfaces:**
- Consumes: the complete `lfeng/gate2c-load-aware-topk` branch at the new commit.
- Produces: an immutable ECR digest for `llm-d-router-endpoint-picker`.

- [ ] **Step 1: Verify branch ancestry and diff**

Run `git log --oneline`, `git status --short`, and `git diff HEAD^` to confirm the new commit sits on top of all prior Gate 2c commits and contains only the replay compatibility change.

- [ ] **Step 2: Run formatting and tests**

Run:

```bash
make format
make test-unit-epp
```

Expected: both commands exit zero.

- [ ] **Step 3: Build and push the EPP image**

Use a tag containing the exact git SHA:

```bash
EPP_TAG=gate2c-replay-$(git rev-parse --short=12 HEAD) \
EPP_IMAGE_REGISTRY=303743157816.dkr.ecr.us-east-2.amazonaws.com/lfeng \
make image-build-epp image-push-epp
```

Expected: the push completes and returns an immutable digest.

- [ ] **Step 4: Verify image provenance**

Inspect the image labels or embedded build-info endpoint and confirm its commit SHA equals the worktree HEAD.

### Task 3: Roll out without reverting live configuration

**Files:**
- Modify surgically: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-gate2c-pd.yaml`
- Refresh: `/home/coder/lm-benchmark-llmd-test/deploy_yamls/glm52-b200-nvfp4-gate2c-pd-prod5-live-20260831.yaml`

**Interfaces:**
- Consumes: immutable EPP digest from Task 2.
- Produces: running prod-5 EPP using the exact topic and upstream-compatible replay parser.

- [ ] **Step 1: Capture the live baseline**

Record the live EPP, prefill, and decode deployment images, pod template annotations, replica counts, and rollout state. Save the current live resources before patching.

- [ ] **Step 2: Patch only the EPP topic and image**

Update the live EPP configuration to `topicFilter: "kv@@Assistant/glm_shadow-traffic"` and replace only its image with the digest from Task 2. Do not apply the full local manifest.

- [ ] **Step 3: Verify the EPP rollout**

Wait for the EPP deployment rollout. Confirm the new pod reports the intended image digest and build commit, discovers all prefill ranks, and has no malformed replay-frame errors.

- [ ] **Step 4: Return prefill to standard SGLang**

Replace only the prefill engine container image with `docker.artifactory.rbx.com/ml-user-images/assistant-sglang-mc:dd8333d522c348c04bb82ee592f08c5516c16a94`, preserving every other live pod-template field.

- [ ] **Step 5: Verify replay and service health**

Wait for the prefill rollout. Verify all eight ranks complete replay, EPP receives ongoing KV events, no replay-format errors appear, pods remain ready with stable restart counts, and the serving endpoint succeeds.

- [ ] **Step 6: Preserve the verified state in YAML**

Change only `topicFilter` and EPP image in the maintained manifest, retain its existing standard SGLang image, and refresh the live snapshot from the verified cluster resources.
