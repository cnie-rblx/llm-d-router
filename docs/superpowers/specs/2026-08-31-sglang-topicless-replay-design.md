# SGLang Topicless Replay Compatibility

## Goal

Allow the EPP KV-event subscriber to rebuild its index from the replay format
emitted by upstream SGLang without requiring a custom SGLang image.

## Scope

This design assumes one EPP deployment serves one model. The deployment must
configure `kvEventsConfig.topicFilter` with the complete SGLang topic rather
than a broad prefix. For the Gate 2c deployment, the value is
`kv@@Assistant/glm_shadow-traffic`.

## Protocol handling

Live SGLang events remain three frames:

```text
[topic, sequence, payload]
```

The replay parser accepts both replay encodings during rollout:

```text
Upstream SGLang: [sequence, payload]
Topic-bearing form: [topic, sequence, payload]
```

For an upstream two-frame replay event, the subscriber supplies its configured
exact `topicFilter` as the topic before adding the event to the processing pool.
For the three-frame form, it preserves the topic carried by the message.

The parser also accepts the corresponding end markers:

```text
Upstream SGLang: [END_SEQ, empty]
Topic-bearing form: [empty, END_SEQ, empty]
```

Malformed frames and non-contiguous sequence numbers retain the existing
failure and index-invalidation behavior.

## Configuration and rollout

The existing `topicFilter` field remains both the ZMQ subscription prefix and
the replay fallback topic. Narrowing it to the complete topic intentionally
limits this EPP to the deployment's single model. No new configuration field is
introduced.

Deploy the compatible EPP before replacing the custom SGLang image. This order
keeps replay functional with either SGLang replay encoding throughout the
rollout.

## Verification

1. Unit-test both replay event encodings and both terminal encodings.
2. Exercise proactive replay with an upstream-format two-frame replay server
   and verify that the index is rebuilt under the configured model.
3. Run the existing KV-event subscriber tests to guard live event, sequence-gap,
   retry, and invalidation behavior.
4. Confirm the built image contains the intended router commit and preserves
   all changes already present on `lfeng/gate2c-load-aware-topk`.
5. After rollout, verify every prefill rank reports successful replay without
   malformed-frame errors, then compare EPP prefix scores with engine cache-hit
   observations.
