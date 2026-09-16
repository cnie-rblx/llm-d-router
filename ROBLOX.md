# Roblox changes

This branch is based on upstream `release-0.10` at `v0.10.0`. It carries the following Roblox-specific changes.

## Internal data-parallel routing

Adds an `internal-lb` sidecar mode that routes requests to a selected data-parallel rank while exposing rank-local metrics to the EPP. The existing external load-balancer mode remains available.

## SGLang tokenizer and KV events

Adds SGLang HTTP tokenization support to the EPP and decodes SGLang bigram KV-event keys. This keeps prefix-cache signals consistent with the tokens used by SGLang workers.

## Rank-aware disaggregated routing

Preserves the selected decode data-parallel rank when routing through the sidecar's primary listener. It also passes the selected prefill rank to SGLang so prefill and decode use the intended virtual endpoints.

## Cross-replica in-flight load

Adds a Kubernetes ConfigMap-backed syncer for sharing in-flight request state across EPP replicas. Scheduling reads cached peer snapshots and does not put Kubernetes API calls on the request path.

## Reliability fixes

Retains valid metrics parsed before a malformed metric family and cancels decode work when prefill fails. These changes avoid discarding usable load signals and prevent orphaned decode requests.
