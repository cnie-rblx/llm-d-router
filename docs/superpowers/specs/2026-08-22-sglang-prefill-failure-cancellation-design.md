# SGLang Prefill Failure Cancellation Design

## Goal

Prevent a failed SGLang prefill HTTP leg from leaving its concurrent decode leg
waiting indefinitely for a bootstrap-room mapping or KV transfer that will never
arrive.

## Design

Prefill and decode remain concurrent. Decode writes through the existing
`deferredCommitWriter`, which withholds its response until the prefill HTTP leg
finishes. A successful prefill commits the decode response and preserves normal
streaming; a non-2xx or transport failure cancels decode, discards its buffered
response, and returns the prefill failure to the client.

The sidecar records `prefillTarget`, `prefillStatusCode`,
`prefillTransportError`, `bootstrapRoom`, and `requestID` in its structured P/D
timing log. It does not log request contents.

## Verification

Unit tests cover an upstream HTTP 500 and a transport failure. Live validation
uses the existing controlled valid-bootstrap/failed-prefill probe, followed by
the same 15-minute m3 TrafficReplay used for Gate 2h.
