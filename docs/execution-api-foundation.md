# Managed execution HTTP foundation

The execution API is an additive, managed-daemon surface for SuperOrch. It is
mounted only when AO starts with a valid managed-control bootstrap and is
reachable only through AO's primary loopback listener. It is absent from
standalone AO and hidden with `404` on the Connect Mobile LAN listener. `Host`,
`X-Forwarded-For`, and `X-Real-IP` do not affect that listener provenance.

Every request requires the existing managed-control headers:

- `Authorization: Bearer <64 lowercase hex characters>`
- `X-AO-Daemon-Generation: <the exact current generation>`

The mutation endpoint also requires exactly one `Idempotency-Key` header. Its
value must exactly equal the request body's `idempotencyKey`; whitespace is not
normalized.

## Endpoints

### `POST /api/v1/execution/operations`

Accepts the A1 execution-journal request without decode/re-encode:

```json
{
  "version": 1,
  "externalRunId": "superorch-run-1",
  "operation": "launch",
  "idempotencyKey": "launch-1",
  "request": {}
}
```

The operation is one of `launch`, `send`, `interrupt`, `resume`, `restore`,
`stop`, or `cleanup`. Non-launch operations also require `runId` and
`expectedProcessGeneration`; launch forbids both because the backend assigns
them. Bodies are strict JSON objects bounded to 1 MiB. Unknown or duplicate
members, lossy Unicode, invalid identifiers, non-finite values, and unsafe
process generations are rejected before the execution backend is called.
Equivalent JSON number and object-order spellings are canonicalized to the same
request identity.

An exact retry returns the durable operation metadata with `replayed: true`.
Reusing the same operation identity for different canonical request bytes
returns a conflict. Responses never include the idempotency key, accepted
request bytes, opaque backend result JSON, request/result hashes, dispatch
owner, or storage error causes. Opaque results, including nested environment,
argument, prompt, token, and private fields, remain internal until a typed,
reviewed public result contract exists.

### `GET /api/v1/execution/operations/{operationId}`

Returns sanitized durable lifecycle plus safe top-level run, generation, and
state metadata for a 64-character lowercase-hex operation id. Private request
identity, opaque backend result JSON, and dispatch fencing data are omitted.

### `GET /api/v1/execution/bindings/{externalRunId}`

Returns sanitized run binding state. Launch idempotency identity and request
hashes are omitted.

## Deliberate production boundary

This foundation wires the durable journal as a read source only. Production
`POST` requests return `503 EXECUTION_UNAVAILABLE` before any journal or binding
mutation because no real harness/session-manager dispatcher has been accepted.
There is no fake production dispatcher and this change does not claim execution
cutover. Tests inject A1 with a deterministic fake solely to prove replay,
conflict, sanitization, and bounded-failure behavior.

The A1 exact-result refinement method is intentionally not exposed over HTTP.
Refinement needs a separately designed trusted authoritative identity and proof
contract; accepting an ordinary root-authenticated request would weaken the
dispatch/reconciliation fence.
