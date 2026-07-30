# SuperOrch managed daemon control boundary

This fork adds a hidden AO daemon mode for SuperOrch-managed launches.

What it does now:

- reads a versioned bootstrap envelope from inherited stdin before config loading, database work, or runtime/session startup
- keeps the 256-bit root secret memory-only and uses it only for the primary loopback daemon surface
- exposes truthful runtime attestation only after a successful managed bootstrap; that runtime-only overlay sets `managedMobileLANDisabled: true`
- requires exactly one semantically canonical `Authorization: Bearer <64-lowercase-hex>` header plus exactly one semantically canonical `X-AO-Daemon-Generation: <generation>` header on the primary loopback daemon surface, except literal public `GET`/`HEAD` on `/healthz` and `/readyz` with no encoded alias and no query string
- only after `ao daemon --superorch-managed` validates the inherited bootstrap, entirely disables Connect Mobile LAN construction, restoration, and control: AO does not construct `LANManager`, does not read or restore persisted enabled mobile state, reports mobile status as disabled, and rejects enable, disable, and regenerate without mutation

What it does not claim yet:

- browser bridge proxy authentication
- guardian or watchdog transport authentication
- restricted-worker or token-isolated child execution
- durable mutation journals or full session mutation generation fencing
- secret persistence, recovery journals, or restart-to-restart control credential rotation beyond the new daemon generation
- proof that every other listener owned by the AO process is loopback-only; `managedMobileLANDisabled` attests only that AO's Connect Mobile LAN listener cannot be constructed, restored, or controlled in this validated managed launch

Threat boundary:

- the managed credential is intended to stop unauthenticated loopback callers, stale supervisors, and accidental route exposure on the primary daemon listener
- the daemon generation detects that a caller is bound to an older daemon instance after restart
- Go's `net/http` stack strips RFC-allowed surrounding header OWS before handler code sees the parsed values; this boundary therefore enforces canonical bearer/generation semantics after that normalization, while still rejecting wrong casing, duplicate headers, and internal/doubled whitespace
- this change does not treat local code execution on the same user account as solved; stronger worker isolation remains future work
