# SuperOrch AO build attestation

The SuperOrch AO fork publishes one compatibility contract from every daemon
surface used during discovery. Consumers must validate this contract before
calling a mutating REST endpoint, opening a terminal or browser channel, or
starting a replacement daemon.

The official upstream baseline is pinned to commit
`9f26112a0194d8ed86722ebb97115cbc678e4f38` from
`https://github.com/Untrivial-ai/agent-orchestrator`. A fork build additionally
identifies its own exact version, full lowercase commit SHA, and build mode.
This is self-reported compatibility identity. It is not authenticated source
provenance, an artifact signature, a transparency-log claim, or SBOM
verification; release distribution must provide those guarantees separately.

## Contract

```json
{
	"contractVersion": 1,
	"distribution": "superorch-ao",
	"upstream": {
		"repository": "https://github.com/Untrivial-ai/agent-orchestrator",
		"commit": "9f26112a0194d8ed86722ebb97115cbc678e4f38"
	},
	"build": {
		"version": "0.10.3",
		"commit": "<full lowercase fork commit>",
		"mode": "release"
	},
	"protocols": {
		"restApi": 1,
		"sseEnvelope": 1,
		"terminalMux": 1,
		"browserBridge": 2,
		"sessionControl": 1,
		"prControl": 1,
		"orchestratorControl": 1,
		"durableEventReplay": 0,
		"durableMutationJournal": 0,
		"generationFencing": 0,
		"authenticatedIpc": 0,
		"databaseSchema": 38
	},
	"capabilities": {
		"authenticatedGuardian": false,
		"authenticatedIpc": false,
		"authenticatedWatchdog": false,
		"browserBridge": true,
		"browserControl": true,
		"buildAttestation": true,
		"desktopAttachCompatibility": true,
		"durableEventReplay": false,
		"durableMutationJournal": false,
		"generationFencing": false,
		"globalSupervisor": false,
		"healthAttestation": true,
		"omp": false,
		"orchestrators": true,
		"prClaim": true,
		"prMerge": false,
		"prPreview": true,
		"prResolveComments": false,
		"restApi": true,
		"reviews": true,
		"restrictedWorkerIsolation": false,
		"runfileAttestation": true,
		"sessionCleanup": true,
		"sessionInterrupt": false,
		"sessionLifecycle": true,
		"sessionMergePolicy": true,
		"sessionResume": true,
		"sessionRollback": true,
		"sessionSend": true,
		"sqlite": true,
		"sseEvents": true,
		"terminalControl": true,
		"terminalMux": true
	}
}
```

Each protocol or schema version is independent. A change to one boundary must
increment that boundary's value; changing only the product version is not a
compatibility signal. New optional capabilities may be added without changing
the attestation contract version. A consumer must reject a missing or false
required capability.

The functional flags describe controls that execute in the pinned fork, not
route declarations or an aspirational API. `sessionInterrupt` is false because
AO exposes termination, resume, rollback, send, and raw terminal input but no
dedicated session interrupt operation. `prMerge` is false because the pinned
controller is a `501 Not Implemented` placeholder; the presence of its route is
not a capability. `prResolveComments` is false for the same reason: production
daemon wiring omits the PR action service, so the resolve-comments route is also
a `501 Not Implemented` placeholder. `reviews` is narrowly the implemented
review observation and review-run workflow; it does not imply comment
resolution. `prPreview` denotes AO's managed session preview controls and
`prClaim` denotes its native PR ownership endpoint.

The hardened SuperOrch guarantees are also explicitly false: authenticated
IPC, authenticated guardian/watchdog control, durable mutation journaling,
generation fencing, a global supervisor, OMP execution, restricted worker
isolation, and durable replay with instance/epoch/high-water semantics. Their
protocol versions are `0` where applicable. AO's SQLite change log and SSE
cursor can replay rows, but this attestation does not upgrade that native
behavior into the stronger cross-restart SuperOrch replay contract. Consumers
that require any false flag must fail closed instead of inferring it from a
neighboring route, table, or transport.

## Surfaces

- `ao version --json` prints the contract without starting the daemon.
- `running.json` includes it under `attestation`.
- `/healthz` and `/readyz` include it under `attestation`.
- SSE responses advertise `X-AO-SSE-Envelope-Version`.
- The `/mux` WebSocket upgrade advertises `X-AO-Terminal-Mux-Version`.
- `frontend/daemon/ao.attestation.json` is emitted beside a bundled daemon and
  is validated against the binary immediately after compilation.

The runfile attach path validates the runfile before API use, then validates
health and readiness and requires all three surfaces to identify the same fork
build and PID. When no usable runfile exists, the direct-port attach path uses
health and readiness only and requires those two live surfaces to agree. A
legacy upstream daemon or incompatible fork is an explicit
`compatibility_mismatch`; it is never treated as permission to spawn over or
replace that process.

On a fresh desktop spawn, stdout's listen line and a new runfile are discovery
signals only. The app does not report `ready` until compatible health and
readiness probes agree; runfile discovery additionally requires the runfile PID
and build to agree with both probes. Bundled, development, and configured
candidates are also inspected with the non-mutating `version --json` command
before the daemon can create or migrate its storage. A configured command that
cannot support that probe is refused before spawn. For bundled releases, the
app additionally parses `ao.attestation.json` beside the executable and
requires the sidecar and live binary output to match exactly before spawn. Both
remain self-reported compatibility records, not authenticated provenance.

### Configured daemon argv

`AO_DAEMON_ARGV` is the preferred configured-launch contract. Its value is a
compact JSON array of non-empty strings containing a direct AO executable,
followed immediately by one literal `daemon` element and the daemon arguments:

```json
["C:\\Program Files\\AO\\ao.exe", "daemon", "--port", "4317"]
```

The executable basename must be `ao` (`ao` or `ao.exe` on Windows). The desktop
parses the array once into one executable/argv object. The actual launch uses
that exact executable with arguments beginning `daemon`; preflight uses the
same executable with exactly `version --json`. Both processes use
`shell:false`; JSON daemon-argument values such as spaces, `$()`, `%PATH%`,
`!PATH!`, or `*` are therefore literal argv and are never expanded. Empty or
non-string elements, controls including NUL, a missing or duplicate `daemon`,
any pre-subcommand argument, and any wrapper or multiplexer executable are
rejected before execution.

Configured executable identity and its emitted attestation remain
self-reported. Requiring the direct AO argv shape closes preflight/launch
branching through wrappers; it does not cryptographically establish that a
user-supplied file named `ao` is an official artifact. Bundled release
consumption still requires the exact binary and matching sidecar, with external
release signing/provenance responsible for artifact authenticity.

`AO_DAEMON_COMMAND` remains only as a fail-closed migration path. It is no
longer executed as a shell command. Its deliberately narrow compatibility
grammar accepts ASCII-space-separated argv, double-quoted paths on every
platform, single-quoted paths only on POSIX, and POSIX backslash escaping. It
rejects tabs/newlines and other controls, shell interpreters, expansions,
substitutions, pipes/redirections, globs, metacharacters, unmatched or
concatenated quotes, a non-AO executable, any argument before `daemon`, and a
missing or duplicate literal `daemon`. Existing values that relied on shell
behavior, `env`, `go run`, or another prefix wrapper are intentionally
incompatible. Move environment settings into the desktop environment and
supported AO daemon flags, then use the direct JSON argv form. Unsupported
wrapper behavior is rejected before preflight or spawn.

Daemon startup validates only attestation/build self-consistency. It does not
require future integration capabilities to be true. The consuming adapter
chooses a required-capability set for its operation (for example, observe-only
versus mutating control) and rejects any missing or false member before that
operation.

## Build rules

Release builds must use an explicit linker-safe version, a full lowercase fork
commit, and mode `release`. `dev`, `development`, `unknown`, a short SHA, or a
missing stamp makes daemon startup fail before config loading or database
migration. Direct `go build` remains available for local work and is
unambiguously attested as version `dev`, commit `unknown`, mode `development`.

When Git metadata exists, every build script derives the commit from `HEAD`,
requires any `AO_FORK_COMMIT` override to equal that exact checkout, and refuses
a release while any tracked change or unignored untracked checkout content is
dirty. This global gate avoids silently omitting root build inputs from an allowlist. A source
archive has no checkout to compare, so it requires both a full `AO_FORK_COMMIT` and
`AO_ARCHIVE_PROVENANCE_VERIFIED=1`; that flag is the caller's assertion that it
verified the archive commit through external release provenance, not proof
created by this attestation.

The Electron daemon build uses `-trimpath -buildvcs=false` and deterministic
linker values, executes `ao version --json`, validates the result, and only then
retains the binary. The npm platform-binary release script applies the same
stamps to every cross-compiled target. `npm run build:daemon` remains a
development build for ordinary local work. The `prepackage` and `premake`
lifecycle hooks and the publish script use `npm run build:daemon:release`, so
local `npm run package`, `npm run make`, and `npm run publish` cannot silently
bundle a development-attested daemon.
