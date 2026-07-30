package daemonmeta

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// AttestationContractVersion versions the shape of Attestation itself.
	AttestationContractVersion = 1
	// Distribution identifies the SuperOrch-maintained AO fork. It deliberately
	// differs from ServiceName: ServiceName identifies the daemon process while
	// Distribution identifies the compatibility and release line.
	Distribution = "superorch-ao"

	// UpstreamRepository and UpstreamCommit pin the official AO source baseline
	// this fork was created from. Fork builds additionally carry their own exact
	// commit in Attestation.Build.Commit.
	UpstreamRepository = "https://github.com/Untrivial-ai/agent-orchestrator"
	UpstreamCommit     = "9f26112a0194d8ed86722ebb97115cbc678e4f38"

	// Independently version every contract SuperOrch consumes. A protocol change
	// must increment its own value instead of relying on the product version.
	RESTAPIVersion                 = 1
	SSEEnvelopeVersion             = 1
	TerminalMuxProtocolVersion     = 1
	BrowserBridgeProtocolVersion   = 2
	SessionControlVersion          = 1
	PRControlVersion               = 1
	OrchestratorControlVersion     = 1
	DurableEventReplayVersion      = 0
	DurableMutationJournalVersion  = 0
	GenerationFencingVersion       = 0
	DaemonControlGenerationVersion = 0
	AuthenticatedIPCVersion        = 0
	DatabaseSchemaVersion          = 40
)

// Build identity is overridden by the deterministic daemon build script using
// linker -X flags. Direct `go build` remains a supported development path and
// is intentionally distinguishable from a distributable release.
var (
	BuildVersion = "dev"
	ForkCommit   = "unknown"
	BuildMode    = "development"
)

// SourceIdentity identifies the exact official AO baseline.
type SourceIdentity struct {
	Repository string `json:"repository"`
	Commit     string `json:"commit"`
}

// BuildIdentity identifies the exact fork build being executed.
type BuildIdentity struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Mode    string `json:"mode"`
}

// ProtocolVersions fences independently evolving AO wire/storage contracts.
type ProtocolVersions struct {
	RESTAPI                 int `json:"restApi"`
	SSEEnvelope             int `json:"sseEnvelope"`
	TerminalMux             int `json:"terminalMux"`
	BrowserBridge           int `json:"browserBridge"`
	SessionControl          int `json:"sessionControl"`
	PRControl               int `json:"prControl"`
	OrchestratorControl     int `json:"orchestratorControl"`
	DurableEventReplay      int `json:"durableEventReplay"`
	DurableMutationJournal  int `json:"durableMutationJournal"`
	GenerationFencing       int `json:"generationFencing"`
	DaemonControlGeneration int `json:"daemonControlGeneration"`
	AuthenticatedIPC        int `json:"authenticatedIpc"`
	DatabaseSchema          int `json:"databaseSchema"`
}

// Attestation is the machine-readable compatibility contract emitted by the
// binary, daemon probes, and runfile. Capabilities are explicit boolean flags so
// consumers can reject a binary before invoking a missing surface.
type Attestation struct {
	ContractVersion int              `json:"contractVersion"`
	Distribution    string           `json:"distribution"`
	Upstream        SourceIdentity   `json:"upstream"`
	Build           BuildIdentity    `json:"build"`
	Protocols       ProtocolVersions `json:"protocols"`
	Capabilities    map[string]bool  `json:"capabilities"`
}

// Current returns a fresh immutable-by-convention snapshot. A new capability
// map is allocated on every call so callers cannot mutate later responses.
func Current() Attestation {
	return Attestation{
		ContractVersion: AttestationContractVersion,
		Distribution:    Distribution,
		Upstream: SourceIdentity{
			Repository: UpstreamRepository,
			Commit:     UpstreamCommit,
		},
		Build: BuildIdentity{
			Version: strings.TrimSpace(BuildVersion),
			Commit:  strings.TrimSpace(ForkCommit),
			Mode:    strings.TrimSpace(BuildMode),
		},
		Protocols: ProtocolVersions{
			RESTAPI:                 RESTAPIVersion,
			SSEEnvelope:             SSEEnvelopeVersion,
			TerminalMux:             TerminalMuxProtocolVersion,
			BrowserBridge:           BrowserBridgeProtocolVersion,
			SessionControl:          SessionControlVersion,
			PRControl:               PRControlVersion,
			OrchestratorControl:     OrchestratorControlVersion,
			DurableEventReplay:      DurableEventReplayVersion,
			DurableMutationJournal:  DurableMutationJournalVersion,
			GenerationFencing:       GenerationFencingVersion,
			DaemonControlGeneration: DaemonControlGenerationVersion,
			AuthenticatedIPC:        AuthenticatedIPCVersion,
			DatabaseSchema:          DatabaseSchemaVersion,
		},
		Capabilities: map[string]bool{
			"authenticatedGuardian":      false,
			"authenticatedIpc":           false,
			"authenticatedWatchdog":      false,
			"browserBridge":              true,
			"browserControl":             true,
			"buildAttestation":           true,
			"daemonControlGeneration":    false,
			"desktopAttachCompatibility": true,
			"durableEventReplay":         false,
			"durableMutationJournal":     false,
			"generationFencing":          false,
			"globalSupervisor":           false,
			"healthAttestation":          true,
			"managedMobileLANDisabled":   false,
			"omp":                        false,
			"orchestrators":              true,
			"prClaim":                    true,
			"prMerge":                    false,
			"prPreview":                  true,
			"prResolveComments":          false,
			"restApi":                    true,
			"reviews":                    true,
			"restrictedWorkerIsolation":  false,
			"runfileAttestation":         true,
			"sessionCleanup":             true,
			"sessionInterrupt":           false,
			"sessionLifecycle":           true,
			"sessionMergePolicy":         true,
			"sessionResume":              true,
			"sessionRollback":            true,
			"sessionSend":                true,
			"sqlite":                     true,
			"sseEvents":                  true,
			"terminalControl":            true,
			"terminalMux":                true,
		},
	}
}

// WithManagedControl overlays the runtime-only managed SuperOrch control
// boundary on an attestation snapshot. Static build attestation remains false
// for these capabilities; callers must opt into the overlay only after a valid
// bootstrap has been read.
func WithManagedControl(att Attestation) Attestation {
	clone := att
	clone.Protocols.AuthenticatedIPC = 1
	clone.Protocols.DaemonControlGeneration = 1
	clone.Capabilities = cloneCapabilities(att.Capabilities)
	clone.Capabilities["authenticatedIpc"] = true
	clone.Capabilities["daemonControlGeneration"] = true
	clone.Capabilities["managedMobileLANDisabled"] = true
	return clone
}

func cloneCapabilities(src map[string]bool) map[string]bool {
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

var (
	fullCommitPattern   = regexp.MustCompile(`^[0-9a-f]{40}$`)
	buildVersionPattern = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z._+-]*$`)
)

// ValidateBuildIdentity fails before config loading or database migration when
// a release binary was not stamped with an unambiguous version and full commit.
// Development builds may use the explicit dev/unknown sentinel pair.
func ValidateBuildIdentity() error {
	att := Current()
	switch att.Build.Mode {
	case "development":
		if att.Build.Commit != "unknown" && !fullCommitPattern.MatchString(att.Build.Commit) {
			return fmt.Errorf("invalid development fork commit %q", att.Build.Commit)
		}
		return nil
	case "release":
		version := strings.ToLower(att.Build.Version)
		if !buildVersionPattern.MatchString(att.Build.Version) ||
			version == "dev" || version == "development" || version == "unknown" {
			return fmt.Errorf("release build version is ambiguous: %q", att.Build.Version)
		}
		if !fullCommitPattern.MatchString(att.Build.Commit) {
			return fmt.Errorf("release fork commit must be a full 40-character SHA, got %q", att.Build.Commit)
		}
		return nil
	default:
		return fmt.Errorf("unsupported build mode %q", att.Build.Mode)
	}
}
