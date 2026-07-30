package daemonmeta

import (
	"strings"
	"testing"
)

func TestCurrentAttestationPinsEveryCompatibilityBoundary(t *testing.T) {
	att := Current()
	if att.ContractVersion != 1 || att.Distribution != "superorch-ao" {
		t.Fatalf("identity = contract %d distribution %q", att.ContractVersion, att.Distribution)
	}
	if att.Upstream.Repository != "https://github.com/Untrivial-ai/agent-orchestrator" ||
		att.Upstream.Commit != "9f26112a0194d8ed86722ebb97115cbc678e4f38" {
		t.Fatalf("upstream = %+v", att.Upstream)
	}
	if att.Protocols.RESTAPI != 1 || att.Protocols.SSEEnvelope != 1 || att.Protocols.TerminalMux != 1 ||
		att.Protocols.BrowserBridge != 2 || att.Protocols.SessionControl != 1 || att.Protocols.PRControl != 1 ||
		att.Protocols.OrchestratorControl != 1 || att.Protocols.DurableEventReplay != 0 ||
		att.Protocols.DurableMutationJournal != 0 || att.Protocols.GenerationFencing != 0 ||
		att.Protocols.AuthenticatedIPC != 0 || att.Protocols.DatabaseSchema != 38 {
		t.Fatalf("protocols = %+v", att.Protocols)
	}
	for _, capability := range []string{
		"browserBridge", "browserControl", "buildAttestation", "desktopAttachCompatibility", "healthAttestation", "orchestrators", "prClaim",
		"prPreview", "restApi", "reviews", "runfileAttestation", "sessionCleanup", "sessionLifecycle",
		"sessionMergePolicy", "sessionResume", "sessionRollback", "sessionSend", "sqlite", "sseEvents",
		"terminalControl", "terminalMux",
	} {
		if !att.Capabilities[capability] {
			t.Errorf("capability %q is not enabled", capability)
		}
	}
	for _, capability := range []string{
		"authenticatedGuardian", "authenticatedIpc", "authenticatedWatchdog", "durableEventReplay",
		"durableMutationJournal", "generationFencing", "globalSupervisor", "omp", "prMerge",
		"restrictedWorkerIsolation", "sessionInterrupt",
	} {
		if att.Capabilities[capability] {
			t.Errorf("capability %q must remain disabled until implemented", capability)
		}
	}
	att.Capabilities["restApi"] = false
	if !Current().Capabilities["restApi"] {
		t.Fatal("Current returned a shared mutable capability map")
	}
}

func TestValidateBuildIdentity(t *testing.T) {
	originalVersion, originalCommit, originalMode := BuildVersion, ForkCommit, BuildMode
	t.Cleanup(func() { BuildVersion, ForkCommit, BuildMode = originalVersion, originalCommit, originalMode })

	BuildVersion, ForkCommit, BuildMode = "dev", "unknown", "development"
	if err := ValidateBuildIdentity(); err != nil {
		t.Fatalf("development sentinel identity rejected: %v", err)
	}

	BuildVersion, ForkCommit, BuildMode = "0.10.3-superorch.1", strings.Repeat("a", 40), "release"
	if err := ValidateBuildIdentity(); err != nil {
		t.Fatalf("release identity rejected: %v", err)
	}

	for _, tc := range []struct {
		name, version, commit, mode string
	}{
		{name: "release dev version", version: "dev", commit: strings.Repeat("a", 40), mode: "release"},
		{name: "release unsafe version", version: "0.10.3 release", commit: strings.Repeat("a", 40), mode: "release"},
		{name: "short release commit", version: "0.10.3", commit: "abc1234", mode: "release"},
		{name: "uppercase release commit", version: "0.10.3", commit: strings.Repeat("A", 40), mode: "release"},
		{name: "invalid development commit", version: "dev", commit: "abc1234", mode: "development"},
		{name: "unknown mode", version: "0.10.3", commit: strings.Repeat("a", 40), mode: "production"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			BuildVersion, ForkCommit, BuildMode = tc.version, tc.commit, tc.mode
			if err := ValidateBuildIdentity(); err == nil {
				t.Fatal("ValidateBuildIdentity succeeded, want error")
			}
		})
	}
}
