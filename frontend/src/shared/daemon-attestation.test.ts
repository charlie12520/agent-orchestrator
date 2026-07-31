import { describe, expect, it } from "vitest";
import {
	daemonCompatibilityError,
	EXPECTED_DAEMON_ATTESTATION,
	parseDaemonAttestation,
	sameDaemonBuild,
	type DaemonAttestation,
} from "./daemon-attestation";

function validAttestation(): DaemonAttestation {
	return {
		contractVersion: EXPECTED_DAEMON_ATTESTATION.contractVersion,
		distribution: EXPECTED_DAEMON_ATTESTATION.distribution,
		upstream: {
			repository: EXPECTED_DAEMON_ATTESTATION.upstreamRepository,
			commit: EXPECTED_DAEMON_ATTESTATION.upstreamCommit,
		},
		build: {
			version: "0.10.3-superorch.1",
			commit: "0123456789abcdef0123456789abcdef01234567",
			mode: "release",
		},
		protocols: { ...EXPECTED_DAEMON_ATTESTATION.protocols },
		capabilities: { ...EXPECTED_DAEMON_ATTESTATION.declaredCapabilities },
	};
}

describe("daemon compatibility attestation", () => {
	it("parses and accepts the exact supported contract", () => {
		const attestation = validAttestation();
		expect(parseDaemonAttestation(attestation)).toEqual(attestation);
		expect(daemonCompatibilityError(attestation)).toBeNull();
	});

	it("allows explicit direct-go-build development identity only", () => {
		const attestation = validAttestation();
		attestation.build = {
			version: "dev",
			commit: "unknown",
			mode: "development",
		};
		expect(daemonCompatibilityError(attestation)).toBeNull();
		attestation.build.mode = "release";
		expect(daemonCompatibilityError(attestation)).toContain("ambiguous");
	});

	it("fails closed when a consumer requires a declared but unavailable capability", () => {
		const attestation = validAttestation();
		for (const capability of [
			"authenticatedGuardian",
			"authenticatedIpc",
			"authenticatedWatchdog",
			"durableEventReplay",
			"durableMutationJournal",
			"generationFencing",
			"globalSupervisor",
			"omp",
			"prMerge",
			"prResolveComments",
			"restrictedWorkerIsolation",
			"sessionInterrupt",
		]) {
			expect(attestation.capabilities[capability], capability).toBe(false);
			expect(daemonCompatibilityError(attestation, [capability]), capability).toContain(capability);
		}
	});

	const driftCases: Array<[string, (attestation: DaemonAttestation) => void]> = [
		["contract", (a) => void (a.contractVersion += 1)],
		["distribution", (a) => void (a.distribution = "agent-orchestrator")],
		["upstream", (a) => void (a.upstream.commit = "f".repeat(40))],
		["REST", (a) => void (a.protocols.restApi += 1)],
		["SSE", (a) => void (a.protocols.sseEnvelope += 1)],
		["terminal", (a) => void (a.protocols.terminalMux += 1)],
		["browser", (a) => void (a.protocols.browserBridge += 1)],
		["database", (a) => void (a.protocols.databaseSchema += 1)],
		["capability", (a) => void (a.capabilities.restApi = false)],
	];

	it.each(driftCases)("rejects %s drift", (_name, mutate) => {
		const attestation = validAttestation();
		mutate(attestation);
		expect(daemonCompatibilityError(attestation)).not.toBeNull();
	});

	it("rejects drift in every independently versioned protocol and schema", () => {
		for (const name of Object.keys(EXPECTED_DAEMON_ATTESTATION.protocols) as (keyof DaemonAttestation["protocols"])[]) {
			const attestation = validAttestation();
			attestation.protocols[name] += 1;
			expect(daemonCompatibilityError(attestation), name).not.toBeNull();
		}
	});

	it("rejects malformed nested structures before compatibility evaluation", () => {
		expect(parseDaemonAttestation(null)).toBeNull();
		expect(
			parseDaemonAttestation({
				...validAttestation(),
				protocols: { restApi: "1" },
			}),
		).toBeNull();
		expect(
			parseDaemonAttestation({
				...validAttestation(),
				capabilities: { restApi: "yes" },
			}),
		).toBeNull();
	});

	it("matches runfile and probe identity by version, full commit, and mode", () => {
		const left = validAttestation();
		const right = validAttestation();
		expect(sameDaemonBuild(left, right)).toBe(true);
		right.build.commit = "f".repeat(40);
		expect(sameDaemonBuild(left, right)).toBe(false);
		expect(sameDaemonBuild(left, undefined)).toBe(false);
	});
});
