export const EXPECTED_DAEMON_ATTESTATION = Object.freeze({
	contractVersion: 1,
	distribution: "superorch-ao",
	upstreamRepository: "https://github.com/Untrivial-ai/agent-orchestrator",
	upstreamCommit: "9f26112a0194d8ed86722ebb97115cbc678e4f38",
	protocols: Object.freeze({
		restApi: 1,
		sseEnvelope: 1,
		terminalMux: 1,
		browserBridge: 2,
		sessionControl: 1,
		prControl: 1,
		orchestratorControl: 1,
		durableEventReplay: 0,
		durableMutationJournal: 0,
		generationFencing: 0,
		authenticatedIpc: 0,
		databaseSchema: 38,
	}),
	declaredCapabilities: Object.freeze({
		authenticatedGuardian: false,
		authenticatedIpc: false,
		authenticatedWatchdog: false,
		browserBridge: true,
		browserControl: true,
		buildAttestation: true,
		desktopAttachCompatibility: true,
		durableEventReplay: false,
		durableMutationJournal: false,
		generationFencing: false,
		globalSupervisor: false,
		healthAttestation: true,
		omp: false,
		orchestrators: true,
		prClaim: true,
		prMerge: false,
		prPreview: true,
		restApi: true,
		reviews: true,
		restrictedWorkerIsolation: false,
		runfileAttestation: true,
		sessionCleanup: true,
		sessionInterrupt: false,
		sessionLifecycle: true,
		sessionMergePolicy: true,
		sessionResume: true,
		sessionRollback: true,
		sessionSend: true,
		sqlite: true,
		sseEvents: true,
		terminalControl: true,
		terminalMux: true,
	}),
	requiredCapabilities: Object.freeze([
		"browserBridge",
		"browserControl",
		"buildAttestation",
		"desktopAttachCompatibility",
		"healthAttestation",
		"orchestrators",
		"prClaim",
		"prPreview",
		"restApi",
		"reviews",
		"runfileAttestation",
		"sessionCleanup",
		"sessionLifecycle",
		"sessionMergePolicy",
		"sessionResume",
		"sessionRollback",
		"sessionSend",
		"sqlite",
		"sseEvents",
		"terminalControl",
		"terminalMux",
	]),
});

export type DaemonAttestation = {
	contractVersion: number;
	distribution: string;
	upstream: { repository: string; commit: string };
	build: { version: string; commit: string; mode: string };
	protocols: {
		restApi: number;
		sseEnvelope: number;
		terminalMux: number;
		browserBridge: number;
		sessionControl: number;
		prControl: number;
		orchestratorControl: number;
		durableEventReplay: number;
		durableMutationJournal: number;
		generationFencing: number;
		authenticatedIpc: number;
		databaseSchema: number;
	};
	capabilities: Record<string, boolean>;
};

const FULL_SHA = /^[0-9a-f]{40}$/;

export function parseDaemonAttestation(value: unknown): DaemonAttestation | null {
	if (typeof value !== "object" || value === null) return null;
	const candidate = value as Partial<DaemonAttestation>;
	if (typeof candidate.contractVersion !== "number" || !Number.isInteger(candidate.contractVersion)) return null;
	if (typeof candidate.distribution !== "string") return null;
	if (typeof candidate.upstream !== "object" || candidate.upstream === null) return null;
	if (typeof candidate.upstream.repository !== "string" || typeof candidate.upstream.commit !== "string") return null;
	if (typeof candidate.build !== "object" || candidate.build === null) return null;
	if (
		typeof candidate.build.version !== "string" ||
		typeof candidate.build.commit !== "string" ||
		typeof candidate.build.mode !== "string"
	)
		return null;
	if (typeof candidate.protocols !== "object" || candidate.protocols === null) return null;
	for (const name of Object.keys(EXPECTED_DAEMON_ATTESTATION.protocols) as (keyof DaemonAttestation["protocols"])[]) {
		if (typeof candidate.protocols[name] !== "number" || !Number.isInteger(candidate.protocols[name])) return null;
	}
	if (typeof candidate.capabilities !== "object" || candidate.capabilities === null) return null;
	const capabilities: Record<string, boolean> = {};
	for (const [name, enabled] of Object.entries(candidate.capabilities)) {
		if (typeof enabled !== "boolean") return null;
		capabilities[name] = enabled;
	}
	return {
		contractVersion: candidate.contractVersion,
		distribution: candidate.distribution,
		upstream: {
			repository: candidate.upstream.repository,
			commit: candidate.upstream.commit,
		},
		build: {
			version: candidate.build.version,
			commit: candidate.build.commit,
			mode: candidate.build.mode,
		},
		protocols: {
			restApi: candidate.protocols.restApi,
			sseEnvelope: candidate.protocols.sseEnvelope,
			terminalMux: candidate.protocols.terminalMux,
			browserBridge: candidate.protocols.browserBridge,
			sessionControl: candidate.protocols.sessionControl,
			prControl: candidate.protocols.prControl,
			orchestratorControl: candidate.protocols.orchestratorControl,
			durableEventReplay: candidate.protocols.durableEventReplay,
			durableMutationJournal: candidate.protocols.durableMutationJournal,
			generationFencing: candidate.protocols.generationFencing,
			authenticatedIpc: candidate.protocols.authenticatedIpc,
			databaseSchema: candidate.protocols.databaseSchema,
		},
		capabilities,
	};
}

export function daemonCompatibilityError(
	attestation: DaemonAttestation | undefined,
	requiredCapabilities: readonly string[] = EXPECTED_DAEMON_ATTESTATION.requiredCapabilities,
): string | null {
	if (!attestation) return "The running AO daemon does not expose the required SuperOrch compatibility attestation.";
	if (attestation.contractVersion !== EXPECTED_DAEMON_ATTESTATION.contractVersion) {
		return `The running AO daemon uses attestation contract ${attestation.contractVersion}; this app requires ${EXPECTED_DAEMON_ATTESTATION.contractVersion}.`;
	}
	if (attestation.distribution !== EXPECTED_DAEMON_ATTESTATION.distribution) {
		return `The running AO daemon is distribution ${attestation.distribution}; this app requires ${EXPECTED_DAEMON_ATTESTATION.distribution}.`;
	}
	if (
		attestation.upstream.repository !== EXPECTED_DAEMON_ATTESTATION.upstreamRepository ||
		attestation.upstream.commit !== EXPECTED_DAEMON_ATTESTATION.upstreamCommit
	) {
		return "The running AO daemon was built from an incompatible upstream baseline.";
	}
	if (attestation.build.mode !== "development" && attestation.build.mode !== "release") {
		return `The running AO daemon reports unsupported build mode ${attestation.build.mode}.`;
	}
	const version = attestation.build.version.trim().toLowerCase();
	if (!version) return "The running AO daemon does not report a build version.";
	if (attestation.build.mode === "release" && ["dev", "development", "unknown"].includes(version)) {
		return "The running AO release has an ambiguous build version.";
	}
	if (
		!FULL_SHA.test(attestation.build.commit) &&
		!(attestation.build.mode === "development" && attestation.build.commit === "unknown")
	) {
		return "The running AO daemon does not report an exact fork build commit.";
	}
	for (const [name, expected] of Object.entries(EXPECTED_DAEMON_ATTESTATION.protocols) as [
		keyof DaemonAttestation["protocols"],
		number,
	][]) {
		if (attestation.protocols[name] !== expected) {
			return `The running AO daemon has incompatible ${name} version ${attestation.protocols[name]}; this app requires ${expected}.`;
		}
	}
	for (const capability of requiredCapabilities) {
		if (attestation.capabilities[capability] !== true) {
			return `The running AO daemon is missing required capability ${capability}.`;
		}
	}
	return null;
}

export function sameDaemonBuild(left: DaemonAttestation | undefined, right: DaemonAttestation | undefined): boolean {
	return (
		left !== undefined &&
		right !== undefined &&
		left.build.version === right.build.version &&
		left.build.commit === right.build.commit &&
		left.build.mode === right.build.mode
	);
}
