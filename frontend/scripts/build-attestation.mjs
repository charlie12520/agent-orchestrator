const BUILD_PACKAGE = "github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta";

export const EXPECTED_ATTESTATION = Object.freeze({
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
		databaseSchema: 40,
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
		prResolveComments: false,
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

const FULL_SHA = /^[0-9a-f]{40}$/;
const SAFE_VERSION = /^[0-9A-Za-z][0-9A-Za-z._+-]*$/;

export function resolveBuildMode({ env = process.env, args = process.argv.slice(2) } = {}) {
	if (args.includes("--release")) return "release";
	return env.AO_BUILD_MODE ?? (env.CI ? "release" : "development");
}

export function validateBuildInputs({ version, commit, mode }) {
	if (!SAFE_VERSION.test(version) || ["dev", "development", "unknown"].includes(version.toLowerCase())) {
		throw new Error(`AO build version must be explicit and linker-safe, got ${JSON.stringify(version)}`);
	}
	if (!FULL_SHA.test(commit)) {
		throw new Error(`AO fork commit must be a lowercase full 40-character SHA, got ${JSON.stringify(commit)}`);
	}
	if (mode !== "development" && mode !== "release") {
		throw new Error(`AO build mode must be development or release, got ${JSON.stringify(mode)}`);
	}
}

export function buildLdflags(metadata) {
	validateBuildInputs(metadata);
	return [
		`-X ${BUILD_PACKAGE}.BuildVersion=${metadata.version}`,
		`-X ${BUILD_PACKAGE}.ForkCommit=${metadata.commit}`,
		`-X ${BUILD_PACKAGE}.BuildMode=${metadata.mode}`,
	].join(" ");
}

export function validateBuiltAttestation(attestation, expectedBuild) {
	validateBuildInputs(expectedBuild);
	if (typeof attestation !== "object" || attestation === null)
		throw new Error("AO binary emitted no attestation object");
	if (attestation.contractVersion !== EXPECTED_ATTESTATION.contractVersion) {
		throw new Error(`unexpected attestation contract version ${JSON.stringify(attestation.contractVersion)}`);
	}
	if (attestation.distribution !== EXPECTED_ATTESTATION.distribution) {
		throw new Error(`unexpected AO distribution ${JSON.stringify(attestation.distribution)}`);
	}
	if (attestation.upstream?.repository !== EXPECTED_ATTESTATION.upstreamRepository) {
		throw new Error(`unexpected AO upstream repository ${JSON.stringify(attestation.upstream?.repository)}`);
	}
	if (attestation.upstream?.commit !== EXPECTED_ATTESTATION.upstreamCommit) {
		throw new Error(`unexpected AO upstream commit ${JSON.stringify(attestation.upstream?.commit)}`);
	}
	for (const [name, version] of Object.entries(EXPECTED_ATTESTATION.protocols)) {
		if (attestation.protocols?.[name] !== version) {
			throw new Error(`unexpected ${name} protocol/schema version ${JSON.stringify(attestation.protocols?.[name])}`);
		}
	}
	for (const [capability, enabled] of Object.entries(EXPECTED_ATTESTATION.declaredCapabilities)) {
		if (attestation.capabilities?.[capability] !== enabled) {
			throw new Error(
				`AO binary capability ${capability} is ${JSON.stringify(attestation.capabilities?.[capability])}, expected ${enabled}`,
			);
		}
	}
	for (const field of ["version", "commit", "mode"]) {
		if (attestation.build?.[field] !== expectedBuild[field]) {
			throw new Error(
				`AO binary build ${field} ${JSON.stringify(attestation.build?.[field])} does not match ${JSON.stringify(expectedBuild[field])}`,
			);
		}
	}
	return attestation;
}
