// @vitest-environment node
import { describe, expect, it } from "vitest";
import {
	buildLdflags,
	EXPECTED_ATTESTATION,
	validateBuildInputs,
	validateBuiltAttestation,
} from "./build-attestation.mjs";

const build = {
	version: "0.10.3-superorch.1",
	commit: "0123456789abcdef0123456789abcdef01234567",
	mode: "release",
};

function attestation(overrides = {}) {
	return {
		contractVersion: EXPECTED_ATTESTATION.contractVersion,
		distribution: EXPECTED_ATTESTATION.distribution,
		upstream: {
			repository: EXPECTED_ATTESTATION.upstreamRepository,
			commit: EXPECTED_ATTESTATION.upstreamCommit,
		},
		build,
		protocols: { ...EXPECTED_ATTESTATION.protocols },
		capabilities: { ...EXPECTED_ATTESTATION.declaredCapabilities },
		...overrides,
	};
}

describe("daemon build attestation", () => {
	it("builds deterministic linker flags without dates or local paths", () => {
		const flags = buildLdflags(build);
		expect(flags).toContain(`BuildVersion=${build.version}`);
		expect(flags).toContain(`ForkCommit=${build.commit}`);
		expect(flags).toContain("BuildMode=release");
		expect(flags).not.toMatch(/Date|20\d\d-|[A-Z]:\\|\/Users\//);
	});

	it.each([
		{ ...build, version: "dev" },
		{ ...build, version: "unknown" },
		{ ...build, commit: "0123456" },
		{ ...build, commit: build.commit.toUpperCase() },
		{ ...build, mode: "production" },
	])("rejects ambiguous build input %#", (candidate) => {
		expect(() => validateBuildInputs(candidate)).toThrow();
	});

	it("accepts an exact binary attestation", () => {
		expect(validateBuiltAttestation(attestation(), build)).toEqual(attestation());
	});

	it("rejects drift in each compatibility boundary", () => {
		expect(() => validateBuiltAttestation(attestation({ distribution: "agent-orchestrator" }), build)).toThrow();
		expect(() =>
			validateBuiltAttestation(
				attestation({
					upstream: {
						...attestation().upstream,
						repository: "https://example.test/not-ao",
					},
				}),
				build,
			),
		).toThrow();
		expect(() =>
			validateBuiltAttestation(
				attestation({
					capabilities: {
						...attestation().capabilities,
						prMerge: true,
					},
				}),
				build,
			),
		).toThrow();
		expect(() =>
			validateBuiltAttestation(
				attestation({
					capabilities: {
						...attestation().capabilities,
						authenticatedIpc: true,
					},
				}),
				build,
			),
		).toThrow();
		expect(() =>
			validateBuiltAttestation(
				attestation({
					protocols: { ...EXPECTED_ATTESTATION.protocols, terminalMux: 2 },
				}),
				build,
			),
		).toThrow();
		expect(() =>
			validateBuiltAttestation(
				attestation({
					capabilities: { ...attestation().capabilities, sseEvents: false },
				}),
				build,
			),
		).toThrow();
		expect(() =>
			validateBuiltAttestation(attestation({ build: { ...build, commit: "f".repeat(40) } }), build),
		).toThrow();
	});
});
