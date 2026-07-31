// @vitest-environment node
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
	buildLdflags,
	EXPECTED_ATTESTATION,
	resolveBuildMode,
	validateBuildInputs,
	validateBuiltAttestation,
} from "./build-attestation.mjs";
import { preflightDaemonLaunch } from "../src/main/daemon-preflight.ts";

const frontendRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

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
	it("keeps ordinary local daemon builds in development mode", () => {
		expect(resolveBuildMode({ env: {}, args: [] })).toBe("development");
		expect(resolveBuildMode({ env: { AO_BUILD_MODE: "development" }, args: [] })).toBe("development");
		expect(resolveBuildMode({ env: { CI: "1" }, args: [] })).toBe("release");
	});

	it("forces release mode for an explicit packaged build", () => {
		expect(resolveBuildMode({ env: {}, args: ["--release"] })).toBe("release");
		expect(resolveBuildMode({ env: { AO_BUILD_MODE: "development" }, args: ["--release"] })).toBe("release");
	});

	it("routes every local packaging lifecycle through the release daemon build", () => {
		const packageJson = JSON.parse(readFileSync(join(frontendRoot, "package.json"), "utf8"));
		expect(packageJson.scripts["build:daemon:release"]).toBe("node ./scripts/build-daemon.mjs --release");
		expect(packageJson.scripts.prepackage).toBe("npm run build:daemon:release");
		expect(packageJson.scripts.premake).toBe("npm run build:daemon:release");
		expect(packageJson.scripts.publish).toContain("npm run build:daemon:release");
		expect(packageJson.scripts.predev).toBe("npm run build:daemon");
	});

	it("produces a local packaged build mode accepted by the bundled startup gate", async () => {
		const packagedBuild = { ...build, mode: resolveBuildMode({ env: {}, args: ["--release"] }) };
		const candidate = attestation({ build: packagedBuild });
		const launch = {
			command: "/app/resources/daemon/ao",
			args: ["daemon"],
			cwd: "/home/user/.ao",
			shell: false,
			source: "bundled",
		};
		const run = async () => ({ exitCode: 0, stdout: JSON.stringify(candidate), stderr: "" });
		const readManifest = async () => JSON.stringify(candidate);

		expect(await preflightDaemonLaunch(launch, run, readManifest)).toBeNull();
	});

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
						prResolveComments: true,
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
