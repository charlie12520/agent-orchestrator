import { describe, expect, it, vi } from "vitest";
import { EXPECTED_DAEMON_ATTESTATION } from "../shared/daemon-attestation";
import type { DaemonLaunchSpec } from "../shared/daemon-launch";
import { daemonPreflightSpec, preflightDaemonLaunch, type DaemonPreflightRunner } from "./daemon-preflight";

const bundled: DaemonLaunchSpec = {
	command: "/app/resources/daemon/ao",
	args: ["daemon"],
	cwd: "/home/user/.ao",
	shell: false,
	source: "bundled",
};

function attestation() {
	return {
		contractVersion: EXPECTED_DAEMON_ATTESTATION.contractVersion,
		distribution: EXPECTED_DAEMON_ATTESTATION.distribution,
		upstream: {
			repository: EXPECTED_DAEMON_ATTESTATION.upstreamRepository,
			commit: EXPECTED_DAEMON_ATTESTATION.upstreamCommit,
		},
		build: { version: "0.10.3", commit: "0123456789abcdef0123456789abcdef01234567", mode: "release" },
		protocols: { ...EXPECTED_DAEMON_ATTESTATION.protocols },
		capabilities: { ...EXPECTED_DAEMON_ATTESTATION.declaredCapabilities },
	};
}

function runner(stdout: string, exitCode = 0): DaemonPreflightRunner {
	return vi.fn().mockResolvedValue({ stdout, stderr: "", exitCode });
}

const manifest = () => Promise.resolve(JSON.stringify(attestation()));

describe("daemon candidate preflight", () => {
	it("runs the bundled binary version probe outside the not-yet-created data directory", () => {
		expect(daemonPreflightSpec(bundled)).toEqual({
			command: bundled.command,
			args: ["version", "--json"],
			cwd: "/app/resources/daemon",
			shell: false,
		});
	});

	it("uses a non-mutating go-run version probe for development", () => {
		const dev = { ...bundled, source: "dev" as const, command: "go", cwd: "/checkout/backend" };
		expect(daemonPreflightSpec(dev)).toEqual({
			command: "go",
			args: ["run", "./cmd/ao", "version", "--json"],
			cwd: "/checkout/backend",
			shell: false,
		});
	});

	it("requires configured shell commands to pass version preflight before spawn", async () => {
		const configured = { ...bundled, source: "configured" as const, shell: true };
		const run = runner(JSON.stringify(attestation()));
		expect(daemonPreflightSpec(configured)).toMatchObject({ args: ["version", "--json"], shell: true });
		expect(await preflightDaemonLaunch(configured, run)).toBeNull();
		expect(run).toHaveBeenCalledOnce();
	});

	it("refuses a configured command when its pre-spawn version probe is incompatible", async () => {
		const configured = { ...bundled, source: "configured" as const, shell: true };
		const run = runner(JSON.stringify({ version: "legacy" }));
		expect(await preflightDaemonLaunch(configured, run)).toContain("compatibility attestation");
		expect(run).toHaveBeenCalledOnce();
	});

	it("accepts an exact compatible release candidate", async () => {
		expect(await preflightDaemonLaunch(bundled, runner(JSON.stringify(attestation())), manifest)).toBeNull();
	});

	it.each([
		["missing", ""],
		["malformed", "{not json"],
		["legacy", JSON.stringify({ version: "0.10.3" })],
	])("rejects %s candidate attestation before spawn", async (_name, output) => {
		expect(await preflightDaemonLaunch(bundled, runner(output), manifest)).not.toBeNull();
	});

	it("rejects protocol/capability mismatch before spawn", async () => {
		const value = attestation();
		(value.capabilities as Record<string, boolean>).sessionSend = false;
		expect(await preflightDaemonLaunch(bundled, runner(JSON.stringify(value)), manifest)).toContain("sessionSend");
	});

	it("rejects a development-stamped bundled artifact", async () => {
		const value = attestation();
		value.build = { version: "dev", commit: "unknown", mode: "development" };
		expect(await preflightDaemonLaunch(bundled, runner(JSON.stringify(value)), manifest)).toContain(
			"release build is required",
		);
	});

	it("rejects a missing bundled sidecar manifest", async () => {
		expect(await preflightDaemonLaunch(bundled, runner(JSON.stringify(attestation())))).toContain(
			"could not be inspected",
		);
	});

	it("rejects binary/manifest build mismatch", async () => {
		const other = attestation();
		other.build.commit = "f".repeat(40);
		expect(
			await preflightDaemonLaunch(bundled, runner(JSON.stringify(attestation())), () =>
				Promise.resolve(JSON.stringify(other)),
			),
		).toContain("different compatibility identities");
	});
});
