// @vitest-environment node
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";
import { EXPECTED_DAEMON_ATTESTATION } from "../shared/daemon-attestation";
import { resolveDaemonLaunch, type DaemonLaunchSpec } from "../shared/daemon-launch";
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
		const configured = {
			...bundled,
			source: "configured" as const,
			shell: true,
			command: "ao daemon --port 4317",
			preflightCommand: "ao version --json",
		};
		const run = runner(JSON.stringify(attestation()));
		expect(daemonPreflightSpec(configured)).toEqual({
			command: "ao version --json",
			args: [],
			cwd: configured.cwd,
			shell: true,
		});
		expect(await preflightDaemonLaunch(configured, run)).toBeNull();
		expect(run).toHaveBeenCalledOnce();
	});

	it("refuses a configured command when its pre-spawn version probe is incompatible", async () => {
		const configured = {
			...bundled,
			source: "configured" as const,
			shell: true,
			preflightCommand: "ao version --json",
		};
		const run = runner(JSON.stringify({ version: "legacy" }));
		expect(await preflightDaemonLaunch(configured, run)).toContain("compatibility attestation");
		expect(run).toHaveBeenCalledOnce();
	});

	it("executes a derived version probe for a quoted configured command without changing the launch", async () => {
		const directory = mkdtempSync(join(tmpdir(), "ao preflight "));
		try {
			const scriptPath = join(directory, "fake ao.cjs");
			const argsPath = join(directory, "args.json");
			writeFileSync(
				scriptPath,
				[
					'const { writeFileSync } = require("node:fs");',
					`writeFileSync(${JSON.stringify(argsPath)}, JSON.stringify(process.argv.slice(2)));`,
					`process.stdout.write(${JSON.stringify(JSON.stringify(attestation()))});`,
				].join("\n"),
				"utf8",
			);
			const command = `"${process.execPath}" "${scriptPath}" daemon --port 4317`;
			const launch = resolveDaemonLaunch(
				{ AO_DAEMON_COMMAND: command },
				true,
				directory,
				directory,
				directory,
				process.platform,
			);
			expect(launch).not.toBeNull();
			if (!launch) throw new Error("configured launch was not resolved");

			const actualRunner: DaemonPreflightRunner = async (spec) => {
				const result = spawnSync(spec.command, spec.args, {
					cwd: spec.cwd,
					shell: spec.shell,
					encoding: "utf8",
					windowsHide: true,
				});
				return {
					exitCode: result.status,
					stdout: result.stdout ?? "",
					stderr: result.stderr ?? "",
					...(result.error ? { error: result.error.message } : {}),
				};
			};

			expect(await preflightDaemonLaunch(launch, actualRunner)).toBeNull();
			expect(JSON.parse(readFileSync(argsPath, "utf8"))).toEqual(["version", "--json"]);
			expect(launch.command).toBe(command);
			expect(launch.args).toEqual([]);
		} finally {
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it("refuses ambiguous configured commands without invoking a runner", async () => {
		const configured = {
			...bundled,
			source: "configured" as const,
			shell: true,
			command: "ao daemon && echo ambiguous",
		};
		const run = runner(JSON.stringify(attestation()));
		expect(await preflightDaemonLaunch(configured, run)).toContain("unambiguous AO daemon subcommand");
		expect(run).not.toHaveBeenCalled();
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
