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

	it("uses the exact configured executable and prefix for version preflight", async () => {
		const configured: DaemonLaunchSpec = {
			...bundled,
			source: "configured",
			command: "go",
			args: ["run", "./cmd/ao", "daemon", "--port", "4317"],
			configuredDaemonArgIndex: 2,
		};
		const run = runner(JSON.stringify(attestation()));
		expect(daemonPreflightSpec(configured)).toEqual({
			command: "go",
			args: ["run", "./cmd/ao", "version", "--json"],
			cwd: configured.cwd,
			shell: false,
		});
		expect(await preflightDaemonLaunch(configured, run)).toBeNull();
		expect(run).toHaveBeenCalledOnce();
	});

	it("refuses configured argv when its pre-spawn version probe is incompatible", async () => {
		const configured: DaemonLaunchSpec = {
			...bundled,
			source: "configured",
			configuredDaemonArgIndex: 0,
		};
		const run = runner(JSON.stringify({ version: "legacy" }));
		expect(await preflightDaemonLaunch(configured, run)).toContain("compatibility attestation");
		expect(run).toHaveBeenCalledOnce();
	});

	it("executes JSON preflight and daemon argv literally through a path containing shell attacks", async () => {
		const directory = mkdtempSync(join(tmpdir(), "ao argv $(not-expanded) "));
		try {
			const scriptPath = join(directory, "fake ao.cjs");
			const argsPath = join(directory, "args.jsonl");
			writeFileSync(
				scriptPath,
				[
					'const { appendFileSync } = require("node:fs");',
					`appendFileSync(${JSON.stringify(argsPath)}, JSON.stringify(process.argv.slice(2)) + "\\n");`,
					`if (process.argv[2] === "version") process.stdout.write(${JSON.stringify(JSON.stringify(attestation()))});`,
				].join("\n"),
				"utf8",
			);
			const argv = [process.execPath, scriptPath, "daemon", "--port", "4317", "--label", "$(literal) * %PATH% !PATH!"];
			const launch = resolveDaemonLaunch(
				{ AO_DAEMON_ARGV: JSON.stringify(argv) },
				true,
				directory,
				directory,
				directory,
				process.platform,
			);
			expect(launch).not.toBeNull();
			if (!launch) throw new Error("configured launch was not resolved");

			expect(await preflightDaemonLaunch(launch, actualRunner)).toBeNull();
			const daemonResult = spawnSync(launch.command, launch.args, {
				cwd: launch.cwd,
				shell: launch.shell,
				encoding: "utf8",
				windowsHide: true,
			});
			expect(daemonResult.status).toBe(0);
			const invocations = readFileSync(argsPath, "utf8")
				.trim()
				.split("\n")
				.map((line) => JSON.parse(line));
			expect(invocations).toEqual([
				["version", "--json"],
				["daemon", "--port", "4317", "--label", "$(literal) * %PATH% !PATH!"],
			]);
			expect(launch).toMatchObject({
				command: process.execPath,
				args: argv.slice(1),
				shell: false,
				configuredDaemonArgIndex: 1,
			});
		} finally {
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it("executes the legacy quoted-path compatibility form with shell disabled", async () => {
		const directory = mkdtempSync(join(tmpdir(), "ao legacy argv "));
		try {
			const scriptPath = join(directory, "fake ao.cjs");
			writeFileSync(
				scriptPath,
				`if (process.argv[2] === "version") process.stdout.write(${JSON.stringify(JSON.stringify(attestation()))});`,
				"utf8",
			);
			const launch = resolveDaemonLaunch(
				{ AO_DAEMON_COMMAND: `"${process.execPath}" "${scriptPath}" daemon --port 4317` },
				true,
				directory,
				directory,
				directory,
				process.platform,
			);
			expect(launch).not.toBeNull();
			if (!launch) throw new Error("legacy configured launch was not resolved");
			expect(await preflightDaemonLaunch(launch, actualRunner)).toBeNull();
			expect(daemonPreflightSpec(launch)).toEqual({
				command: launch.command,
				args: [scriptPath, "version", "--json"],
				cwd: directory,
				shell: false,
			});
			expect(launch.args).toEqual([scriptPath, "daemon", "--port", "4317"]);
		} finally {
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it("refuses configured launch metadata without invoking a runner", async () => {
		const configured: DaemonLaunchSpec = { ...bundled, source: "configured" };
		const run = runner(JSON.stringify(attestation()));
		expect(await preflightDaemonLaunch(configured, run)).toContain("exactly one literal daemon subcommand");
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
