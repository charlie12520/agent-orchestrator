// @vitest-environment node
import { spawnSync } from "node:child_process";
import {
	chmodSync,
	copyFileSync,
	existsSync,
	linkSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
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

const gnuEnvExecutable = (
	process.platform === "win32"
		? ["C:\\Program Files\\Git\\usr\\bin\\env.exe", "C:\\Program Files\\Git\\bin\\env.exe"]
		: ["/usr/bin/env", "/bin/env"]
).find(existsSync);

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

function processRunner(env: NodeJS.ProcessEnv): DaemonPreflightRunner {
	return async (spec) => {
		const result = spawnSync(spec.command, spec.args, {
			cwd: spec.cwd,
			shell: spec.shell,
			encoding: "utf8",
			env,
			windowsHide: true,
		});
		return {
			exitCode: result.status,
			stdout: result.stdout ?? "",
			stderr: result.stderr ?? "",
			...(result.error ? { error: result.error.message } : {}),
		};
	};
}

function fakeDirectAo(directory: string, argsPath: string): { executable: string; env: NodeJS.ProcessEnv } {
	const executable = join(directory, process.platform === "win32" ? "ao.exe" : "ao");
	try {
		linkSync(process.execPath, executable);
	} catch {
		copyFileSync(process.execPath, executable);
	}
	if (process.platform !== "win32") chmodSync(executable, 0o755);

	const preloadPath = join(directory, "fake-ao-preload.cjs");
	writeFileSync(
		preloadPath,
		[
			'const { appendFileSync } = require("node:fs");',
			'const { basename } = require("node:path");',
			'const subcommand = basename(process.argv[1] || "");',
			`appendFileSync(${JSON.stringify(argsPath)}, JSON.stringify([subcommand, ...process.argv.slice(2)]) + "\\n");`,
			`if (subcommand === "version") { process.stdout.write(${JSON.stringify(JSON.stringify(attestation()))}); process.exit(0); }`,
			'if (subcommand === "daemon") process.exit(0);',
			"process.exit(3);",
		].join("\n"),
		"utf8",
	);
	const requirePath = preloadPath.replace(/\\/g, "/");
	return { executable, env: { ...process.env, NODE_OPTIONS: `--require=${JSON.stringify(requirePath)}` } };
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

	it("uses the exact direct configured executable with no prefix for version preflight", async () => {
		const configured: DaemonLaunchSpec = {
			...bundled,
			source: "configured",
			command: "/opt/ao",
			args: ["daemon", "--port", "4317"],
		};
		const run = runner(JSON.stringify(attestation()));
		expect(daemonPreflightSpec(configured)).toEqual({
			command: "/opt/ao",
			args: ["version", "--json"],
			cwd: configured.cwd,
			shell: false,
		});
		expect(await preflightDaemonLaunch(configured, run)).toBeNull();
		expect(run).toHaveBeenCalledOnce();
	});

	it("refuses configured argv when its direct pre-spawn version probe is incompatible", async () => {
		const configured: DaemonLaunchSpec = { ...bundled, source: "configured", command: "/opt/ao" };
		const run = runner(JSON.stringify({ version: "legacy" }));
		expect(await preflightDaemonLaunch(configured, run)).toContain("compatibility attestation");
		expect(run).toHaveBeenCalledOnce();
	});

	it("executes direct JSON and legacy AO paths with identical commands and no shell", async () => {
		const directory = mkdtempSync(join(tmpdir(), "ao direct path "));
		try {
			const argsPath = join(directory, "args.jsonl");
			const fake = fakeDirectAo(directory, argsPath);
			const daemonArgs = ["daemon", "--port", "4317", "--label", "$(literal) * %PATH% !PATH!"];
			const environments = [
				{ AO_DAEMON_ARGV: JSON.stringify([fake.executable, ...daemonArgs]) },
				{ AO_DAEMON_COMMAND: `"${fake.executable}" daemon --port 4317` },
			];

			for (const configuredEnv of environments) {
				const launch = resolveDaemonLaunch(configuredEnv, true, directory, directory, directory, process.platform);
				expect(launch).not.toBeNull();
				if (!launch) throw new Error("direct configured launch was not resolved");
				expect(daemonPreflightSpec(launch)).toEqual({
					command: fake.executable,
					args: ["version", "--json"],
					cwd: directory,
					shell: false,
				});
				expect(await preflightDaemonLaunch(launch, processRunner(fake.env))).toBeNull();
				const daemonResult = spawnSync(launch.command, launch.args, {
					cwd: launch.cwd,
					shell: launch.shell,
					encoding: "utf8",
					env: fake.env,
					windowsHide: true,
				});
				expect(daemonResult.status).toBe(0);
				expect(launch.command).toBe(fake.executable);
				expect(launch.args[0]).toBe("daemon");
			}

			const invocations = readFileSync(argsPath, "utf8")
				.trim()
				.split("\n")
				.map((line) => JSON.parse(line));
			expect(invocations).toEqual([
				["version", "--json"],
				["daemon", "--port", "4317", "--label", "$(literal) * %PATH% !PATH!"],
				["version", "--json"],
				["daemon", "--port", "4317"],
			]);
		} finally {
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it.skipIf(!gnuEnvExecutable)("rejects the live GNU env -S version/daemon branching bypass before execution", () => {
		if (!gnuEnvExecutable) throw new Error("GNU env is unavailable");
		const directory = mkdtempSync(join(tmpdir(), "ao env-s branch "));
		try {
			const scriptPath = join(directory, "branch.cjs");
			const sentinelPath = join(directory, "daemon-branch-ran");
			writeFileSync(
				scriptPath,
				[
					'const { writeFileSync } = require("node:fs");',
					`if (process.argv[2] === "version") { process.stdout.write(${JSON.stringify(JSON.stringify(attestation()))}); process.exit(0); }`,
					`if (process.argv[2] === "daemon") { writeFileSync(${JSON.stringify(sentinelPath)}, "ran"); process.exit(0); }`,
					"process.exit(3);",
				].join("\n"),
				"utf8",
			);
			const splitCommand = `"${process.execPath.replace(/\\/g, "/")}" "${scriptPath.replace(/\\/g, "/")}"`;

			const liveVersion = spawnSync(gnuEnvExecutable, ["-S", splitCommand, "version", "--json"], {
				cwd: directory,
				encoding: "utf8",
				windowsHide: true,
			});
			expect(liveVersion.status).toBe(0);
			expect(JSON.parse(liveVersion.stdout)).toMatchObject({ distribution: EXPECTED_DAEMON_ATTESTATION.distribution });
			const liveDaemon = spawnSync(gnuEnvExecutable, ["-S", splitCommand, "daemon", "--port", "4317"], {
				cwd: directory,
				encoding: "utf8",
				windowsHide: true,
			});
			expect(liveDaemon.status).toBe(0);
			expect(existsSync(sentinelPath)).toBe(true);
			rmSync(sentinelPath);

			const bypassArgv = [gnuEnvExecutable, "-S", splitCommand, "daemon", "--port", "4317"];
			const launch = resolveDaemonLaunch(
				{ AO_DAEMON_ARGV: JSON.stringify(bypassArgv) },
				true,
				directory,
				directory,
				directory,
				process.platform,
			);
			if (launch) spawnSync(launch.command, launch.args, { cwd: launch.cwd, shell: launch.shell });
			expect(launch).toBeNull();
			expect(existsSync(sentinelPath)).toBe(false);
		} finally {
			rmSync(directory, { recursive: true, force: true });
		}
	});

	it("refuses configured launch metadata with a prefix before invoking a runner", async () => {
		const configured: DaemonLaunchSpec = {
			...bundled,
			source: "configured",
			command: "/usr/bin/env",
			args: ["-S", "branch-on-subcommand", "daemon"],
		};
		const run = runner(JSON.stringify(attestation()));
		expect(await preflightDaemonLaunch(configured, run)).toContain("begin with one literal daemon subcommand");
		expect(run).not.toHaveBeenCalled();
	});

	it("refuses non-AO configured executable metadata before invoking a runner", async () => {
		const configured: DaemonLaunchSpec = {
			...bundled,
			source: "configured",
			command: process.platform === "win32" ? "C:\\tools\\env.exe" : "/usr/bin/env",
		};
		const run = runner(JSON.stringify(attestation()));
		expect(await preflightDaemonLaunch(configured, run)).toContain("direct executable");
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
