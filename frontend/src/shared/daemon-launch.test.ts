import { describe, expect, it, vi } from "vitest";
import { parseConfiguredDaemonCommand, resolveDaemonLaunch } from "./daemon-launch";

function configured(env: Record<string, string | undefined>, platform: NodeJS.Platform = "darwin") {
	return resolveDaemonLaunch(env, false, "/resources", "/app", "/home/user", platform);
}

describe("resolveDaemonLaunch", () => {
	it("accepts only a direct AO executable followed immediately by daemon", () => {
		const argv = ["C:\\Program Files\\Agent Orchestrator\\ao.exe", "daemon", "--port", "4317"];
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32")).toEqual({
			command: argv[0],
			args: argv.slice(1),
			cwd: "/app",
			shell: false,
			source: "configured",
		});
	});

	it("preserves literal JSON daemon arguments without shell expansion", () => {
		const argv = [
			"C:\\Program Files (x86)\\AO $literal\\ao.exe",
			"daemon",
			"--pattern",
			"*",
			"--value",
			"$(not-expanded) %PATH% !PATH!",
		];
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32")).toMatchObject({
			command: argv[0],
			args: argv.slice(1),
			shell: false,
		});
	});

	it("strictly parses the legacy quoted POSIX compatibility form into direct argv", () => {
		expect(configured({ AO_DAEMON_COMMAND: '"/opt/AO Builds/ao" daemon --port "4317"' })).toEqual({
			command: "/opt/AO Builds/ao",
			args: ["daemon", "--port", "4317"],
			cwd: "/app",
			shell: false,
			source: "configured",
		});
	});

	it("strictly parses a direct quoted Windows executable without treating single quotes as quoting", () => {
		const command = '"C:\\Program Files\\Agent Orchestrator\\ao.exe" daemon --port 4317';
		expect(configured({ AO_DAEMON_COMMAND: command }, "win32")).toMatchObject({
			command: "C:\\Program Files\\Agent Orchestrator\\ao.exe",
			args: ["daemon", "--port", "4317"],
			shell: false,
		});
		expect(configured({ AO_DAEMON_COMMAND: "ao 'daemon'" }, "win32")).toBeNull();
	});

	it("preserves POSIX backslashes while rejecting the resulting wrapper prefix", () => {
		const command = 'wrapper "d\\aemon" /opt/ao daemon';
		expect(parseConfiguredDaemonCommand(command, "darwin")).toEqual(["wrapper", "d\\aemon", "/opt/ao", "daemon"]);
		expect(configured({ AO_DAEMON_COMMAND: command })).toBeNull();
	});

	it.each([
		["newline suffix", "ao daemon\nprintf trailing", "darwin"],
		["newline prefix", "cd /tmp\nao daemon", "darwin"],
		["carriage return", "ao daemon\rprintf trailing", "darwin"],
		["tab separator", "ao\tdaemon", "darwin"],
		["command substitution", "$(true) ao daemon", "darwin"],
		["backtick substitution", "`true` ao daemon", "darwin"],
		["Windows percent expansion", "ao %AO_MODE% daemon", "win32"],
		["Windows delayed expansion", "ao !AO_MODE! daemon", "win32"],
		["glob expansion", "ao * daemon", "darwin"],
		["concatenated quote", '"/opt/ao"daemon', "darwin"],
		["pipeline", "ao daemon && echo unsafe", "darwin"],
	] as const)("rejects legacy %s syntax", (_name, command, platform) => {
		expect(configured({ AO_DAEMON_COMMAND: command }, platform)).toBeNull();
	});

	it.each([
		["cmd", ["cmd.exe", "/c", "ao", "daemon"]],
		["sh", ["/bin/sh", "-c", "ao", "daemon"]],
		["PowerShell", ["C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe", "-Command", "ao", "daemon"]],
		["GNU env -S", ["/usr/bin/env", "-S", "branch-on-subcommand", "ao", "daemon"]],
		["env.exe", ["C:\\Program Files\\Git\\usr\\bin\\env.exe", "daemon", "--port", "4317"]],
	] as const)("rejects an explicit %s multiplexer", (_name, argv) => {
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32")).toBeNull();
	});

	it.each([
		["cmd", "cmd.exe /c ao daemon", "win32"],
		["sh", "sh -c ao daemon", "darwin"],
		["PowerShell", "powershell.exe -Command ao daemon", "win32"],
		["go run", 'go run "./cmd/ao" daemon --port 4317', "darwin"],
		["arbitrary wrapper", 'renamed-wrapper "/opt/ao" daemon --port 4317', "darwin"],
	] as const)("rejects a legacy %s prefix", (_name, command, platform) => {
		expect(configured({ AO_DAEMON_COMMAND: command }, platform)).toBeNull();
	});

	it("rejects arbitrary renamed wrappers and pre-subcommand arguments regardless of behavior", () => {
		const execute = vi.fn();
		for (const argv of [
			["renamed-wrapper", "/opt/ao", "daemon", "--port", "4317"],
			["C:\\tools\\renamed-wrapper.exe", "C:\\AO\\ao.exe", "daemon"],
			["/opt/ao", "--config", "/tmp/config", "daemon"],
		]) {
			const launch = configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32");
			if (launch) execute(launch);
			expect(launch).toBeNull();
		}
		expect(execute).not.toHaveBeenCalled();
	});

	it("rejects the live GNU env -S branch shape before any execution", () => {
		const execute = vi.fn();
		const branchingBypass = [
			"/usr/bin/env",
			"-S",
			`sh -c 'if [ "$1" = version ]; then exec /opt/ao "$@"; else touch /tmp/branch-ran; fi' --`,
			"ao",
			"daemon",
			"--port",
			"4317",
		];
		const launch = configured({ AO_DAEMON_ARGV: JSON.stringify(branchingBypass) });
		if (launch) execute(launch);
		expect(launch).toBeNull();
		expect(execute).not.toHaveBeenCalled();
	});

	it("requires one unique literal daemon element immediately after the executable", () => {
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["ao", "start"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["daemon"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["ao", "--config", "x", "daemon"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["ao", "daemon", "daemon"]) })).toBeNull();
	});

	it("requires the direct executable name to be AO", () => {
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["renamed-ao", "daemon"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["/opt/AO", "daemon"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["C:\\opt\\not-ao.exe", "daemon"]) }, "win32")).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["AO.EXE", "daemon"]) }, "win32")).not.toBeNull();
	});

	it.each([
		["empty array", "[]"],
		["empty executable", JSON.stringify(["", "daemon"])],
		["blank executable", JSON.stringify(["   ", "daemon"])],
		["empty argument", JSON.stringify(["ao", "daemon", ""])],
		["non-string", JSON.stringify(["ao", "daemon", 42])],
		["NUL", JSON.stringify(["ao", "daemon", "\u0000"])],
		["not JSON", '"ao", "daemon"'],
	] as const)("rejects %s JSON argv", (_name, value) => {
		expect(configured({ AO_DAEMON_ARGV: value })).toBeNull();
	});

	it("does not fall back to a legacy command when preferred JSON argv is invalid", () => {
		expect(configured({ AO_DAEMON_ARGV: "not-json", AO_DAEMON_COMMAND: "ao daemon" })).toBeNull();
	});

	it("runs the backend daemon from source in dev without explicit argv", () => {
		expect(resolveDaemonLaunch({}, false, "/resources", "/repo/frontend", "/home/user", "darwin")).toEqual({
			command: "go",
			args: ["run", "./cmd/ao", "daemon"],
			cwd: "/repo/frontend/../backend",
			shell: false,
			source: "dev",
		});
	});

	it("uses the bundled daemon binary for packaged macOS/Linux builds", () => {
		expect(
			resolveDaemonLaunch(
				{},
				true,
				"/Applications/Agent Orchestrator.app/Contents/Resources",
				"/app",
				"/Users/alice",
				"darwin",
			),
		).toEqual({
			command: "/Applications/Agent Orchestrator.app/Contents/Resources/daemon/ao",
			args: ["daemon"],
			cwd: "/Users/alice/.ao",
			shell: false,
			source: "bundled",
		});
	});

	it("uses the bundled daemon exe for packaged Windows builds", () => {
		expect(
			resolveDaemonLaunch(
				{},
				true,
				"C:\\Program Files\\AO\\resources",
				"C:\\Program Files\\AO\\resources\\app.asar",
				"C:\\Users\\alice",
				"win32",
			),
		).toEqual({
			command: "C:\\Program Files\\AO\\resources/daemon/ao.exe",
			args: ["daemon"],
			cwd: "C:\\Users\\alice/.ao",
			shell: false,
			source: "bundled",
		});
	});
});
