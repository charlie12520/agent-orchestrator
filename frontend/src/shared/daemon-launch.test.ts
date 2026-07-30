import { describe, expect, it } from "vitest";
import { parseConfiguredDaemonCommand, resolveDaemonLaunch } from "./daemon-launch";

function configured(env: Record<string, string | undefined>, platform: NodeJS.Platform = "darwin") {
	return resolveDaemonLaunch(env, false, "/resources", "/app", "/home/user", platform);
}

describe("resolveDaemonLaunch", () => {
	it("prefers an explicit JSON argv contract and never enables a shell", () => {
		const argv = ["C:\\Program Files\\Agent Orchestrator\\ao.exe", "daemon", "--port", "4317"];
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32")).toEqual({
			command: argv[0],
			args: argv.slice(1),
			cwd: "/app",
			shell: false,
			source: "configured",
			configuredDaemonArgIndex: 0,
		});
	});

	it("preserves literal JSON arguments that would be shell syntax", () => {
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

	it("strictly parses the legacy quoted POSIX compatibility form into argv", () => {
		expect(configured({ AO_DAEMON_COMMAND: '"/opt/AO Builds/ao" daemon --port "4317"' })).toEqual({
			command: "/opt/AO Builds/ao",
			args: ["daemon", "--port", "4317"],
			cwd: "/app",
			shell: false,
			source: "configured",
			configuredDaemonArgIndex: 0,
		});
	});

	it("strictly parses a quoted Windows executable without treating single quotes as quoting", () => {
		const command = '"C:\\Program Files\\Agent Orchestrator\\ao.exe" daemon --port 4317';
		expect(configured({ AO_DAEMON_COMMAND: command }, "win32")).toMatchObject({
			command: "C:\\Program Files\\Agent Orchestrator\\ao.exe",
			args: ["daemon", "--port", "4317"],
			shell: false,
			configuredDaemonArgIndex: 0,
		});
		expect(configured({ AO_DAEMON_COMMAND: "ao 'daemon'" }, "win32")).toBeNull();
	});

	it("preserves an exact executable prefix and daemon arguments", () => {
		expect(configured({ AO_DAEMON_COMMAND: 'go run "./cmd/ao" daemon --port 4317 --label "two words"' })).toEqual({
			command: "go",
			args: ["run", "./cmd/ao", "daemon", "--port", "4317", "--label", "two words"],
			cwd: "/app",
			shell: false,
			source: "configured",
			configuredDaemonArgIndex: 2,
		});
	});

	it("preserves POSIX backslashes inside double quotes unless POSIX makes them special", () => {
		expect(parseConfiguredDaemonCommand('wrapper "d\\aemon" /opt/ao daemon', "darwin")).toEqual([
			"wrapper",
			"d\\aemon",
			"/opt/ao",
			"daemon",
		]);
		expect(configured({ AO_DAEMON_COMMAND: 'wrapper "d\\aemon" /opt/ao daemon' })).toMatchObject({
			command: "wrapper",
			args: ["d\\aemon", "/opt/ao", "daemon"],
			configuredDaemonArgIndex: 2,
		});
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
	] as const)("rejects an explicit %s interpreter wrapper", (_name, argv) => {
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(argv) }, "win32")).toBeNull();
	});

	it.each([
		["cmd", "cmd.exe /c ao daemon", "win32"],
		["sh", "sh -c ao daemon", "darwin"],
		["PowerShell", "powershell.exe -Command ao daemon", "win32"],
	] as const)("rejects a legacy %s interpreter wrapper", (_name, command, platform) => {
		expect(configured({ AO_DAEMON_COMMAND: command }, platform)).toBeNull();
	});

	it("requires one unique literal daemon element after the executable", () => {
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["ao", "start"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["daemon"]) })).toBeNull();
		expect(configured({ AO_DAEMON_ARGV: JSON.stringify(["ao", "daemon", "daemon"]) })).toBeNull();
	});

	it.each([
		["empty array", "[]"],
		["empty executable", JSON.stringify(["", "daemon"])],
		["blank executable", JSON.stringify(["   ", "daemon"])],
		["empty argument", JSON.stringify(["ao", "", "daemon"])],
		["non-string", JSON.stringify(["ao", 42, "daemon"])],
		["NUL", JSON.stringify(["ao", "\u0000", "daemon"])],
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
