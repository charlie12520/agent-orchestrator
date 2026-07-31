import path from "node:path";

export type DaemonLaunchSpec = {
	command: string;
	args: string[];
	cwd: string;
	shell: false;
	source: "configured" | "bundled" | "dev";
};

const CONTROL_CHARACTER = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/;
const COMPATIBILITY_SHELL_SYNTAX = /[|&;<>()$`%!*?\[\]{}#~^]/;

type ConfiguredArgvResolution = { present: false } | { present: true; argv: string[] | null };

/**
 * Parse the legacy AO_DAEMON_COMMAND compatibility grammar. This is
 * deliberately narrower than a shell: ASCII spaces separate arguments,
 * quoted paths are supported, and no expansion/control syntax is accepted.
 * The resulting argv is used directly with shell:false and is never reparsed.
 */
export function parseConfiguredDaemonCommand(command: string, platform: NodeJS.Platform): string[] | null {
	if (!command || CONTROL_CHARACTER.test(command) || COMPATIBILITY_SHELL_SYNTAX.test(command)) return null;

	const argv: string[] = [];
	let index = 0;
	while (index < command.length) {
		while (command[index] === " ") index += 1;
		if (index >= command.length) break;

		let value = "";
		const openingQuote = command[index];
		if (openingQuote === '"' || openingQuote === "'") {
			if (openingQuote === "'" && platform === "win32") return null;
			index += 1;
			let closed = false;
			while (index < command.length) {
				const char = command[index];
				if (char === openingQuote) {
					closed = true;
					index += 1;
					break;
				}
				if (openingQuote === '"' && char === "\\" && platform !== "win32") {
					const next = command[index + 1];
					if (next === '"' || next === "\\") {
						value += next;
						index += 2;
						continue;
					}
					value += "\\";
					index += 1;
					continue;
				}
				if (openingQuote === '"' && char === "\\" && platform === "win32" && command[index + 1] === '"') {
					return null;
				}
				value += char;
				index += 1;
			}
			if (!closed || (index < command.length && command[index] !== " ")) return null;
		} else {
			while (index < command.length && command[index] !== " ") {
				const char = command[index];
				if (char === '"' || char === "'") return null;
				if (char === "\\" && platform !== "win32") {
					if (index + 1 >= command.length) return null;
					value += command[index + 1];
					index += 2;
					continue;
				}
				value += char;
				index += 1;
			}
		}
		if (!value) return null;
		argv.push(value);
	}
	return argv.length > 0 ? argv : null;
}

function parseConfiguredDaemonJson(raw: string): string[] | null {
	if (!raw || CONTROL_CHARACTER.test(raw)) return null;
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch {
		return null;
	}
	if (!Array.isArray(value) || value.length < 2) return null;
	if (value.some((entry) => typeof entry !== "string" || entry.length === 0 || CONTROL_CHARACTER.test(entry))) {
		return null;
	}
	return value as string[];
}

function configuredArgv(env: Record<string, string | undefined>, platform: NodeJS.Platform): ConfiguredArgvResolution {
	if (env.AO_DAEMON_ARGV !== undefined) {
		return { present: true, argv: parseConfiguredDaemonJson(env.AO_DAEMON_ARGV) };
	}
	if (env.AO_DAEMON_COMMAND !== undefined) {
		return { present: true, argv: parseConfiguredDaemonCommand(env.AO_DAEMON_COMMAND, platform) };
	}
	return { present: false };
}

export function isDirectAoExecutable(value: string, platform: NodeJS.Platform): boolean {
	if (CONTROL_CHARACTER.test(value)) return false;
	const basename = platform === "win32" ? path.win32.basename(value) : path.posix.basename(value);
	if (platform !== "win32") return basename === "ao";
	if (basename.includes(":") || /[. ]$/.test(basename)) return false;
	return basename.toLowerCase() === "ao" || basename.toLowerCase() === "ao.exe";
}

function configuredLaunch(argv: string[], cwd: string, platform: NodeJS.Platform): DaemonLaunchSpec | null {
	if (!argv[0] || !isDirectAoExecutable(argv[0], platform)) return null;
	if (argv[1] !== "daemon" || argv.slice(2).some((value) => value === "daemon")) return null;
	return {
		command: argv[0],
		args: argv.slice(1),
		cwd,
		shell: false,
		source: "configured",
	};
}

function joinPath(...segments: string[]): string {
	return segments.map((segment) => segment.replace(/[/\\]+$/, "")).join("/");
}

export function bundledDaemonBinaryName(platform: NodeJS.Platform): string {
	return platform === "win32" ? "ao.exe" : "ao";
}

export function resolveDaemonLaunch(
	env: Record<string, string | undefined>,
	isPackaged: boolean,
	resourcesPath: string,
	appPath: string,
	homeDir: string,
	platform: NodeJS.Platform,
): DaemonLaunchSpec | null {
	const configured = configuredArgv(env, platform);
	if (configured.present) {
		return configured.argv ? configuredLaunch(configured.argv, appPath, platform) : null;
	}

	if (!isPackaged) {
		return {
			command: "go",
			args: ["run", "./cmd/ao", "daemon"],
			cwd: joinPath(appPath, "..", "backend"),
			shell: false,
			source: "dev",
		};
	}

	return {
		command: joinPath(resourcesPath, "daemon", bundledDaemonBinaryName(platform)),
		args: ["daemon"],
		cwd: joinPath(homeDir, ".ao"),
		shell: false,
		source: "bundled",
	};
}
