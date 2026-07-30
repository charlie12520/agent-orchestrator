export type DaemonLaunchSpec = {
	command: string;
	args: string[];
	cwd: string;
	shell: false;
	source: "configured" | "bundled" | "dev";
	/** Index of the literal `daemon` subcommand in `args` for a configured launch. */
	configuredDaemonArgIndex?: number;
};

const CONTROL_CHARACTER = /[\u0000-\u001f\u007f-\u009f\u2028\u2029]/;
const COMPATIBILITY_SHELL_SYNTAX = /[|&;<>()$`%!*?\[\]{}#~^]/;
const SHELL_INTERPRETERS = new Set([
	"bash",
	"bash.exe",
	"cmd",
	"cmd.exe",
	"command.com",
	"csh",
	"dash",
	"fish",
	"ksh",
	"nu",
	"powershell",
	"powershell.exe",
	"powershell_ise.exe",
	"pwsh",
	"pwsh.exe",
	"sh",
	"sh.exe",
	"tcsh",
	"wsl",
	"wsl.exe",
	"zsh",
]);

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

function shellInterpreter(value: string): boolean {
	const normalized = value.replace(/\\/g, "/");
	return SHELL_INTERPRETERS.has(normalized.slice(normalized.lastIndexOf("/") + 1).toLowerCase());
}

function configuredLaunch(argv: string[], cwd: string): DaemonLaunchSpec | null {
	if (!argv[0] || argv[0].trim().length === 0) return null;
	const daemonIndexes = argv.flatMap((value, index) => (value === "daemon" ? [index] : []));
	if (daemonIndexes.length !== 1 || daemonIndexes[0] === 0) return null;
	const daemonIndex = daemonIndexes[0];
	if (argv.slice(0, daemonIndex).some(shellInterpreter)) return null;
	return {
		command: argv[0],
		args: argv.slice(1),
		cwd,
		shell: false,
		source: "configured",
		configuredDaemonArgIndex: daemonIndex - 1,
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
		return configured.argv ? configuredLaunch(configured.argv, appPath) : null;
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
