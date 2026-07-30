export type DaemonLaunchSpec = {
	command: string;
	args: string[];
	cwd: string;
	shell: boolean;
	source: "configured" | "bundled" | "dev";
	/** Non-mutating shell command derived from AO_DAEMON_COMMAND for preflight. */
	preflightCommand?: string;
};

type ShellToken = { value: string; start: number; end: number };

/**
 * Replace the configured AO `daemon` subcommand with `version --json` while
 * preserving the exact executable/prefix quoting used for the actual launch.
 * Complex shell pipelines/redirections are refused instead of partially
 * executing during preflight.
 */
export function configuredDaemonPreflightCommand(command: string, platform: NodeJS.Platform): string | null {
	const tokens = tokenizeConfiguredCommand(command, platform);
	if (!tokens) return null;
	const daemon = tokens.find((token, index) => index > 0 && token.value === "daemon");
	if (!daemon) return null;
	return `${command.slice(0, daemon.start).trimEnd()} version --json`;
}

function tokenizeConfiguredCommand(command: string, platform: NodeJS.Platform): ShellToken[] | null {
	const tokens: ShellToken[] = [];
	let start = -1;
	let value = "";
	let quote: '"' | "'" | null = null;

	const finish = (end: number) => {
		if (start < 0) return;
		tokens.push({ value, start, end });
		start = -1;
		value = "";
	};

	for (let index = 0; index < command.length; index += 1) {
		const char = command[index];
		if (quote) {
			if (char === quote) {
				quote = null;
				continue;
			}
			if (char === "\\" && quote === '"' && platform !== "win32" && index + 1 < command.length) {
				value += command[++index];
				continue;
			}
			value += char;
			continue;
		}
		if (/\s/.test(char)) {
			finish(index);
			continue;
		}
		if (char === '"' || char === "'") {
			if (start < 0) start = index;
			quote = char;
			continue;
		}
		if ("|&;<>".includes(char)) return null;
		if (char === "\\" && platform !== "win32" && index + 1 < command.length) {
			if (start < 0) start = index;
			value += command[++index];
			continue;
		}
		if (start < 0) start = index;
		value += char;
	}
	if (quote) return null;
	finish(command.length);
	return tokens;
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
	const configuredCommand = env.AO_DAEMON_COMMAND?.trim();
	if (configuredCommand) {
		const preflightCommand = configuredDaemonPreflightCommand(configuredCommand, platform);
		return {
			command: configuredCommand,
			args: [],
			cwd: appPath,
			shell: true,
			source: "configured",
			...(preflightCommand ? { preflightCommand } : {}),
		};
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
