import type { DaemonStatus } from "./daemon-status";
import type { DaemonProber, DaemonProbe } from "./daemon-attach";
import type { DaemonLaunchSpec } from "./daemon-launch";
import { resolveDaemonFromPort, resolveDaemonFromRunFile } from "./daemon-attach";
import { parseRunFile } from "./daemon-discovery";

export type SpawnDaemonDiscovery =
	| { source: "listen"; port: number }
	| { source: "runfile"; port: number; contents: string; notBeforeMs: number };

export type SpawnDaemonVerifyDeps = {
	discovery: SpawnDaemonDiscovery;
	isProcessAlive: (pid: number) => boolean;
	probe: DaemonProber;
	identityError: (probe: DaemonProbe) => string | null;
	/** Exact child PID when the launch mechanism does not introduce a wrapper process. */
	expectedPid?: number;
};

/**
 * Configured and bundled launches execute AO directly, so their child PID is
 * part of the fresh-spawn identity. Development uses `go run`, whose child is
 * a wrapper process rather than the daemon itself.
 */
export function authoritativeSpawnPid(
	source: DaemonLaunchSpec["source"],
	childPid: number | undefined,
): number | undefined {
	return source === "dev" ? undefined : childPid;
}

/**
 * Treat stdout and running.json as port discovery only. A spawned daemon is not
 * ready until compatible /healthz and /readyz surfaces agree on PID and build;
 * running.json discovery additionally has to agree with both live surfaces.
 */
export async function verifySpawnedDaemon(deps: SpawnDaemonVerifyDeps): Promise<DaemonStatus | null> {
	const { discovery, expectedPid, identityError, isProcessAlive, probe } = deps;
	let status: DaemonStatus | null;

	if (discovery.source === "runfile") {
		const info = parseRunFile(discovery.contents);
		if (!info || info.pid < 1 || !Number.isFinite(info.startedAtMs) || info.port !== discovery.port) {
			return compatibilityFailure(
				discovery.port,
				undefined,
				"The newly spawned AO daemon wrote a missing or malformed compatibility runfile.",
			);
		}
		if (info.startedAtMs < discovery.notBeforeMs) return null;
		status = await resolveDaemonFromRunFile({
			runFileContents: discovery.contents,
			isProcessAlive,
			probe,
			identityError,
		});
	} else {
		status = await resolveDaemonFromPort({ expectedPort: discovery.port, probe, identityError });
	}

	if (status?.state === "ready" && expectedPid !== undefined && status.pid !== expectedPid) {
		return {
			state: "error",
			port: discovery.port,
			pid: status.pid,
			executablePath: status.executablePath,
			workingDirectory: status.workingDirectory,
			message: `The AO daemon reported PID ${status.pid ?? "unknown"}; expected spawned PID ${expectedPid}.`,
			code: "identity_mismatch",
		};
	}
	return status;
}

function compatibilityFailure(port: number, pid: number | undefined, message: string): DaemonStatus {
	return { state: "error", port, pid, message, code: "compatibility_mismatch" };
}
