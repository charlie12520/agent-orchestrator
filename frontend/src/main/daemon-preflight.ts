import path from "node:path";
import type { DaemonLaunchSpec } from "../shared/daemon-launch";
import {
	daemonCompatibilityError,
	EXPECTED_DAEMON_ATTESTATION,
	parseDaemonAttestation,
	type DaemonAttestation,
} from "../shared/daemon-attestation";

export type DaemonPreflightSpec = { command: string; args: string[]; cwd: string; shell: boolean };
export type DaemonPreflightResult = { exitCode: number | null; stdout: string; stderr: string; error?: string };
export type DaemonPreflightRunner = (spec: DaemonPreflightSpec) => Promise<DaemonPreflightResult>;
export type DaemonManifestReader = (path: string) => Promise<string>;

export function daemonPreflightSpec(launch: DaemonLaunchSpec): DaemonPreflightSpec {
	if (launch.source === "bundled") {
		return { command: launch.command, args: ["version", "--json"], cwd: path.dirname(launch.command), shell: false };
	}
	if (launch.source === "dev") {
		return {
			command: launch.command,
			args: ["run", "./cmd/ao", "version", "--json"],
			cwd: launch.cwd,
			shell: false,
		};
	}
	return { command: launch.command, args: ["version", "--json"], cwd: launch.cwd, shell: true };
}

/** Validate the candidate executable before it can open or migrate AO storage. */
export async function preflightDaemonLaunch(
	launch: DaemonLaunchSpec,
	runner: DaemonPreflightRunner,
	readManifest?: DaemonManifestReader,
): Promise<string | null> {
	const spec = daemonPreflightSpec(launch);
	const result = await runner(spec);
	if (result.error) return `Could not inspect the AO daemon candidate: ${result.error}`;
	if (result.exitCode !== 0) {
		return `The AO daemon candidate attestation probe exited with code ${result.exitCode ?? "unknown"}: ${result.stderr.trim()}`;
	}
	let value: unknown;
	try {
		value = JSON.parse(result.stdout);
	} catch {
		return "The AO daemon candidate emitted malformed compatibility attestation JSON.";
	}
	const attestation = parseDaemonAttestation(value);
	const compatibilityError = daemonCompatibilityError(attestation ?? undefined);
	if (compatibilityError) return compatibilityError;
	if (launch.source === "bundled" && attestation?.build.mode !== "release") {
		return `The bundled AO daemon reports ${attestation?.build.mode ?? "an unknown"} build mode; a release build is required.`;
	}
	if (launch.source === "bundled") {
		if (!readManifest) return "The bundled AO daemon attestation manifest could not be inspected.";
		let manifestValue: unknown;
		try {
			manifestValue = JSON.parse(await readManifest(path.join(path.dirname(launch.command), "ao.attestation.json")));
		} catch {
			return "The bundled AO daemon attestation manifest is missing or malformed.";
		}
		const manifest = parseDaemonAttestation(manifestValue);
		const manifestError = daemonCompatibilityError(manifest ?? undefined);
		if (manifestError) return `The bundled AO daemon attestation manifest is incompatible: ${manifestError}`;
		if (!attestation || !manifest || !sameAttestation(attestation, manifest)) {
			return "The bundled AO daemon binary and attestation manifest report different compatibility identities.";
		}
	}
	return null;
}

function sameAttestation(left: DaemonAttestation, right: DaemonAttestation): boolean {
	if (
		left.contractVersion !== right.contractVersion ||
		left.distribution !== right.distribution ||
		left.upstream.repository !== right.upstream.repository ||
		left.upstream.commit !== right.upstream.commit ||
		left.build.version !== right.build.version ||
		left.build.commit !== right.build.commit ||
		left.build.mode !== right.build.mode
	)
		return false;
	for (const name of Object.keys(EXPECTED_DAEMON_ATTESTATION.protocols) as (keyof DaemonAttestation["protocols"])[]) {
		if (left.protocols[name] !== right.protocols[name]) return false;
	}
	const capabilityNames = new Set([...Object.keys(left.capabilities), ...Object.keys(right.capabilities)]);
	for (const name of capabilityNames) {
		if (left.capabilities[name] !== right.capabilities[name]) return false;
	}
	return true;
}
