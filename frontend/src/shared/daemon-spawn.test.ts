import { describe, expect, it, vi } from "vitest";
import { DAEMON_SERVICE_NAME, type DaemonProbe, type DaemonProber } from "./daemon-attach";
import { EXPECTED_DAEMON_ATTESTATION, type DaemonAttestation } from "./daemon-attestation";
import { authoritativeSpawnPid, verifySpawnedDaemon } from "./daemon-spawn";

const ATTESTATION: DaemonAttestation = {
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

function probe(status: "ok" | "ready", pid = 4242, attestation: DaemonAttestation | null = ATTESTATION): DaemonProbe {
	return { status, service: DAEMON_SERVICE_NAME, pid, ...(attestation ? { attestation } : {}) };
}

function prober(health: DaemonProbe | null, ready: DaemonProbe | null): DaemonProber {
	return vi.fn((_, endpoint) => Promise.resolve(endpoint === "healthz" ? health : ready));
}

const BASE = {
	isProcessAlive: () => true,
	identityError: () => null,
};

describe("fresh-spawn compatibility gate", () => {
	it("treats a listen line as discovery and requires both compatible live probes", async () => {
		const liveProbe = prober(probe("ok"), probe("ready"));
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: liveProbe,
			expectedPid: 4242,
		});
		expect(result).toMatchObject({ state: "ready", port: 4317, pid: 4242 });
		expect(liveProbe).toHaveBeenNthCalledWith(1, 4317, "healthz");
		expect(liveProbe).toHaveBeenNthCalledWith(2, 4317, "readyz");
	});

	it("does not mark a discovered port ready while health is missing", async () => {
		expect(
			await verifySpawnedDaemon({
				...BASE,
				discovery: { source: "listen", port: 4317 },
				probe: prober(null, null),
			}),
		).toBeNull();
	});

	it.each(["missing", "malformed"])("rejects a %s live attestation", async (kind) => {
		const malformed = { ...ATTESTATION, protocols: { ...ATTESTATION.protocols, terminalMux: 99 } };
		const health = probe("ok", 4242, kind === "missing" ? null : malformed);
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(health, probe("ready")),
		});
		expect(result).toMatchObject({ state: "error", code: "compatibility_mismatch" });
	});

	it("rejects health/readiness build mismatch", async () => {
		const other = { ...ATTESTATION, build: { ...ATTESTATION.build, commit: "f".repeat(40) } };
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(probe("ok"), probe("ready", 4242, other)),
		});
		expect(result).toMatchObject({ state: "error", code: "compatibility_mismatch" });
	});

	it("rejects health/readiness PID mismatch", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(probe("ok", 4242), probe("ready", 9999)),
		});
		expect(result).toMatchObject({ state: "error", code: "not_ready", pid: 4242 });
	});

	it("rejects a compatible daemon whose PID is not the directly spawned bundled child", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(probe("ok"), probe("ready")),
			expectedPid: 9999,
		});
		expect(result).toMatchObject({ state: "error", code: "identity_mismatch", pid: 4242 });
	});

	it.each(["configured", "bundled"] as const)(
		"accepts a %s launch when the compatible daemon reports the direct child PID",
		async (source) => {
			const result = await verifySpawnedDaemon({
				...BASE,
				discovery: { source: "listen", port: 4317 },
				probe: prober(probe("ok"), probe("ready")),
				expectedPid: authoritativeSpawnPid(source, 4242),
			});
			expect(result).toMatchObject({ state: "ready", port: 4317, pid: 4242 });
		},
	);

	it("rejects a configured launch when the compatible daemon does not report the direct child PID", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(probe("ok"), probe("ready")),
			expectedPid: authoritativeSpawnPid("configured", 9999),
		});
		expect(result).toMatchObject({ state: "error", code: "identity_mismatch", pid: 4242 });
	});

	it("keeps the development go-run wrapper exempt from direct-child PID matching", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "listen", port: 4317 },
			probe: prober(probe("ok"), probe("ready")),
			expectedPid: authoritativeSpawnPid("dev", 9999),
		});
		expect(result).toMatchObject({ state: "ready", port: 4317, pid: 4242 });
	});

	it("rejects a legacy runfile before live API use", async () => {
		const liveProbe = prober(probe("ok"), probe("ready"));
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: {
				source: "runfile",
				port: 4317,
				notBeforeMs: 0,
				contents: JSON.stringify({ pid: 4242, port: 4317, startedAt: "2026-07-30T00:00:00Z" }),
			},
			probe: liveProbe,
		});
		expect(result).toMatchObject({ state: "error", code: "compatibility_mismatch" });
		expect(liveProbe).not.toHaveBeenCalled();
	});

	it("rejects a malformed fresh runfile", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: { source: "runfile", port: 4317, notBeforeMs: 0, contents: "{not json" },
			probe: prober(probe("ok"), probe("ready")),
		});
		expect(result).toMatchObject({ state: "error", code: "compatibility_mismatch" });
	});

	it("rejects a runfile without a PID or timestamp even when its attestation is present", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: {
				source: "runfile",
				port: 4317,
				notBeforeMs: 0,
				contents: JSON.stringify({ port: 4317, attestation: ATTESTATION }),
			},
			probe: prober(probe("ok"), probe("ready")),
		});
		expect(result).toMatchObject({ state: "error", code: "compatibility_mismatch" });
	});

	it("requires a fresh runfile, its PID/build, and both live surfaces to agree", async () => {
		const result = await verifySpawnedDaemon({
			...BASE,
			discovery: {
				source: "runfile",
				port: 4317,
				notBeforeMs: 1,
				contents: JSON.stringify({
					pid: 4242,
					port: 4317,
					startedAt: "2026-07-30T00:00:00Z",
					attestation: ATTESTATION,
				}),
			},
			probe: prober(probe("ok"), probe("ready")),
		});
		expect(result).toMatchObject({ state: "ready", port: 4317, pid: 4242 });
	});
});
