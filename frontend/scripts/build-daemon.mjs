import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { buildLdflags, validateBuildInputs, validateBuiltAttestation } from "./build-attestation.mjs";
import { resolveBuildProvenance } from "./build-provenance.mjs";
import { meetsMinimumVersion, parseGoVersion, parseMinimumGoVersion } from "./go-version.mjs";

const scriptsDir = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(scriptsDir, "..");
const repoRoot = resolve(frontendRoot, "..");
const backendRoot = join(repoRoot, "backend");
const outDir = join(frontendRoot, "daemon");
const outPath = join(outDir, process.platform === "win32" ? "ao.exe" : "ao");
const manifestPath = join(outDir, "ao.attestation.json");
const minimumGoVersion = parseMinimumGoVersion(readFileSync(join(backendRoot, "go.mod"), "utf8"));

const packageVersion = JSON.parse(readFileSync(join(frontendRoot, "package.json"), "utf8")).version;
const buildMode = process.env.AO_BUILD_MODE ?? (process.env.CI ? "release" : "development");
let forkCommit;
try {
	forkCommit = resolveBuildProvenance({ repoRoot, mode: buildMode });
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}
const buildMetadata = {
	version: process.env.AO_BUILD_VERSION ?? packageVersion,
	commit: forkCommit,
	mode: buildMode,
};
try {
	validateBuildInputs(buildMetadata);
} catch (error) {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
}

if (!minimumGoVersion) {
	console.error("Could not determine the required Go version from backend/go.mod.");
	process.exit(1);
}

const versionResult = spawnSync("go", ["version"], { encoding: "utf8" });
if (versionResult.error) {
	console.error(`Go ${minimumGoVersion.join(".")}+ is required, but Go could not be started: ${versionResult.error.message}`);
	process.exit(1);
}
const actualGoVersion = parseGoVersion(versionResult.stdout);
if (versionResult.status !== 0 || !actualGoVersion || !meetsMinimumVersion(actualGoVersion, minimumGoVersion)) {
	const found = actualGoVersion ? actualGoVersion.join(".") : versionResult.stdout.trim() || "unknown";
	console.error(`Go ${minimumGoVersion.join(".")}+ required, found ${found} — upgrade at https://go.dev/dl/`);
	process.exit(1);
}

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

const result = spawnSync("go", ["build", "-trimpath", "-buildvcs=false", "-ldflags", buildLdflags(buildMetadata), "-o", outPath, "./cmd/ao"], {
	cwd: backendRoot,
	stdio: "inherit",
});

if (result.error) {
	console.error(`failed to start go build: ${result.error.message}`);
	process.exit(1);
}

if (result.status !== 0) {
	process.exit(result.status ?? 1);
}

const attestationResult = spawnSync(outPath, ["version", "--json"], {
	encoding: "utf8",
});
if (attestationResult.error || attestationResult.status !== 0) {
	console.error(`built daemon attestation probe failed: ${attestationResult.error?.message ?? attestationResult.stderr}`);
	process.exit(attestationResult.status ?? 1);
}
try {
	const attestation = validateBuiltAttestation(JSON.parse(attestationResult.stdout), buildMetadata);
	writeFileSync(manifestPath, `${JSON.stringify(attestation, null, 2)}\n`, "utf8");
} catch (error) {
	console.error(`built daemon attestation is invalid: ${error instanceof Error ? error.message : String(error)}`);
	process.exit(1);
}
