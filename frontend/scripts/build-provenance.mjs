import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join, resolve } from "node:path";

const FULL_SHA = /^[0-9a-f]{40}$/;

function defaultRunGit(repoRoot, args) {
	return spawnSync("git", args, { cwd: repoRoot, encoding: "utf8" });
}

/**
 * Resolve a self-reported source identity without allowing a release build to
 * stamp a clean checkout commit over a dirty checkout. Outside Git, the
 * caller must supply both an exact commit and an explicit assertion that it
 * verified the archive's provenance through an external channel.
 */
export function resolveBuildProvenance({
	repoRoot,
	mode,
	override = process.env.AO_FORK_COMMIT,
	archiveProvenanceVerified = process.env.AO_ARCHIVE_PROVENANCE_VERIFIED === "1",
	runGit = defaultRunGit,
}) {
	const inside = runGit(repoRoot, ["rev-parse", "--is-inside-work-tree"]);
	const inCheckout = inside.status === 0 && inside.stdout?.trim() === "true";
	const requested = override?.trim();

	if (!inCheckout) {
		if (existsSync(join(repoRoot, ".git"))) {
			throw new Error("Git metadata exists, but the checkout HEAD could not be verified; refusing archive fallback.");
		}
		if (!requested || !FULL_SHA.test(requested)) {
			throw new Error("Outside a Git checkout, AO_FORK_COMMIT must be a lowercase full 40-character SHA.");
		}
		if (!archiveProvenanceVerified) {
			throw new Error(
				"Archive builds require AO_ARCHIVE_PROVENANCE_VERIFIED=1 after the caller verifies AO_FORK_COMMIT through external release provenance.",
			);
		}
		return requested;
	}

	const headResult = runGit(repoRoot, ["rev-parse", "HEAD"]);
	const head = headResult.stdout?.trim();
	if (headResult.status !== 0 || !head || !FULL_SHA.test(head)) {
		throw new Error("Could not resolve a lowercase full Git HEAD commit for the AO build.");
	}
	if (requested && requested !== head) {
		throw new Error(`AO_FORK_COMMIT ${requested} does not equal checkout HEAD ${head}.`);
	}
	if (mode === "release") {
		const status = runGit(repoRoot, ["status", "--porcelain=v1", "--untracked-files=all"]);
		if (status.status !== 0) throw new Error("Could not inspect the AO checkout before release build.");
		if (status.stdout?.trim()) {
			throw new Error(`Release build refused because the AO checkout is dirty:\n${status.stdout.trimEnd()}`);
		}
	}
	return head;
}

const invokedPath = process.argv[1] ? resolve(process.argv[1]) : "";
if (invokedPath === resolve(fileURLToPath(import.meta.url))) {
	try {
		const repoRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));
		const mode = process.env.AO_BUILD_MODE ?? (process.env.CI ? "release" : "development");
		process.stdout.write(`${resolveBuildProvenance({ repoRoot, mode })}\n`);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
