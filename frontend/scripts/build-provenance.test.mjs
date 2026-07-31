// @vitest-environment node
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { resolveBuildProvenance } from "./build-provenance.mjs";

const roots = [];

afterEach(() => {
	for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function git(root, ...args) {
	return execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
}

function checkout() {
	const root = mkdtempSync(join(tmpdir(), "ao-provenance-"));
	roots.push(root);
	mkdirSync(join(root, "backend"), { recursive: true });
	mkdirSync(join(root, "frontend"), { recursive: true });
	writeFileSync(join(root, "package.json"), '{"private":true}\n', "utf8");
	writeFileSync(join(root, "backend", "main.go"), "package main\n", "utf8");
	writeFileSync(join(root, "frontend", "package.json"), '{"version":"1.0.0"}\n', "utf8");
	git(root, "init");
	git(root, "config", "user.email", "attestation@example.test");
	git(root, "config", "user.name", "Attestation Test");
	git(root, "add", ".");
	git(root, "commit", "-m", "fixture");
	return { root, head: git(root, "rev-parse", "HEAD") };
}

describe("release build source identity", () => {
	it("accepts a clean checkout and a matching explicit override", () => {
		const { root, head } = checkout();
		expect(resolveBuildProvenance({ repoRoot: root, mode: "release" })).toBe(head);
		expect(resolveBuildProvenance({ repoRoot: root, mode: "release", override: head })).toBe(head);
	});

	it("rejects an override that does not equal checkout HEAD", () => {
		const { root } = checkout();
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release", override: "f".repeat(40) })).toThrow(
			/does not equal checkout HEAD/,
		);
	});

	it("rejects unstaged relevant source changes", () => {
		const { root } = checkout();
		writeFileSync(join(root, "backend", "main.go"), "package changed\n", "utf8");
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release" })).toThrow(/checkout is dirty/);
	});

	it("rejects staged relevant source changes", () => {
		const { root } = checkout();
		writeFileSync(join(root, "backend", "main.go"), "package changed\n", "utf8");
		git(root, "add", "backend/main.go");
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release" })).toThrow(/checkout is dirty/);
	});

	it("rejects untracked relevant source", () => {
		const { root } = checkout();
		writeFileSync(join(root, "backend", "untracked.go"), "package main\n", "utf8");
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release" })).toThrow(/checkout is dirty/);
	});

	it("rejects a modified tracked root build input", () => {
		const { root } = checkout();
		writeFileSync(join(root, "package.json"), '{"private":false}\n', "utf8");
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release" })).toThrow(/checkout is dirty/);
	});

	it("allows dirty relevant source only for an explicitly non-release development build", () => {
		const { root, head } = checkout();
		writeFileSync(join(root, "backend", "main.go"), "package changed\n", "utf8");
		expect(resolveBuildProvenance({ repoRoot: root, mode: "development" })).toBe(head);
	});

	it("rejects untracked root-level or documentation input instead of relying on an incomplete allowlist", () => {
		const { root } = checkout();
		mkdirSync(join(root, "docs"));
		writeFileSync(join(root, "docs", "note.md"), "draft\n", "utf8");
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release" })).toThrow(/checkout is dirty/);
	});

	it("requires explicit externally verified provenance for a source archive", () => {
		const root = mkdtempSync(join(tmpdir(), "ao-archive-"));
		roots.push(root);
		const commit = "a".repeat(40);
		expect(() => resolveBuildProvenance({ repoRoot: root, mode: "release", override: commit })).toThrow(
			/AO_ARCHIVE_PROVENANCE_VERIFIED=1/,
		);
		expect(
			resolveBuildProvenance({
				repoRoot: root,
				mode: "release",
				override: commit,
				archiveProvenanceVerified: true,
			}),
		).toBe(commit);
	});
});
