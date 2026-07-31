#!/usr/bin/env bash
# Cross-compile the Go `ao` binary (backend/cmd/ao) for every supported
# platform and drop each into the matching platform package's bin/ dir.
#
# Run this from any cwd before `npm publish`. It is the ONLY way the binaries
# get into the platform packages; they are gitignored and produced here, then
# shipped in each npm tarball via that package's `files` entry.
#
# CGO-free build (modernc.org/sqlite driver) so cross-compilation needs no C
# toolchain. These binaries are publishable release artifacts, so every one is
# stamped with the exact package version, fork commit, and compatibility mode.
set -euo pipefail

# Repo layout: this script lives at <repo>/packages/build-binaries.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BACKEND_DIR="${REPO_ROOT}/backend"
BUILD_VERSION="${AO_BUILD_VERSION:-$(cd "${REPO_ROOT}" && node -p "require('./frontend/package.json').version")}"
BUILD_MODE="${AO_BUILD_MODE:-release}"
FORK_COMMIT="$(AO_BUILD_MODE="${BUILD_MODE}" node "${REPO_ROOT}/frontend/scripts/build-provenance.mjs")"

if [[ "${BUILD_VERSION}" =~ ^(dev|development|unknown)$ || ! "${BUILD_VERSION}" =~ ^[0-9A-Za-z][0-9A-Za-z._+-]*$ ]]; then
  printf 'AO_BUILD_VERSION must be explicit and linker-safe, got %s\n' "${BUILD_VERSION}" >&2
  exit 1
fi
if [[ ! "${FORK_COMMIT}" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'AO_FORK_COMMIT must be a lowercase full 40-character SHA, got %s\n' "${FORK_COMMIT}" >&2
  exit 1
fi
if [[ "${BUILD_MODE}" != "release" ]]; then
  printf 'npm platform binaries must use AO_BUILD_MODE=release, got %s\n' "${BUILD_MODE}" >&2
  exit 1
fi
BUILD_PACKAGE="github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
LDFLAGS="-X ${BUILD_PACKAGE}.BuildVersion=${BUILD_VERSION} -X ${BUILD_PACKAGE}.ForkCommit=${FORK_COMMIT} -X ${BUILD_PACKAGE}.BuildMode=${BUILD_MODE}"

# pkg_dir : npm_os : npm_arch : GOOS : GOARCH : bin_name
TARGETS=(
  "ao-darwin-arm64:darwin:arm64:darwin:arm64:ao"
  "ao-darwin-x64:darwin:x64:darwin:amd64:ao"
  "ao-win32-x64:win32:x64:windows:amd64:ao.exe"
  "ao-linux-x64:linux:x64:linux:amd64:ao"
)

echo "Building ao binaries from ${BACKEND_DIR}/cmd/ao"
for t in "${TARGETS[@]}"; do
  IFS=":" read -r pkg npm_os npm_arch goos goarch bin <<<"$t"
  out="${SCRIPT_DIR}/${pkg}/bin/${bin}"
  mkdir -p "${SCRIPT_DIR}/${pkg}/bin"
  echo "  -> ${pkg} (GOOS=${goos} GOARCH=${goarch}) -> bin/${bin}"
  (cd "${BACKEND_DIR}" && CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" \
    go build -trimpath -buildvcs=false -ldflags "${LDFLAGS}" -o "${out}" ./cmd/ao)
  chmod 0755 "${out}"
done

echo "Done. Built binaries:"
for t in "${TARGETS[@]}"; do
  IFS=":" read -r pkg _ _ _ _ bin <<<"$t"
  file "${SCRIPT_DIR}/${pkg}/bin/${bin}"
done
