package sqlite

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/pressly/goose/v3"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

// TestMigrationVersionsAreUnique scans the embedded migration filenames and
// parses each version with goose.NumericComponent — the same function goose
// itself uses — so prefixes that parse to the same int64 (e.g. "014" vs
// "0014") are caught as a collision, not just identical strings. Catches the
// conflict with a clear message instead of a goose panic at runtime.
func TestMigrationVersionsAreUnique(t *testing.T) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}

	seen := map[int64]string{} // parsed version -> filename
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}

		version, err := goose.NumericComponent(name)
		if err != nil {
			t.Errorf("migration %q has no version goose can parse: %v", name, err)
			continue
		}

		if other, dup := seen[version]; dup {
			t.Errorf("duplicate migration version %d: %s vs %s", version, other, name)
			continue
		}
		seen[version] = name
	}
}

func TestAttestedDatabaseSchemaMatchesLatestMigration(t *testing.T) {
	latest := latestEmbeddedMigrationVersion(t)
	if latest != int64(daemonmeta.DatabaseSchemaVersion) {
		t.Fatalf("attested database schema = %d, latest embedded migration = %d", daemonmeta.DatabaseSchemaVersion, latest)
	}
}

func TestDatabaseSchemaConsumersMatchLatestEmbeddedMigration(t *testing.T) {
	latest := latestEmbeddedMigrationVersion(t)
	if backend := int64(daemonmeta.DatabaseSchemaVersion); backend != latest {
		t.Fatalf("backend daemon database schema = %d, latest embedded migration = %d", backend, latest)
	}
	repositoryRoot := filepath.Clean(filepath.Join("..", "..", "..", ".."))
	consumers := []struct {
		name    string
		path    string
		pattern *regexp.Regexp
	}{
		{
			name:    "frontend runtime",
			path:    filepath.Join(repositoryRoot, "frontend", "src", "shared", "daemon-attestation.ts"),
			pattern: regexp.MustCompile(`databaseSchema:[[:space:]]*([0-9]+)`),
		},
		{
			name:    "daemon build",
			path:    filepath.Join(repositoryRoot, "frontend", "scripts", "build-attestation.mjs"),
			pattern: regexp.MustCompile(`databaseSchema:[[:space:]]*([0-9]+)`),
		},
		{
			name:    "documented contract",
			path:    filepath.Join(repositoryRoot, "docs", "superorch-build-attestation.md"),
			pattern: regexp.MustCompile(`"databaseSchema":[[:space:]]*([0-9]+)`),
		},
	}
	for _, consumer := range consumers {
		t.Run(consumer.name, func(t *testing.T) {
			source, err := os.ReadFile(consumer.path)
			if err != nil {
				t.Fatalf("read %s: %v", consumer.path, err)
			}
			match := consumer.pattern.FindSubmatch(source)
			if len(match) != 2 {
				t.Fatalf("database schema expectation missing from %s", consumer.path)
			}
			version, err := strconv.ParseInt(string(match[1]), 10, 64)
			if err != nil {
				t.Fatalf("parse database schema expectation in %s: %v", consumer.path, err)
			}
			if version != latest {
				t.Fatalf("%s database schema = %d, latest embedded migration = %d", consumer.name, version, latest)
			}
		})
	}
}

func latestEmbeddedMigrationVersion(t *testing.T) int64 {
	t.Helper()
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}
	var latest int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		version, err := goose.NumericComponent(entry.Name())
		if err != nil {
			continue
		}
		if version > latest {
			latest = version
		}
	}
	return latest
}
