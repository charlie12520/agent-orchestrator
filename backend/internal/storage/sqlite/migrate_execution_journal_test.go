package sqlite

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestMigration0040ExecutionJournalRoundTripPreservesDB39(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "ao.db")+pragmas)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	upTo(t, db, 39)
	if executionTableExists(t, db, "execution_operation_journal") || executionTableExists(t, db, "execution_run_bindings") {
		t.Fatal("execution tables exist before migration 0040")
	}
	if _, err := db.Exec(`
INSERT INTO integration_merge_leases (
    id, repository, source_repository, pr_number, source_branch,
    expected_head_sha, base_repository, base_branch, merge_strategy,
    check_policy_json, review_policy_json, manual_approval_required,
    capability_digest, status, created_at, expires_at
) VALUES (?, ?, ?, 42, 'feature/execution', ?, ?, 'main', 'squash', '{}', '{}', 0, ?, 'active', ?, ?)
`, "lease-execution-0040", "https://github.com/acme/source-repository", "https://github.com/acme/source-repository",
		strings.Repeat("a", 40), "https://github.com/acme/base-repository", bytes.Repeat([]byte{7}, 32),
		"2026-07-30T12:00:00Z", "2026-07-30T13:00:00Z"); err != nil {
		t.Fatalf("seed DB39 lease: %v", err)
	}

	upTo(t, db, 40)
	for _, table := range []string{"execution_operation_journal", "execution_run_bindings"} {
		if !executionTableExists(t, db, table) {
			t.Fatalf("%s missing after migration 0040", table)
		}
	}
	var schema string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='execution_operation_journal'`).Scan(&schema); err != nil {
		t.Fatalf("read journal schema: %v", err)
	}
	if !strings.Contains(schema, "length(CAST(request_json AS BLOB))") || !strings.Contains(schema, "length(CAST(result_json AS BLOB))") {
		t.Fatalf("migration does not enforce byte limits:\n%s", schema)
	}

	downExecutionMigrationTo(t, db, 39)
	if executionTableExists(t, db, "execution_operation_journal") || executionTableExists(t, db, "execution_run_bindings") {
		t.Fatal("execution tables survived rollback to DB39")
	}
	var leases int
	if err := db.QueryRow(`SELECT COUNT(*) FROM integration_merge_leases WHERE id = 'lease-execution-0040'`).Scan(&leases); err != nil || leases != 1 {
		t.Fatalf("DB39 lease after rollback count=%d err=%v", leases, err)
	}

	upTo(t, db, 40)
	if !executionTableExists(t, db, "execution_operation_journal") || !executionTableExists(t, db, "execution_run_bindings") {
		t.Fatal("execution tables missing after reapplying migration 0040")
	}
	version, err := goose.GetDBVersion(db)
	if err != nil || version != 40 {
		t.Fatalf("goose version=%d err=%v, want 40", version, err)
	}
}

func downExecutionMigrationTo(t *testing.T, db *sql.DB, version int64) {
	t.Helper()
	gooseMu.Lock()
	defer gooseMu.Unlock()
	goose.SetBaseFS(migrationsFS)
	goose.SetLogger(goose.NopLogger())
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("set dialect: %v", err)
	}
	if err := goose.DownTo(db, "migrations", version); err != nil {
		t.Fatalf("migrate down to %d: %v", version, err)
	}
}

func executionTableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?`, name).Scan(&count); err != nil {
		t.Fatalf("check table %s: %v", name, err)
	}
	return count == 1
}
