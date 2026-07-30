package controllers_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	integrationmerge "github.com/aoagents/agent-orchestrator/backend/internal/service/integrationmerge"
)

type fakeIntegrationMergeManager struct {
	issueCalls int
	mergeCalls int
	issueInput integrationmerge.IssueLeaseInput
	mergeInput integrationmerge.MergeInput
	revokeID   string
	issued     integrationmerge.IssuedLease
	revoked    domain.IntegrationMergeLease
	outcome    domain.IntegrationMergeOutcome
	issueErr   error
	revokeErr  error
	mergeErr   error
}

func (f *fakeIntegrationMergeManager) IssueLease(_ context.Context, in integrationmerge.IssueLeaseInput) (integrationmerge.IssuedLease, error) {
	f.issueCalls++
	f.issueInput = in
	return f.issued, f.issueErr
}

func (f *fakeIntegrationMergeManager) RevokeLease(_ context.Context, id string) (domain.IntegrationMergeLease, error) {
	f.revokeID = id
	return f.revoked, f.revokeErr
}

func (f *fakeIntegrationMergeManager) Merge(_ context.Context, in integrationmerge.MergeInput) (domain.IntegrationMergeOutcome, error) {
	f.mergeCalls++
	f.mergeInput = in
	return f.outcome, f.mergeErr
}

func integrationMergeServer(t *testing.T, manager integrationmerge.Manager) *httptest.Server {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := httptest.NewServer(httpd.NewRouterWithControl(config.Config{}, log, nil, httpd.APIDeps{
		IntegrationMerges: manager,
	}, httpd.ControlDeps{}))
	t.Cleanup(srv.Close)
	return srv
}

func TestIntegrationMergeProductionBoundaryAndLegacySeparation(t *testing.T) {
	if daemonmeta.Current().Capabilities["prMerge"] {
		t.Fatal("prMerge attested true without a production MergeBroker/GitHub App")
	}
	srv := integrationMergeServer(t, nil)
	paths := []string{
		"/api/v1/integration/merge-leases",
		"/api/v1/integration/merge-leases/iml_not-configured-001/revoke",
		"/api/v1/integration/merges",
		"/api/v1/prs/42/merge",
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			body, status, _ := doRequest(t, srv, http.MethodPost, path, `{}`)
			assertErrorCode(t, body, status, http.StatusNotImplemented, "NOT_IMPLEMENTED")
		})
	}
}

func TestIntegrationMergeControllerIssueConsumeAndRevoke(t *testing.T) {
	now := time.Date(2026, time.July, 30, 17, 0, 0, 0, time.UTC)
	lease := domain.IntegrationMergeLease{
		ID: "iml_controller-lease-001", Repository: "https://github.com/acme/repo",
		SourceRepository: "https://github.com/acme/repo", PRNumber: 42,
		SourceBranch: "feature/hardened", ExpectedHeadSHA: strings.Repeat("a", 40),
		BaseRepository: "https://github.com/acme/repo", BaseBranch: "main",
		Strategy:               domain.MergeStrategySquash,
		CheckPolicy:            domain.IntegrationCheckPolicy{Revision: "checks-v1", RequiredChecks: []string{"build"}, RequireAllObservedPassing: true},
		ReviewPolicy:           domain.IntegrationReviewPolicy{Revision: "reviews-v1", RequiredApprovals: 1, RequiredReviewers: []string{"alice"}, RequireResolvedThreads: true},
		ManualApprovalRequired: true, Status: domain.IntegrationMergeLeaseActive,
		CreatedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	}
	manager := &fakeIntegrationMergeManager{
		issued: integrationmerge.IssuedLease{Lease: lease, GateCapability: "imc_one-time-capability-value"},
		revoked: func() domain.IntegrationMergeLease {
			out := lease
			out.Status = domain.IntegrationMergeLeaseRevoked
			out.RevokedAt = now.Add(time.Minute)
			return out
		}(),
		outcome: domain.IntegrationMergeOutcome{
			Version: 1, Status: domain.IntegrationMergeOutcomeMerged,
			IdempotencyKey: "controller-operation-01", Repository: lease.Repository,
			SourceRepository: lease.SourceRepository, PRNumber: 42,
			SourceBranch: lease.SourceBranch, ExpectedHeadSHA: lease.ExpectedHeadSHA,
			BaseRepository: lease.BaseRepository, BaseBranch: lease.BaseBranch,
			Strategy: domain.MergeStrategySquash, CheckPolicy: lease.CheckPolicy,
			Checks: []domain.IntegrationCheckEvidence{}, ReviewPolicy: lease.ReviewPolicy,
			Reviews: []domain.IntegrationReviewEvidence{}, MergeCommitSHA: strings.Repeat("b", 40),
			AcceptedAt: now, DispatchedAt: now.Add(time.Second), CompletedAt: now.Add(2 * time.Second),
		},
	}
	srv := integrationMergeServer(t, manager)
	issueBody := `{
		"repository":"https://github.com/acme/repo",
		"sourceRepository":"https://github.com/acme/repo",
		"prNumber":42,
		"sourceBranch":"feature/hardened",
		"expectedHeadSha":"` + strings.Repeat("a", 40) + `",
		"baseRepository":"https://github.com/acme/repo",
		"baseBranch":"main",
		"checkPolicy":{"revision":"checks-v1","requiredChecks":["build"],"requireAllObservedPassing":true},
		"reviewPolicy":{"revision":"reviews-v1","requiredApprovals":1,"requiredReviewers":["alice"],"requireResolvedThreads":true},
		"manualApprovalRequired":true
	}`
	body, status, _ := doRequest(t, srv, http.MethodPost, "/api/v1/integration/merge-leases", issueBody)
	if status != http.StatusCreated || !containsAll(body, `"integrationLease":"iml_controller-lease-001"`, `"gateCapability":"imc_one-time-capability-value"`, `"mergeStrategy":"squash"`) {
		t.Fatalf("issue = %d %s", status, body)
	}
	if manager.issueInput.ExpectedHeadSHA != strings.Repeat("a", 40) || !manager.issueInput.ManualApprovalRequired {
		t.Fatalf("issue input = %#v", manager.issueInput)
	}

	consumeBody := `{"integrationLease":"iml_controller-lease-001","gateCapability":"imc_one-time-capability-value","idempotencyKey":"controller-operation-01","manualApproval":true}`
	body, status, _ = doRequest(t, srv, http.MethodPost, "/api/v1/integration/merges", consumeBody)
	if status != http.StatusOK || !containsAll(body, `"status":"merged"`, `"mergeCommitSha":"`+strings.Repeat("b", 40)+`"`) {
		t.Fatalf("merge = %d %s", status, body)
	}
	if !manager.mergeInput.ManualApproval || manager.mergeInput.IdempotencyKey != "controller-operation-01" {
		t.Fatalf("merge input = %#v", manager.mergeInput)
	}

	body, status, _ = doRequest(t, srv, http.MethodPost, "/api/v1/integration/merge-leases/iml_controller-lease-001/revoke", "")
	if status != http.StatusOK || !containsAll(body, `"status":"revoked"`) || manager.revokeID != "iml_controller-lease-001" {
		t.Fatalf("revoke = %d %s id=%q", status, body, manager.revokeID)
	}
}

func TestIntegrationMergeControllerBoundsAndSafeErrors(t *testing.T) {
	manager := &fakeIntegrationMergeManager{}
	srv := integrationMergeServer(t, manager)
	body, status, _ := doRequest(t, srv, http.MethodPost, "/api/v1/integration/merges", `{"integrationLease":"iml_valid-shape-001","gateCapability":"secret","idempotencyKey":"operation-01","unknown":true}`)
	assertErrorCode(t, body, status, http.StatusBadRequest, "INVALID_JSON")

	manager.mergeErr = &integrationmerge.OperationError{Code: integrationmerge.CodeUnauthorized, Reason: "root_authentication_required"}
	body, status, _ = doRequest(t, srv, http.MethodPost, "/api/v1/integration/merges", `{"integrationLease":"iml_valid-shape-001","gateCapability":"never-print-this-secret","idempotencyKey":"operation-01","manualApproval":false}`)
	assertErrorCode(t, body, status, http.StatusForbidden, "ROOT_AUTH_REQUIRED")
	if strings.Contains(string(body), "never-print-this-secret") {
		t.Fatalf("secret reflected in error: %s", body)
	}

	manager.mergeErr = &integrationmerge.OperationError{Code: integrationmerge.CodeRevalidationRequired, Reason: "merge_conflict"}
	body, status, _ = doRequest(t, srv, http.MethodPost, "/api/v1/integration/merges", `{"integrationLease":"iml_valid-shape-001","gateCapability":"never-print-this-secret","idempotencyKey":"operation-01","manualApproval":false}`)
	assertErrorCode(t, body, status, http.StatusUnprocessableEntity, "REVALIDATION_REQUIRED")
	if !strings.Contains(string(body), `"reason":"merge_conflict"`) || strings.Contains(string(body), "never-print-this-secret") {
		t.Fatalf("unsafe revalidation body: %s", body)
	}

	manager.mergeErr = &integrationmerge.OperationError{Code: integrationmerge.CodeReconciliationInProgress, Reason: "dispatch_reconciliation_in_progress"}
	body, status, _ = doRequest(t, srv, http.MethodPost, "/api/v1/integration/merges", `{"integrationLease":"iml_valid-shape-001","gateCapability":"never-print-this-secret","idempotencyKey":"operation-01","manualApproval":false}`)
	assertErrorCode(t, body, status, http.StatusConflict, "RECONCILIATION_IN_PROGRESS")
	if strings.Contains(string(body), "never-print-this-secret") {
		t.Fatalf("secret reflected in reconciliation response: %s", body)
	}
}

func TestIntegrationMergeControllerRejectsDuplicateAndMalformedJSONBeforeBinding(t *testing.T) {
	manager := &fakeIntegrationMergeManager{}
	srv := integrationMergeServer(t, manager)
	tests := []struct {
		name string
		path string
		body string
	}{
		{
			name: "top-level duplicate",
			path: "/api/v1/integration/merges",
			body: `{"integrationLease":"iml_first-valid-shape","integrationLease":"iml_second-valid-shape","gateCapability":"secret-value","idempotencyKey":"operation-01"}`,
		},
		{
			name: "case-folded duplicate",
			path: "/api/v1/integration/merges",
			body: `{"integrationLease":"iml_first-valid-shape","IntegrationLease":"iml_second-valid-shape","gateCapability":"secret-value","idempotencyKey":"operation-01"}`,
		},
		{
			name: "escaped duplicate",
			path: "/api/v1/integration/merges",
			body: `{"integrationLease":"iml_first-valid-shape","integrationLeas\u0065":"iml_second-valid-shape","gateCapability":"secret-value","idempotencyKey":"operation-01"}`,
		},
		{
			name: "nested check-policy duplicate",
			path: "/api/v1/integration/merge-leases",
			body: `{"checkPolicy":{"revision":"checks-v1","revision":"checks-v2"}}`,
		},
		{
			name: "nested review-policy duplicate",
			path: "/api/v1/integration/merge-leases",
			body: `{"reviewPolicy":{"requiredApprovals":1,"requiredApprovals":2}}`,
		},
		{
			name: "trailing value",
			path: "/api/v1/integration/merges",
			body: `{ } { }`,
		},
		{
			name: "wrong field type",
			path: "/api/v1/integration/merges",
			body: `{"integrationLease":"iml_valid-shape-001","gateCapability":"secret-value","idempotencyKey":"operation-01","manualApproval":"false"}`,
		},
		{
			name: "null body",
			path: "/api/v1/integration/merges",
			body: `null`,
		},
		{
			name: "array body",
			path: "/api/v1/integration/merges",
			body: `[]`,
		},
		{
			name: "unknown field",
			path: "/api/v1/integration/merges",
			body: `{"integrationLease":"iml_valid-shape-001","gateCapability":"secret-value","idempotencyKey":"operation-01","unknown":true}`,
		},
		{
			name: "excessive nesting",
			path: "/api/v1/integration/merges",
			body: `{"unknown":` + strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34) + "}",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			body, status, _ := doRequest(t, srv, http.MethodPost, tc.path, tc.body)
			assertErrorCode(t, body, status, http.StatusBadRequest, "INVALID_JSON")
		})
	}
	if manager.issueCalls != 0 || manager.mergeCalls != 0 {
		t.Fatalf("malformed JSON reached manager: issue=%d merge=%d", manager.issueCalls, manager.mergeCalls)
	}
}
