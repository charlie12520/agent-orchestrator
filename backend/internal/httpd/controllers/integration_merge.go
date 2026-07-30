package controllers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apispec"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	integrationmerge "github.com/aoagents/agent-orchestrator/backend/internal/service/integrationmerge"
)

const maxIntegrationMergeBodyBytes = 64 << 10

// IntegrationMergeController owns the root-authenticated deterministic merge
// contract. Production leaves Svc nil until managed control and a separate
// MergeBroker/GitHub App client are both wired.
type IntegrationMergeController struct {
	Svc integrationmerge.Manager
}

func (c *IntegrationMergeController) Register(r chi.Router) {
	r.Post("/integration/merge-leases", c.issueLease)
	r.Post("/integration/merge-leases/{leaseId}/revoke", c.revokeLease)
	r.Post("/integration/merges", c.merge)
}

// IssueIntegrationMergeLeaseRequest is the immutable single-candidate policy
// snapshot authorized by SuperOrch.
type IssueIntegrationMergeLeaseRequest struct {
	Repository             string                         `json:"repository" maxLength:"256"`
	SourceRepository       string                         `json:"sourceRepository" maxLength:"256"`
	PRNumber               int                            `json:"prNumber" minimum:"1" maximum:"1000000000"`
	SourceBranch           string                         `json:"sourceBranch" maxLength:"255"`
	ExpectedHeadSHA        string                         `json:"expectedHeadSha" minLength:"40" maxLength:"40"`
	BaseRepository         string                         `json:"baseRepository" maxLength:"256"`
	BaseBranch             string                         `json:"baseBranch" maxLength:"255"`
	MergeStrategy          domain.MergeStrategy           `json:"mergeStrategy,omitempty" enum:"squash,merge,rebase"`
	CheckPolicy            domain.IntegrationCheckPolicy  `json:"checkPolicy"`
	ReviewPolicy           domain.IntegrationReviewPolicy `json:"reviewPolicy"`
	ManualApprovalRequired bool                           `json:"manualApprovalRequired"`
	ExpiresAt              time.Time                      `json:"expiresAt,omitempty"`
}

// IssueIntegrationMergeLeaseResponse contains the one-time plaintext
// capability. The daemon persists only its SHA-256 digest.
type IssueIntegrationMergeLeaseResponse struct {
	Version                int                            `json:"version"`
	IntegrationLease       string                         `json:"integrationLease"`
	GateCapability         string                         `json:"gateCapability"`
	Repository             string                         `json:"repository"`
	SourceRepository       string                         `json:"sourceRepository"`
	PRNumber               int                            `json:"prNumber"`
	SourceBranch           string                         `json:"sourceBranch"`
	ExpectedHeadSHA        string                         `json:"expectedHeadSha"`
	BaseRepository         string                         `json:"baseRepository"`
	BaseBranch             string                         `json:"baseBranch"`
	MergeStrategy          domain.MergeStrategy           `json:"mergeStrategy"`
	CheckPolicy            domain.IntegrationCheckPolicy  `json:"checkPolicy"`
	ReviewPolicy           domain.IntegrationReviewPolicy `json:"reviewPolicy"`
	ManualApprovalRequired bool                           `json:"manualApprovalRequired"`
	IssuedAt               time.Time                      `json:"issuedAt"`
	ExpiresAt              time.Time                      `json:"expiresAt"`
}

type IntegrationMergeLeaseIDParam struct {
	LeaseID string `path:"leaseId" description:"Opaque SuperOrch-issued integration lease id."`
}

type RevokeIntegrationMergeLeaseResponse struct {
	Version          int       `json:"version"`
	IntegrationLease string    `json:"integrationLease"`
	Status           string    `json:"status" enum:"revoked"`
	RevokedAt        time.Time `json:"revokedAt"`
}

// ConsumeIntegrationMergeRequest carries only operation identity, the bound
// capability, and an optional explicit manual release. Candidate facts are
// always read from the authoritative broker.
type ConsumeIntegrationMergeRequest struct {
	IntegrationLease string `json:"integrationLease" maxLength:"64"`
	GateCapability   string `json:"gateCapability" maxLength:"128"`
	IdempotencyKey   string `json:"idempotencyKey" minLength:"8" maxLength:"128"`
	ManualApproval   bool   `json:"manualApproval"`
}

type ConsumeIntegrationMergeResponse struct {
	Outcome domain.IntegrationMergeOutcome `json:"outcome"`
}

func (c *IntegrationMergeController) issueLease(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/integration/merge-leases")
		return
	}
	var request IssueIntegrationMergeLeaseRequest
	if err := decodeBoundedStrictJSON(w, r, &request); err != nil {
		writeIntegrationMergeInvalidJSON(w, r)
		return
	}
	issued, err := c.Svc.IssueLease(r.Context(), integrationmerge.IssueLeaseInput{
		Repository: request.Repository, SourceRepository: request.SourceRepository,
		PRNumber: request.PRNumber, SourceBranch: request.SourceBranch,
		ExpectedHeadSHA: request.ExpectedHeadSHA, BaseRepository: request.BaseRepository,
		BaseBranch: request.BaseBranch, Strategy: request.MergeStrategy,
		CheckPolicy: request.CheckPolicy, ReviewPolicy: request.ReviewPolicy,
		ManualApprovalRequired: request.ManualApprovalRequired, ExpiresAt: request.ExpiresAt,
	})
	if err != nil {
		writeIntegrationMergeError(w, r, err)
		return
	}
	lease := issued.Lease
	envelope.WriteJSON(w, http.StatusCreated, IssueIntegrationMergeLeaseResponse{
		Version: integrationmerge.ContractVersion, IntegrationLease: lease.ID,
		GateCapability: issued.GateCapability, Repository: lease.Repository,
		SourceRepository: lease.SourceRepository, PRNumber: lease.PRNumber,
		SourceBranch: lease.SourceBranch, ExpectedHeadSHA: lease.ExpectedHeadSHA,
		BaseRepository: lease.BaseRepository, BaseBranch: lease.BaseBranch,
		MergeStrategy: lease.Strategy, CheckPolicy: lease.CheckPolicy,
		ReviewPolicy: lease.ReviewPolicy, ManualApprovalRequired: lease.ManualApprovalRequired,
		IssuedAt: lease.CreatedAt, ExpiresAt: lease.ExpiresAt,
	})
}

func (c *IntegrationMergeController) revokeLease(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/integration/merge-leases/{leaseId}/revoke")
		return
	}
	lease, err := c.Svc.RevokeLease(r.Context(), chi.URLParam(r, "leaseId"))
	if err != nil {
		writeIntegrationMergeError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, RevokeIntegrationMergeLeaseResponse{
		Version: integrationmerge.ContractVersion, IntegrationLease: lease.ID,
		Status: string(lease.Status), RevokedAt: lease.RevokedAt,
	})
}

func (c *IntegrationMergeController) merge(w http.ResponseWriter, r *http.Request) {
	if c.Svc == nil {
		apispec.NotImplemented(w, r, "POST", "/api/v1/integration/merges")
		return
	}
	var request ConsumeIntegrationMergeRequest
	if err := decodeBoundedStrictJSON(w, r, &request); err != nil {
		writeIntegrationMergeInvalidJSON(w, r)
		return
	}
	outcome, err := c.Svc.Merge(r.Context(), integrationmerge.MergeInput{
		LeaseID: request.IntegrationLease, GateCapability: request.GateCapability,
		IdempotencyKey: request.IdempotencyKey, ManualApproval: request.ManualApproval,
	})
	if err != nil {
		writeIntegrationMergeError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, ConsumeIntegrationMergeResponse{Outcome: outcome})
}

func decodeBoundedStrictJSON(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxIntegrationMergeBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeIntegrationMergeInvalidJSON(w http.ResponseWriter, r *http.Request) {
	envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid JSON body", nil)
}

func writeIntegrationMergeError(w http.ResponseWriter, r *http.Request, err error) {
	code, reason, ok := integrationmerge.ErrorInfo(err)
	if !ok {
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "INTEGRATION_MERGE_FAILED", "Integration merge operation failed", nil)
		return
	}
	switch code {
	case integrationmerge.CodeInvalidInput:
		envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_INTEGRATION_MERGE_REQUEST", "Invalid integration merge request", nil)
	case integrationmerge.CodeUnauthorized:
		envelope.WriteAPIError(w, r, http.StatusForbidden, "forbidden", "ROOT_AUTH_REQUIRED", "Root authentication is required", nil)
	case integrationmerge.CodeNotConfigured:
		envelope.WriteAPIError(w, r, http.StatusNotImplemented, "not_implemented", "INTEGRATION_MERGE_NOT_CONFIGURED", "Integration merge is not configured", nil)
	case integrationmerge.CodeLeaseNotFound:
		envelope.WriteAPIError(w, r, http.StatusNotFound, "not_found", "INTEGRATION_LEASE_NOT_FOUND", "Integration lease not found", nil)
	case integrationmerge.CodeLeaseExpired:
		envelope.WriteAPIError(w, r, http.StatusGone, "gone", "INTEGRATION_LEASE_EXPIRED", "Integration lease expired", nil)
	case integrationmerge.CodeActiveLeaseExists, integrationmerge.CodeLeaseConsumed,
		integrationmerge.CodeLeaseRevoked, integrationmerge.CodeCapabilityRejected,
		integrationmerge.CodeIdempotencyConflict:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "INTEGRATION_MERGE_CONFLICT", "Integration merge authorization conflict", nil)
	case integrationmerge.CodeManualApprovalRequired:
		envelope.WriteAPIError(w, r, http.StatusConflict, "conflict", "MANUAL_APPROVAL_REQUIRED", "Manual approval is required", nil)
	case integrationmerge.CodeRevalidationRequired:
		envelope.WriteAPIError(w, r, http.StatusUnprocessableEntity, "unprocessable", "REVALIDATION_REQUIRED", "Candidate revalidation is required", map[string]any{"reason": reason})
	case integrationmerge.CodeBrokerUnavailable:
		envelope.WriteAPIError(w, r, http.StatusServiceUnavailable, "unavailable", "MERGE_BROKER_UNAVAILABLE", "Merge broker is unavailable", nil)
	default:
		envelope.WriteAPIError(w, r, http.StatusInternalServerError, "internal", "INTEGRATION_MERGE_FAILED", "Integration merge operation failed", nil)
	}
}
