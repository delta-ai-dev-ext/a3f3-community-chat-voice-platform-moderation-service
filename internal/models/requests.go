package models

// CreateSanctionRequest is the body of POST /communities/{id}/sanctions.
type CreateSanctionRequest struct {
	TargetUserID    string       `json:"target_user_id"`
	Type            SanctionType `json:"type"`
	Reason          *string      `json:"reason"`
	DurationSeconds *int64       `json:"duration_seconds"`
}

// LiftSanctionRequest is the optional body of the lift endpoint.
type LiftSanctionRequest struct {
	Reason *string `json:"reason"`
}

// CreateAppealRequest is the body of POST /sanctions/{id}/appeals.
type CreateAppealRequest struct {
	Statement string `json:"statement"`
}

// AppealDecisionRequest is the body of the appeal decision endpoint.
type AppealDecisionRequest struct {
	Decision Decision `json:"decision"`
	Note     *string  `json:"note"`
}

// SanctionPage is a cursor page of sanctions.
type SanctionPage struct {
	Data       []Sanction `json:"data"`
	NextCursor *string    `json:"next_cursor"`
}

// MySanctionPage is the caller's active sanctions. It is not paginated because
// a member's active sanction set is small and fully actionable.
type MySanctionPage struct {
	Data []MySanction `json:"data"`
}

// AppealPage is a cursor page of appeals.
type AppealPage struct {
	Data       []Appeal `json:"data"`
	NextCursor *string  `json:"next_cursor"`
}

// AuditLogPage is a cursor page of audit entries.
type AuditLogPage struct {
	Data       []AuditLogEntry `json:"data"`
	NextCursor *string         `json:"next_cursor"`
}

// ErrorResponse is the platform error shape returned by every failing request.
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail"`
}

// HealthStatus is the probe response body.
type HealthStatus struct {
	Status string `json:"status"`
}

// NewHealthStatus returns the only probe payload shape the API documents.
func NewHealthStatus() HealthStatus { return HealthStatus{Status: "ok"} }
