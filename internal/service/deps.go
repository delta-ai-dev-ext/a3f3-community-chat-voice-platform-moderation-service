// Package service holds the business rules of the moderation domain. Services
// depend on repository ports and the authorization client; they never touch gin
// contexts and never build SQL.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"moderation_service/internal/authz"
	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/internal/repository"
)

// Deps bundles the collaborators shared by every service. A single struct keeps
// constructors stable as the domain grows.
type Deps struct {
	// Store is the repository seam (pool reads plus explicit transactions).
	Store repository.Store
	// Authorizer performs the real-time, fail-closed API-001 check.
	Authorizer authz.Checker
	// Events builds the mandatory envelope for every published event.
	Events *events.Builder
	// Topic is the Kafka topic every event is written to.
	Topic string
	// Logger is the structured JSON logger.
	Logger *zap.Logger
	// Metrics holds the business and technical metric handles.
	Metrics *observability.Metrics
	// Now is the injected clock.
	Now func() time.Time
}

// now returns the injected clock value in UTC.
func (d Deps) now() time.Time {
	if d.Now == nil {
		return time.Now().UTC()
	}
	return d.Now().UTC()
}

// logger returns a non-nil logger so services never branch on nil.
func (d Deps) logger() *zap.Logger {
	if d.Logger == nil {
		return zap.NewNop()
	}
	return d.Logger
}

// builder returns the envelope builder, defaulting to a real one.
func (d Deps) builder() *events.Builder {
	if d.Events == nil {
		return events.NewBuilder()
	}
	return d.Events
}

// authorize runs the real-time API-001 authorization check before a moderator
// action. It fails closed: an unreachable provider returns
// ErrAuthorizationUnavailable and the caller performs no write (ADR-001).
func (d Deps) authorize(ctx context.Context, actorUserID, communityID string, permission models.Permission, targetUserID *string) error {
	if d.Authorizer == nil {
		return models.ErrAuthorizationUnavailable
	}
	decision, err := d.Authorizer.Check(ctx, communityID, authz.CheckRequest{
		ActorUserID:  actorUserID,
		Permission:   permission,
		TargetUserID: targetUserID,
	})
	if err != nil {
		// ErrAuthorizationUnavailable / ErrCommunityNotFound are already domain
		// errors; anything unexpected still fails closed.
		if domainErr := models.AsError(err); domainErr != nil {
			return domainErr
		}
		return models.ErrAuthorizationUnavailable
	}
	if !decision.Allowed {
		return models.DenialError(decision.DenialReason)
	}
	return nil
}

// stringPtr returns a pointer to value, or nil when the value is empty.
func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

// newAuditEntry builds an append-only audit row. detail is marshalled as jsonb
// so the recorded context stays queryable.
func newAuditEntry(now time.Time, action models.AuditAction, communityID string, actorUserID, targetUserID *string, reason *string, detail any) (*models.AuditLogEntry, error) {
	entry := &models.AuditLogEntry{
		ID:           models.NewUUID(),
		CommunityID:  communityID,
		ActorUserID:  actorUserID,
		TargetUserID: targetUserID,
		Action:       action,
		Reason:       reason,
		CreatedAt:    now.UTC(),
	}
	if detail != nil {
		encoded, err := json.Marshal(detail)
		if err != nil {
			return nil, fmt.Errorf("encode audit detail: %w", err)
		}
		entry.Detail = encoded
	}
	return entry, nil
}

// appendEvent converts a message into an outbox row and stages it in the
// caller's transaction, so the event is committed with the state change it
// describes.
func (d Deps) appendEvent(ctx context.Context, repos repository.Repositories, message events.Message, communityID string, now time.Time) error {
	row, err := message.OutboxRow(d.Topic, communityID, now)
	if err != nil {
		return models.ErrInternal.WithDetail("An unexpected error occurred.")
	}
	if err := repos.Outbox.Append(ctx, row); err != nil {
		return fmt.Errorf("append outbox event: %w", err)
	}
	return nil
}

// traceIDOf returns the trace id propagated on the request context.
func traceIDOf(ctx context.Context) string { return observability.TraceID(ctx) }

// repositories aliases the repository bundle so transaction callbacks read
// tersely.
type repositories = repository.Repositories

// repositoryLift builds the lifecycle transition applied to a lifted sanction.
func repositoryLift(at time.Time, source models.SanctionSource, reason *string) repository.LiftDetails {
	return repository.LiftDetails{At: at, Source: source, Reason: reason}
}

// repositorySanctionFilter builds a moderator listing filter.
func repositorySanctionFilter(communityID string, status *models.SanctionStatus, page models.Pagination, now time.Time) repository.SanctionFilter {
	return repository.SanctionFilter{CommunityID: communityID, Status: status, Page: page, Now: now}
}
