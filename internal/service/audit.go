package service

import (
	"context"

	"moderation_service/internal/models"
	"moderation_service/internal/repository"
)

// AuditService exposes the append-only audit log for reading. Writes happen
// inside the transaction of the action they record (ADR-002); the retention
// purge is a scheduled job.
type AuditService struct {
	deps Deps
}

// NewAuditService builds the audit service.
func NewAuditService(deps Deps) *AuditService { return &AuditService{deps: deps} }

// List returns one page of the community audit log, newest first, always
// filtered to the retention window even before the daily purge has run.
func (s *AuditService) List(ctx context.Context, actorUserID, communityID string, action *models.AuditAction, actorFilter *string, page models.Pagination) (*models.AuditLogPage, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if action != nil && !action.Valid() {
		return nil, models.ErrInvalidRequest.WithDetail("action_type must be a known audit action.")
	}
	if actorFilter != nil && *actorFilter != "" {
		if err := requireUUID(*actorFilter, "actor"); err != nil {
			return nil, err
		}
	}
	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionViewAuditLog, nil); err != nil {
		return nil, err
	}

	since := s.deps.now().Add(-models.AuditRetention)
	entries, hasMore, err := s.deps.Store.Read().Audit.List(ctx, repository.AuditFilter{
		CommunityID: communityID,
		Action:      action,
		ActorUserID: actorFilter,
		Since:       since,
		Page:        page,
	})
	if err != nil {
		return nil, err
	}
	return &models.AuditLogPage{
		Data: entries,
		NextCursor: models.NextCursor(entries, hasMore, func(entry models.AuditLogEntry) models.Cursor {
			return models.Cursor{Timestamp: entry.CreatedAt, ID: entry.ID}
		}),
	}, nil
}
