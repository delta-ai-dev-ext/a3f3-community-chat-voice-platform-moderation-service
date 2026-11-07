package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// maxDurationSeconds caps a timed mute at one year, matching the API contract.
const maxDurationSeconds = 31536000

// SanctionService implements issue, lift, expiry-supporting reads, and the
// member-facing sanction status of this service.
type SanctionService struct {
	deps Deps
}

// NewSanctionService builds the sanction service.
func NewSanctionService(deps Deps) *SanctionService { return &SanctionService{deps: deps} }

// Issue records a mute, kick, or ban after a real-time authorization check and
// commits the sanction, its audit entry, and its EVT-004 event in one
// transaction.
func (s *SanctionService) Issue(ctx context.Context, actorUserID, communityID string, request models.CreateSanctionRequest, idempotencyKey string) (*models.Sanction, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	if err := requireUUID(request.TargetUserID, "target_user_id"); err != nil {
		return nil, err
	}
	if !request.Type.Valid() {
		return nil, models.ErrInvalidRequest.WithDetail("type must be one of mute, kick, ban.")
	}
	reason := normalizeText(request.Reason)
	if reason != nil && len(*reason) > 1000 {
		return nil, models.ErrReasonTooLong
	}
	// Only mutes have a duration; kicks and bans are instantaneous/permanent.
	switch request.Type {
	case models.SanctionTypeKick:
		if request.DurationSeconds != nil {
			return nil, models.ErrKickNoDuration
		}
	case models.SanctionTypeBan:
		if request.DurationSeconds != nil {
			return nil, models.ErrBanNoDuration
		}
	case models.SanctionTypeMute:
		if request.DurationSeconds != nil && (*request.DurationSeconds < 1 || *request.DurationSeconds > maxDurationSeconds) {
			return nil, models.ErrInvalidRequest.WithDetailf("duration_seconds must be between 1 and %d.", maxDurationSeconds)
		}
	}

	now := s.deps.now()
	scope := idempotencyScope("sanction.create", communityID, actorUserID)
	if replayed, err := s.replay(ctx, scope, idempotencyKey); err != nil || replayed != nil {
		return replayed, err
	}

	targetUserID := request.TargetUserID
	if err := s.deps.authorize(ctx, actorUserID, communityID, request.Type.Permission(), &targetUserID); err != nil {
		return nil, err
	}

	sanction := &models.Sanction{
		ID:           models.NewUUID(),
		CommunityID:  communityID,
		TargetUserID: request.TargetUserID,
		ActorUserID:  stringPtr(actorUserID),
		Type:         request.Type,
		Reason:       reason,
		Source:       models.SourceModerator,
		IssuedAt:     now,
	}
	if request.Type == models.SanctionTypeMute && request.DurationSeconds != nil {
		expiresAt := now.Add(time.Duration(*request.DurationSeconds) * time.Second)
		sanction.ExpiresAt = &expiresAt
	}
	sanction.Status = sanction.ComputeStatus(now)

	message, err := s.deps.builder().MemberSanctionChanged(events.Meta{
		TraceID:       traceIDOf(ctx),
		CorrelationID: models.NewUUID(),
	}, events.MemberSanctionChangedPayload{
		SanctionID:   sanction.ID,
		CommunityID:  communityID,
		TargetUserID: sanction.TargetUserID,
		ActorUserID:  sanction.ActorUserID,
		Action:       string(sanction.Type),
		Source:       models.SourceModerator,
		Reason:       sanction.Reason,
		ExpiresAt:    sanction.ExpiresAt,
		Appealable:   sanction.Type == models.SanctionTypeMute || sanction.Type == models.SanctionTypeBan,
	})
	if err != nil {
		return nil, fmt.Errorf("build member_sanction_changed event: %w", err)
	}

	auditEntry, err := newAuditEntry(now, models.AuditActionSanctionIssued, communityID, sanction.ActorUserID, &targetUserID, reason, map[string]any{
		"sanction_id":     sanction.ID,
		"sanction_action": string(sanction.Type),
		"source":          string(models.SourceModerator),
	})
	if err != nil {
		return nil, err
	}

	err = s.deps.Store.InTx(ctx, func(ctx context.Context, repos repositories) error {
		if err := repos.Sanctions.Create(ctx, sanction); err != nil {
			return err
		}
		if err := repos.Audit.Append(ctx, auditEntry); err != nil {
			return err
		}
		if err := s.deps.appendEvent(ctx, repos, message, communityID, now); err != nil {
			return err
		}
		return storeResponse(ctx, repos, scope, idempotencyKey, request, 201, sanction, now)
	})
	if err != nil {
		return nil, err
	}

	s.deps.Metrics.RecordSanction(string(sanction.Type), string(models.SourceModerator))
	s.deps.logger().Info("sanction issued",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.String("sanction_id", sanction.ID),
		zap.String("community_id", communityID),
		zap.String("sanction_type", string(sanction.Type)),
		zap.String("operation", "sanction_issue"),
		zap.String("status", "accepted"),
	)
	return sanction, nil
}

// Lift unmutes or unbans a member. Kicks cannot be lifted and an inactive
// sanction cannot be lifted twice.
func (s *SanctionService) Lift(ctx context.Context, actorUserID, communityID, sanctionID string, request models.LiftSanctionRequest) (*models.Sanction, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(sanctionID, "sanction_id"); err != nil {
		return nil, err
	}
	reason := normalizeText(request.Reason)
	if reason != nil && len(*reason) > 1000 {
		return nil, models.ErrReasonTooLong
	}

	now := s.deps.now()
	sanction, err := s.deps.Store.Read().Sanctions.GetByIDInCommunity(ctx, communityID, sanctionID, now)
	if err != nil {
		return nil, err
	}
	if sanction.Type == models.SanctionTypeKick {
		return nil, models.ErrKickNotLiftable
	}
	if !sanction.Active(now) {
		return nil, models.ErrSanctionNotActive
	}

	targetUserID := sanction.TargetUserID
	if err := s.deps.authorize(ctx, actorUserID, communityID, sanction.Type.Permission(), &targetUserID); err != nil {
		return nil, err
	}

	message, err := s.deps.builder().MemberSanctionChanged(events.Meta{
		TraceID:       traceIDOf(ctx),
		CorrelationID: models.NewUUID(),
	}, events.MemberSanctionChangedPayload{
		SanctionID:   sanction.ID,
		CommunityID:  communityID,
		TargetUserID: sanction.TargetUserID,
		ActorUserID:  stringPtr(actorUserID),
		Action:       sanction.Type.LiftedAction(),
		Source:       models.SourceModerator,
		Reason:       reason,
		Appealable:   false,
	})
	if err != nil {
		return nil, fmt.Errorf("build member_sanction_changed event: %w", err)
	}

	auditEntry, err := newAuditEntry(now, models.AuditActionSanctionLifted, communityID, stringPtr(actorUserID), &targetUserID, reason, map[string]any{
		"sanction_id":     sanction.ID,
		"sanction_action": sanction.Type.LiftedAction(),
		"source":          string(models.SourceModerator),
	})
	if err != nil {
		return nil, err
	}

	liftSource := models.SourceModerator
	err = s.deps.Store.InTx(ctx, func(ctx context.Context, repos repositories) error {
		if err := repos.Sanctions.MarkLifted(ctx, sanction.ID, repositoryLift(now, liftSource, reason)); err != nil {
			return err
		}
		if err := repos.Audit.Append(ctx, auditEntry); err != nil {
			return err
		}
		return s.deps.appendEvent(ctx, repos, message, communityID, now)
	})
	if err != nil {
		return nil, err
	}

	sanction.LiftedAt = &now
	sanction.LiftSource = &liftSource
	sanction.LiftReason = reason
	sanction.Status = sanction.ComputeStatus(now)
	s.deps.logger().Info("sanction lifted",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.String("sanction_id", sanction.ID),
		zap.String("community_id", communityID),
		zap.String("operation", "sanction_lift"),
		zap.String("status", "accepted"),
	)
	return sanction, nil
}

// List returns a community's sanctions for moderators, newest first. Reading
// the moderation list is itself a moderator action, so it requires the lowest
// moderation permission.
func (s *SanctionService) List(ctx context.Context, actorUserID, communityID string, status *models.SanctionStatus, page models.Pagination) (*models.SanctionPage, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if status != nil && !status.Valid() {
		return nil, models.ErrInvalidRequest.WithDetail("status must be one of active, lifted, expired.")
	}
	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionMuteMembers, nil); err != nil {
		return nil, err
	}

	now := s.deps.now()
	rows, hasMore, err := s.deps.Store.Read().Sanctions.List(ctx, repositorySanctionFilter(communityID, status, page, now))
	if err != nil {
		return nil, err
	}
	return &models.SanctionPage{
		Data: rows,
		NextCursor: models.NextCursor(rows, hasMore, func(sanction models.Sanction) models.Cursor {
			return models.Cursor{Timestamp: sanction.IssuedAt, ID: sanction.ID}
		}),
	}, nil
}

// replay returns the first accepted response for an Idempotency-Key, or nil
// when the key is new and the write must be performed (US-015).
func (s *SanctionService) replay(ctx context.Context, scope, key string) (*models.Sanction, error) {
	if key == "" {
		return nil, nil
	}
	var sanction models.Sanction
	found, err := replayResponse(ctx, s.deps.Store.Read(), scope, key, &sanction)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &sanction, nil
}

// Mine returns the caller's active sanctions across every community, including
// bans for communities the caller has left. It needs no API-001 call because it
// only ever exposes the caller's own rows (ADR-001).
func (s *SanctionService) Mine(ctx context.Context, actorUserID string) (*models.MySanctionPage, error) {
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	repos := s.deps.Store.Read()
	sanctions, err := repos.Sanctions.ActiveByTarget(ctx, actorUserID, s.deps.now())
	if err != nil {
		return nil, err
	}
	openAppeals, err := repos.Appeals.OpenByTarget(ctx, actorUserID)
	if err != nil {
		return nil, err
	}
	for i := range sanctions {
		if appealID, ok := openAppeals[sanctions[i].ID]; ok {
			open := appealID
			sanctions[i].OpenAppealID = &open
			sanctions[i].Appealable = false
		}
	}
	if sanctions == nil {
		sanctions = []models.MySanction{}
	}
	return &models.MySanctionPage{Data: sanctions}, nil
}
