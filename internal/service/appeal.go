package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/internal/repository"
)

// Free-text bounds of the appeal schema.
const (
	maxStatementLength    = 2000
	maxDecisionNoteLength = 2000
)

// AppealService implements member appeal submission and moderator review,
// including the overturn cascade that lifts the underlying sanction.
type AppealService struct {
	deps Deps
}

// NewAppealService builds the appeal service.
func NewAppealService(deps Deps) *AppealService { return &AppealService{deps: deps} }

// Submit records one open appeal for an active mute or ban owned by the caller.
func (s *AppealService) Submit(ctx context.Context, actorUserID, sanctionID string, request models.CreateAppealRequest, idempotencyKey string) (*models.Appeal, error) {
	if err := requireUUID(sanctionID, "sanction_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	statement := normalizeText(&request.Statement)
	if statement == nil {
		return nil, models.ErrInvalidRequest.WithDetail("statement must not be empty.")
	}
	if len(*statement) > maxStatementLength {
		return nil, models.ErrStatementTooLong
	}

	now := s.deps.now()
	scope := idempotencyScope("appeal.create", actorUserID)
	if replayed, err := s.replay(ctx, scope, idempotencyKey); err != nil || replayed != nil {
		return replayed, err
	}

	sanction, err := s.deps.Store.Read().Sanctions.GetByID(ctx, sanctionID, now)
	if err != nil {
		return nil, err
	}
	if sanction.TargetUserID != actorUserID {
		return nil, models.ErrNotSanctionTarget
	}
	if sanction.Type == models.SanctionTypeKick {
		return nil, models.ErrSanctionNotAppealable
	}
	if !sanction.Active(now) {
		return nil, models.ErrSanctionNotActive
	}

	appeal := &models.Appeal{
		ID:              models.NewUUID(),
		SanctionID:      sanction.ID,
		CommunityID:     sanction.CommunityID,
		AppellantUserID: actorUserID,
		Statement:       *statement,
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       now,
	}

	message, err := s.deps.builder().AppealStatusChanged(events.Meta{
		TraceID:       traceIDOf(ctx),
		CorrelationID: models.NewUUID(),
	}, events.AppealStatusChangedPayload{
		AppealID:        appeal.ID,
		SanctionID:      sanction.ID,
		CommunityID:     sanction.CommunityID,
		AppellantUserID: actorUserID,
		SanctionType:    sanction.Type,
		Status:          models.AppealStatusSubmitted,
	})
	if err != nil {
		return nil, fmt.Errorf("build appeal_status_changed event: %w", err)
	}

	err = s.deps.Store.InTx(ctx, func(ctx context.Context, repos repositories) error {
		// Pre-check for a clear 409; the partial unique index is the race-safe
		// authority and maps to the same error in the repository.
		existing, err := repos.Appeals.FindOpenBySanction(ctx, sanction.ID)
		if err != nil {
			return err
		}
		if existing != nil {
			return models.ErrAppealAlreadyOpen
		}
		if err := repos.Appeals.Create(ctx, appeal); err != nil {
			return err
		}
		if err := s.deps.appendEvent(ctx, repos, message, sanction.CommunityID, now); err != nil {
			return err
		}
		return storeResponse(ctx, repos, scope, idempotencyKey, request, 201, appeal, now)
	})
	if err != nil {
		return nil, err
	}

	s.deps.Metrics.RecordAppeal(string(models.AppealStatusSubmitted))
	s.deps.logger().Info("appeal submitted",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.String("appeal_id", appeal.ID),
		zap.String("sanction_id", sanction.ID),
		zap.String("operation", "appeal_submit"),
		zap.String("status", "accepted"),
	)
	return appeal, nil
}

// decidePlan holds everything a decision writes, built before the transaction
// so the unit of work only performs writes.
type decidePlan struct {
	// outcome is the resulting appeal status.
	outcome models.AppealStatus
	// appealMessage is the EVT-006 event of the decision.
	appealMessage events.Message
	// liftMessage is the EVT-004 event, present only on overturn.
	liftMessage *events.Message
	// liftEntry is the sanction_lifted audit entry, present only on overturn.
	liftEntry *models.AuditLogEntry
	// decisionEntry is the appeal_decided audit entry.
	decisionEntry *models.AuditLogEntry
	// decided is the appeal projected with the decision applied.
	decided *models.Appeal
}

// Decide upholds or overturns an open appeal. An overturn lifts the sanction
// with lift_source appeal and emits both EVT-004 and EVT-006 in one transaction.
func (s *AppealService) Decide(ctx context.Context, actorUserID, communityID, appealID string, request models.AppealDecisionRequest, idempotencyKey string) (*models.Appeal, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(appealID, "appeal_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	if !request.Decision.Valid() {
		return nil, models.ErrInvalidRequest.WithDetail("decision must be one of upheld, overturned.")
	}
	note := normalizeText(request.Note)
	if note != nil && len(*note) > maxDecisionNoteLength {
		return nil, models.ErrDecisionNoteTooLong
	}

	now := s.deps.now()
	scope := idempotencyScope("appeal.decide", communityID, actorUserID)
	if replayed, err := s.replay(ctx, scope, idempotencyKey); err != nil || replayed != nil {
		return replayed, err
	}

	repos := s.deps.Store.Read()
	appeal, err := repos.Appeals.GetInCommunity(ctx, communityID, appealID)
	if err != nil {
		return nil, err
	}
	if appeal.Status != models.AppealStatusSubmitted {
		return nil, models.ErrAppealAlreadyDecided
	}

	// A reviewer must outrank the appellant, so the API-001 target is the member
	// who was sanctioned.
	appellantUserID := appeal.AppellantUserID
	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionReviewAppeals, &appellantUserID); err != nil {
		return nil, err
	}

	sanction, err := repos.Sanctions.GetByID(ctx, appeal.SanctionID, now)
	if err != nil {
		return nil, err
	}

	plan, err := s.planDecision(ctx, actorUserID, communityID, appeal, sanction, request.Decision, note, now)
	if err != nil {
		return nil, err
	}

	lifted := false
	err = s.deps.Store.InTx(ctx, func(ctx context.Context, repos repositories) error {
		// Recording the decision first serializes concurrent decisions through
		// the status = 'submitted' guard, so only one caller mutates state.
		if err := repos.Appeals.Decide(ctx, appeal.ID, repository.AppealDecision{
			Status:         plan.outcome,
			ReviewerUserID: actorUserID,
			Note:           note,
			DecidedAt:      now,
		}); err != nil {
			return err
		}
		if plan.liftMessage != nil {
			if err := repos.Sanctions.MarkLifted(ctx, sanction.ID, repositoryLift(now, models.SourceAppeal, note)); err != nil {
				if err != models.ErrSanctionNotActive {
					return err
				}
				// A concurrent moderator lift already ended the sanction: the
				// appeal is still decided but no second lift event is emitted.
			} else {
				lifted = true
				if err := s.deps.appendEvent(ctx, repos, *plan.liftMessage, communityID, now); err != nil {
					return err
				}
				if err := repos.Audit.Append(ctx, plan.liftEntry); err != nil {
					return err
				}
			}
		}
		if err := repos.Audit.Append(ctx, plan.decisionEntry); err != nil {
			return err
		}
		if err := s.deps.appendEvent(ctx, repos, plan.appealMessage, communityID, now); err != nil {
			return err
		}
		return storeResponse(ctx, repos, scope, idempotencyKey, request, 200, plan.decided, now)
	})
	if err != nil {
		return nil, err
	}

	s.deps.Metrics.RecordAppeal(string(plan.outcome))
	if plan.decided.DecidedAt != nil {
		s.deps.Metrics.AppealDecisionSeconds.Observe(plan.decided.DecidedAt.Sub(appeal.CreatedAt).Seconds())
	}
	if lifted {
		s.deps.Metrics.RecordSanction(string(sanction.Type), string(models.SourceAppeal))
	}
	s.deps.logger().Info("appeal decided",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.String("appeal_id", appeal.ID),
		zap.String("sanction_id", sanction.ID),
		zap.String("decision", string(plan.outcome)),
		zap.String("operation", "appeal_decide"),
		zap.String("status", "accepted"),
	)
	return plan.decided, nil
}

// planDecision builds every message and audit row of a decision.
func (s *AppealService) planDecision(ctx context.Context, actorUserID, communityID string, appeal *models.Appeal, sanction *models.Sanction, decision models.Decision, note *string, now time.Time) (*decidePlan, error) {
	outcome := decision.Status()
	plan := &decidePlan{
		outcome: outcome,
		decided: decidedAppeal(appeal, outcome, actorUserID, note, now),
	}

	appealMessage, err := s.deps.builder().AppealStatusChanged(events.Meta{
		TraceID:       traceIDOf(ctx),
		CorrelationID: appeal.ID,
	}, events.AppealStatusChangedPayload{
		AppealID:        appeal.ID,
		SanctionID:      sanction.ID,
		CommunityID:     communityID,
		AppellantUserID: appeal.AppellantUserID,
		SanctionType:    sanction.Type,
		Status:          outcome,
		ReviewerUserID:  stringPtr(actorUserID),
		DecisionNote:    note,
	})
	if err != nil {
		return nil, fmt.Errorf("build appeal_status_changed event: %w", err)
	}
	plan.appealMessage = appealMessage

	targetUserID := sanction.TargetUserID
	if outcome == models.AppealStatusOverturned {
		liftMessage, err := s.deps.builder().MemberSanctionChanged(events.Meta{
			TraceID:       traceIDOf(ctx),
			CorrelationID: appeal.ID,
		}, events.MemberSanctionChangedPayload{
			SanctionID:   sanction.ID,
			CommunityID:  communityID,
			TargetUserID: sanction.TargetUserID,
			ActorUserID:  stringPtr(actorUserID),
			Action:       sanction.Type.LiftedAction(),
			Source:       models.SourceAppeal,
			Reason:       note,
			Appealable:   false,
		})
		if err != nil {
			return nil, fmt.Errorf("build member_sanction_changed event: %w", err)
		}
		plan.liftMessage = &liftMessage

		liftEntry, err := newAuditEntry(now, models.AuditActionSanctionLifted, communityID, stringPtr(actorUserID), &targetUserID, note, map[string]any{
			"sanction_id":     sanction.ID,
			"sanction_action": sanction.Type.LiftedAction(),
			"source":          string(models.SourceAppeal),
			"appeal_id":       appeal.ID,
		})
		if err != nil {
			return nil, err
		}
		plan.liftEntry = liftEntry
	}

	decisionEntry, err := newAuditEntry(now, models.AuditActionAppealDecided, communityID, stringPtr(actorUserID), &targetUserID, note, map[string]any{
		"appeal_id":   appeal.ID,
		"sanction_id": sanction.ID,
		"decision":    string(outcome),
	})
	if err != nil {
		return nil, err
	}
	plan.decisionEntry = decisionEntry
	return plan, nil
}

// ListCommunity returns the community appeal queue newest first.
func (s *AppealService) ListCommunity(ctx context.Context, actorUserID, communityID string, status *models.AppealStatus, page models.Pagination) (*models.AppealPage, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if status != nil && !status.Valid() {
		return nil, models.ErrInvalidRequest.WithDetail("status must be one of submitted, upheld, overturned.")
	}
	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionReviewAppeals, nil); err != nil {
		return nil, err
	}
	rows, hasMore, err := s.deps.Store.Read().Appeals.List(ctx, repository.AppealFilter{
		CommunityID: communityID,
		Status:      status,
		Page:        page,
	})
	if err != nil {
		return nil, err
	}
	return newAppealPage(rows, hasMore), nil
}

// ListMine returns the caller's own appeals with their outcomes. No API-001 call
// is needed because the caller only ever sees their own rows.
func (s *AppealService) ListMine(ctx context.Context, actorUserID string, page models.Pagination) (*models.AppealPage, error) {
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	appellant := actorUserID
	rows, hasMore, err := s.deps.Store.Read().Appeals.List(ctx, repository.AppealFilter{
		AppellantUserID: &appellant,
		Page:            page,
	})
	if err != nil {
		return nil, err
	}
	return newAppealPage(rows, hasMore), nil
}

// replay returns the first accepted response for an Idempotency-Key.
func (s *AppealService) replay(ctx context.Context, scope, key string) (*models.Appeal, error) {
	if key == "" {
		return nil, nil
	}
	var appeal models.Appeal
	found, err := replayResponse(ctx, s.deps.Store.Read(), scope, key, &appeal)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &appeal, nil
}

// newAppealPage projects rows into a cursor page.
func newAppealPage(rows []models.Appeal, hasMore bool) *models.AppealPage {
	return &models.AppealPage{
		Data: rows,
		NextCursor: models.NextCursor(rows, hasMore, func(appeal models.Appeal) models.Cursor {
			return models.Cursor{Timestamp: appeal.CreatedAt, ID: appeal.ID}
		}),
	}
}

// decidedAppeal returns the appeal with the decision applied.
func decidedAppeal(appeal *models.Appeal, outcome models.AppealStatus, reviewerUserID string, note *string, at time.Time) *models.Appeal {
	decided := *appeal
	decided.Status = outcome
	decided.ReviewerUserID = stringPtr(reviewerUserID)
	decided.DecisionNote = note
	decided.DecidedAt = &at
	return &decided
}
