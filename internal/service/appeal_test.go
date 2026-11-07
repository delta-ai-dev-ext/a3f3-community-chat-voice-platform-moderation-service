package service_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/authz"
	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/service"
)

// activeBan returns an active ban owned by the test target.
func activeBan() models.Sanction {
	return models.Sanction{
		ID:           sanctionID,
		CommunityID:  communityID,
		TargetUserID: targetID,
		ActorUserID:  ptr(moderatorID),
		Type:         models.SanctionTypeBan,
		Reason:       ptr("harassment"),
		Source:       models.SourceModerator,
		IssuedAt:     time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
	}
}

// activeKick returns a kick sanction, which can never be appealed.
func activeKick() models.Sanction {
	return models.Sanction{
		ID:           sanctionID,
		CommunityID:  communityID,
		TargetUserID: targetID,
		ActorUserID:  ptr(moderatorID),
		Type:         models.SanctionTypeKick,
		Source:       models.SourceModerator,
		IssuedAt:     time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
	}
}

// TestSubmitAppealRules covers ownership, appealability, and lifecycle guards.
func TestSubmitAppealRules(t *testing.T) {
	t.Parallel()

	liftedAt := time.Date(2026, 1, 11, 9, 0, 0, 0, time.UTC)
	liftSource := models.SourceModerator

	tests := []struct {
		name      string
		sanction  models.Sanction
		actor     string
		expectErr error
	}{
		{name: "member appeals an active ban", sanction: activeBan(), actor: targetID},
		{name: "stranger cannot appeal", sanction: activeBan(), actor: peerID, expectErr: models.ErrNotSanctionTarget},
		{name: "kick is not appealable", sanction: activeKick(), actor: targetID, expectErr: models.ErrSanctionNotAppealable},
		{
			name: "lifted sanction is not active",
			sanction: models.Sanction{
				ID:           sanctionID,
				CommunityID:  communityID,
				TargetUserID: targetID,
				Type:         models.SanctionTypeBan,
				Source:       models.SourceModerator,
				IssuedAt:     time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC),
				LiftedAt:     &liftedAt,
				LiftSource:   &liftSource,
			},
			actor:     targetID,
			expectErr: models.ErrSanctionNotActive,
		},
		{
			name: "unknown sanction is a 404",
			sanction: models.Sanction{
				ID:          "99999999-9999-4999-8999-999999999999",
				CommunityID: communityID,
			},
			actor:     targetID,
			expectErr: models.ErrSanctionNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			_, present := f.store.Sanctions.ByID(test.sanction.ID)
			if !present && test.expectErr != models.ErrSanctionNotFound {
				f.seedSanction(test.sanction)
			}
			if test.expectErr == models.ErrSanctionNotFound {
				f.seedSanction(models.Sanction{ID: sanctionID, CommunityID: communityID})
			}
			appeals := service.NewAppealService(f.deps)

			appeal, err := appeals.Submit(ctx(), test.actor, test.sanction.ID, models.CreateAppealRequest{
				Statement: "I would like this reviewed.",
			}, "")

			if test.expectErr != nil {
				assert.Equal(t, test.expectErr, err)
				assert.Zero(t, f.store.TxCount)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, models.AppealStatusSubmitted, appeal.Status)
			assert.Nil(t, appeal.ReviewerUserID)
			assert.Nil(t, appeal.DecidedAt)

			rows := f.store.Outbox.All()
			require.Len(t, rows, 1)
			payload := decodeEnvelope(t, rows[0])
			assert.Equal(t, string(models.AppealStatusSubmitted), payload["status"])
			assert.Equal(t, string(models.SanctionTypeBan), payload["sanction_type"])
			assert.Empty(t, f.store.Audit.All(), "appeal submission has no audit action type")
			f.authorizer.AssertNotCalled(t, "Check")
		})
	}
}

// TestSubmitRejectsSecondOpenAppeal proves the one-open-appeal rule.
func TestSubmitRejectsSecondOpenAppeal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.seedSanction(activeBan())
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "first",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now,
	})
	appeals := service.NewAppealService(f.deps)

	_, err := appeals.Submit(ctx(), targetID, sanctionID, models.CreateAppealRequest{Statement: "second"}, "")

	assert.Equal(t, models.ErrAppealAlreadyOpen, err)
	assert.Len(t, f.store.Outbox.All(), 0)
	assert.Equal(t, 1, f.store.Appeals.Len())
}

// TestSubmitRejectsBadStatements covers the free-text bounds.
func TestSubmitRejectsBadStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		code      string
	}{
		{name: "empty statement", statement: "   ", code: "invalid_request"},
		{name: "statement too long", statement: longString(2001), code: "statement_too_long"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.seedSanction(activeBan())
			appeals := service.NewAppealService(f.deps)

			_, err := appeals.Submit(ctx(), targetID, sanctionID, models.CreateAppealRequest{Statement: test.statement}, "")

			domainErr := models.AsError(err)
			require.NotNil(t, domainErr)
			assert.Equal(t, test.code, domainErr.Code)
			assert.Zero(t, f.store.TxCount)
		})
	}
}

// TestSubmitReplaysIdempotentKey proves a retried submission is not duplicated.
func TestSubmitReplaysIdempotentKey(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.seedSanction(activeBan())
	appeals := service.NewAppealService(f.deps)
	request := models.CreateAppealRequest{Statement: "retry me"}

	first, err := appeals.Submit(ctx(), targetID, sanctionID, request, "appeal-key")
	require.NoError(t, err)
	second, err := appeals.Submit(ctx(), targetID, sanctionID, request, "appeal-key")
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, 1, f.store.Appeals.Len())
	assert.Len(t, f.store.Outbox.All(), 1)
}

// seedOpenAppeal stores a submitted appeal for the test sanction.
func seedOpenAppeal(f *fixture, sanction models.Sanction) models.Appeal {
	appeal := models.Appeal{
		ID:              appealID,
		SanctionID:      sanction.ID,
		CommunityID:     sanction.CommunityID,
		AppellantUserID: sanction.TargetUserID,
		Statement:       "unfair",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now.Add(-2 * time.Hour),
	}
	f.store.Appeals.Add(appeal)
	return appeal
}

// TestDecideUpheldKeepsSanctionActive proves an upheld appeal changes nothing
// about the sanction while recording the decision and EVT-006.
func TestDecideUpheldKeepsSanctionActive(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	seedOpenAppeal(f, sanction)
	f.authorizer.Allow()
	appeals := service.NewAppealService(f.deps)

	decided, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: models.DecisionUpheld,
		Note:     ptr("sanction stands"),
	}, "")

	require.NoError(t, err)
	assert.Equal(t, models.AppealStatusUpheld, decided.Status)
	assert.Equal(t, ptr(reviewerID), decided.ReviewerUserID)
	require.NotNil(t, decided.DecidedAt)

	stored, ok := f.store.Sanctions.ByID(sanction.ID)
	require.True(t, ok)
	assert.Nil(t, stored.LiftedAt, "an upheld appeal leaves the sanction active")

	assert.Equal(t, []models.AuditAction{models.AuditActionAppealDecided}, f.store.Audit.Actions())
	rows := f.store.Outbox.All()
	require.Len(t, rows, 1)
	assert.Equal(t, string(models.AppealStatusUpheld), decodeEnvelope(t, rows[0])["status"])
	assert.Equal(t, 1, f.store.TxCount)
}

// TestDecideOverturnedLiftsSanction proves the overturn cascade: the sanction is
// lifted with source appeal and both events are committed in one transaction.
func TestDecideOverturnedLiftsSanction(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	seedOpenAppeal(f, sanction)
	f.authorizer.Allow()
	appeals := service.NewAppealService(f.deps)

	decided, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: models.DecisionOverturned,
		Note:     ptr("misidentified"),
	}, "")

	require.NoError(t, err)
	assert.Equal(t, models.AppealStatusOverturned, decided.Status)

	stored, ok := f.store.Sanctions.ByID(sanction.ID)
	require.True(t, ok)
	require.NotNil(t, stored.LiftedAt)
	require.NotNil(t, stored.LiftSource)
	assert.Equal(t, models.SourceAppeal, *stored.LiftSource)

	assert.Equal(t, []models.AuditAction{
		models.AuditActionSanctionLifted,
		models.AuditActionAppealDecided,
	}, f.store.Audit.Actions())

	rows := f.store.Outbox.All()
	require.Len(t, rows, 2, "an overturn emits EVT-004 and EVT-006")
	assert.Equal(t, 1, f.store.TxCount, "both events are committed in one transaction")
	assert.Equal(t, 1, f.store.TxCommitCount)

	liftPayload := decodeEnvelope(t, rows[0])
	assert.Equal(t, "unban", liftPayload["action"])
	assert.Equal(t, string(models.SourceAppeal), liftPayload["source"])
	assert.Equal(t, reviewerID, liftPayload["actor_user_id"])

	outcomePayload := decodeEnvelope(t, rows[1])
	assert.Equal(t, string(models.AppealStatusOverturned), outcomePayload["status"])
	assert.Equal(t, reviewerID, outcomePayload["reviewer_user_id"])
}

// TestDecideRejectsAlreadyDecidedAppeal covers the 409 guard.
func TestDecideRejectsAlreadyDecidedAppeal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	decidedAt := f.now.Add(-time.Hour)
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanction.ID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "already handled",
		Status:          models.AppealStatusUpheld,
		ReviewerUserID:  ptr(reviewerID),
		DecidedAt:       &decidedAt,
		CreatedAt:       f.now.Add(-3 * time.Hour),
	})
	appeals := service.NewAppealService(f.deps)

	_, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: models.DecisionOverturned,
	}, "")

	assert.Equal(t, models.ErrAppealAlreadyDecided, err)
	assert.Zero(t, f.store.TxCount)
	f.authorizer.AssertNotCalled(t, "Check")
}

// TestDecideRequiresReviewPermission proves the review permission is enforced
// with the appellant as the hierarchy target.
func TestDecideRequiresReviewPermission(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	seedOpenAppeal(f, sanction)
	f.authorizer.On("Check", mock.Anything, communityID, mock.MatchedBy(func(request authz.CheckRequest) bool {
		return request.Permission == models.PermissionReviewAppeals &&
			request.TargetUserID != nil && *request.TargetUserID == targetID
	})).Return(authz.Decision{Allowed: false, DenialReason: "missing_permission"}, nil).Once()
	appeals := service.NewAppealService(f.deps)

	_, err := appeals.Decide(ctx(), peerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: models.DecisionUpheld,
	}, "")

	assert.Equal(t, models.ErrInsufficientPermission, err)
	assert.Empty(t, f.store.Outbox.All())

	stored, ok := f.store.Appeals.ByID(appealID)
	require.True(t, ok)
	assert.Equal(t, models.AppealStatusSubmitted, stored.Status)
	f.authorizer.AssertExpectations(t)
}

// TestDecideRejectsInvalidDecision covers request validation.
func TestDecideRejectsInvalidDecision(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	seedOpenAppeal(f, sanction)
	appeals := service.NewAppealService(f.deps)

	_, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: "maybe",
	}, "")
	assertErrorCode(t, err, "invalid_request")

	_, err = appeals.Decide(ctx(), reviewerID, communityID, appealID, models.AppealDecisionRequest{
		Decision: models.DecisionUpheld,
		Note:     ptr(longString(2001)),
	}, "")
	assertErrorCode(t, err, "decision_note_too_long")
	assert.Zero(t, f.store.TxCount)
}

// TestDecideReplaysIdempotentKey proves a retried decision is not applied twice.
func TestDecideReplaysIdempotentKey(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	sanction := f.seedSanction(activeBan())
	seedOpenAppeal(f, sanction)
	f.authorizer.Allow()
	appeals := service.NewAppealService(f.deps)
	request := models.AppealDecisionRequest{Decision: models.DecisionUpheld}

	first, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, request, "decision-key")
	require.NoError(t, err)
	second, err := appeals.Decide(ctx(), reviewerID, communityID, appealID, request, "decision-key")
	require.NoError(t, err)

	assert.Equal(t, first.Status, second.Status)
	assert.Len(t, f.store.Outbox.All(), 1)
	f.authorizer.AssertExpectations(t)
}

// TestListMineReturnsOwnAppeals proves the member view needs no API-001 call.
func TestListMineReturnsOwnAppeals(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	appeals := service.NewAppealService(f.deps)
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "mine",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now,
	})
	f.store.Appeals.Add(models.Appeal{
		ID:              "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		SanctionID:      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		CommunityID:     communityID,
		AppellantUserID: peerID,
		Statement:       "someone else",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now,
	})

	page, err := appeals.ListMine(ctx(), targetID, models.Pagination{Limit: 10})
	require.NoError(t, err)

	require.Len(t, page.Data, 1)
	assert.Equal(t, targetID, page.Data[0].AppellantUserID)
	f.authorizer.AssertNotCalled(t, "Check")
}

// TestListCommunityQueueFiltersAndAuthorizes proves the moderator queue requires
// review_appeals and filters by status.
func TestListCommunityQueueFiltersAndAuthorizes(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	submitted := models.AppealStatusSubmitted
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "queue",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now,
	})
	f.store.Appeals.Add(models.Appeal{
		ID:              "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		SanctionID:      "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		CommunityID:     communityID,
		AppellantUserID: peerID,
		Statement:       "decided",
		Status:          models.AppealStatusUpheld,
		CreatedAt:       f.now,
	})
	f.authorizer.Allow()
	appeals := service.NewAppealService(f.deps)

	page, err := appeals.ListCommunity(ctx(), reviewerID, communityID, &submitted, models.Pagination{Limit: 10})
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Equal(t, models.AppealStatusSubmitted, page.Data[0].Status)
	f.authorizer.AssertExpectations(t)
}

// TestEventsBuilderIsUsableFromServices guards the shared builder wiring.
func TestEventsBuilderIsUsableFromServices(t *testing.T) {
	t.Parallel()

	message, err := events.NewBuilder().AppealStatusChanged(events.Meta{TraceID: traceID}, events.AppealStatusChangedPayload{
		AppealID:        appealID,
		SanctionID:      sanctionID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		SanctionType:    models.SanctionTypeBan,
		Status:          models.AppealStatusSubmitted,
	})
	require.NoError(t, err)
	assert.Equal(t, traceID, message.Envelope.TraceID)
}
