package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/authz"
	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/internal/service"
	"moderation_service/tests/mocks"
)

// Fixed identifiers and clock used across the service tests.
const (
	communityID    = "11111111-1111-4111-8111-111111111111"
	otherCommunity = "12121212-1212-4212-8212-121212121212"
	moderatorID    = "22222222-2222-4222-8222-222222222222"
	targetID       = "33333333-3333-4333-8333-333333333333"
	sanctionID     = "44444444-4444-4444-8444-444444444444"
	appealID       = "55555555-5555-4555-8555-555555555555"
	reviewerID     = "66666666-6666-4666-8666-666666666666"
	peerID         = "77777777-7777-4777-8777-777777777777"
)

// traceID is the propagated trace id asserted on every produced envelope.
const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

// fixture bundles the in-memory dependencies of one service test.
type fixture struct {
	store      *mocks.Store
	authorizer *mocks.Authorizer
	metrics    *observability.Metrics
	now        time.Time
	deps       service.Deps
}

// newFixture wires a Deps value over in-memory fakes and a deterministic clock.
func newFixture(t *testing.T) *fixture {
	t.Helper()

	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	require.NoError(t, err)

	f := &fixture{
		store:      mocks.NewStore(),
		authorizer: &mocks.Authorizer{},
		metrics:    metrics,
		now:        time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC),
	}
	f.deps = service.Deps{
		Store:      f.store,
		Authorizer: f.authorizer,
		Events:     events.NewBuilder(),
		Topic:      "moderation_events",
		Metrics:    metrics,
		Now:        func() time.Time { return f.now },
	}
	return f
}

// ctx returns a context carrying the propagated trace id.
func ctx() context.Context { return observability.WithTraceID(context.Background(), traceID) }

// seedSanction stores an active sanction.
func (f *fixture) seedSanction(sanction models.Sanction) models.Sanction {
	f.store.Sanctions.Add(sanction)
	return sanction
}

// activeMute returns an active indefinite mute owned by the test target.
func activeMute() models.Sanction {
	issued := time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC)
	return models.Sanction{
		ID:           sanctionID,
		CommunityID:  communityID,
		TargetUserID: targetID,
		ActorUserID:  ptr(moderatorID),
		Type:         models.SanctionTypeMute,
		Reason:       ptr("spam"),
		Source:       models.SourceModerator,
		IssuedAt:     issued,
	}
}

// decodeEnvelope asserts the stored outbox envelope has the nine mandatory
// fields and the propagated trace id, and returns the payload map.
func decodeEnvelope(t *testing.T, row models.OutboxEvent) map[string]any {
	t.Helper()

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(row.Envelope, &envelope))
	for _, field := range []string{
		"event_id", "event_type", "event_version", "occurred_at", "producer",
		"trace_id", "correlation_id", "causation_id", "schema_ref",
	} {
		assert.Contains(t, envelope, field)
	}
	assert.Equal(t, traceID, envelope["trace_id"])
	assert.Equal(t, "moderation_service", envelope["producer"])

	var payload map[string]any
	require.NoError(t, json.Unmarshal(row.Payload, &payload))
	for field := range envelope {
		assert.NotContains(t, payload, field, "a payload must not redeclare envelope fields")
	}
	return payload
}

// TestIssueSanctionTypes proves the three sanction types are stored with the
// documented duration rules, their audit entry, and their EVT-004 event.
func TestIssueSanctionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		sanctionType     models.SanctionType
		durationSeconds  *int64
		expectExpiry     bool
		expectAppealable bool
	}{
		{name: "timed mute expires", sanctionType: models.SanctionTypeMute, durationSeconds: int64Ptr(600), expectExpiry: true, expectAppealable: true},
		{name: "indefinite mute has no expiry", sanctionType: models.SanctionTypeMute, expectExpiry: false, expectAppealable: true},
		{name: "kick is instantaneous", sanctionType: models.SanctionTypeKick, expectExpiry: false, expectAppealable: false},
		{name: "ban is permanent until lifted", sanctionType: models.SanctionTypeBan, expectExpiry: false, expectAppealable: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.authorizer.Allow()
			sanctions := service.NewSanctionService(f.deps)

			sanction, err := sanctions.Issue(ctx(), moderatorID, communityID, models.CreateSanctionRequest{
				TargetUserID:    targetID,
				Type:            test.sanctionType,
				Reason:          ptr("disruptive"),
				DurationSeconds: test.durationSeconds,
			}, "")

			require.NoError(t, err)
			assert.Equal(t, test.sanctionType, sanction.Type)
			assert.Equal(t, models.SourceModerator, sanction.Source)
			assert.Equal(t, models.StatusActive, sanction.Status)
			assert.Equal(t, ptr("disruptive"), sanction.Reason)
			assert.Equal(t, ptr(moderatorID), sanction.ActorUserID)
			if test.expectExpiry {
				require.NotNil(t, sanction.ExpiresAt)
				assert.True(t, sanction.ExpiresAt.After(f.now))
			} else {
				assert.Nil(t, sanction.ExpiresAt)
			}

			assert.Equal(t, 1, f.store.Sanctions.Len())
			assert.Equal(t, []models.AuditAction{models.AuditActionSanctionIssued}, f.store.Audit.Actions())

			rows := f.store.Outbox.All()
			require.Len(t, rows, 1)
			assert.Equal(t, "moderation_events", rows[0].Topic)
			assert.Equal(t, communityID, rows[0].MessageKey)
			payload := decodeEnvelope(t, rows[0])
			assert.Equal(t, string(test.sanctionType), payload["action"])
			assert.Equal(t, string(models.SourceModerator), payload["source"])
			assert.Equal(t, test.expectAppealable, payload["appealable"])
			assert.Equal(t, 1, f.store.TxCommitCount)

			f.authorizer.AssertExpectations(t)
		})
	}
}

// TestIssueRejectsDurationForKickAndBan pins the 422 errors a client must see.
func TestIssueRejectsDurationForKickAndBan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sanctionType models.SanctionType
		expect       error
	}{
		{name: "kick rejects duration", sanctionType: models.SanctionTypeKick, expect: models.ErrKickNoDuration},
		{name: "ban rejects duration", sanctionType: models.SanctionTypeBan, expect: models.ErrBanNoDuration},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			sanctions := service.NewSanctionService(f.deps)

			_, err := sanctions.Issue(ctx(), moderatorID, communityID, models.CreateSanctionRequest{
				TargetUserID:    targetID,
				Type:            test.sanctionType,
				DurationSeconds: int64Ptr(60),
			}, "")

			assert.Equal(t, test.expect, err)
			assert.Zero(t, f.store.TxCount, "a validation failure must not open a transaction")
			assert.Empty(t, f.store.Outbox.All())
			f.authorizer.AssertNotCalled(t, "Check")
		})
	}
}

// TestIssueRejectsInvalidPayloads covers the remaining request validation.
func TestIssueRejectsInvalidPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request models.CreateSanctionRequest
		code    string
	}{
		{
			name:    "unknown type",
			request: models.CreateSanctionRequest{TargetUserID: targetID, Type: "warn"},
			code:    "invalid_request",
		},
		{
			name:    "malformed target id",
			request: models.CreateSanctionRequest{TargetUserID: "not-a-uuid", Type: models.SanctionTypeMute},
			code:    "invalid_request",
		},
		{
			name: "reason too long",
			request: models.CreateSanctionRequest{
				TargetUserID: targetID,
				Type:         models.SanctionTypeMute,
				Reason:       ptr(longString(1001)),
			},
			code: "reason_too_long",
		},
		{
			name: "duration out of range",
			request: models.CreateSanctionRequest{
				TargetUserID:    targetID,
				Type:            models.SanctionTypeMute,
				DurationSeconds: int64Ptr(31536001),
			},
			code: "invalid_request",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			sanctions := service.NewSanctionService(f.deps)

			_, err := sanctions.Issue(ctx(), moderatorID, communityID, test.request, "")

			domainErr := models.AsError(err)
			require.NotNil(t, domainErr)
			assert.Equal(t, test.code, domainErr.Code)
			assert.Zero(t, f.store.TxCount)
		})
	}
}

// TestIssueMapsAuthorizationDenials proves a denial becomes the documented 403
// code and that nothing is written.
func TestIssueMapsAuthorizationDenials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		denialReason string
		expect       *models.Error
	}{
		{name: "missing permission", denialReason: "missing_permission", expect: models.ErrInsufficientPermission},
		{name: "target outranks actor", denialReason: "target_outranks_actor", expect: models.ErrTargetOutranksActor},
		{name: "target is owner", denialReason: "target_is_owner", expect: models.ErrTargetIsOwner},
		{name: "target is self", denialReason: "target_is_self", expect: models.ErrTargetIsSelf},
		{name: "actor is not a member", denialReason: "actor_not_member", expect: models.ErrInsufficientPermission},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.authorizer.Deny(test.denialReason)
			sanctions := service.NewSanctionService(f.deps)

			_, err := sanctions.Issue(ctx(), moderatorID, communityID, models.CreateSanctionRequest{
				TargetUserID: targetID,
				Type:         models.SanctionTypeBan,
			}, "")

			assertErrorCode(t, err, test.expect.Code)
			assert.Zero(t, f.store.TxCount, "a denial must not open a transaction")
			assert.Zero(t, f.store.Sanctions.Len())
			assert.Empty(t, f.store.Audit.All())
			assert.Empty(t, f.store.Outbox.All())
			f.authorizer.AssertExpectations(t)
		})
	}
}

// TestIssueDenialCodesMatchTheAPIContract asserts the exact 403 codes the
// openapi spec documents for each API-001 denial reason.
func TestIssueDenialCodesMatchTheAPIContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		denialReason string
		code         string
	}{
		{denialReason: "missing_permission", code: "insufficient_permission"},
		{denialReason: "target_outranks_actor", code: "target_outranks_actor"},
		{denialReason: "target_is_owner", code: "target_is_owner"},
		{denialReason: "target_is_self", code: "target_is_self"},
		{denialReason: "actor_not_member", code: "insufficient_permission"},
	}

	for _, test := range tests {
		t.Run(test.denialReason, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			f.authorizer.Deny(test.denialReason)
			sanctions := service.NewSanctionService(f.deps)

			_, err := sanctions.Issue(ctx(), moderatorID, communityID, models.CreateSanctionRequest{
				TargetUserID: targetID,
				Type:         models.SanctionTypeBan,
			}, "")

			assertErrorCode(t, err, test.code)
			assert.Zero(t, f.store.TxCount)
		})
	}
}

// TestIssueFailsClosedWhenAuthorizationIsUnavailable pins US-005: an
// unreachable API-001 blocks the sanction with no state change at all.
func TestIssueFailsClosedWhenAuthorizationIsUnavailable(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Unavailable()
	sanctions := service.NewSanctionService(f.deps)

	_, err := sanctions.Issue(ctx(), moderatorID, communityID, models.CreateSanctionRequest{
		TargetUserID: targetID,
		Type:         models.SanctionTypeBan,
	}, "")

	assert.Equal(t, models.ErrAuthorizationUnavailable, err)
	assert.Zero(t, f.store.TxCount)
	assert.Zero(t, f.store.Sanctions.Len())
	assert.Empty(t, f.store.Audit.All())
	assert.Empty(t, f.store.Outbox.All())
	f.authorizer.AssertExpectations(t)
}

// TestIssueReplaysIdempotentKey proves a retried write returns the first
// accepted sanction instead of creating a second one (US-015).
func TestIssueReplaysIdempotentKey(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	sanctions := service.NewSanctionService(f.deps)

	request := models.CreateSanctionRequest{TargetUserID: targetID, Type: models.SanctionTypeBan}
	first, err := sanctions.Issue(ctx(), moderatorID, communityID, request, "retry-key")
	require.NoError(t, err)

	second, err := sanctions.Issue(ctx(), moderatorID, communityID, request, "retry-key")
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, 1, f.store.Sanctions.Len())
	assert.Len(t, f.store.Outbox.All(), 1)
	assert.Equal(t, 1, f.store.Idempotency.Len())
	f.authorizer.AssertExpectations(t)
}

// TestLiftRules covers unmute/unban, the kick rejection, and the inactive guard.
func TestLiftRules(t *testing.T) {
	t.Parallel()

	alreadyLiftedAt := time.Date(2026, 1, 12, 8, 0, 0, 0, time.UTC)
	liftedSource := models.SourceModerator

	tests := []struct {
		name      string
		sanction  models.Sanction
		expectErr error
		expect    func(t *testing.T, f *fixture, sanction models.Sanction)
	}{
		{
			name:     "moderator lifts an active mute",
			sanction: activeMute(),
			expect: func(t *testing.T, f *fixture, sanction models.Sanction) {
				stored, ok := f.store.Sanctions.ByID(sanction.ID)
				require.True(t, ok)
				require.NotNil(t, stored.LiftSource)
				assert.Equal(t, models.SourceModerator, *stored.LiftSource)
				assert.Equal(t, models.StatusLifted, stored.Status)

				rows := f.store.Outbox.All()
				require.Len(t, rows, 1)
				payload := decodeEnvelope(t, rows[0])
				assert.Equal(t, "unmute", payload["action"])
				assert.Equal(t, string(models.SourceModerator), payload["source"])
				assert.Equal(t, []models.AuditAction{models.AuditActionSanctionLifted}, f.store.Audit.Actions())
			},
		},
		{
			name: "kick cannot be lifted",
			sanction: models.Sanction{
				ID:           sanctionID,
				CommunityID:  communityID,
				TargetUserID: targetID,
				Type:         models.SanctionTypeKick,
				Source:       models.SourceModerator,
				IssuedAt:     time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
			},
			expectErr: models.ErrKickNotLiftable,
			expect: func(t *testing.T, f *fixture, _ models.Sanction) {
				assert.Empty(t, f.store.Outbox.All())
				f.authorizer.AssertNotCalled(t, "Check")
			},
		},
		{
			name: "already lifted sanction is not active",
			sanction: models.Sanction{
				ID:           sanctionID,
				CommunityID:  communityID,
				TargetUserID: targetID,
				Type:         models.SanctionTypeBan,
				Source:       models.SourceModerator,
				IssuedAt:     time.Date(2026, 1, 10, 9, 0, 0, 0, time.UTC),
				LiftedAt:     &alreadyLiftedAt,
				LiftSource:   &liftedSource,
			},
			expectErr: models.ErrSanctionNotActive,
			expect: func(t *testing.T, f *fixture, _ models.Sanction) {
				assert.Empty(t, f.store.Outbox.All())
			},
		},
		{
			name: "unknown sanction is a 404",
			sanction: models.Sanction{
				ID:          "88888888-8888-4888-8888-888888888888",
				CommunityID: communityID,
			},
			expectErr: models.ErrSanctionNotFound,
			expect: func(t *testing.T, f *fixture, _ models.Sanction) {
				assert.Empty(t, f.store.Outbox.All())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			if test.expectErr != models.ErrSanctionNotFound {
				f.seedSanction(test.sanction)
			}
			if test.expectErr == nil {
				f.authorizer.Allow()
			}
			sanctions := service.NewSanctionService(f.deps)

			lifted, err := sanctions.Lift(ctx(), moderatorID, communityID, test.sanction.ID, models.LiftSanctionRequest{
				Reason: ptr("appealed in good faith"),
			})

			if test.expectErr != nil {
				assert.Equal(t, test.expectErr, err)
				f.authorizer.AssertExpectations(t)
				test.expect(t, f, test.sanction)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, models.StatusLifted, lifted.Status)
			test.expect(t, f, test.sanction)
		})
	}
}

// TestListSanctionsRequiresPermissionAndPages proves the moderator listing is
// authorized and paginates with the opaque cursor.
func TestListSanctionsRequiresPermissionAndPages(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	for index := 0; index < 3; index++ {
		f.seedSanction(models.Sanction{
			ID:           models.NewUUID(),
			CommunityID:  communityID,
			TargetUserID: targetID,
			Type:         models.SanctionTypeBan,
			Source:       models.SourceModerator,
			IssuedAt:     f.now.Add(-time.Duration(index) * time.Hour),
		})
	}
	f.authorizer.On("Check", mock.Anything, communityID, mock.Anything).
		Return(allowedDecision(), nil)
	sanctions := service.NewSanctionService(f.deps)

	first, err := sanctions.List(ctx(), moderatorID, communityID, nil, models.Pagination{Limit: 2})
	require.NoError(t, err)
	require.Len(t, first.Data, 2)
	require.NotNil(t, first.NextCursor)

	second, err := sanctions.List(ctx(), moderatorID, communityID, nil, models.Pagination{
		Limit:  2,
		Cursor: mustCursor(t, *first.NextCursor),
	})
	require.NoError(t, err)
	assert.Len(t, second.Data, 1)
	assert.Nil(t, second.NextCursor)
	assert.NotEqual(t, first.Data[0].ID, second.Data[0].ID)
}

// TestMineMergesOpenAppeal proves /me/sanctions exposes appeal eligibility and
// the open appeal id of the caller's own sanctions.
func TestMineMergesOpenAppeal(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	mute := activeMute()
	f.seedSanction(mute)
	f.store.Appeals.Add(models.Appeal{
		ID:              appealID,
		SanctionID:      mute.ID,
		CommunityID:     communityID,
		AppellantUserID: targetID,
		Statement:       "please",
		Status:          models.AppealStatusSubmitted,
		CreatedAt:       f.now,
	})
	sanctions := service.NewSanctionService(f.deps)

	page, err := sanctions.Mine(ctx(), targetID)
	require.NoError(t, err)

	require.Len(t, page.Data, 1)
	require.NotNil(t, page.Data[0].OpenAppealID)
	assert.Equal(t, appealID, *page.Data[0].OpenAppealID)
	assert.False(t, page.Data[0].Appealable, "an already-appealed sanction is not appealable again")
	f.authorizer.AssertNotCalled(t, "Check")
}

// assertErrorCode asserts the domain error code of err.
func assertErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	domainErr := models.AsError(err)
	require.NotNil(t, domainErr)
	assert.Equal(t, code, domainErr.Code, "unexpected error: %v", err)
}

// allowedDecision is the API-001 allow answer.
func allowedDecision() authz.Decision { return authz.Decision{Allowed: true} }

// ptr returns a pointer to value.
func ptr(value string) *string { return &value }

// int64Ptr returns a pointer to value.
func int64Ptr(value int64) *int64 { return &value }

// longString returns a string of length n.
func longString(n int) string {
	runes := make([]byte, n)
	for i := range runes {
		runes[i] = 'a'
	}
	return string(runes)
}

// mustCursor decodes an opaque cursor or fails the test.
func mustCursor(t *testing.T, token string) *models.Cursor {
	t.Helper()
	cursor, err := models.DecodeCursor(token)
	require.NoError(t, err)
	return cursor
}
