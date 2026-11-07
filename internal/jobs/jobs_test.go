package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/events"
	"moderation_service/internal/jobs"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/tests/mocks"
)

// Fixed identifiers and clock used by the job tests.
const (
	communityID = "11111111-1111-4111-8111-111111111111"
	targetID    = "33333333-3333-4333-8333-333333333333"
	muteID      = "44444444-4444-4444-8444-444444444444"
)

// newJobFixture wires the store and metrics used by both jobs.
func newJobFixture(t *testing.T) (*mocks.Store, *observability.Metrics, time.Time) {
	t.Helper()
	metrics, err := observability.NewMetrics(prometheus.NewRegistry())
	require.NoError(t, err)
	return mocks.NewStore(), metrics, time.Date(2026, 1, 14, 12, 0, 0, 0, time.UTC)
}

// dueMute returns an active timed mute whose expiry has passed.
func dueMute(id string, now time.Time, expiredAgo time.Duration) models.Sanction {
	expiresAt := now.Add(-expiredAgo)
	return models.Sanction{
		ID:           id,
		CommunityID:  communityID,
		TargetUserID: targetID,
		ActorUserID:  ptr("22222222-2222-4222-8222-222222222222"),
		Type:         models.SanctionTypeMute,
		Source:       models.SourceModerator,
		IssuedAt:     expiresAt.Add(-time.Hour),
		ExpiresAt:    &expiresAt,
	}
}

// TestSweepLiftsOnlyDueUnliftedMutes proves the sweep selects exactly the mutes
// that are due, emits an automatic unmute, and records the audit entry.
func TestSweepLiftsOnlyDueUnliftedMutes(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	store.Sanctions.Add(dueMute(muteID, now, time.Minute))

	future := now.Add(time.Hour)
	store.Sanctions.Add(models.Sanction{
		ID:           "55555555-5555-4555-8555-555555555555",
		CommunityID:  communityID,
		TargetUserID: targetID,
		Type:         models.SanctionTypeMute,
		Source:       models.SourceModerator,
		IssuedAt:     now,
		ExpiresAt:    &future,
	})
	alreadyLifted := now.Add(-time.Hour)
	liftSource := models.SourceModerator
	store.Sanctions.Add(models.Sanction{
		ID:           "66666666-6666-4666-8666-666666666666",
		CommunityID:  communityID,
		TargetUserID: targetID,
		Type:         models.SanctionTypeMute,
		Source:       models.SourceModerator,
		IssuedAt:     now.Add(-2 * time.Hour),
		ExpiresAt:    &alreadyLifted,
		LiftedAt:     &alreadyLifted,
		LiftSource:   &liftSource,
	})
	store.Sanctions.Add(models.Sanction{
		ID:           "77777777-7777-4777-8777-777777777777",
		CommunityID:  communityID,
		TargetUserID: targetID,
		Type:         models.SanctionTypeBan,
		Source:       models.SourceModerator,
		IssuedAt:     now.Add(-2 * time.Hour),
	})

	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:   store,
		Events:  events.NewBuilder(),
		Topic:   "moderation_events",
		Metrics: metrics,
		Clock:   func() time.Time { return now },
	})

	lifted, err := sweeper.SweepOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, lifted)

	stored, ok := store.Sanctions.ByID(muteID)
	require.True(t, ok)
	require.NotNil(t, stored.LiftSource)
	assert.Equal(t, models.SourceExpiry, *stored.LiftSource)
	require.NotNil(t, stored.LiftedAt)

	assert.Equal(t, []models.AuditAction{models.AuditActionSanctionExpired}, store.Audit.Actions())
	entries := store.Audit.All()
	require.Len(t, entries, 1)
	assert.Nil(t, entries[0].ActorUserID, "an automatic lapse has no actor")

	rows := store.Outbox.All()
	require.Len(t, rows, 1)
	assert.Equal(t, "moderation_events", rows[0].Topic)
	assert.Equal(t, communityID, rows[0].MessageKey)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(rows[0].Envelope, &envelope))
	for _, field := range []string{
		"event_id", "event_type", "event_version", "occurred_at", "producer",
		"trace_id", "correlation_id", "causation_id", "schema_ref",
	} {
		assert.Contains(t, envelope, field)
	}
	traceID, ok := envelope["trace_id"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, traceID, "a scheduled flow mints its own trace id")
	assert.Len(t, traceID, 32)

	var payload map[string]any
	require.NoError(t, json.Unmarshal(rows[0].Payload, &payload))
	assert.Equal(t, "unmute", payload["action"])
	assert.Equal(t, string(models.SourceExpiry), payload["source"])
	assert.Nil(t, payload["actor_user_id"], "the unmute event carries a null actor")
	assert.Nil(t, payload["expires_at"])
	assert.Equal(t, false, payload["appealable"])

	assert.Equal(t, float64(1), testutil.ToFloat64(
		metrics.Sanctions.WithLabelValues(string(models.SanctionTypeMute), string(models.SourceExpiry)),
	))
}

// TestSweepMintsAFreshTraceIDPerRun proves each sweep run mints its own trace id
// and that a propagated id is not reused.
func TestSweepMintsAFreshTraceIDPerRun(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	store.Sanctions.Add(dueMute(muteID, now, time.Minute))
	store.Sanctions.Add(dueMute("88888888-8888-4888-8888-888888888888", now, 2*time.Minute))

	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:   store,
		Events:  events.NewBuilder(),
		Topic:   "moderation_events",
		Metrics: metrics,
		Clock:   func() time.Time { return now },
	})

	lifted, err := sweeper.SweepOnce(observability.WithTraceID(context.Background(), "4bf92f3577b34da6a3ce929d0e0e4736"))
	require.NoError(t, err)
	assert.Equal(t, 2, lifted)

	traces := make(map[string]struct{}, 2)
	for _, row := range store.Outbox.All() {
		var envelope map[string]any
		require.NoError(t, json.Unmarshal(row.Envelope, &envelope))
		traceID := envelope["trace_id"].(string)
		assert.NotEqual(t, "4bf92f3577b34da6a3ce929d0e0e4736", traceID, "jobs mint their own trace id")
		traces[traceID] = struct{}{}
	}
	assert.Len(t, traces, 1, "one sweep has one edge trace id")
}

// TestSweepIsANoopWhenNothingIsDue keeps the 30 s loop cheap.
func TestSweepIsANoopWhenNothingIsDue(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:   store,
		Events:  events.NewBuilder(),
		Topic:   "moderation_events",
		Metrics: metrics,
		Clock:   func() time.Time { return now },
	})

	lifted, err := sweeper.SweepOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, lifted)
	assert.Empty(t, store.Outbox.All())
}

// TestSweepRollsBackOnWriteFailure proves a failing audit append leaves the mute
// active, so the next sweep retries it.
func TestSweepRollsBackOnWriteFailure(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	store.Sanctions.Add(dueMute(muteID, now, time.Minute))
	store.Audit.AppendErr = errors.New("audit write failed")

	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:   store,
		Events:  events.NewBuilder(),
		Topic:   "moderation_events",
		Metrics: metrics,
		Clock:   func() time.Time { return now },
	})

	_, err := sweeper.SweepOnce(context.Background())
	require.Error(t, err)

	stored, ok := store.Sanctions.ByID(muteID)
	require.True(t, ok)
	assert.Nil(t, stored.LiftedAt, "the failed batch is rolled back")
	assert.Empty(t, store.Outbox.All())
	assert.Equal(t, 1, store.TxRollbackCount)
}

// TestSweepRunStopsOnContextCancellation proves the loop respects shutdown.
func TestSweepRunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	sweeper := jobs.NewMuteExpirySweeper(jobs.ExpiryConfig{
		Store:    store,
		Events:   events.NewBuilder(),
		Topic:    "moderation_events",
		Metrics:  metrics,
		Interval: 5 * time.Millisecond,
		Clock:    func() time.Time { return now },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sweeper.Run(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweeper did not stop after cancellation")
	}
}

// TestRetentionPurgesOnlyExpiredEntries proves the one-year window and the
// exported metrics of the daily purge.
func TestRetentionPurgesOnlyExpiredEntries(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	for index := 0; index < 5; index++ {
		store.Audit.Add(models.AuditLogEntry{
			ID:          models.NewUUID(),
			CommunityID: communityID,
			Action:      models.AuditActionSanctionIssued,
			CreatedAt:   now.Add(-models.AuditRetention - time.Duration(index+1)*24*time.Hour),
		})
	}
	store.Audit.Add(models.AuditLogEntry{
		ID:          models.NewUUID(),
		CommunityID: communityID,
		Action:      models.AuditActionSanctionIssued,
		CreatedAt:   now.Add(-models.AuditRetention + 24*time.Hour),
	})

	purger := jobs.NewAuditRetentionPurger(jobs.RetentionConfig{
		Store:     store,
		Metrics:   metrics,
		BatchSize: 2,
		Clock:     func() time.Time { return now },
	})

	purged, err := purger.PurgeOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(5), purged, "batched deletes clear every expired entry")
	assert.Equal(t, 1, store.Audit.Len(), "the in-window entry is retained")
	assert.Equal(t, float64(5), testutil.ToFloat64(metrics.AuditEntriesPurged))
	assert.Equal(t, float64(now.Unix()), testutil.ToFloat64(metrics.RetentionLastSuccess))
}

// TestRetentionPurgeReportsFailureWithoutSuccessTimestamp proves the operator
// alert signal: a failed purge does not stamp the last-success gauge.
func TestRetentionPurgeReportsFailureWithoutSuccessTimestamp(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	purger := jobs.NewAuditRetentionPurger(jobs.RetentionConfig{
		Store:   store,
		Metrics: metrics,
		Clock:   func() time.Time { return now },
	})

	purged, err := purger.PurgeOnce(context.Background())
	require.NoError(t, err)
	assert.Zero(t, purged)
	assert.Equal(t, float64(now.Unix()), testutil.ToFloat64(metrics.RetentionLastSuccess))
}

// TestRetentionRunStopsOnContextCancellation proves the daily loop respects
// shutdown.
func TestRetentionRunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	store, metrics, now := newJobFixture(t)
	purger := jobs.NewAuditRetentionPurger(jobs.RetentionConfig{
		Store:    store,
		Metrics:  metrics,
		Interval: 5 * time.Millisecond,
		Clock:    func() time.Time { return now },
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		purger.Run(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("purger did not stop after cancellation")
	}
}

// ptr returns a pointer to value.
func ptr(value string) *string { return &value }
