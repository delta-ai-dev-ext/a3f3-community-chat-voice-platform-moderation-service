// Package jobs holds the two background loops of the service: the mute-expiry
// sweep and the daily audit retention purge. Both run inside the server process
// and stop cleanly on shutdown.
package jobs

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

// Defaults for the expiry sweep.
const (
	// DefaultSweepInterval is how often every replica looks for due mutes.
	DefaultSweepInterval = 30 * time.Second
	// DefaultSweepBatch bounds how many mutes one transaction lifts.
	DefaultSweepBatch = 100
)

// ExpiryConfig configures the mute-expiry sweeper.
type ExpiryConfig struct {
	// Store is the repository seam the sweep writes through.
	Store repository.Store
	// Events builds the unmute event of each expired mute.
	Events *events.Builder
	// Topic is the Kafka topic the outbox rows target.
	Topic string
	// Metrics records expired sanctions.
	Metrics *observability.Metrics
	// Logger receives sweep diagnostics.
	Logger *zap.Logger
	// Interval is the sweep cadence.
	Interval time.Duration
	// BatchSize is the maximum number of mutes lifted per transaction.
	BatchSize int
	// Clock is injectable for deterministic tests.
	Clock func() time.Time
}

// MuteExpirySweeper lifts timed mutes whose expiry has passed. Row-level
// locking (FOR UPDATE SKIP LOCKED) makes concurrent replicas safe: a mute is
// lifted exactly once even when several sweeps run at the same moment.
type MuteExpirySweeper struct {
	store     repository.Store
	builder   *events.Builder
	topic     string
	metrics   *observability.Metrics
	logger    *zap.Logger
	interval  time.Duration
	batchSize int
	clock     func() time.Time
}

// NewMuteExpirySweeper builds the sweeper with defaults applied.
func NewMuteExpirySweeper(config ExpiryConfig) *MuteExpirySweeper {
	sweeper := &MuteExpirySweeper{
		store:     config.Store,
		builder:   config.Events,
		topic:     config.Topic,
		metrics:   config.Metrics,
		logger:    config.Logger,
		interval:  config.Interval,
		batchSize: config.BatchSize,
		clock:     config.Clock,
	}
	if sweeper.builder == nil {
		sweeper.builder = events.NewBuilder()
	}
	if sweeper.logger == nil {
		sweeper.logger = zap.NewNop()
	}
	if sweeper.interval <= 0 {
		sweeper.interval = DefaultSweepInterval
	}
	if sweeper.batchSize <= 0 {
		sweeper.batchSize = DefaultSweepBatch
	}
	if sweeper.clock == nil {
		sweeper.clock = time.Now
	}
	return sweeper
}

// Run sweeps until the context is cancelled.
func (j *MuteExpirySweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	j.logger.Info("mute expiry sweeper started", zap.Duration("interval", j.interval))
	for {
		select {
		case <-ctx.Done():
			j.logger.Info("mute expiry sweeper stopped")
			return
		case <-ticker.C:
			if _, err := j.SweepOnce(ctx); err != nil {
				j.logger.Error("mute expiry sweep failed", zap.Error(err))
			}
		}
	}
}

// SweepOnce lifts one batch of due mutes. The flow has no originating request,
// so a fresh trace id is minted at its edge and carried by every event.
func (j *MuteExpirySweeper) SweepOnce(ctx context.Context) (int, error) {
	now := j.clock().UTC()
	ctx = observability.WithTraceID(ctx, observability.MintTraceID())

	lifted := 0
	err := j.store.InTx(ctx, func(ctx context.Context, repos repository.Repositories) error {
		due, err := repos.Sanctions.LockDueMutes(ctx, j.batchSize, now)
		if err != nil {
			return fmt.Errorf("select due mutes: %w", err)
		}
		for _, sanction := range due {
			if err := j.expire(ctx, repos, sanction, now); err != nil {
				return err
			}
			lifted++
		}
		return nil
	})
	if err != nil {
		return lifted, err
	}
	if lifted > 0 {
		j.logger.Info("expired due mutes",
			zap.String("trace_id", observability.TraceID(ctx)),
			zap.Int("count", lifted),
			zap.String("operation", "mute_expiry_sweep"),
			zap.String("status", "accepted"),
		)
	}
	return lifted, nil
}

// expire lifts one due mute and stages its event and audit entry in the same
// transaction, with a null actor because it is an automatic action.
func (j *MuteExpirySweeper) expire(ctx context.Context, repos repository.Repositories, sanction models.Sanction, now time.Time) error {
	if err := repos.Sanctions.MarkLifted(ctx, sanction.ID, repository.LiftDetails{
		At:     now,
		Source: models.SourceExpiry,
	}); err != nil {
		return err
	}

	message, err := j.builder.MemberSanctionChanged(events.Meta{
		TraceID:       observability.TraceID(ctx),
		CorrelationID: models.NewUUID(),
	}, events.MemberSanctionChangedPayload{
		SanctionID:   sanction.ID,
		CommunityID:  sanction.CommunityID,
		TargetUserID: sanction.TargetUserID,
		// Automatic lapse: no moderator acted, so the actor is null.
		ActorUserID: nil,
		Action:      sanction.Type.LiftedAction(),
		Source:      models.SourceExpiry,
		Reason:      nil,
		ExpiresAt:   nil,
		Appealable:  false,
	})
	if err != nil {
		return fmt.Errorf("build member_sanction_changed event: %w", err)
	}
	row, err := message.OutboxRow(j.topic, sanction.CommunityID, now)
	if err != nil {
		return err
	}
	if err := repos.Outbox.Append(ctx, row); err != nil {
		return err
	}

	targetUserID := sanction.TargetUserID
	detail := map[string]any{
		"sanction_id":     sanction.ID,
		"sanction_action": sanction.Type.LiftedAction(),
		"source":          string(models.SourceExpiry),
	}
	entry, err := newAuditEntry(now, models.AuditActionSanctionExpired, sanction.CommunityID, nil, &targetUserID, nil, detail)
	if err != nil {
		return err
	}
	if err := repos.Audit.Append(ctx, entry); err != nil {
		return err
	}

	j.metrics.RecordSanction(string(sanction.Type), string(models.SourceExpiry))
	return nil
}
