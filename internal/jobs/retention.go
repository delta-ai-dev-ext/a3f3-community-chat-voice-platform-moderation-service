package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"moderation_service/internal/models"
	"moderation_service/internal/observability"
	"moderation_service/internal/repository"
)

// Defaults for the retention purge.
const (
	// DefaultPurgeInterval is the daily cadence of the retention job.
	DefaultPurgeInterval = 24 * time.Hour
	// DefaultPurgeBatch bounds one delete statement so the job stays short.
	DefaultPurgeBatch = 1000
)

// RetentionConfig configures the audit retention purger.
type RetentionConfig struct {
	// Store is the repository seam the purge deletes through.
	Store repository.Store
	// Metrics exports the purged count and the last-success timestamp.
	Metrics *observability.Metrics
	// Logger receives purge diagnostics.
	Logger *zap.Logger
	// Interval is the purge cadence.
	Interval time.Duration
	// BatchSize is the maximum number of rows deleted per statement.
	BatchSize int
	// Retention is the retention window; zero means the platform's one year.
	Retention time.Duration
	// Clock is injectable for deterministic tests.
	Clock func() time.Time
}

// AuditRetentionPurger permanently deletes audit entries older than the
// one-year commitment in bounded batches (ADR-002). It only ever deletes; the
// application never updates an audit row.
type AuditRetentionPurger struct {
	store     repository.Store
	metrics   *observability.Metrics
	logger    *zap.Logger
	interval  time.Duration
	batchSize int
	retention time.Duration
	clock     func() time.Time
}

// NewAuditRetentionPurger builds the purger with defaults applied.
func NewAuditRetentionPurger(config RetentionConfig) *AuditRetentionPurger {
	purger := &AuditRetentionPurger{
		store:     config.Store,
		metrics:   config.Metrics,
		logger:    config.Logger,
		interval:  config.Interval,
		batchSize: config.BatchSize,
		retention: config.Retention,
		clock:     config.Clock,
	}
	if purger.logger == nil {
		purger.logger = zap.NewNop()
	}
	if purger.interval <= 0 {
		purger.interval = DefaultPurgeInterval
	}
	if purger.batchSize <= 0 {
		purger.batchSize = DefaultPurgeBatch
	}
	if purger.retention <= 0 {
		purger.retention = models.AuditRetention
	}
	if purger.clock == nil {
		purger.clock = time.Now
	}
	return purger
}

// Run purges daily until the context is cancelled.
func (j *AuditRetentionPurger) Run(ctx context.Context) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	j.logger.Info("audit retention purger started", zap.Duration("interval", j.interval))
	for {
		select {
		case <-ctx.Done():
			j.logger.Info("audit retention purger stopped")
			return
		case <-ticker.C:
			if _, err := j.PurgeOnce(ctx); err != nil {
				j.logger.Error("audit retention purge failed", zap.Error(err))
			}
		}
	}
}

// PurgeOnce deletes every audit entry older than the retention window and
// reports one success timestamp, so a dashboard can alert on a missed day.
func (j *AuditRetentionPurger) PurgeOnce(ctx context.Context) (int64, error) {
	now := j.clock().UTC()
	cutoff := now.Add(-j.retention)
	repos := j.store.Read()

	var total int64
	for {
		deleted, err := repos.Audit.PurgeOlderThan(ctx, cutoff, j.batchSize)
		if err != nil {
			return total, fmt.Errorf("purge audit entries older than %s: %w", cutoff.Format(time.RFC3339), err)
		}
		total += deleted
		j.metrics.RecordAuditPurged(deleted)
		if deleted < int64(j.batchSize) {
			break
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}

	j.metrics.RecordRetentionSuccess(float64(now.Unix()))
	j.logger.Info("audit retention purge complete",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.Int64("purged", total),
		zap.String("operation", "audit_retention_purge"),
		zap.String("status", "accepted"),
	)
	return total, nil
}

// newAuditEntry builds the audit row of an automatic action.
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
