package events

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

// Defaults for the outbox relay loop.
const (
	// DefaultDispatchInterval is how often unpublished rows are polled.
	DefaultDispatchInterval = 2 * time.Second
	// DefaultDispatchBatch bounds how many events one relay batch carries.
	DefaultDispatchBatch = 100
	// DefaultOutboxPruneGrace is how long a published row is kept for
	// inspection before it is pruned.
	DefaultOutboxPruneGrace = 24 * time.Hour
)

// Dispatcher relays unpublished outbox rows to Kafka. Every replica runs one:
// a crash between commit and relay is healed because any replica picks the row
// up, and re-relayed duplicates are safe because consumers deduplicate on
// envelope.event_id (ADR-003).
type Dispatcher struct {
	store        repository.Store
	producer     Producer
	logger       *zap.Logger
	metrics      *observability.Metrics
	interval     time.Duration
	batchSize    int
	pruneGrace   time.Duration
	clock        func() time.Time
	lastPruneRun time.Time
}

// DispatcherConfig configures a Dispatcher; zero values fall back to defaults.
type DispatcherConfig struct {
	// Store is the repository seam the relay reads and acknowledges from.
	Store repository.Store
	// Producer publishes the relayed events.
	Producer Producer
	// Logger receives relay diagnostics.
	Logger *zap.Logger
	// Metrics records relay failures.
	Metrics *observability.Metrics
	// Interval is the poll cadence.
	Interval time.Duration
	// BatchSize is the maximum number of events per batch.
	BatchSize int
	// PruneGrace is how long published rows are retained.
	PruneGrace time.Duration
	// Clock is injectable for deterministic tests.
	Clock func() time.Time
}

// NewDispatcher builds the relay.
func NewDispatcher(config DispatcherConfig) *Dispatcher {
	dispatcher := &Dispatcher{
		store:      config.Store,
		producer:   config.Producer,
		logger:     config.Logger,
		metrics:    config.Metrics,
		interval:   config.Interval,
		batchSize:  config.BatchSize,
		pruneGrace: config.PruneGrace,
		clock:      config.Clock,
	}
	if dispatcher.logger == nil {
		dispatcher.logger = zap.NewNop()
	}
	if dispatcher.interval <= 0 {
		dispatcher.interval = DefaultDispatchInterval
	}
	if dispatcher.batchSize <= 0 {
		dispatcher.batchSize = DefaultDispatchBatch
	}
	if dispatcher.pruneGrace <= 0 {
		dispatcher.pruneGrace = DefaultOutboxPruneGrace
	}
	if dispatcher.clock == nil {
		dispatcher.clock = time.Now
	}
	return dispatcher
}

// Run relays until the context is cancelled, so a bounded shutdown timeout
// always wins over the poll loop.
func (d *Dispatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	d.logger.Info("outbox dispatcher started", zap.Duration("interval", d.interval))
	for {
		select {
		case <-ctx.Done():
			d.logger.Info("outbox dispatcher stopped")
			return
		case <-ticker.C:
			if _, err := d.DispatchOnce(ctx); err != nil {
				d.logger.Error("outbox dispatch failed", zap.Error(err))
			}
		}
	}
}

// DispatchOnce publishes one batch of unpublished events in creation order and
// acknowledges each row only after the broker accepted it. The first failure
// stops the batch so per-community ordering is preserved.
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	repos := d.store.Read()
	outbox := repos.Outbox

	rows, err := outbox.FetchUnpublished(ctx, d.batchSize)
	if err != nil {
		return 0, fmt.Errorf("fetch unpublished events: %w", err)
	}
	published := 0
	for _, row := range rows {
		if err := d.publish(ctx, row); err != nil {
			return published, err
		}
		if err := outbox.MarkPublished(ctx, row.ID, d.clock().UTC()); err != nil {
			return published, fmt.Errorf("acknowledge outbox event %s: %w", row.ID, err)
		}
		published++
	}
	d.prune(ctx)
	return published, nil
}

// publish sends one outbox row, replaying the envelope headers from the stored
// envelope so the wire contract does not depend on the payload.
func (d *Dispatcher) publish(ctx context.Context, row models.OutboxEvent) error {
	value, headers, err := render(row)
	if err != nil {
		return err
	}
	// The dispatcher runs on a background context; the trace id of the
	// originating request travels in the envelope, not in this context.
	if err := d.producer.Publish(ctx, row.Topic, row.MessageKey, value, headers); err != nil {
		d.metrics.RecordKafkaError(row.Topic)
		return fmt.Errorf("publish outbox event %s: %w", row.ID, err)
	}
	d.logger.Debug("relayed moderation event",
		zap.String("event_id", row.ID),
		zap.String("event_type", headers["event_type"]),
		zap.String("trace_id", headers["trace_id"]),
		zap.String("operation", "outbox_dispatch"),
	)
	return nil
}

// render rebuilds the wire value and the mirrored headers from a stored row.
func render(row models.OutboxEvent) ([]byte, map[string]string, error) {
	var envelope Envelope
	if err := json.Unmarshal(row.Envelope, &envelope); err != nil {
		return nil, nil, fmt.Errorf("decode stored envelope %s: %w", row.ID, err)
	}
	value, err := json.Marshal(struct {
		Envelope json.RawMessage `json:"envelope"`
		Payload  json.RawMessage `json:"payload"`
	}{Envelope: row.Envelope, Payload: row.Payload})
	if err != nil {
		return nil, nil, fmt.Errorf("encode outbox event %s: %w", row.ID, err)
	}
	return value, Message{Envelope: envelope}.Headers(), nil
}

// prune drops published rows older than the grace window, at most once per
// poll interval so the relay stays cheap.
func (d *Dispatcher) prune(ctx context.Context) {
	now := d.clock().UTC()
	if !d.lastPruneRun.IsZero() && now.Sub(d.lastPruneRun) < d.interval {
		return
	}
	d.lastPruneRun = now
	if _, err := d.store.Read().Outbox.PrunePublishedBefore(ctx, now.Add(-d.pruneGrace)); err != nil {
		d.logger.Warn("outbox prune failed", zap.Error(err))
	}
}

// Headers returns the Kafka headers for a stored outbox row.
func Headers(row models.OutboxEvent) (map[string]string, error) {
	var envelope Envelope
	if err := json.Unmarshal(row.Envelope, &envelope); err != nil {
		return nil, fmt.Errorf("decode stored envelope %s: %w", row.ID, err)
	}
	return Message{Envelope: envelope}.Headers(), nil
}
