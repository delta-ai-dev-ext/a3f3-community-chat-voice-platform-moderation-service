package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"moderation_service/internal/models"
)

// auditColumns is the projection every audit query uses.
const auditColumns = `id::text, community_id::text, actor_user_id::text, target_user_id::text,
	action, reason, detail, created_at`

// auditRepository is the pgx implementation of AuditRepository. It only ever
// INSERTs (append-only, ADR-002); DELETE exists solely for the retention purge.
type auditRepository struct {
	q Querier
}

var _ AuditRepository = (*auditRepository)(nil)

// Append inserts one immutable audit entry.
func (r *auditRepository) Append(ctx context.Context, entry *models.AuditLogEntry) error {
	const query = `INSERT INTO audit_log_entry
		(id, community_id, actor_user_id, target_user_id, action, reason, detail, created_at)
		VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5, $6, $7::text::jsonb, $8::timestamptz)`
	var detail *string
	if len(entry.Detail) > 0 {
		value := string(entry.Detail)
		detail = &value
	}
	if _, err := r.q.Exec(ctx, query, entry.ID, entry.CommunityID, entry.ActorUserID, entry.TargetUserID,
		string(entry.Action), entry.Reason, detail, entry.CreatedAt); err != nil {
		return fmt.Errorf("insert audit entry: %w", err)
	}
	return nil
}

// List returns one page of audit entries matching the filter, newest first.
func (r *auditRepository) List(ctx context.Context, filter AuditFilter) ([]models.AuditLogEntry, bool, error) {
	const query = `SELECT ` + auditColumns + ` FROM audit_log_entry
		WHERE community_id = $1::text::uuid
		  AND created_at >= $2::timestamptz
		  AND ($3::text IS NULL OR action = $3::text)
		  AND ($4::text::uuid IS NULL OR actor_user_id = $4::text::uuid)
		  AND ($5::timestamptz IS NULL OR (created_at, id) < ($5::timestamptz, $6::text::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $7`

	var action *string
	if filter.Action != nil {
		value := string(*filter.Action)
		action = &value
	}
	rows, err := r.q.Query(ctx, query, filter.CommunityID, filter.Since, action, nullableText(filter.ActorUserID),
		cursorTimestamp(filter.Page.Cursor), cursorID(filter.Page.Cursor), fetchLimit(filter.Page))
	if err != nil {
		return nil, false, fmt.Errorf("list audit entries: %w", err)
	}
	defer rows.Close()

	entries := make([]models.AuditLogEntry, 0, 8)
	for rows.Next() {
		var (
			entry     models.AuditLogEntry
			rawAction string
		)
		if err := rows.Scan(&entry.ID, &entry.CommunityID, &entry.ActorUserID, &entry.TargetUserID,
			&rawAction, &entry.Reason, &entry.Detail, &entry.CreatedAt); err != nil {
			return nil, false, fmt.Errorf("scan audit entry: %w", err)
		}
		entry.Action = models.AuditAction(rawAction)
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate audit entries: %w", err)
	}
	return trimPage(entries, filter.Page.Limit)
}

// PurgeOlderThan deletes at most batchLimit entries older than cutoff.
func (r *auditRepository) PurgeOlderThan(ctx context.Context, cutoff time.Time, batchLimit int) (int64, error) {
	const query = `DELETE FROM audit_log_entry
		WHERE id IN (
		    SELECT id FROM audit_log_entry
		    WHERE created_at < $1::timestamptz
		    ORDER BY created_at
		    LIMIT $2
		)`
	tag, err := r.q.Exec(ctx, query, cutoff, batchLimit)
	if err != nil {
		return 0, fmt.Errorf("purge audit entries: %w", err)
	}
	return tag.RowsAffected(), nil
}

// outboxRepository is the pgx implementation of OutboxRepository.
type outboxRepository struct {
	q Querier
}

var _ OutboxRepository = (*outboxRepository)(nil)

// Append inserts an unpublished event row in the caller's transaction.
func (r *outboxRepository) Append(ctx context.Context, event *models.OutboxEvent) error {
	const query = `INSERT INTO outbox_event (id, topic, message_key, envelope, payload, created_at)
		VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5::text::jsonb, $6::timestamptz)`
	if _, err := r.q.Exec(ctx, query, event.ID, event.Topic, event.MessageKey,
		string(event.Envelope), string(event.Payload), event.CreatedAt); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// FetchUnpublished returns the oldest unpublished rows.
func (r *outboxRepository) FetchUnpublished(ctx context.Context, limit int) ([]models.OutboxEvent, error) {
	const query = `SELECT id::text, topic, message_key, envelope, payload, created_at
		FROM outbox_event
		WHERE published_at IS NULL
		ORDER BY created_at, id
		LIMIT $1`
	rows, err := r.q.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("fetch unpublished events: %w", err)
	}
	defer rows.Close()

	events := make([]models.OutboxEvent, 0, limit)
	for rows.Next() {
		var event models.OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.MessageKey, &event.Envelope, &event.Payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}
	return events, nil
}

// MarkPublished stamps published_at on a relayed row.
func (r *outboxRepository) MarkPublished(ctx context.Context, id string, publishedAt time.Time) error {
	const query = `UPDATE outbox_event SET published_at = $2::timestamptz WHERE id = $1::text::uuid`
	if _, err := r.q.Exec(ctx, query, id, publishedAt); err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}
	return nil
}

// PrunePublishedBefore deletes relayed rows older than the grace window.
func (r *outboxRepository) PrunePublishedBefore(ctx context.Context, publishedBefore time.Time) (int64, error) {
	const query = `DELETE FROM outbox_event WHERE published_at IS NOT NULL AND published_at < $1::timestamptz`
	tag, err := r.q.Exec(ctx, query, publishedBefore)
	if err != nil {
		return 0, fmt.Errorf("prune outbox events: %w", err)
	}
	return tag.RowsAffected(), nil
}

// idempotencyRepository is the pgx implementation of IdempotencyRepository.
type idempotencyRepository struct {
	q Querier
}

var _ IdempotencyRepository = (*idempotencyRepository)(nil)

// Get returns the stored record, or nil when the key is unknown.
func (r *idempotencyRepository) Get(ctx context.Context, scope, key string) (*models.IdempotencyRecord, error) {
	const query = `SELECT scope, idempotency_key, request_hash, response_status, response_body, created_at
		FROM idempotency_key WHERE scope = $1 AND idempotency_key = $2`
	var record models.IdempotencyRecord
	err := r.q.QueryRow(ctx, query, scope, key).Scan(&record.Scope, &record.Key, &record.RequestHash,
		&record.ResponseCode, &record.ResponseBody, &record.CreatedAt)
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load idempotency record: %w", err)
	}
	return &record, nil
}

// Put stores the first accepted response for the key.
func (r *idempotencyRepository) Put(ctx context.Context, record *models.IdempotencyRecord) error {
	const query = `INSERT INTO idempotency_key
		(scope, idempotency_key, request_hash, response_status, response_body, created_at)
		VALUES ($1, $2, $3, $4, $5::text::jsonb, $6::timestamptz)`
	if _, err := r.q.Exec(ctx, query, record.Scope, record.Key, record.RequestHash,
		record.ResponseCode, string(record.ResponseBody), record.CreatedAt); err != nil {
		if isUniqueViolation(err, "") {
			return ErrDuplicate
		}
		return fmt.Errorf("store idempotency record: %w", err)
	}
	return nil
}

// isNoRows reports whether err is the pgx "no rows" sentinel.
func isNoRows(err error) bool { return err == pgx.ErrNoRows }
