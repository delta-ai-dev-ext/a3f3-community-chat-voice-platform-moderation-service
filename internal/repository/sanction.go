package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"moderation_service/internal/models"
)

// sanctionColumns is the projection every sanction query uses. UUID columns are
// cast to text so the scanner stays in the plain Go string domain.
const sanctionColumns = `id::text, community_id::text, target_user_id::text, actor_user_id::text,
	type, reason, source, issued_at, expires_at, lifted_at, lift_source, lift_reason`

// sanctionRepository is the pgx implementation of SanctionRepository.
type sanctionRepository struct {
	q Querier
}

var _ SanctionRepository = (*sanctionRepository)(nil)

// Create inserts a new sanction row.
func (r *sanctionRepository) Create(ctx context.Context, sanction *models.Sanction) error {
	const query = `INSERT INTO sanction
		(id, community_id, target_user_id, actor_user_id, type, reason, source, issued_at, expires_at, lifted_at, lift_source, lift_reason)
		VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5, $6, $7, $8::timestamptz,
		        $9::timestamptz, $10::timestamptz, $11, $12)`
	_, err := r.q.Exec(ctx, query,
		sanction.ID, sanction.CommunityID, sanction.TargetUserID, sanction.ActorUserID,
		string(sanction.Type), sanction.Reason, string(sanction.Source), sanction.IssuedAt,
		sanction.ExpiresAt, sanction.LiftedAt, nullableSource(sanction.LiftSource), sanction.LiftReason,
	)
	if isUniqueViolation(err, "") {
		return ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("insert sanction: %w", err)
	}
	return nil
}

// GetByID loads one sanction by id regardless of community.
func (r *sanctionRepository) GetByID(ctx context.Context, id string, now time.Time) (*models.Sanction, error) {
	const query = `SELECT ` + sanctionColumns + ` FROM sanction WHERE id = $1::text::uuid`
	return r.scanOne(ctx, query, now, id)
}

// GetByIDInCommunity loads one sanction scoped to its community.
func (r *sanctionRepository) GetByIDInCommunity(ctx context.Context, communityID, id string, now time.Time) (*models.Sanction, error) {
	const query = `SELECT ` + sanctionColumns + ` FROM sanction
		WHERE id = $1::text::uuid AND community_id = $2::text::uuid`
	return r.scanOne(ctx, query, now, id, communityID)
}

// scanOne runs a single-row sanction query, mapping no-rows to the domain error.
func (r *sanctionRepository) scanOne(ctx context.Context, query string, now time.Time, args ...any) (*models.Sanction, error) {
	sanction, err := scanSanction(r.q.QueryRow(ctx, query, args...), now)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, models.ErrSanctionNotFound
		}
		return nil, fmt.Errorf("load sanction: %w", err)
	}
	return sanction, nil
}

// List returns one page of a community's sanctions, newest first.
func (r *sanctionRepository) List(ctx context.Context, filter SanctionFilter) ([]models.Sanction, bool, error) {
	const query = `SELECT ` + sanctionColumns + ` FROM sanction
		WHERE community_id = $1::text::uuid
		  AND (
		        $2::text IS NULL
		     OR ($2::text = 'active'  AND lifted_at IS NULL AND (expires_at IS NULL OR expires_at > $3::timestamptz))
		     OR ($2::text = 'lifted'  AND lifted_at IS NOT NULL)
		     OR ($2::text = 'expired' AND lifted_at IS NULL AND expires_at IS NOT NULL AND expires_at <= $3::timestamptz)
		  )
		  AND ($4::timestamptz IS NULL OR (issued_at, id) < ($4::timestamptz, $5::text::uuid))
		ORDER BY issued_at DESC, id DESC
		LIMIT $6`

	var status *string
	if filter.Status != nil {
		value := string(*filter.Status)
		status = &value
	}
	rows, err := r.q.Query(ctx, query, filter.CommunityID, status, filter.Now, cursorTimestamp(filter.Page.Cursor), cursorID(filter.Page.Cursor), fetchLimit(filter.Page))
	if err != nil {
		return nil, false, fmt.Errorf("list sanctions: %w", err)
	}
	defer rows.Close()

	sanctions, err := collectSanctions(rows, filter.Now)
	if err != nil {
		return nil, false, err
	}
	return trimPage(sanctions, filter.Page.Limit)
}

// ActiveByTarget returns the caller's active sanctions across communities,
// including bans in communities the caller is no longer a member of.
func (r *sanctionRepository) ActiveByTarget(ctx context.Context, targetUserID string, now time.Time) ([]models.MySanction, error) {
	const query = `SELECT id::text, community_id::text, type, reason, issued_at, expires_at
		FROM sanction
		WHERE target_user_id = $1::text::uuid
		  AND lifted_at IS NULL
		  AND (expires_at IS NULL OR expires_at > $2::timestamptz)
		ORDER BY issued_at DESC, id DESC`

	rows, err := r.q.Query(ctx, query, targetUserID, now)
	if err != nil {
		return nil, fmt.Errorf("list active sanctions: %w", err)
	}
	defer rows.Close()

	sanctions := make([]models.MySanction, 0, 8)
	for rows.Next() {
		var (
			item    models.MySanction
			rawType string
		)
		if err := rows.Scan(&item.ID, &item.CommunityID, &rawType, &item.Reason, &item.IssuedAt, &item.ExpiresAt); err != nil {
			return nil, fmt.Errorf("scan active sanction: %w", err)
		}
		item.Type = models.SanctionType(rawType)
		item.Appealable = item.Type == models.SanctionTypeMute || item.Type == models.SanctionTypeBan
		sanctions = append(sanctions, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active sanctions: %w", err)
	}
	return sanctions, nil
}

// LockDueMutes selects due timed mutes with row-level locking; concurrent
// replicas skip rows another replica already holds.
func (r *sanctionRepository) LockDueMutes(ctx context.Context, limit int, now time.Time) ([]models.Sanction, error) {
	const query = `SELECT ` + sanctionColumns + ` FROM sanction
		WHERE type = 'mute' AND lifted_at IS NULL AND expires_at IS NOT NULL AND expires_at <= $1::timestamptz
		ORDER BY expires_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`

	rows, err := r.q.Query(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("select due mutes: %w", err)
	}
	defer rows.Close()

	sanctions, err := collectSanctions(rows, now)
	if err != nil {
		return nil, err
	}
	return sanctions, nil
}

// MarkLifted records a lift on a still-active sanction.
func (r *sanctionRepository) MarkLifted(ctx context.Context, id string, details LiftDetails) error {
	const query = `UPDATE sanction
		SET lifted_at = $2::timestamptz, lift_source = $3, lift_reason = $4
		WHERE id = $1::text::uuid AND lifted_at IS NULL`
	tag, err := r.q.Exec(ctx, query, id, details.At, string(details.Source), details.Reason)
	if err != nil {
		return fmt.Errorf("lift sanction: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return models.ErrSanctionNotActive
	}
	return nil
}

// collectSanctions drains and decodes a sanction result set.
func collectSanctions(rows pgx.Rows, now time.Time) ([]models.Sanction, error) {
	sanctions := make([]models.Sanction, 0, 8)
	for rows.Next() {
		sanction, err := scanSanction(rows, now)
		if err != nil {
			return nil, err
		}
		sanctions = append(sanctions, *sanction)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sanctions: %w", err)
	}
	return sanctions, nil
}

// scanSanction decodes one sanction row. Text enums are scanned as plain
// strings first and then converted, keeping the pgx scan targets homogeneous.
func scanSanction(row pgx.Row, now time.Time) (*models.Sanction, error) {
	var (
		sanction   models.Sanction
		rawType    string
		rawSource  string
		rawLiftSrc *string
	)
	err := row.Scan(
		&sanction.ID, &sanction.CommunityID, &sanction.TargetUserID, &sanction.ActorUserID,
		&rawType, &sanction.Reason, &rawSource, &sanction.IssuedAt, &sanction.ExpiresAt,
		&sanction.LiftedAt, &rawLiftSrc, &sanction.LiftReason,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan sanction: %w", err)
	}
	sanction.Type = models.SanctionType(rawType)
	sanction.Source = models.SanctionSource(rawSource)
	if rawLiftSrc != nil {
		liftSource := models.SanctionSource(*rawLiftSrc)
		sanction.LiftSource = &liftSource
	}
	sanction.Status = sanction.ComputeStatus(now)
	return &sanction, nil
}

// nullableSource converts an optional sanction source into a scan-safe value.
func nullableSource(source *models.SanctionSource) *string {
	if source == nil {
		return nil
	}
	value := string(*source)
	return &value
}

// fetchLimit asks for one extra row so the caller can detect a next page.
func fetchLimit(page models.Pagination) int {
	limit := page.Limit
	if limit <= 0 {
		limit = models.DefaultPageSize
	}
	return limit + 1
}

// trimPage drops the look-ahead row and reports whether a next page exists.
func trimPage[T any](rows []T, limit int) ([]T, bool, error) {
	if limit <= 0 {
		limit = models.DefaultPageSize
	}
	if len(rows) > limit {
		return rows[:limit], true, nil
	}
	return rows, false, nil
}

// cursorTimestamp returns the keyset timestamp of a cursor, or nil to start at
// the first page.
func cursorTimestamp(cursor *models.Cursor) *time.Time {
	if cursor == nil {
		return nil
	}
	return &cursor.Timestamp
}

// cursorID returns the keyset id of a cursor, or nil to start at the first page.
func cursorID(cursor *models.Cursor) *string {
	if cursor == nil {
		return nil
	}
	return &cursor.ID
}
