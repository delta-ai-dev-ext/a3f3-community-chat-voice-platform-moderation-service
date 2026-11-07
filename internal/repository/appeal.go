package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"moderation_service/internal/models"
)

// appealColumns is the projection every appeal query uses.
const appealColumns = `id::text, sanction_id::text, community_id::text, appellant_user_id::text,
	statement, status, reviewer_user_id::text, decision_note, created_at, decided_at`

// appealRepository is the pgx implementation of AppealRepository.
type appealRepository struct {
	q Querier
}

var _ AppealRepository = (*appealRepository)(nil)

// Create inserts a new submitted appeal. The partial unique index uq_appeal_open
// is the authority on "one open appeal per sanction".
func (r *appealRepository) Create(ctx context.Context, appeal *models.Appeal) error {
	const query = `INSERT INTO appeal
		(id, sanction_id, community_id, appellant_user_id, statement, status, reviewer_user_id, decision_note, created_at, decided_at)
		VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4::text::uuid, $5, $6, $7::text::uuid, $8, $9::timestamptz, $10::timestamptz)`
	_, err := r.q.Exec(ctx, query,
		appeal.ID, appeal.SanctionID, appeal.CommunityID, appeal.AppellantUserID, appeal.Statement,
		string(appeal.Status), appeal.ReviewerUserID, appeal.DecisionNote, appeal.CreatedAt, appeal.DecidedAt,
	)
	if isUniqueViolation(err, "uq_appeal_open") || isUniqueViolation(err, "appeal_pkey") {
		return models.ErrAppealAlreadyOpen
	}
	if isUniqueViolation(err, "") {
		return ErrDuplicate
	}
	if err != nil {
		return fmt.Errorf("insert appeal: %w", err)
	}
	return nil
}

// GetInCommunity loads one appeal scoped to its community.
func (r *appealRepository) GetInCommunity(ctx context.Context, communityID, id string) (*models.Appeal, error) {
	const query = `SELECT ` + appealColumns + ` FROM appeal
		WHERE id = $1::text::uuid AND community_id = $2::text::uuid`
	appeal, err := scanAppeal(r.q.QueryRow(ctx, query, id, communityID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, models.ErrAppealNotFound
		}
		return nil, fmt.Errorf("load appeal: %w", err)
	}
	return appeal, nil
}

// FindOpenBySanction returns the open appeal of a sanction, if any.
func (r *appealRepository) FindOpenBySanction(ctx context.Context, sanctionID string) (*models.Appeal, error) {
	const query = `SELECT ` + appealColumns + ` FROM appeal
		WHERE sanction_id = $1::text::uuid AND status = 'submitted'`
	appeal, err := scanAppeal(r.q.QueryRow(ctx, query, sanctionID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("load open appeal: %w", err)
	}
	return appeal, nil
}

// Decide records a moderator decision on a still-open appeal.
func (r *appealRepository) Decide(ctx context.Context, id string, decision AppealDecision) error {
	const query = `UPDATE appeal
		SET status = $2, reviewer_user_id = $3::text::uuid, decision_note = $4, decided_at = $5::timestamptz
		WHERE id = $1::text::uuid AND status = 'submitted'`
	tag, err := r.q.Exec(ctx, query, id, string(decision.Status), decision.ReviewerUserID, decision.Note, decision.DecidedAt)
	if err != nil {
		return fmt.Errorf("decide appeal: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return models.ErrAppealAlreadyDecided
	}
	return nil
}

// List returns one page of appeals matching the filter, newest first.
func (r *appealRepository) List(ctx context.Context, filter AppealFilter) ([]models.Appeal, bool, error) {
	const query = `SELECT ` + appealColumns + ` FROM appeal
		WHERE community_id = $1::text::uuid
		  AND ($2::text IS NULL OR status = $2::text)
		  AND ($3::text::uuid IS NULL OR appellant_user_id = $3::text::uuid)
		  AND ($4::timestamptz IS NULL OR (created_at, id) < ($4::timestamptz, $5::text::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $6`

	var status *string
	if filter.Status != nil {
		value := string(*filter.Status)
		status = &value
	}
	rows, err := r.q.Query(ctx, query, filter.CommunityID, status, nullableText(filter.AppellantUserID),
		cursorTimestamp(filter.Page.Cursor), cursorID(filter.Page.Cursor), fetchLimit(filter.Page))
	if err != nil {
		return nil, false, fmt.Errorf("list appeals: %w", err)
	}
	defer rows.Close()

	appeals := make([]models.Appeal, 0, 8)
	for rows.Next() {
		appeal, err := scanAppeal(rows)
		if err != nil {
			return nil, false, err
		}
		appeals = append(appeals, *appeal)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate appeals: %w", err)
	}
	return trimPage(appeals, filter.Page.Limit)
}

// OpenByTarget maps sanction id to its open appeal id for one member.
func (r *appealRepository) OpenByTarget(ctx context.Context, targetUserID string) (map[string]string, error) {
	const query = `SELECT a.sanction_id::text, a.id::text
		FROM appeal a
		JOIN sanction s ON s.id = a.sanction_id
		WHERE a.status = 'submitted' AND s.target_user_id = $1::text::uuid`
	rows, err := r.q.Query(ctx, query, targetUserID)
	if err != nil {
		return nil, fmt.Errorf("list open appeals: %w", err)
	}
	defer rows.Close()

	open := make(map[string]string, 4)
	for rows.Next() {
		var sanctionID, appealID string
		if err := rows.Scan(&sanctionID, &appealID); err != nil {
			return nil, fmt.Errorf("scan open appeal: %w", err)
		}
		open[sanctionID] = appealID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate open appeals: %w", err)
	}
	return open, nil
}

// scanAppeal decodes one appeal row.
func scanAppeal(row pgx.Row) (*models.Appeal, error) {
	var (
		appeal    models.Appeal
		rawStatus string
	)
	err := row.Scan(
		&appeal.ID, &appeal.SanctionID, &appeal.CommunityID, &appeal.AppellantUserID, &appeal.Statement,
		&rawStatus, &appeal.ReviewerUserID, &appeal.DecisionNote, &appeal.CreatedAt, &appeal.DecidedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan appeal: %w", err)
	}
	appeal.Status = models.AppealStatus(rawStatus)
	return &appeal, nil
}
