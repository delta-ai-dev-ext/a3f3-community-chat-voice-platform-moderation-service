package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"moderation_service/internal/models"
)

// automodRepository is the pgx implementation of AutomodRepository.
type automodRepository struct {
	q Querier
}

var _ AutomodRepository = (*automodRepository)(nil)

// Current returns the community's current rule set.
func (r *automodRepository) Current(ctx context.Context, communityID string) (*models.AutomodRuleSet, error) {
	const query = `SELECT community_id::text, rule_set_version, spam, banned_words, exempt_role_ids::text[], created_at
		FROM automod_rule_set
		WHERE community_id = $1::text::uuid AND is_current`
	ruleSet, err := scanAutomodRuleSet(r.q.QueryRow(ctx, query, communityID))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, models.ErrAutomodNotConfigured
		}
		return nil, fmt.Errorf("load automod rule set: %w", err)
	}
	return ruleSet, nil
}

// InsertVersion retires the current version and stores the new rule set as
// max(rule_set_version) + 1, so versions are strictly increasing per community
// and exactly one row is current.
func (r *automodRepository) InsertVersion(ctx context.Context, ruleSet *models.AutomodRuleSet) error {
	const retire = `UPDATE automod_rule_set SET is_current = false
		WHERE community_id = $1::text::uuid AND is_current`
	if _, err := r.q.Exec(ctx, retire, ruleSet.CommunityID); err != nil {
		return fmt.Errorf("retire automod rule set: %w", err)
	}

	spam, err := json.Marshal(ruleSet.Spam)
	if err != nil {
		return fmt.Errorf("encode spam rules: %w", err)
	}
	bannedWords, err := json.Marshal(ruleSet.BannedWords)
	if err != nil {
		return fmt.Errorf("encode banned words: %w", err)
	}

	const insert = `INSERT INTO automod_rule_set
		(community_id, rule_set_version, spam, banned_words, exempt_role_ids, is_current, created_at)
		VALUES (
		    $1::text::uuid,
		    COALESCE((SELECT max(rule_set_version) FROM automod_rule_set WHERE community_id = $1::text::uuid), 0) + 1,
		    $2::text::jsonb, $3::text::jsonb, $4::text[]::uuid[], true, $5::timestamptz)
		RETURNING rule_set_version, created_at`

	var storedAt time.Time
	err = r.q.QueryRow(ctx, insert, ruleSet.CommunityID, string(spam), string(bannedWords), ruleSet.ExemptRoleIDs, ruleSet.UpdatedAt).
		Scan(&ruleSet.RuleSetVersion, &storedAt)
	if err != nil {
		return fmt.Errorf("insert automod rule set: %w", err)
	}
	ruleSet.UpdatedAt = storedAt
	return nil
}

// scanAutomodRuleSet decodes one rule set row.
func scanAutomodRuleSet(row pgx.Row) (*models.AutomodRuleSet, error) {
	var (
		ruleSet     models.AutomodRuleSet
		spam        []byte
		bannedWords []byte
		updatedAt   time.Time
	)
	err := row.Scan(&ruleSet.CommunityID, &ruleSet.RuleSetVersion, &spam, &bannedWords, &ruleSet.ExemptRoleIDs, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan automod rule set: %w", err)
	}
	if err := json.Unmarshal(spam, &ruleSet.Spam); err != nil {
		return nil, fmt.Errorf("decode spam rules: %w", err)
	}
	if err := json.Unmarshal(bannedWords, &ruleSet.BannedWords); err != nil {
		return nil, fmt.Errorf("decode banned words: %w", err)
	}
	ruleSet.UpdatedAt = updatedAt
	if ruleSet.ExemptRoleIDs == nil {
		ruleSet.ExemptRoleIDs = []string{}
	}
	if ruleSet.BannedWords.Words == nil {
		ruleSet.BannedWords.Words = []string{}
	}
	return &ruleSet, nil
}
