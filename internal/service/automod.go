package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"moderation_service/internal/events"
	"moderation_service/internal/models"
	"moderation_service/internal/observability"
)

// Automod validation bounds, taken from the AutomodSpamRules /
// AutomodBannedWords schemas.
const (
	maxWindowSeconds    = 3600
	maxBannedWords      = 1000
	maxBannedWordLength = 100
	maxExemptRoleIDs    = 250
)

// AutomodService owns the versioned, per-community automatic filter
// configuration. messaging_service consumes the published snapshot.
type AutomodService struct {
	deps Deps
}

// NewAutomodService builds the automod service.
func NewAutomodService(deps Deps) *AutomodService { return &AutomodService{deps: deps} }

// Get returns the community's current rule set. A community that never
// configured automod gets 404 automod_not_configured.
func (s *AutomodService) Get(ctx context.Context, actorUserID, communityID string) (*models.AutomodRuleSet, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionManageAutomod, nil); err != nil {
		return nil, err
	}
	return s.deps.Store.Read().Automod.Current(ctx, communityID)
}

// Put validates a full rule set, stores it as max(rule_set_version)+1, marks it
// current, and commits the EVT-005 snapshot plus the audit entry with it.
func (s *AutomodService) Put(ctx context.Context, actorUserID, communityID string, input models.AutomodRuleSetInput, idempotencyKey string) (*models.AutomodRuleSet, error) {
	if err := requireUUID(communityID, "community_id"); err != nil {
		return nil, err
	}
	if err := requireUUID(actorUserID, "actor"); err != nil {
		return nil, err
	}
	normalized, err := validateAutomodInput(input)
	if err != nil {
		return nil, err
	}

	now := s.deps.now()
	scope := idempotencyScope("automod.put", communityID, actorUserID)
	if replayed, err := s.replay(ctx, scope, idempotencyKey); err != nil || replayed != nil {
		return replayed, err
	}

	if err := s.deps.authorize(ctx, actorUserID, communityID, models.PermissionManageAutomod, nil); err != nil {
		return nil, err
	}

	ruleSet := &models.AutomodRuleSet{
		CommunityID:   communityID,
		Spam:          normalized.Spam,
		BannedWords:   normalized.BannedWords,
		ExemptRoleIDs: normalized.ExemptRoleIDs,
		UpdatedAt:     now,
	}

	// The version is only known after the insert, so the envelope is built with
	// the stored snapshot; the message is finalised inside the transaction.
	err = s.deps.Store.InTx(ctx, func(ctx context.Context, repos repositories) error {
		if err := repos.Automod.InsertVersion(ctx, ruleSet); err != nil {
			return err
		}
		message, buildErr := s.deps.builder().AutomodRulesChanged(events.Meta{
			TraceID:       traceIDOf(ctx),
			CorrelationID: models.NewUUID(),
		}, events.AutomodRulesChangedPayload{
			CommunityID:    ruleSet.CommunityID,
			RuleSetVersion: ruleSet.RuleSetVersion,
			Spam:           ruleSet.Spam,
			BannedWords:    ruleSet.BannedWords,
			ExemptRoleIDs:  ruleSet.ExemptRoleIDs,
		})
		if buildErr != nil {
			return fmt.Errorf("build automod_rules_changed event: %w", buildErr)
		}
		if err := s.deps.appendEvent(ctx, repos, message, communityID, now); err != nil {
			return err
		}
		auditEntry, auditErr := newAuditEntry(now, models.AuditActionAutomodConfigChanged, communityID, stringPtr(actorUserID), nil, nil, map[string]any{
			"rule_set_version": ruleSet.RuleSetVersion,
		})
		if auditErr != nil {
			return auditErr
		}
		if err := repos.Audit.Append(ctx, auditEntry); err != nil {
			return err
		}
		return storeResponse(ctx, repos, scope, idempotencyKey, input, 200, ruleSet, now)
	})
	if err != nil {
		return nil, err
	}

	s.deps.Metrics.RecordAutomodUpdate()
	s.deps.logger().Info("automod rule set stored",
		zap.String("trace_id", observability.TraceID(ctx)),
		zap.String("community_id", communityID),
		zap.Int("rule_set_version", ruleSet.RuleSetVersion),
		zap.String("operation", "automod_put"),
		zap.String("status", "accepted"),
	)
	return ruleSet, nil
}

// replay returns the first accepted response for an Idempotency-Key.
func (s *AutomodService) replay(ctx context.Context, scope, key string) (*models.AutomodRuleSet, error) {
	if key == "" {
		return nil, nil
	}
	var ruleSet models.AutomodRuleSet
	found, err := replayResponse(ctx, s.deps.Store.Read(), scope, key, &ruleSet)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return &ruleSet, nil
}

// validateAutomodInput enforces the documented rule-set constraints and returns
// the normalized copy that will be stored and published. Every violation is a
// 422 invalid_automod_config, so the previous version stays current.
func validateAutomodInput(input models.AutomodRuleSetInput) (models.AutomodRuleSetInput, error) {
	normalized := input
	invalid := func(detail string) error { return models.ErrInvalidAutomodConfig.WithDetail(detail) }

	spam := input.Spam
	if spam.Action != models.AutomodActionBlock && spam.Action != models.AutomodActionRemove {
		return normalized, invalid("spam.action must be block or remove.")
	}
	if spam.MaxMessagesPerWindow < 1 {
		return normalized, invalid("spam.max_messages_per_window must be at least 1.")
	}
	if spam.WindowSeconds < 1 || spam.WindowSeconds > maxWindowSeconds {
		return normalized, invalid("spam.window_seconds must be between 1 and 3600.")
	}
	if spam.MaxDuplicateMessages < 1 {
		return normalized, invalid("spam.max_duplicate_messages must be at least 1.")
	}
	if spam.MaxMentionsPerMessage < 1 {
		return normalized, invalid("spam.max_mentions_per_message must be at least 1.")
	}
	if spam.MaxLinksPerMessage < 0 {
		return normalized, invalid("spam.max_links_per_message must be zero or greater.")
	}

	bannedWords := input.BannedWords
	if bannedWords.Action != models.AutomodActionBlock && bannedWords.Action != models.AutomodActionRemove {
		return normalized, invalid("banned_words.action must be block or remove.")
	}
	if bannedWords.MatchMode != models.AutomodMatchWholeWord && bannedWords.MatchMode != models.AutomodMatchSubstring {
		return normalized, invalid("banned_words.match_mode must be whole_word or substring.")
	}
	if len(bannedWords.Words) > maxBannedWords {
		return normalized, invalid(fmt.Sprintf("banned_words.words must contain at most %d entries.", maxBannedWords))
	}
	seenWords := make(map[string]struct{}, len(bannedWords.Words))
	words := make([]string, 0, len(bannedWords.Words))
	for _, word := range bannedWords.Words {
		if word == "" || len(word) > maxBannedWordLength {
			return normalized, invalid(fmt.Sprintf("each banned word must be between 1 and %d characters.", maxBannedWordLength))
		}
		if _, duplicate := seenWords[word]; duplicate {
			return normalized, invalid("banned_words.words must be unique.")
		}
		seenWords[word] = struct{}{}
		words = append(words, word)
	}
	bannedWords.Words = words
	normalized.BannedWords = bannedWords

	if len(input.ExemptRoleIDs) > maxExemptRoleIDs {
		return normalized, invalid(fmt.Sprintf("exempt_role_ids must contain at most %d entries.", maxExemptRoleIDs))
	}
	seenRoles := make(map[string]struct{}, len(input.ExemptRoleIDs))
	roles := make([]string, 0, len(input.ExemptRoleIDs))
	for _, roleID := range input.ExemptRoleIDs {
		if !models.IsUUID(roleID) {
			return normalized, invalid("exempt_role_ids must contain UUIDs.")
		}
		if _, duplicate := seenRoles[roleID]; duplicate {
			return normalized, invalid("exempt_role_ids must be unique.")
		}
		seenRoles[roleID] = struct{}{}
		roles = append(roles, roleID)
	}
	normalized.ExemptRoleIDs = roles
	normalized.Spam = spam
	return normalized, nil
}
