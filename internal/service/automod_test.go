package service_test

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"moderation_service/internal/models"
	"moderation_service/internal/service"
)

// validAutomodInput returns a rule set that passes validation.
func validAutomodInput() models.AutomodRuleSetInput {
	return models.AutomodRuleSetInput{
		Spam: models.AutomodSpamRules{
			Enabled:               true,
			Action:                models.AutomodActionBlock,
			MaxMessagesPerWindow:  5,
			WindowSeconds:         10,
			MaxDuplicateMessages:  2,
			MaxMentionsPerMessage: 3,
			MaxLinksPerMessage:    1,
		},
		BannedWords: models.AutomodBannedWords{
			Enabled:   true,
			Action:    models.AutomodActionRemove,
			MatchMode: models.AutomodMatchWholeWord,
			Words:     []string{"spam", "scam"},
		},
		ExemptRoleIDs: []string{peerID},
	}
}

// TestAutomodGetReturns404WhenNotConfigured pins the documented 404.
func TestAutomodGetReturns404WhenNotConfigured(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	automod := service.NewAutomodService(f.deps)

	_, err := automod.Get(ctx(), moderatorID, communityID)

	assert.Equal(t, models.ErrAutomodNotConfigured, err)
	f.authorizer.AssertExpectations(t)
}

// TestAutomodGetRequiresManageAutomod proves the read is authorized too.
func TestAutomodGetRequiresManageAutomod(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Deny("missing_permission")
	automod := service.NewAutomodService(f.deps)

	_, err := automod.Get(ctx(), peerID, communityID)

	assert.Equal(t, models.ErrInsufficientPermission, err)
	f.authorizer.AssertExpectations(t)
}

// TestAutomodPutVersionsRuleSets proves consecutive PUTs increment the version,
// keep exactly one current row, emit a full snapshot, and append an audit entry.
func TestAutomodPutVersionsRuleSets(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	f.authorizer.Allow()
	automod := service.NewAutomodService(f.deps)

	first, err := automod.Put(ctx(), moderatorID, communityID, validAutomodInput(), "")
	require.NoError(t, err)
	assert.Equal(t, 1, first.RuleSetVersion)

	second, err := automod.Put(ctx(), moderatorID, communityID, validAutomodInput(), "")
	require.NoError(t, err)
	assert.Equal(t, 2, second.RuleSetVersion)

	assert.Equal(t, 1, f.store.Automod.CurrentCount(communityID), "exactly one version stays current")
	versions := f.store.Automod.Versions(communityID)
	require.Len(t, versions, 2)
	assert.Less(t, versions[0].RuleSetVersion, versions[1].RuleSetVersion)

	rows := f.store.Outbox.All()
	require.Len(t, rows, 2)
	payload := decodeEnvelope(t, rows[1])
	assert.Equal(t, float64(2), payload["rule_set_version"])
	assert.Contains(t, payload, "spam")
	assert.Contains(t, payload, "banned_words")
	assert.Contains(t, payload, "exempt_role_ids")

	assert.Equal(t, []models.AuditAction{
		models.AuditActionAutomodConfigChanged,
		models.AuditActionAutomodConfigChanged,
	}, f.store.Audit.Actions())
	assert.Equal(t, float64(2), testutil.ToFloat64(f.metrics.AutomodConfigUpdates))
	f.authorizer.AssertExpectations(t)
}

// TestAutomodPutRejectsInvalidConfigs proves semantic violations are 422 and
// leave the stored state untouched.
func TestAutomodPutRejectsInvalidConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*models.AutomodRuleSetInput)
	}{
		{name: "spam action", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.Action = "mute" }},
		{name: "window too small", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.WindowSeconds = 0 }},
		{name: "window too large", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.WindowSeconds = 3601 }},
		{name: "zero message budget", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.MaxMessagesPerWindow = 0 }},
		{name: "zero duplicate budget", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.MaxDuplicateMessages = 0 }},
		{name: "zero mention budget", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.MaxMentionsPerMessage = 0 }},
		{name: "negative link budget", mutate: func(in *models.AutomodRuleSetInput) { in.Spam.MaxLinksPerMessage = -1 }},
		{name: "banned word action", mutate: func(in *models.AutomodRuleSetInput) { in.BannedWords.Action = "report" }},
		{name: "match mode", mutate: func(in *models.AutomodRuleSetInput) { in.BannedWords.MatchMode = "fuzzy" }},
		{name: "empty word", mutate: func(in *models.AutomodRuleSetInput) { in.BannedWords.Words = []string{""} }},
		{name: "word too long", mutate: func(in *models.AutomodRuleSetInput) {
			in.BannedWords.Words = []string{longString(101)}
		}},
		{name: "duplicate word", mutate: func(in *models.AutomodRuleSetInput) {
			in.BannedWords.Words = []string{"spam", "spam"}
		}},
		{name: "invalid exempt role", mutate: func(in *models.AutomodRuleSetInput) { in.ExemptRoleIDs = []string{"role-1"} }},
		{name: "duplicate exempt role", mutate: func(in *models.AutomodRuleSetInput) {
			in.ExemptRoleIDs = []string{peerID, peerID}
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t)
			input := validAutomodInput()
			test.mutate(&input)
			automod := service.NewAutomodService(f.deps)

			_, err := automod.Put(ctx(), moderatorID, communityID, input, "")

			assertErrorCode(t, err, "invalid_automod_config")
			assert.Zero(t, f.store.TxCount)
			assert.Empty(t, f.store.Outbox.All())
			assert.Empty(t, f.store.Automod.Versions(communityID))
			f.authorizer.AssertNotCalled(t, "Check")
		})
	}
}

// TestAutomodPutFailureRollsBack proves a write failure leaves the previous
// version current and publishes nothing.
func TestAutomodPutFailureRollsBack(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	f.authorizer.Allow()
	automod := service.NewAutomodService(f.deps)

	first, err := automod.Put(ctx(), moderatorID, communityID, validAutomodInput(), "")
	require.NoError(t, err)

	f.store.Automod.InsertErr = errors.New("insert failed")
	_, err = automod.Put(ctx(), moderatorID, communityID, validAutomodInput(), "")
	require.Error(t, err)

	current, err := f.store.Automod.Current(ctx(), communityID)
	require.NoError(t, err)
	assert.Equal(t, first.RuleSetVersion, current.RuleSetVersion)
	assert.Len(t, f.store.Outbox.All(), 1)
	assert.Equal(t, 1, f.store.TxRollbackCount)
}

// TestAutomodPutReplaysIdempotentKey proves the PUT is idempotent.
func TestAutomodPutReplaysIdempotentKey(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	automod := service.NewAutomodService(f.deps)
	input := validAutomodInput()

	first, err := automod.Put(ctx(), moderatorID, communityID, input, "automod-key")
	require.NoError(t, err)
	second, err := automod.Put(ctx(), moderatorID, communityID, input, "automod-key")
	require.NoError(t, err)

	assert.Equal(t, first.RuleSetVersion, second.RuleSetVersion)
	assert.Len(t, f.store.Automod.Versions(communityID), 1)
	assert.Len(t, f.store.Outbox.All(), 1)
}

// TestAuditListFiltersToRetentionWindow proves reads never cross the one-year
// boundary, even before the purge job has run.
func TestAuditListFiltersToRetentionWindow(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.authorizer.Allow()
	old := f.now.Add(-models.AuditRetention - 24*time.Hour)
	recent := f.now.Add(-time.Hour)
	f.store.Audit.Add(models.AuditLogEntry{
		ID:          "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
		CommunityID: communityID,
		Action:      models.AuditActionSanctionIssued,
		CreatedAt:   old,
	})
	f.store.Audit.Add(models.AuditLogEntry{
		ID:          "ffffffff-ffff-4fff-8fff-ffffffffffff",
		CommunityID: communityID,
		ActorUserID: ptr(moderatorID),
		Action:      models.AuditActionSanctionLifted,
		CreatedAt:   recent,
	})
	audit := service.NewAuditService(f.deps)

	page, err := audit.List(ctx(), moderatorID, communityID, nil, nil, models.Pagination{Limit: 10})
	require.NoError(t, err)

	require.Len(t, page.Data, 1)
	assert.Equal(t, "ffffffff-ffff-4fff-8fff-ffffffffffff", page.Data[0].ID)

	action := models.AuditActionSanctionIssued
	empty, err := audit.List(ctx(), moderatorID, communityID, &action, nil, models.Pagination{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, empty.Data, "the out-of-window entry is never returned")

	actor := moderatorID
	filtered, err := audit.List(ctx(), moderatorID, communityID, nil, &actor, models.Pagination{Limit: 10})
	require.NoError(t, err)
	require.Len(t, filtered.Data, 1)
	assert.Equal(t, ptr(moderatorID), filtered.Data[0].ActorUserID)
}

// TestAuditListRejectsInvalidFilters covers query validation.
func TestAuditListRejectsInvalidFilters(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	audit := service.NewAuditService(f.deps)

	action := models.AuditAction("deleted_message")
	_, err := audit.List(ctx(), moderatorID, communityID, &action, nil, models.Pagination{Limit: 10})
	assertErrorCode(t, err, "invalid_request")

	actor := "not-a-uuid"
	_, err = audit.List(ctx(), moderatorID, communityID, nil, &actor, models.Pagination{Limit: 10})
	assertErrorCode(t, err, "invalid_request")
	assert.Zero(t, f.store.TxCount)
	f.authorizer.AssertNotCalled(t, "Check")
}
