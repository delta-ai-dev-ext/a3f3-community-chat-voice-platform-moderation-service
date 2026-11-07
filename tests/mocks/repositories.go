package mocks

import (
	"context"
	"slices"
	"sort"
	"time"

	"moderation_service/internal/models"
	"moderation_service/internal/repository"
)

// AppealRepo is an in-memory AppealRepository.
type AppealRepo struct {
	items []models.Appeal
	// CreateErr overrides Create when set.
	CreateErr error
	// DecideErr overrides Decide when set.
	DecideErr error
}

var _ repository.AppealRepository = (*AppealRepo)(nil)

// Add seeds an appeal.
func (r *AppealRepo) Add(appeal models.Appeal) { r.items = append(r.items, appeal) }

// All returns every stored appeal.
func (r *AppealRepo) All() []models.Appeal { return slices.Clone(r.items) }

// Len returns the number of stored appeals.
func (r *AppealRepo) Len() int { return len(r.items) }

// ByID returns the stored appeal with the given id.
func (r *AppealRepo) ByID(id string) (models.Appeal, bool) {
	for _, item := range r.items {
		if item.ID == id {
			return item, true
		}
	}
	return models.Appeal{}, false
}

// Create stores a submitted appeal, enforcing one open appeal per sanction.
func (r *AppealRepo) Create(_ context.Context, appeal *models.Appeal) error {
	if r.CreateErr != nil {
		return r.CreateErr
	}
	for _, item := range r.items {
		if item.SanctionID == appeal.SanctionID && item.Status == models.AppealStatusSubmitted {
			return models.ErrAppealAlreadyOpen
		}
	}
	r.items = append(r.items, *appeal)
	return nil
}

// GetInCommunity loads an appeal scoped to its community.
func (r *AppealRepo) GetInCommunity(_ context.Context, communityID, id string) (*models.Appeal, error) {
	for _, item := range r.items {
		if item.ID == id && item.CommunityID == communityID {
			copied := item
			return &copied, nil
		}
	}
	return nil, models.ErrAppealNotFound
}

// FindOpenBySanction returns the open appeal of a sanction.
func (r *AppealRepo) FindOpenBySanction(_ context.Context, sanctionID string) (*models.Appeal, error) {
	for _, item := range r.items {
		if item.SanctionID == sanctionID && item.Status == models.AppealStatusSubmitted {
			copied := item
			return &copied, nil
		}
	}
	return nil, nil
}

// Decide records a decision on a still-open appeal.
func (r *AppealRepo) Decide(_ context.Context, id string, decision repository.AppealDecision) error {
	if r.DecideErr != nil {
		return r.DecideErr
	}
	for index := range r.items {
		if r.items[index].ID != id {
			continue
		}
		if r.items[index].Status != models.AppealStatusSubmitted {
			return models.ErrAppealAlreadyDecided
		}
		decidedAt := decision.DecidedAt
		reviewer := decision.ReviewerUserID
		r.items[index].Status = decision.Status
		r.items[index].ReviewerUserID = &reviewer
		r.items[index].DecisionNote = decision.Note
		r.items[index].DecidedAt = &decidedAt
		return nil
	}
	return models.ErrAppealNotFound
}

// List returns one page of matching appeals, newest first.
func (r *AppealRepo) List(_ context.Context, filter repository.AppealFilter) ([]models.Appeal, bool, error) {
	matched := make([]models.Appeal, 0, len(r.items))
	for _, item := range r.items {
		if filter.CommunityID != "" && item.CommunityID != filter.CommunityID {
			continue
		}
		if filter.Status != nil && item.Status != *filter.Status {
			continue
		}
		if filter.AppellantUserID != nil && item.AppellantUserID != *filter.AppellantUserID {
			continue
		}
		matched = append(matched, item)
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].ID > matched[j].ID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})
	if filter.Page.Cursor != nil {
		filtered := matched[:0]
		for _, row := range matched {
			if row.CreatedAt.After(filter.Page.Cursor.Timestamp) {
				continue
			}
			if row.CreatedAt.Equal(filter.Page.Cursor.Timestamp) && row.ID >= filter.Page.Cursor.ID {
				continue
			}
			filtered = append(filtered, row)
		}
		matched = filtered
	}
	limit := filter.Page.Limit
	if limit <= 0 {
		limit = models.DefaultPageSize
	}
	if len(matched) > limit {
		return matched[:limit], true, nil
	}
	return matched, false, nil
}

// OpenByTarget maps sanction id to open appeal id for one member.
func (r *AppealRepo) OpenByTarget(_ context.Context, targetUserID string) (map[string]string, error) {
	open := make(map[string]string, 2)
	for _, item := range r.items {
		if item.AppellantUserID == targetUserID && item.Status == models.AppealStatusSubmitted {
			open[item.SanctionID] = item.ID
		}
	}
	return open, nil
}

// automodRow is one stored automod version.
type automodRow struct {
	ruleSet   models.AutomodRuleSet
	isCurrent bool
}

// AutomodRepo is an in-memory AutomodRepository.
type AutomodRepo struct {
	items []automodRow
	// InsertErr overrides InsertVersion when set.
	InsertErr error
}

var _ repository.AutomodRepository = (*AutomodRepo)(nil)

// Current returns the community's current rule set.
func (r *AutomodRepo) Current(_ context.Context, communityID string) (*models.AutomodRuleSet, error) {
	for _, item := range r.items {
		if item.ruleSet.CommunityID == communityID && item.isCurrent {
			copied := item.ruleSet
			copied.ExemptRoleIDs = slices.Clone(copied.ExemptRoleIDs)
			copied.BannedWords.Words = slices.Clone(copied.BannedWords.Words)
			return &copied, nil
		}
	}
	return nil, models.ErrAutomodNotConfigured
}

// InsertVersion stores a new version as max+1 and marks it current.
func (r *AutomodRepo) InsertVersion(_ context.Context, ruleSet *models.AutomodRuleSet) error {
	if r.InsertErr != nil {
		return r.InsertErr
	}
	maxVersion := 0
	for index := range r.items {
		if r.items[index].ruleSet.CommunityID != ruleSet.CommunityID {
			continue
		}
		r.items[index].isCurrent = false
		if r.items[index].ruleSet.RuleSetVersion > maxVersion {
			maxVersion = r.items[index].ruleSet.RuleSetVersion
		}
	}
	ruleSet.RuleSetVersion = maxVersion + 1
	stored := *ruleSet
	stored.ExemptRoleIDs = slices.Clone(ruleSet.ExemptRoleIDs)
	stored.BannedWords.Words = slices.Clone(ruleSet.BannedWords.Words)
	r.items = append(r.items, automodRow{ruleSet: stored, isCurrent: true})
	return nil
}

// Versions returns the stored versions in insertion order.
func (r *AutomodRepo) Versions(communityID string) []models.AutomodRuleSet {
	versions := make([]models.AutomodRuleSet, 0, len(r.items))
	for _, item := range r.items {
		if item.ruleSet.CommunityID == communityID {
			versions = append(versions, item.ruleSet)
		}
	}
	return versions
}

// CurrentCount returns how many versions are flagged current for a community.
func (r *AutomodRepo) CurrentCount(communityID string) int {
	count := 0
	for _, item := range r.items {
		if item.ruleSet.CommunityID == communityID && item.isCurrent {
			count++
		}
	}
	return count
}

// AuditRepo is an in-memory AuditRepository.
type AuditRepo struct {
	items []models.AuditLogEntry
	// AppendErr overrides Append when set.
	AppendErr error
}

var _ repository.AuditRepository = (*AuditRepo)(nil)

// Add seeds an audit entry.
func (r *AuditRepo) Add(entry models.AuditLogEntry) { r.items = append(r.items, entry) }

// All returns every stored entry.
func (r *AuditRepo) All() []models.AuditLogEntry { return slices.Clone(r.items) }

// Len returns the number of stored entries.
func (r *AuditRepo) Len() int { return len(r.items) }

// Actions returns the action types in insertion order.
func (r *AuditRepo) Actions() []models.AuditAction {
	actions := make([]models.AuditAction, 0, len(r.items))
	for _, item := range r.items {
		actions = append(actions, item.Action)
	}
	return actions
}

// Append stores one entry.
func (r *AuditRepo) Append(_ context.Context, entry *models.AuditLogEntry) error {
	if r.AppendErr != nil {
		return r.AppendErr
	}
	r.items = append(r.items, *entry)
	return nil
}

// List returns one page of entries since the retention boundary.
func (r *AuditRepo) List(_ context.Context, filter repository.AuditFilter) ([]models.AuditLogEntry, bool, error) {
	matched := make([]models.AuditLogEntry, 0, len(r.items))
	for _, item := range r.items {
		if item.CommunityID != filter.CommunityID {
			continue
		}
		if item.CreatedAt.Before(filter.Since) {
			continue
		}
		if filter.Action != nil && item.Action != *filter.Action {
			continue
		}
		if filter.ActorUserID != nil && (item.ActorUserID == nil || *item.ActorUserID != *filter.ActorUserID) {
			continue
		}
		matched = append(matched, item)
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].ID > matched[j].ID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})
	if filter.Page.Cursor != nil {
		filtered := matched[:0]
		for _, row := range matched {
			if row.CreatedAt.After(filter.Page.Cursor.Timestamp) {
				continue
			}
			if row.CreatedAt.Equal(filter.Page.Cursor.Timestamp) && row.ID >= filter.Page.Cursor.ID {
				continue
			}
			filtered = append(filtered, row)
		}
		matched = filtered
	}
	limit := filter.Page.Limit
	if limit <= 0 {
		limit = models.DefaultPageSize
	}
	if len(matched) > limit {
		return matched[:limit], true, nil
	}
	return matched, false, nil
}

// PurgeOlderThan deletes at most batchLimit entries older than cutoff.
func (r *AuditRepo) PurgeOlderThan(_ context.Context, cutoff time.Time, batchLimit int) (int64, error) {
	kept := make([]models.AuditLogEntry, 0, len(r.items))
	var deleted int64
	for _, item := range r.items {
		if item.CreatedAt.Before(cutoff) && deleted < int64(batchLimit) {
			deleted++
			continue
		}
		kept = append(kept, item)
	}
	r.items = kept
	return deleted, nil
}

// OutboxRepo is an in-memory OutboxRepository.
type OutboxRepo struct {
	items []models.OutboxEvent
	// AppendErr overrides Append when set.
	AppendErr error
}

var _ repository.OutboxRepository = (*OutboxRepo)(nil)

// Append stores an unpublished event.
func (r *OutboxRepo) Append(_ context.Context, event *models.OutboxEvent) error {
	if r.AppendErr != nil {
		return r.AppendErr
	}
	r.items = append(r.items, *event)
	return nil
}

// All returns every stored event.
func (r *OutboxRepo) All() []models.OutboxEvent { return slices.Clone(r.items) }

// Len returns the number of stored events.
func (r *OutboxRepo) Len() int { return len(r.items) }

// Unpublished returns the events that are still waiting for relay.
func (r *OutboxRepo) Unpublished() []models.OutboxEvent {
	pending := make([]models.OutboxEvent, 0, len(r.items))
	for _, item := range r.items {
		if item.PublishedAt == nil {
			pending = append(pending, item)
		}
	}
	return pending
}

// FetchUnpublished returns the oldest unpublished events.
func (r *OutboxRepo) FetchUnpublished(_ context.Context, limit int) ([]models.OutboxEvent, error) {
	pending := r.Unpublished()
	sort.Slice(pending, func(i, j int) bool { return pending[i].CreatedAt.Before(pending[j].CreatedAt) })
	if len(pending) > limit {
		return pending[:limit], nil
	}
	return pending, nil
}

// MarkPublished stamps an event as relayed.
func (r *OutboxRepo) MarkPublished(_ context.Context, id string, publishedAt time.Time) error {
	for index := range r.items {
		if r.items[index].ID == id {
			stamp := publishedAt
			r.items[index].PublishedAt = &stamp
			return nil
		}
	}
	return nil
}

// PrunePublishedBefore deletes relayed events older than the grace window.
func (r *OutboxRepo) PrunePublishedBefore(_ context.Context, publishedBefore time.Time) (int64, error) {
	kept := make([]models.OutboxEvent, 0, len(r.items))
	var deleted int64
	for _, item := range r.items {
		if item.PublishedAt != nil && item.PublishedAt.Before(publishedBefore) {
			deleted++
			continue
		}
		kept = append(kept, item)
	}
	r.items = kept
	return deleted, nil
}

// IdempotencyRepo is an in-memory IdempotencyRepository.
type IdempotencyRepo struct {
	items []models.IdempotencyRecord
}

var _ repository.IdempotencyRepository = (*IdempotencyRepo)(nil)

// Get returns the stored record for a key, or nil.
func (r *IdempotencyRepo) Get(_ context.Context, scope, key string) (*models.IdempotencyRecord, error) {
	for _, item := range r.items {
		if item.Scope == scope && item.Key == key {
			copied := item
			return &copied, nil
		}
	}
	return nil, nil
}

// Put stores a record once per key.
func (r *IdempotencyRepo) Put(_ context.Context, record *models.IdempotencyRecord) error {
	for _, item := range r.items {
		if item.Scope == record.Scope && item.Key == record.Key {
			return repository.ErrDuplicate
		}
	}
	r.items = append(r.items, *record)
	return nil
}

// Len returns the number of stored records.
func (r *IdempotencyRepo) Len() int { return len(r.items) }
