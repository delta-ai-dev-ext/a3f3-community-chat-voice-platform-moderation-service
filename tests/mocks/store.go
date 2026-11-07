// Package mocks provides in-memory fakes and testify doubles for every
// dependency of the service, so the whole suite runs offline with no database
// and no network. The store reproduces the transactional contract of the pgx
// store: a failing unit of work is rolled back.
package mocks

import (
	"context"
	"slices"
	"sort"
	"time"

	"moderation_service/internal/models"
	"moderation_service/internal/repository"
)

// Store is an in-memory repository.Store.
type Store struct {
	// Sanctions is the sanction fake.
	Sanctions *SanctionRepo
	// Appeals is the appeal fake.
	Appeals *AppealRepo
	// Automod is the automod fake.
	Automod *AutomodRepo
	// Audit is the audit log fake.
	Audit *AuditRepo
	// Outbox is the outbox fake.
	Outbox *OutboxRepo
	// Idempotency is the idempotency fake.
	Idempotency *IdempotencyRepo

	// PingErr is returned by Ping for readiness tests.
	PingErr error
	// TxCount counts InTx invocations.
	TxCount int
	// TxCommitCount counts committed units of work.
	TxCommitCount int
	// TxRollbackCount counts rolled back units of work.
	TxRollbackCount int
}

var _ repository.Store = (*Store)(nil)

// NewStore returns an empty in-memory store.
func NewStore() *Store {
	return &Store{
		Sanctions:   &SanctionRepo{},
		Appeals:     &AppealRepo{},
		Automod:     &AutomodRepo{},
		Audit:       &AuditRepo{},
		Outbox:      &OutboxRepo{},
		Idempotency: &IdempotencyRepo{},
	}
}

// Read returns the fake repositories.
func (s *Store) Read() repository.Repositories { return s.repositories() }

// InTx runs fn and rolls every repository back when fn fails, reproducing the
// all-or-nothing guarantee the services rely on.
func (s *Store) InTx(ctx context.Context, fn func(ctx context.Context, repos repository.Repositories) error) error {
	s.TxCount++
	snapshot := s.snapshot()
	if err := fn(ctx, s.repositories()); err != nil {
		s.restore(snapshot)
		s.TxRollbackCount++
		return err
	}
	s.TxCommitCount++
	return nil
}

// Ping reports the configured error, or success.
func (s *Store) Ping(context.Context) error { return s.PingErr }

// repositories bundles the fakes.
func (s *Store) repositories() repository.Repositories {
	return repository.Repositories{
		Sanctions:   s.Sanctions,
		Appeals:     s.Appeals,
		Automod:     s.Automod,
		Audit:       s.Audit,
		Outbox:      s.Outbox,
		Idempotency: s.Idempotency,
	}
}

// snapshot copies every repository's rows.
type storeSnapshot struct {
	sanctions   []models.Sanction
	appeals     []models.Appeal
	ruleSets    []automodRow
	audit       []models.AuditLogEntry
	outbox      []models.OutboxEvent
	idempotency []models.IdempotencyRecord
}

// snapshot captures the current state.
func (s *Store) snapshot() storeSnapshot {
	return storeSnapshot{
		sanctions:   slices.Clone(s.Sanctions.items),
		appeals:     slices.Clone(s.Appeals.items),
		ruleSets:    slices.Clone(s.Automod.items),
		audit:       slices.Clone(s.Audit.items),
		outbox:      slices.Clone(s.Outbox.items),
		idempotency: slices.Clone(s.Idempotency.items),
	}
}

// restore puts a captured state back.
func (s *Store) restore(snapshot storeSnapshot) {
	s.Sanctions.items = snapshot.sanctions
	s.Appeals.items = snapshot.appeals
	s.Automod.items = snapshot.ruleSets
	s.Audit.items = snapshot.audit
	s.Outbox.items = snapshot.outbox
	s.Idempotency.items = snapshot.idempotency
}

// SanctionRepo is an in-memory SanctionRepository.
type SanctionRepo struct {
	items []models.Sanction
	// CreateErr overrides Create when set.
	CreateErr error
	// MarkLiftedErr overrides MarkLifted when set.
	MarkLiftedErr error
	// ListErr overrides List when set.
	ListErr error
}

var _ repository.SanctionRepository = (*SanctionRepo)(nil)

// Add seeds a sanction.
func (r *SanctionRepo) Add(sanction models.Sanction) { r.items = append(r.items, sanction) }

// All returns every stored sanction.
func (r *SanctionRepo) All() []models.Sanction { return slices.Clone(r.items) }

// Len returns the number of stored sanctions.
func (r *SanctionRepo) Len() int { return len(r.items) }

// ByID returns the stored sanction with the given id.
func (r *SanctionRepo) ByID(id string) (models.Sanction, bool) {
	for _, item := range r.items {
		if item.ID == id {
			return item, true
		}
	}
	return models.Sanction{}, false
}

// Create stores a sanction.
func (r *SanctionRepo) Create(_ context.Context, sanction *models.Sanction) error {
	if r.CreateErr != nil {
		return r.CreateErr
	}
	r.items = append(r.items, *sanction)
	return nil
}

// GetByID loads a sanction by id.
func (r *SanctionRepo) GetByID(_ context.Context, id string, now time.Time) (*models.Sanction, error) {
	return r.get(func(sanction models.Sanction) bool { return sanction.ID == id }, now)
}

// GetByIDInCommunity loads a sanction scoped to its community.
func (r *SanctionRepo) GetByIDInCommunity(_ context.Context, communityID, id string, now time.Time) (*models.Sanction, error) {
	return r.get(func(sanction models.Sanction) bool {
		return sanction.ID == id && sanction.CommunityID == communityID
	}, now)
}

// get finds the first matching sanction and returns a defensive copy.
func (r *SanctionRepo) get(match func(models.Sanction) bool, now time.Time) (*models.Sanction, error) {
	for _, item := range r.items {
		if match(item) {
			copied := item
			copied.Status = copied.ComputeStatus(now)
			return &copied, nil
		}
	}
	return nil, models.ErrSanctionNotFound
}

// List returns one page of a community's sanctions, newest first.
func (r *SanctionRepo) List(_ context.Context, filter repository.SanctionFilter) ([]models.Sanction, bool, error) {
	if r.ListErr != nil {
		return nil, false, r.ListErr
	}
	matched := make([]models.Sanction, 0, len(r.items))
	for _, item := range r.items {
		if item.CommunityID != filter.CommunityID {
			continue
		}
		copied := item
		copied.Status = copied.ComputeStatus(filter.Now)
		if filter.Status != nil && copied.Status != *filter.Status {
			continue
		}
		matched = append(matched, copied)
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].IssuedAt.Equal(matched[j].IssuedAt) {
			return matched[i].ID > matched[j].ID
		}
		return matched[i].IssuedAt.After(matched[j].IssuedAt)
	})
	return pageSanctions(matched, filter.Page)
}

// ActiveByTarget returns the caller's active sanctions.
func (r *SanctionRepo) ActiveByTarget(_ context.Context, targetUserID string, now time.Time) ([]models.MySanction, error) {
	active := make([]models.MySanction, 0, 4)
	for _, item := range r.items {
		if item.TargetUserID != targetUserID || item.LiftedAt != nil {
			continue
		}
		if item.ExpiresAt != nil && !item.ExpiresAt.After(now) {
			continue
		}
		active = append(active, models.MySanction{
			ID:          item.ID,
			CommunityID: item.CommunityID,
			Type:        item.Type,
			Reason:      item.Reason,
			IssuedAt:    item.IssuedAt,
			ExpiresAt:   item.ExpiresAt,
			Appealable:  item.Type == models.SanctionTypeMute || item.Type == models.SanctionTypeBan,
		})
	}
	return active, nil
}

// LockDueMutes returns due timed mutes, mirroring the SKIP LOCKED query.
func (r *SanctionRepo) LockDueMutes(_ context.Context, limit int, now time.Time) ([]models.Sanction, error) {
	due := make([]models.Sanction, 0, limit)
	for _, item := range r.items {
		if item.Type != models.SanctionTypeMute || item.LiftedAt != nil || item.ExpiresAt == nil {
			continue
		}
		if item.ExpiresAt.After(now) {
			continue
		}
		copied := item
		copied.Status = copied.ComputeStatus(now)
		due = append(due, copied)
		if len(due) == limit {
			break
		}
	}
	return due, nil
}

// MarkLifted lifts a still-active sanction.
func (r *SanctionRepo) MarkLifted(_ context.Context, id string, details repository.LiftDetails) error {
	if r.MarkLiftedErr != nil {
		return r.MarkLiftedErr
	}
	for index := range r.items {
		if r.items[index].ID != id {
			continue
		}
		if r.items[index].LiftedAt != nil {
			return models.ErrSanctionNotActive
		}
		liftedAt := details.At
		source := details.Source
		r.items[index].LiftedAt = &liftedAt
		r.items[index].LiftSource = &source
		r.items[index].LiftReason = details.Reason
		r.items[index].Status = models.StatusLifted
		return nil
	}
	return models.ErrSanctionNotFound
}

// pageSanctions applies a keyset page with a look-ahead row.
func pageSanctions(rows []models.Sanction, page models.Pagination) ([]models.Sanction, bool, error) {
	limit := page.Limit
	if limit <= 0 {
		limit = models.DefaultPageSize
	}
	filtered := make([]models.Sanction, 0, len(rows))
	for _, row := range rows {
		if page.Cursor != nil {
			if row.IssuedAt.After(page.Cursor.Timestamp) {
				continue
			}
			if row.IssuedAt.Equal(page.Cursor.Timestamp) && row.ID >= page.Cursor.ID {
				continue
			}
		}
		filtered = append(filtered, row)
	}
	if len(filtered) > limit {
		return filtered[:limit], true, nil
	}
	return filtered, false, nil
}
