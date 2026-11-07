// Package repository owns all PostgreSQL access. Every query is parameterized
// and lives behind an interface so services can be unit-tested without a
// database. Business rules never live here (see docs/engineering_guide.md).
package repository

import (
	"context"
	"errors"
	"time"

	"moderation_service/internal/models"
)

// ErrDuplicate reports a unique-constraint violation. Callers decide whether it
// is a conflict worth surfacing or a benign concurrent-insert race.
var ErrDuplicate = errors.New("repository: duplicate row")

// ErrNotFound is the internal "no rows" signal; repositories translate it into
// the domain-specific not-found error before returning.
var ErrNotFound = errors.New("repository: not found")

// LiftDetails is the lifecycle transition applied when a sanction is lifted.
type LiftDetails struct {
	// At is the lift timestamp stored in lifted_at.
	At time.Time
	// Source records who or what lifted the sanction.
	Source models.SanctionSource
	// Reason is the optional human reason retained for the audit trail.
	Reason *string
}

// AppealDecision is the transition applied when a moderator decides an appeal.
type AppealDecision struct {
	// Status is the resulting appeal status (upheld or overturned).
	Status models.AppealStatus
	// ReviewerUserID is the moderator who decided the appeal.
	ReviewerUserID string
	// Note is the optional decision note.
	Note *string
	// DecidedAt is the decision timestamp stored in decided_at.
	DecidedAt time.Time
}

// SanctionFilter narrows a moderator sanction listing.
type SanctionFilter struct {
	// CommunityID scopes the listing to one community.
	CommunityID string
	// Status optionally filters on the computed lifecycle status.
	Status *models.SanctionStatus
	// Page is the resolved limit and keyset position.
	Page models.Pagination
	// Now is the clock used for the computed active/expired split.
	Now time.Time
}

// AppealFilter narrows an appeal listing.
type AppealFilter struct {
	// CommunityID scopes the listing to one community.
	CommunityID string
	// Status optionally filters on the appeal status.
	Status *models.AppealStatus
	// AppellantUserID optionally scopes the listing to one member.
	AppellantUserID *string
	// Page is the resolved limit and keyset position.
	Page models.Pagination
}

// AuditFilter narrows an audit log listing. Since is the retention lower bound
// and is always set by the service so reads never cross the one-year window.
type AuditFilter struct {
	// CommunityID scopes the listing to one community.
	CommunityID string
	// Action optionally filters on the audit action type.
	Action *models.AuditAction
	// ActorUserID optionally filters on the acting user.
	ActorUserID *string
	// Since is the inclusive lower bound for created_at.
	Since time.Time
	// Page is the resolved limit and keyset position.
	Page models.Pagination
}

// SanctionRepository is the persistence port for sanctions.
type SanctionRepository interface {
	// Create inserts a new sanction.
	Create(ctx context.Context, sanction *models.Sanction) error
	// GetByID loads one sanction by id regardless of community.
	GetByID(ctx context.Context, id string, now time.Time) (*models.Sanction, error)
	// GetByIDInCommunity loads one sanction scoped to its community.
	GetByIDInCommunity(ctx context.Context, communityID, id string, now time.Time) (*models.Sanction, error)
	// List returns one page of a community's sanctions, newest first.
	List(ctx context.Context, filter SanctionFilter) ([]models.Sanction, bool, error)
	// ActiveByTarget returns the caller's active sanctions across communities.
	ActiveByTarget(ctx context.Context, targetUserID string, now time.Time) ([]models.MySanction, error)
	// LockDueMutes selects due timed mutes with FOR UPDATE SKIP LOCKED so
	// concurrent replicas never lift the same mute twice.
	LockDueMutes(ctx context.Context, limit int, now time.Time) ([]models.Sanction, error)
	// MarkLifted records a lift on a still-active sanction.
	MarkLifted(ctx context.Context, id string, details LiftDetails) error
}

// AppealRepository is the persistence port for appeals.
type AppealRepository interface {
	// Create inserts a new submitted appeal.
	Create(ctx context.Context, appeal *models.Appeal) error
	// GetInCommunity loads one appeal scoped to its community.
	GetInCommunity(ctx context.Context, communityID, id string) (*models.Appeal, error)
	// FindOpenBySanction returns the open appeal of a sanction, if any.
	FindOpenBySanction(ctx context.Context, sanctionID string) (*models.Appeal, error)
	// Decide records a moderator decision on a still-open appeal.
	Decide(ctx context.Context, id string, decision AppealDecision) error
	// List returns one page of appeals matching the filter.
	List(ctx context.Context, filter AppealFilter) ([]models.Appeal, bool, error)
	// OpenByTarget maps sanction id to its open appeal id for a member.
	OpenByTarget(ctx context.Context, targetUserID string) (map[string]string, error)
}

// AutomodRepository is the persistence port for automod rule sets.
type AutomodRepository interface {
	// Current returns the community's current rule set.
	Current(ctx context.Context, communityID string) (*models.AutomodRuleSet, error)
	// InsertVersion stores a new rule set as max(version)+1 and marks it
	// current, retiring the previous version in the same transaction.
	InsertVersion(ctx context.Context, ruleSet *models.AutomodRuleSet) error
}

// AuditRepository is the persistence port for the append-only audit log.
type AuditRepository interface {
	// Append inserts one immutable audit entry.
	Append(ctx context.Context, entry *models.AuditLogEntry) error
	// List returns one page of audit entries matching the filter.
	List(ctx context.Context, filter AuditFilter) ([]models.AuditLogEntry, bool, error)
	// PurgeOlderThan deletes at most batchLimit entries older than cutoff.
	PurgeOlderThan(ctx context.Context, cutoff time.Time, batchLimit int) (int64, error)
}

// OutboxRepository is the persistence port for the transactional outbox.
type OutboxRepository interface {
	// Append inserts an unpublished event row.
	Append(ctx context.Context, event *models.OutboxEvent) error
	// FetchUnpublished returns the oldest unpublished rows.
	FetchUnpublished(ctx context.Context, limit int) ([]models.OutboxEvent, error)
	// MarkPublished stamps published_at on a relayed row.
	MarkPublished(ctx context.Context, id string, publishedAt time.Time) error
	// PrunePublishedBefore deletes relayed rows older than the grace window.
	PrunePublishedBefore(ctx context.Context, publishedBefore time.Time) (int64, error)
}

// IdempotencyRepository is the persistence port for Idempotency-Key replay.
type IdempotencyRepository interface {
	// Get returns the stored record, or nil when the key is unknown.
	Get(ctx context.Context, scope, key string) (*models.IdempotencyRecord, error)
	// Put stores the first accepted response for the key.
	Put(ctx context.Context, record *models.IdempotencyRecord) error
}

// Repositories bundles every data-access port. A single value is handed to a
// unit of work so all writes in one business action share one transaction.
type Repositories struct {
	Sanctions   SanctionRepository
	Appeals     AppealRepository
	Automod     AutomodRepository
	Audit       AuditRepository
	Outbox      OutboxRepository
	Idempotency IdempotencyRepository
}

// Store multiplexes repository access over the connection pool or, inside
// InTx, over a single transaction. It is the only seam services need.
type Store interface {
	// Read returns repositories bound to the pool (auto-commit).
	Read() Repositories
	// InTx runs fn with repositories bound to one transaction; the transaction
	// commits when fn returns nil and rolls back otherwise.
	InTx(ctx context.Context, fn func(ctx context.Context, repos Repositories) error) error
	// Ping verifies database connectivity for the readiness probe.
	Ping(ctx context.Context) error
}
