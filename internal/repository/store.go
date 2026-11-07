package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier is the subset of pgx used by repositories, satisfied by both
// *pgxpool.Pool and pgx.Tx. Keeping it narrow is what lets every repository run
// unchanged inside or outside a transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PgxStore implements Store over a pgxpool.
type PgxStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*PgxStore)(nil)

// NewPgxStore wraps an existing pool.
func NewPgxStore(pool *pgxpool.Pool) *PgxStore { return &PgxStore{pool: pool} }

// NewPool opens a pgx pool for the given connection string.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}
	return pool, nil
}

// Read returns repositories bound to the pool.
func (s *PgxStore) Read() Repositories { return newRepositories(s.pool) }

// InTx runs fn inside a single transaction.
func (s *PgxStore) InTx(ctx context.Context, fn func(ctx context.Context, repos Repositories) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	// Rollback is a no-op once the transaction is committed; using a
	// cancellation-free context guarantees the cleanup runs on shutdown.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := fn(ctx, newRepositories(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// Ping checks database connectivity for the readiness probe.
func (s *PgxStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// newRepositories binds every repository to one querier.
func newRepositories(q Querier) Repositories {
	return Repositories{
		Sanctions:   &sanctionRepository{q: q},
		Appeals:     &appealRepository{q: q},
		Automod:     &automodRepository{q: q},
		Audit:       &auditRepository{q: q},
		Outbox:      &outboxRepository{q: q},
		Idempotency: &idempotencyRepository{q: q},
	}
}

// isUniqueViolation reports whether err is a PostgreSQL unique violation,
// optionally for a specific constraint name.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code != "23505" {
		return false
	}
	return constraint == "" || pgErr.ConstraintName == constraint
}

// uniqueViolationConstraint returns the violated constraint name, if any.
func uniqueViolationConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return pgErr.ConstraintName
	}
	return ""
}

// nullableText returns nil for an empty string so optional filters stay NULL.
func nullableText(value *string) *string {
	if value == nil || *value == "" {
		return nil
	}
	return value
}
