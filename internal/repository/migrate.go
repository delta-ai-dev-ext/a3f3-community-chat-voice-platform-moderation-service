package repository

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	// The migration runner needs a database/sql driver; pgx stdlib provides it.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// migrationsFS embeds the SQL migrations in the binary so a stateless container
// carries its own schema.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// migrationsDir is the embed path holding the migration files.
const migrationsDir = "migrations"

// Migrate applies every pending migration. It is called before the HTTP
// listener starts, so readiness fails until the schema is current.
func Migrate(databaseURL string) error {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open migration connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	source, err := iofs.New(migrationsFS, migrationsDir)
	if err != nil {
		return fmt.Errorf("load embedded migrations: %w", err)
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("create migration driver: %w", err)
	}
	runner, err := migrate.NewWithInstance("iofs", source, "postgres", driver)
	if err != nil {
		return fmt.Errorf("create migration runner: %w", err)
	}
	if err := runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
