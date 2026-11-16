package outbox

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SchemaSQL is the full outbox schema (table, index, notify trigger),
// embedded so tests and demos can apply it without external tooling.
//
//go:embed migrations/0001_outbox.up.sql
var SchemaSQL string

// Migrate applies the outbox schema on the given connection. It is
// idempotent and runs the statements in one round trip.
//
// This is a convenience for demos, tests and bootstrap code. In production,
// prefer running migrations/0001_outbox.up.sql through your usual migration
// tool (golang-migrate, goose, ...) so the schema is versioned like the rest
// of your DDL.
func Migrate(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.PgConn().Exec(ctx, SchemaSQL).ReadAll(); err != nil {
		return fmt.Errorf("apply outbox schema: %w", err)
	}
	return nil
}
