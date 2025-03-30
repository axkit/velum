package sqlw

import (
	"context"
	"database/sql"

	"github.com/axkit/velum"
)

type DatabaseWrapper struct {
	db *sql.DB
}

// type RowsWrapper struct {
// 	*sql.Rows
// }

// func (rw *RowsWrapper) Close() error {
// 	_ = rw.Rows.Close()
// }

func NewDatabaseWrapper(db *sql.DB) *DatabaseWrapper {
	return &DatabaseWrapper{db: db}
}

func (w *DatabaseWrapper) DB() *sql.DB {
	return w.db
}

func (w *DatabaseWrapper) ExecContext(ctx context.Context, query string, args ...any) (velum.Result, error) {
	return w.db.ExecContext(ctx, query, args...)
}

func (w *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	return w.db.QueryContext(ctx, sql, args...)
}

func (w *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	return w.db.QueryRowContext(ctx, sql, args...)
}
