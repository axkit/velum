package pgxw

import (
	"context"
	"fmt"

	"github.com/axkit/velum"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DatabaseWrapper struct {
	db *pgxpool.Pool
}

func NewDatabaseWrapper(db *pgxpool.Pool) *DatabaseWrapper {
	return &DatabaseWrapper{db: db}
}

type ResultWrapper struct {
	rowsAffected int64
}

func (r *ResultWrapper) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type RowsWrapper struct {
	pgx.Rows
}

func (rw *RowsWrapper) Close() error {
	rw.Rows.Close()
	return nil
}

type RowWrapper struct {
	pgx.Row
}

func (rw *RowWrapper) Err() error {
	return nil
}

func (w *DatabaseWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {

	fmt.Printf("ExecContext: %d: %s\n", len(args), sql)

	commangTag, err := w.db.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &ResultWrapper{rowsAffected: commangTag.RowsAffected()}, nil
}

func (w *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	fmt.Printf("QueryContext: %d: %s\n", len(args), sql)

	res, err := w.db.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

func (w *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	fmt.Printf("QueryRowContext: %d: %s\n", len(args), sql)

	row := w.db.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}
