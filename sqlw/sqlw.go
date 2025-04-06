package sqlw

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/axkit/velum"
)

type DatabaseWrapper struct {
	db *sql.DB
}

type TransactionWrapper struct {
	tx *sql.Tx
}

type RowsWrapper struct {
	sql.Rows
}

func (rw *RowsWrapper) Close() error {
	rw.Rows.Close()
	return nil
}

type ResultWrapper struct {
	rowsAffected int64
}

func (r *ResultWrapper) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type RowWrapper struct {
	sql.Row
}

func (rw *RowWrapper) Err() error {
	return nil
}

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

func (w *DatabaseWrapper) InTx(ctx context.Context, fn func(tx velum.Transaction) error) error {
	tx, err := w.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	return fn(&tx)
}

func (w *DatabaseWrapper) Begin(ctx context.Context) (TransactionWrapper, error) {
	tx, err := w.db.Begin()
	if err != nil {
		return TransactionWrapper{}, err
	}
	return TransactionWrapper{tx: tx}, nil
}

func (tx *TransactionWrapper) Commit(ctx context.Context) error {
	return tx.tx.Commit()
}

func (tx *TransactionWrapper) Rollback(ctx context.Context) error {
	return tx.tx.Rollback()
}

const doPrint = false

func (tw *TransactionWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	if doPrint {
		fmt.Printf("TransactionWrapper.ExecContext: %d: %s\n", len(args), sql)
	}
	return tw.tx.ExecContext(ctx, sql, args...)
}

func (tw *TransactionWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryContext: %d: %s\n", len(args), sql)
	}
	return tw.tx.QueryContext(ctx, sql, args...)
	//return &RowsWrapper{res}, err
}

func (tw *TransactionWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryRowContext: %d: %s\n", len(args), sql)
	}
	return tw.tx.QueryRowContext(ctx, sql, args...)
}
