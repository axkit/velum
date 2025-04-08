package pgxw

import (
	"context"
	"errors"
	"fmt"

	"github.com/axkit/velum"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DatabaseWrapper struct {
	db *pgxpool.Pool
}

type TransactionWrapper struct {
	tx pgx.Tx
}

func NewDatabaseWrapper(db *pgxpool.Pool) *DatabaseWrapper {
	return &DatabaseWrapper{db: db}
}

func (w *DatabaseWrapper) DB() *pgxpool.Pool {
	return w.db
}
func (w *DatabaseWrapper) IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
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

	return fn(tx)
}

func (w *DatabaseWrapper) Begin(ctx context.Context) (velum.Transaction, error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TransactionWrapper{tx: tx}, nil
}

func (tx *TransactionWrapper) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

func (tx *TransactionWrapper) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
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

const doPrint = true

func (w *DatabaseWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {

	if doPrint {
		fmt.Printf("ExecContext: %d: %s\n", len(args), sql)
	}
	commangTag, err := w.db.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &ResultWrapper{rowsAffected: commangTag.RowsAffected()}, nil
}

func (w *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	if doPrint {
		fmt.Printf("QueryContext: %d: %s\n", len(args), sql)
	}

	res, err := w.db.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

func (w *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	if doPrint {
		fmt.Printf("QueryRowContext: %d: %s\n", len(args), sql)
	}

	row := w.db.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}

func (tw *TransactionWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	if doPrint {
		fmt.Printf("TransactionWrapper.ExecContext: %d: %s\n", len(args), sql)
	}

	commangTag, err := tw.tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &ResultWrapper{rowsAffected: commangTag.RowsAffected()}, nil
}
func (tw *TransactionWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryContext: %d: %s\n", len(args), sql)
	}

	res, err := tw.tx.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

func (tw *TransactionWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryRowContext: %d: %s\n", len(args), sql)
	}

	row := tw.tx.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}
