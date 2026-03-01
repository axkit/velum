package pgxw

import (
	"context"
	"errors"
	"fmt"

	"github.com/axkit/velum"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseWrapper wraps *pgxpool.Pool to implement velum.DatabaseWrapper.
// Create it with NewDatabaseWrapper and pass it to Table and Dataset methods.
type DatabaseWrapper struct {
	db *pgxpool.Pool
}

// TransactionWrapper wraps pgx.Tx to implement velum.Transaction.
// It is returned by DatabaseWrapper.Begin and used inside DatabaseWrapper.InTx.
type TransactionWrapper struct {
	tx pgx.Tx
}

// NewDatabaseWrapper wraps db and returns a DatabaseWrapper that implements
// velum.DatabaseWrapper using pgx v5.
func NewDatabaseWrapper(db *pgxpool.Pool) *DatabaseWrapper {
	return &DatabaseWrapper{db: db}
}

// DB returns the underlying *pgxpool.Pool.
func (w *DatabaseWrapper) DB() *pgxpool.Pool {
	return w.db
}

// IsNotFound reports whether err represents a "no rows" condition
// (pgx.ErrNoRows).
func (w *DatabaseWrapper) IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// InTx executes fn inside a pgx transaction. The transaction is committed if
// fn returns nil; otherwise it is rolled back.
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

// Begin starts a new pgx transaction and returns it as a velum.Transaction.
func (w *DatabaseWrapper) Begin(ctx context.Context) (velum.Transaction, error) {
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TransactionWrapper{tx: tx}, nil
}

// Commit commits the transaction.
func (tx *TransactionWrapper) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

// Rollback aborts the transaction.
func (tx *TransactionWrapper) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

// ResultWrapper adapts pgconn.CommandTag's RowsAffected count to velum.Result.
type ResultWrapper struct {
	rowsAffected int64
}

// RowsAffected returns the number of rows affected by the statement.
func (r *ResultWrapper) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// RowsWrapper wraps pgx.Rows to implement velum.Rows. It overrides Close so
// that it returns error (pgx.Rows.Close returns void).
type RowsWrapper struct {
	pgx.Rows
}

// Close closes the rows iterator.
func (rw *RowsWrapper) Close() error {
	rw.Rows.Close()
	return nil
}

// RowWrapper wraps pgx.Row to implement velum.Row. It adds the Err() method
// required by velum.Row.
type RowWrapper struct {
	pgx.Row
}

// Err always returns nil because pgx.Row surfaces errors through Scan.
func (rw *RowWrapper) Err() error {
	return nil
}

const doPrint = true

// ExecContext executes a statement that does not return rows.
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

// QueryContext executes a query that returns multiple rows.
func (w *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	if doPrint {
		fmt.Printf("QueryContext: %d: %s\n", len(args), sql)
	}

	res, err := w.db.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

// QueryRowContext executes a query that returns at most one row.
func (w *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	if doPrint {
		fmt.Printf("QueryRowContext: %d: %s\n", len(args), sql)
	}

	row := w.db.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}

// ExecContext executes a statement inside the transaction that does not return rows.
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

// QueryContext executes a query inside the transaction that returns multiple rows.
func (tw *TransactionWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryContext: %d: %s\n", len(args), sql)
	}

	res, err := tw.tx.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

// QueryRowContext executes a query inside the transaction that returns at most one row.
func (tw *TransactionWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	if doPrint {
		fmt.Printf("TransactionWrapper.QueryRowContext: %d: %s\n", len(args), sql)
	}

	row := tw.tx.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}
