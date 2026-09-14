// Package sqlw adapts *sql.DB and *sql.Tx from the standard database/sql
// package to the velum.DatabaseWrapper and velum.Transaction interfaces.
// Use NewDatabaseWrapper to create a wrapper around an existing *sql.DB.
package sqlw

import (
	"context"
	"database/sql"
	"errors"

	"github.com/axkit/velum"
)

// DatabaseWrapper wraps *sql.DB to implement velum.DatabaseWrapper.
// Create it with NewDatabaseWrapper and pass it to Table and Dataset methods.
type DatabaseWrapper struct {
	db *sql.DB
}

// TransactionWrapper wraps *sql.Tx to implement velum.Transaction.
// It is returned by DatabaseWrapper.Begin and used inside DatabaseWrapper.InTx.
type TransactionWrapper struct {
	tx *sql.Tx
}

// RowsWrapper wraps *sql.Rows to implement velum.Rows. It overrides Close so
// that it always returns a non-nil error interface, matching the velum.Rows
// contract (sql.Rows.Close returns void before Go 1.22 in some contexts).
type RowsWrapper struct {
	sql.Rows
}

func (rw *RowsWrapper) Close() error {
	rw.Rows.Close()
	return nil
}

// ResultWrapper adapts a row-count value to velum.Result.
type ResultWrapper struct {
	rowsAffected int64
}

// RowsAffected returns the number of rows affected by the statement.
func (rw *ResultWrapper) RowsAffected() (int64, error) {
	return rw.rowsAffected, nil
}

// RowWrapper wraps *sql.Row to implement velum.Row. It adds the Err() method
// required by velum.Row (sql.Row exposes Err() starting in Go 1.15, but the
// velum.Row interface requires it unconditionally).
type RowWrapper struct {
	sql.Row
}

// Err always returns nil because sql.Row surfaces errors through Scan.
func (rw *RowWrapper) Err() error {
	return nil
}

// NewDatabaseWrapper wraps db and returns a DatabaseWrapper that implements
// velum.DatabaseWrapper using database/sql.
func NewDatabaseWrapper(db *sql.DB) *DatabaseWrapper {
	return &DatabaseWrapper{db: db}
}

// DB returns the underlying *sql.DB.
func (dw *DatabaseWrapper) DB() *sql.DB {
	return dw.db
}

// IsNotFound reports whether err represents a "no rows" condition
// (sql.ErrNoRows).
func (dw *DatabaseWrapper) IsNotFound(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}

// ExecContext executes a statement that does not return rows.
func (dw *DatabaseWrapper) ExecContext(ctx context.Context, query string, args ...any) (velum.Result, error) {
	return dw.db.ExecContext(ctx, query, args...)
}

// QueryRowContext executes a query that returns at most one row.
func (dw *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	return dw.db.QueryRowContext(ctx, sql, args...)
}

// QueryContext executes a query that returns multiple rows.
func (dw *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	return dw.db.QueryContext(ctx, sql, args...)
}

// InTx executes fn inside a database/sql transaction. The transaction is
// committed if fn returns nil; otherwise it is rolled back.
func (dw *DatabaseWrapper) InTx(ctx context.Context, fn func(tx velum.Transaction) error) (err error) {
	tx, err := dw.Begin(ctx)
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

// Begin starts a new database/sql transaction and returns a TransactionWrapper.
func (dw *DatabaseWrapper) Begin(ctx context.Context) (TransactionWrapper, error) {
	tx, err := dw.db.BeginTx(ctx, nil)
	if err != nil {
		return TransactionWrapper{}, err
	}
	return TransactionWrapper{tx: tx}, nil
}

// Commit commits the transaction.
func (tw *TransactionWrapper) Commit(ctx context.Context) error {
	return tw.tx.Commit()
}

// Rollback aborts the transaction.
func (tw *TransactionWrapper) Rollback(ctx context.Context) error {
	return tw.tx.Rollback()
}

const doPrint = false

// ExecContext executes a statement inside the transaction that does not return rows.
func (tw *TransactionWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	return tw.tx.ExecContext(ctx, sql, args...)
}

// QueryRowContext executes a query inside the transaction that returns at most one row.
func (tw *TransactionWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	return tw.tx.QueryRowContext(ctx, sql, args...)
}

// QueryContext executes a query inside the transaction that returns multiple rows.
func (tw *TransactionWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	return tw.tx.QueryContext(ctx, sql, args...)
}
