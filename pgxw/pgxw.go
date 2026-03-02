// Package pgxw adapts *pgxpool.Pool and pgx.Tx from the jackc/pgx/v5 driver
// to the velum.DatabaseWrapper and velum.Transaction interfaces.
// Use NewDatabaseWrapper to create a wrapper around an existing *pgxpool.Pool.
package pgxw

import (
	"context"
	"errors"

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
func (dw *DatabaseWrapper) DB() *pgxpool.Pool {
	return dw.db
}

// IsNotFound reports whether err represents a "no rows" condition
// (pgx.ErrNoRows).
func (dw *DatabaseWrapper) IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

// InTx executes fn inside a pgx transaction. The transaction is committed if
// fn returns nil; otherwise it is rolled back.
func (dw *DatabaseWrapper) InTx(ctx context.Context, fn func(tx velum.Transaction) error) error {
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

	return fn(tx)
}

// Begin starts a new pgx transaction and returns it as a velum.Transaction.
func (dw *DatabaseWrapper) Begin(ctx context.Context) (velum.Transaction, error) {
	tx, err := dw.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TransactionWrapper{tx: tx}, nil
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

// Columns returns the column names for the result set. It satisfies the
// logw.columnsNamer interface so that logw.Wrapper can label scanned values
// by column name instead of positional placeholders.
func (rw *RowsWrapper) Columns() ([]string, error) {
	fds := rw.Rows.FieldDescriptions()
	cols := make([]string, len(fds))
	for i, fd := range fds {
		cols[i] = fd.Name
	}
	return cols, nil
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
func (dw *DatabaseWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {

	commangTag, err := dw.db.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}

	return &ResultWrapper{rowsAffected: commangTag.RowsAffected()}, nil
}

// QueryRowContext executes a query that returns at most one row.
func (dw *DatabaseWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {

	row := dw.db.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}

// QueryContext executes a query that returns multiple rows.
func (dw *DatabaseWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	res, err := dw.db.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}

// Commit commits the transaction.
func (tw *TransactionWrapper) Commit(ctx context.Context) error {
	return tw.tx.Commit(ctx)
}

// Rollback aborts the transaction.
func (tw *TransactionWrapper) Rollback(ctx context.Context) error {
	return tw.tx.Rollback(ctx)
}

// ExecContext executes a statement inside the transaction that does not return rows.
func (tw *TransactionWrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	commangTag, err := tw.tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &ResultWrapper{rowsAffected: commangTag.RowsAffected()}, nil
}

// QueryRowContext executes a query inside the transaction that returns at most one row.
func (tw *TransactionWrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	row := tw.tx.QueryRow(ctx, sql, args...)
	return &RowWrapper{row}
}

// QueryContext executes a query inside the transaction that returns multiple rows.
func (tw *TransactionWrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	res, err := tw.tx.Query(ctx, sql, args...)
	return &RowsWrapper{res}, err
}
