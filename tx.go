package velum

import (
	"context"
	"errors"
)

// IsoLevel is a transaction isolation level. The zero value, IsoDefault,
// leaves the level unset so that the server default applies, which keeps the
// behaviour of Begin and InTx unchanged.
type IsoLevel uint8

const (
	// IsoDefault does not set an isolation level explicitly.
	IsoDefault IsoLevel = iota

	// ReadCommitted is the PostgreSQL default: every statement sees rows
	// committed before it began.
	ReadCommitted

	// RepeatableRead gives the whole transaction a single snapshot. Concurrent
	// writes to the same row abort the transaction with SQLSTATE 40001, so the
	// caller must be ready to retry it; see IsRetryable.
	RepeatableRead

	// Serializable behaves as if transactions ran one after another. It aborts
	// with SQLSTATE 40001 like RepeatableRead, but for more kinds of conflict.
	Serializable
)

// String returns the SQL name of the level, or "default" for IsoDefault.
func (l IsoLevel) String() string {
	switch l {
	case ReadCommitted:
		return "read committed"
	case RepeatableRead:
		return "repeatable read"
	case Serializable:
		return "serializable"
	default:
		return "default"
	}
}

// TxOptions carries the settings applied when a transaction starts. The zero
// value means "use the server defaults".
type TxOptions struct {
	// IsoLevel is the isolation level of the transaction.
	IsoLevel IsoLevel

	// ReadOnly starts the transaction in read-only mode. Writes then fail with
	// SQLSTATE 25006.
	ReadOnly bool
}

// ErrTxOptionsUnsupported is returned by the package-level BeginTx and
// InTxWith helpers when the wrapper does not implement TxDatabaseWrapper.
var ErrTxOptionsUnsupported = errors.New("velum: wrapper does not support transaction options")

// TxDatabaseWrapper is an optional interface for wrappers that can start a
// transaction with explicit options. It is kept separate from DatabaseWrapper
// so that existing implementations keep compiling. The sqlw, pgxw and logw
// packages all implement it.
type TxDatabaseWrapper interface {
	DatabaseWrapper

	// BeginTx starts a transaction with opts.
	BeginTx(context.Context, TxOptions) (Transaction, error)

	// InTxWith executes fn inside a transaction started with opts, committing
	// on success and rolling back on any non-nil error returned by fn.
	InTxWith(context.Context, TxOptions, func(Transaction) error) error

	// IsRetryable reports whether err is a transient conflict, so that the
	// caller may retry the whole transaction.
	IsRetryable(error) bool
}

// BeginTx starts a transaction with opts on db. It returns
// ErrTxOptionsUnsupported when db does not implement TxDatabaseWrapper, so
// that an unsupported isolation level fails loudly instead of silently
// falling back to the default.
func BeginTx(ctx context.Context, db DatabaseWrapper, opts TxOptions) (Transaction, error) {
	tdw, ok := db.(TxDatabaseWrapper)
	if !ok {
		return nil, ErrTxOptionsUnsupported
	}
	return tdw.BeginTx(ctx, opts)
}

// InTxWith executes fn inside a transaction started with opts. It saves the
// caller a type assertion when holding a plain DatabaseWrapper, and returns
// ErrTxOptionsUnsupported when db does not implement TxDatabaseWrapper.
func InTxWith(ctx context.Context, db DatabaseWrapper, opts TxOptions, fn func(Transaction) error) error {
	tdw, ok := db.(TxDatabaseWrapper)
	if !ok {
		return ErrTxOptionsUnsupported
	}
	return tdw.InTxWith(ctx, opts, fn)
}

// IsRetryable reports whether err is a transient conflict that justifies
// retrying the transaction. It returns false when db cannot classify errors.
func IsRetryable(db DatabaseWrapper, err error) bool {
	tdw, ok := db.(TxDatabaseWrapper)
	if !ok {
		return false
	}
	return tdw.IsRetryable(err)
}

// IsRetryableSQLState reports whether err carries a PostgreSQL SQLSTATE that
// marks a transient conflict: 40001 serialization_failure or 40P01
// deadlock_detected. Both abort the whole transaction, so the caller has to
// run it again from the start.
//
// It matches any error exposing SQLState() string, which both
// pgconn.PgError and pq.Error do, so wrappers stay free of driver imports.
func IsRetryableSQLState(err error) bool {
	var se interface{ SQLState() string }
	if !errors.As(err, &se) {
		return false
	}
	switch se.SQLState() {
	case "40001", "40P01":
		return true
	}
	return false
}
