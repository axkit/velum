package velum

import (
	"context"
	"errors"
)

var (
	// ErrScopeMismatch is returned when the column positions required by a
	// command do not match the struct fields available in the pool.
	ErrScopeMismatch = errors.New("velum: scope mismatch")
)

// StructFieldPtrExtractor extracts a slice of pointers to the struct fields
// identified by fieldPositions, using an internal pool to avoid allocations.
// Callers must call Release when the slice is no longer needed.
type StructFieldPtrExtractor[T any] interface {
	// StructFieldPtrs returns a pooled *[]any whose elements are pointers to
	// the fields of v at the given column positions.
	StructFieldPtrs(v *T, fieldPositions []int) *[]any
	// Release returns the slice to the pool. It must be called exactly once
	// per StructFieldPtrs call, typically via defer.
	Release(*[]any)
}

// FunctionalCommand holds a pre-built SQL string for scalar-result queries
// such as EXISTS or COUNT. It carries no column-position metadata because the
// result is scanned into a caller-provided variable.
type FunctionalCommand[T any] struct {
	sql string
}

// Command holds a pre-built SQL string together with the column positions used
// to extract argument values from a struct (for INSERT SET columns or UPDATE
// SET columns). It is the base building block for all statement types.
type Command[T any] struct {
	sql  string
	cpos []int // column positions in the struct whose values are bound as SQL args
	sfpe StructFieldPtrExtractor[T]
}

// NewCommand constructs a Command with an explicit SQL string, extractor, and
// column-position list. It is intended for advanced use cases where the caller
// wants to supply a hand-written SQL statement while still benefiting from
// Velum's pointer-pool scanning.
func NewCommand[T any](sql string, sfpe StructFieldPtrExtractor[T], cpos []int) Command[T] {
	return Command[T]{
		sql:  sql,
		cpos: cpos,
		sfpe: sfpe,
	}
}

// SelectCommand holds a pre-built SELECT statement. It is a type alias for
// Command[T] with methods that scan query results into structs.
type SelectCommand[T any] Command[T]

// ReturningCommand holds a pre-built INSERT, UPDATE, or DELETE statement with
// a RETURNING clause. It carries two sets of column positions: cpos for the
// arguments passed to the statement, and rets for the columns to scan from
// the returned row.
type ReturningCommand[T any] struct {
	Command[T]
	rets []int // column positions in the struct to scan from RETURNING
}

// Exec executes a non-returning statement (INSERT without RETURNING, UPDATE,
// DELETE). Values at c.cpos in row are extracted and passed as SQL arguments.
// Any additional args are appended after the struct-derived arguments.
func (c *Command[T]) Exec(ctx context.Context, q Executer, row *T, args ...any) (Result, error) {
	ptrs := c.sfpe.StructFieldPtrs(row, c.cpos)
	defer c.sfpe.Release(ptrs)

	joinedPtrs := *ptrs
	if len(args) > 0 {
		joinedPtrs = append(joinedPtrs, args...)
	}

	return q.ExecContext(ctx, c.sql, joinedPtrs...)
}

// Get executes the SELECT statement and scans the single result row into a
// newly allocated T. It returns sql.ErrNoRows (or the driver equivalent) when
// no row is found.
func (c *SelectCommand[T]) Get(ctx context.Context, q QueryRowExecuter, args ...any) (*T, error) {
	row := q.QueryRowContext(ctx, c.sql, args...)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.cpos)
	defer c.sfpe.Release(rets)
	if err := row.Scan(*rets...); err != nil {
		return nil, err
	}
	return &res, nil
}

// GetToPtr executes the SELECT statement and scans the result row into the
// caller-provided dst slice of pointers. The caller is responsible for
// supplying correctly typed destination pointers.
func (c *SelectCommand[T]) GetToPtr(ctx context.Context, q QueryRowExecuter, dst []any, args ...any) error {
	row := q.QueryRowContext(ctx, c.sql, args...)
	if err := row.Err(); err != nil {
		return err
	}
	return row.Scan(dst...)
}

// GetMany executes the SELECT statement and returns all matching rows as a
// slice of T. It returns a nil slice (not an error) when no rows are found.
func (c *SelectCommand[T]) GetMany(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, c.sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var row, zero T
	rets := c.sfpe.StructFieldPtrs(&row, c.cpos)
	defer c.sfpe.Release(rets)
	for rows.Next() {
		// Reset row so values from the previous row can't leak into this one
		// (e.g. a Scanner that reuses the existing slice backing array).
		row = zero
		if err := rows.Scan(*rets...); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// QueryRow executes an INSERT/UPDATE/DELETE … RETURNING statement. Values at
// c.cpos in str are used as statement arguments; the RETURNING columns
// (c.rets) are scanned into a newly allocated T and returned.
// Any additional args are appended after the struct-derived arguments.
func (c *ReturningCommand[T]) QueryRow(ctx context.Context, q QueryRowExecuter, str *T, args ...any) (*T, error) {
	ptrs := c.sfpe.StructFieldPtrs(str, c.cpos)
	defer c.sfpe.Release(ptrs)

	joinedPtrs := *ptrs
	if len(args) > 0 {
		joinedPtrs = append(joinedPtrs, args...)
	}

	row := q.QueryRowContext(ctx, c.sql, joinedPtrs...)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.rets)
	defer c.sfpe.Release(rets)
	if err := row.Scan(*rets...); err != nil {
		return nil, err
	}
	return &res, nil
}

// QueryRowTo is like QueryRow but scans the RETURNING columns back into str
// instead of allocating a new struct. It is used by Insert to populate
// database-generated fields (e.g. ID, created_at) in place.
func (c *ReturningCommand[T]) QueryRowTo(ctx context.Context, q QueryRowExecuter, str *T, args ...any) error {
	ptrs := c.sfpe.StructFieldPtrs(str, c.cpos)
	defer c.sfpe.Release(ptrs)

	joinedPtrs := *ptrs
	if len(args) > 0 {
		joinedPtrs = append(joinedPtrs, args...)
	}

	row := q.QueryRowContext(ctx, c.sql, joinedPtrs...)
	if err := row.Err(); err != nil {
		return err
	}

	rets := c.sfpe.StructFieldPtrs(str, c.rets)
	defer c.sfpe.Release(rets)
	if err := row.Scan(*rets...); err != nil {
		return err
	}
	return nil
}

// Query executes an INSERT/UPDATE … RETURNING statement that may affect
// multiple rows and returns all returned rows as a slice of T.
func (c *ReturningCommand[T]) Query(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {
	rows, err := q.QueryContext(ctx, c.sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var row, zero T
	rets := c.sfpe.StructFieldPtrs(&row, c.rets)
	defer c.sfpe.Release(rets)
	for rows.Next() {
		// Reset row so values from the previous row can't leak into this one
		// (e.g. a Scanner that reuses the existing slice backing array).
		row = zero
		if err := rows.Scan(*rets...); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Call executes a scalar query (EXISTS, COUNT, etc.) and scans the single
// result value into dst, which must be a pointer to a compatible Go type.
func (c *FunctionalCommand[T]) Call(ctx context.Context, q QueryRowExecuter, dst any, args ...any) error {
	row := q.QueryRowContext(ctx, c.sql, args...)
	if err := row.Err(); err != nil {
		return err
	}
	return row.Scan(dst)
}
