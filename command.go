package velum

import (
	"context"
	"errors"
)

var (
	ErrScopeMismatch = errors.New("scope mismatch")
)

type StructFieldPtrExtractor[T any] interface {
	StructFieldPtrs(v *T, fieldPositions []int) *[]any
	Release(*[]any)
}

type FunctionalCommand[T any] struct {
	sql string
}

type Command[T any] struct {
	sql  string
	cpos []int
	sfpe StructFieldPtrExtractor[T]
}

type SelectCommand[T any] Command[T]

type ReturningCommand[T any] struct {
	Command[T]
	rets []int
}

func (c *Command[T]) Exec(ctx context.Context, q Executer, row *T, args ...any) (Result, error) {

	ptrs := c.sfpe.StructFieldPtrs(row, c.cpos)
	defer c.sfpe.Release(ptrs)

	joinedPtrs := *ptrs
	if len(args) > 0 {
		joinedPtrs = append(joinedPtrs, args...)
	}

	return q.ExecContext(ctx, c.sql, joinedPtrs...)
}

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

func (c *SelectCommand[T]) GetToPtr(ctx context.Context, q QueryRowExecuter, dst []any, args ...any) error {

	row := q.QueryRowContext(ctx, c.sql, args...)
	if err := row.Err(); err != nil {
		return err
	}
	return row.Scan(dst...)
}

func (c *SelectCommand[T]) GetMany(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {

	rows, err := q.QueryContext(ctx, c.sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var row T
	rets := c.sfpe.StructFieldPtrs(&row, c.cpos)
	defer c.sfpe.Release(rets)
	for rows.Next() {
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

func (c *ReturningCommand[T]) Query(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {

	rows, err := q.QueryContext(ctx, c.sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var row T
	rets := c.sfpe.StructFieldPtrs(&row, c.rets)
	defer c.sfpe.Release(rets)
	for rows.Next() {
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

func (c *FunctionalCommand[T]) Call(ctx context.Context, q QueryRowExecuter, dst any, args ...any) error {

	row := q.QueryRowContext(ctx, c.sql, args...)
	if err := row.Err(); err != nil {
		return err
	}

	return row.Scan(dst)
}
