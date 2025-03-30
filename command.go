package velum

import (
	"context"
	"errors"
)

var (
	ErrScopeMismatch = errors.New("scope mismatch")
)

type StructFieldPtrExtractor[T any] interface {
	StructFieldPtrs(v *T, index [][]int) []any
	StructFieldPtrsXXX(v *T, scopeColumnPos []int) []any
	Release(*[]any)
}

type Command[T any] struct {
	sql  string
	args [][]int
	sfpe StructFieldPtrExtractor[T]
}

type SelectCommand[T any] Command[T]

type ReturningCommand[T any] struct {
	Command[T]
	rets [][]int
}

func (c *Command[T]) Exec(ctx context.Context, q Executer, row *T, whereArgs ...any) (Result, error) {

	args := c.sfpe.StructFieldPtrs(row, c.args)
	defer c.sfpe.Release(&args)

	joinedArgs := args
	if len(whereArgs) > 0 {
		joinedArgs = append(joinedArgs, whereArgs...)
	}

	return q.ExecContext(ctx, c.sql, joinedArgs...)
}

func (c *SelectCommand[T]) GetOne(ctx context.Context, q QueryRowExecuter, clauseArgs ...any) (*T, error) {

	row := q.QueryRowContext(ctx, c.sql, clauseArgs...)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.args)
	defer c.sfpe.Release(&rets)
	if err := row.Scan(rets...); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *SelectCommand[T]) GetMany(ctx context.Context, q QueryExecuter, clauseArgs ...any) ([]T, error) {

	rows, err := q.QueryContext(ctx, c.sql, clauseArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.args)
	defer c.sfpe.Release(&rets)
	for rows.Next() {
		if err := rows.Scan(rets...); err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}

func (c *ReturningCommand[T]) QueryRow(ctx context.Context, q QueryRowExecuter, str *T, whereArgs ...any) (*T, error) {

	args := c.sfpe.StructFieldPtrs(str, c.args)
	defer c.sfpe.Release(&args)

	joinedArgs := args
	if len(whereArgs) > 0 {
		joinedArgs = append(joinedArgs, whereArgs...)
	}

	row := q.QueryRowContext(ctx, c.sql, joinedArgs...)
	if err := row.Err(); err != nil {
		return nil, err
	}

	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.rets)
	defer c.sfpe.Release(&rets)
	if err := row.Scan(rets...); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *ReturningCommand[T]) Query(ctx context.Context, q QueryExecuter, whereArgs ...any) ([]T, error) {

	rows, err := q.QueryContext(ctx, c.sql, whereArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []T
	var res T
	rets := c.sfpe.StructFieldPtrs(&res, c.rets)
	defer c.sfpe.Release(&rets)
	for rows.Next() {
		if err := rows.Scan(rets...); err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}
