package velum_test

import (
	"context"
	"testing"

	"github.com/axkit/velum"
)

func Benchmark_Select1_Sql_Raw_Outer(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	var c CustomerSerial

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		row := dbSql.QueryRowContext(ctx, `SELECT id, first_name, last_name, age,
	                              ssn, row_version,created_at, updated_at, deleted_at, deleted_by
							FROM customers_pk_serial WHERE id = $1`, 1)
		if err := row.Err(); err != nil {
			b.Fatalf("failed to select customers: %v", err)
		}

		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.Age,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}

func Benchmark_Select1_Pgx_Tbl_GetByPK(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	tbl := velum.NewTable[CustomerSerial]("customers_pk_serial")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c, err := tbl.GetByPK(ctx, dbwPgx, 1)
		if err != nil {
			b.Fatalf("failed to select customer: %v", err)
		}
		_ = c
	}
}

func Benchmark_Select1_Pgx_Tbl_GetTo(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	tbl := velum.NewTable[CustomerSerial]("customers_pk_serial")
	b.ReportAllocs()
	b.ResetTimer()
	c, ptrs := tbl.Object(velum.FullScope)
	for b.Loop() {
		err := tbl.GetTo(ctx, dbwPgx, *ptrs, 1)
		if err != nil {
			b.Fatalf("failed to select customer: %v", err)
		}
	}
	tbl.ObjectPut(c, ptrs)
}

func Benchmark_Select1_Pgx_Raw_Inner(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var c CustomerSerial

		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, age,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers_pk_serial WHERE id = $1`, 1)
		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.Age,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}
func Benchmark_Select1_Pgx_Raw_Inner_Slice(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var c CustomerSerial
		ptrs := []any{&c.ID, &c.FirstName,
			&c.LastName, &c.Age,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy}
		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, age,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers_pk_serial WHERE id = $1`, 1)
		err := row.Scan(ptrs...)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}

func Benchmark_Select1_Pgx_Raw_Outer(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	b.ReportAllocs()
	b.ResetTimer()
	var c CustomerSerial

	for b.Loop() {
		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, age,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers_pk_serial WHERE id = $1`, 1)
		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.Age,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}

func Benchmark_Select1_Pgx_Raw_Outer_Slice(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	var c CustomerSerial

	ptrs := []any{&c.ID, &c.FirstName,
		&c.LastName, &c.Age,
		&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, age,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers_pk_serial WHERE id = $1`, 1)
		err := row.Scan(ptrs...)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
		a := c
		_ = a
	}
}

func Benchmark_SliceOfSlices(b *testing.B) {
	// Create a 10x10 slice of slices
	data := make([][]int, 10)
	for i := 0; i < 10; i++ {
		data[i] = make([]int, 10)
		for j := 0; j < 10; j++ {
			data[i][j] = i*10 + j
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sum := 0
		for i := 0; i < 10; i++ {
			for j := 0; j < 10; j++ {
				sum += data[i][j]
			}
		}
		_ = sum // Prevent compiler optimizations
	}
}

func Benchmark_SliceSingle(b *testing.B) {
	// Create a slice with 100 elements
	data := make([]int, 100)
	for i := 0; i < 100; i++ {
		data[i] = i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		sum := 0
		for i := 0; i < 100; i++ {
			sum += data[i]
		}
		_ = sum // Prevent compiler optimizations
	}
}
