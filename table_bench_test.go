package velum_test

import (
	"context"
	"testing"
)

// func BenchmarkTable_Array8(b *testing.B) {

// 	//tbl := velum.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	for b.Loop() {
// 		_ = velum.FieldAddrArray[[8]any](customer, [][]int{{0}, {1}, {2}, {3}, {4}, {5, 0}, {5, 1}})

// 	}
// }

// func BenchmarkTable_Array2(b *testing.B) {

// 	//tbl := velum.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	for b.Loop() {
// 		_ = velum.FieldAddrArray[[2]any](customer, [][]int{{0}, {1}})
// 	}
// }

// func BenchmarkTable_Array32(b *testing.B) {

// 	//tbl := velum.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	for b.Loop() {
// 		_ = velum.FieldAddrArray[[32]any](customer, [][]int{{0}, {1}, {2}, {3}, {4}, {5, 0}, {5, 1}})

// 	}
// }

// func BenchmarkTable_FieldAddrs(b *testing.B) {

// 	tbl := velum.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	for b.Loop() {
// 		ptrs := tbl.FieldAddrs(customer, [][]int{{0}, {1}, {2}, {3}, {4}, {5, 0}, {5, 1}})
// 		tbl.ReleaseArgs(&ptrs)
// 	}
// }

// func BenchmarkTable_FieldAddrsX(b *testing.B) {

// 	tbl := velum.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	for b.Loop() {
// 		_ = tbl.FieldAddrsX(customer, [][]int{{0}, {1}, {2}, {3}, {4}, {5, 0}, {5, 1}})
// 	}
// }

func Benchmark_Select1PgxRaw_Inner(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	//q := tbl.Select("", strup.OrderBy("id"), strup.Limit(1))

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		var c CustomerSerial

		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, birth_date,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers WHERE id = $1`, 1)
		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.BirthDate,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}

func Benchmark_Select1PgxRaw_Outer(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	//q := tbl.Select("", strup.OrderBy("id"), strup.Limit(1))

	b.ReportAllocs()
	b.ResetTimer()
	var c CustomerSerial

	for b.Loop() {
		row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, birth_date,
								ssn, row_version,created_at, updated_at, deleted_at, deleted_by
						FROM customers WHERE id = $1`, 1)
		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.BirthDate,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
	}
}

// func Benchmark_Select1_PgxTbl_SelectByPK(b *testing.B) {
// 	ctx := context.Background()

// 	initConnections(b)

// 	tbl := velum.NewTable[Customer]("customers")
// 	b.ReportAllocs()
// 	b.ResetTimer()
// 	for b.Loop() {
// 		c, err := tbl.SelectByPK(ctx, dbwPgx, 1)
// 		if err != nil {
// 			b.Fatalf("failed to select customer: %v", err)
// 		}
// 		_ = c
// 	}
// }

func Benchmark_Select1SqlRaw(b *testing.B) {
	ctx := context.Background()

	initConnections(b)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var c CustomerSerial
		row := dbSql.QueryRowContext(ctx, `SELECT id, first_name, last_name, birth_date,
	                              ssn, row_version,created_at, updated_at, deleted_at, deleted_by
							FROM customers WHERE id = $1`, 1)
		if err := row.Err(); err != nil {
			b.Fatalf("failed to select customers: %v", err)
		}

		err := row.Scan(&c.ID, &c.FirstName,
			&c.LastName, &c.BirthDate,
			&c.SSN, &c.RowVersion, &c.CreatedAt, &c.UpdatedAt, &c.DeletedAt, &c.DeletedBy)
		if err != nil {
			b.Fatalf("failed to scan customer: %v", err)
		}
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
