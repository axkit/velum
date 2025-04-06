package velum_test

import (
	"context"
	"testing"
	"time"

	"github.com/axkit/velum"
)

func TestTable_Validate(t *testing.T) {
	ctx := context.Background()

	initConnections(t)

	tbl := velum.NewTable[CustomerSerial]("customers_pk_serial")
	if tbl == nil {
		t.Fatalf("failed to create table")
	}

	if err := tbl.Validate(ctx); err != nil {
		t.Fatalf("failed to validate table: %v", err)
	}
}

func TestTable_Name(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers_pk_serial")
	if name := tbl.Name(); name != "customers_pk_serial" {
		t.Fatalf("expected name 'customers', got %q", name)
	}
}

func TestTable_BasicDML(t *testing.T) {

	ctx := context.Background()

	initConnections(t)

	customer := Customer{
		FirstName: "John",
		LastName:  "Doe",
		Age:       18,
		SSN:       nil,
		SystemColumns: SystemColumns{
			CreatedAt:  time.Now(),
			RowVersion: 1,
		},
	}

	t.Run("customers_pk_serial", func(t *testing.T) {
		tbl := velum.NewTable[CustomerSerial]("customers_pk_serial")
		c := CustomerSerial{
			Customer: customer,
		}

		t.Run("Insert", func(t *testing.T) {

			cIns, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			if cIns.ID == 0 {
				t.Fatalf("expected customer ID to be asigned by the database")
			}

			if !cIns.Customer.Equal(&c.Customer) {
				t.Fatalf("\nexp:%#v\ngot:%#v", c, cIns)
			}
		})

		t.Run("GetByPK", func(t *testing.T) {

			cIns, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			cGet, err := tbl.GetByPK(ctx, dbwPgx, cIns.ID)
			if err != nil {
				t.Fatalf("failed to select customer: %v", err)
			}

			if !cGet.Customer.Equal(&cIns.Customer) || cGet.ID != cIns.ID {
				t.Fatalf("\nexp:%#v\ngot:%#v", cIns, cGet)
			}
		})

		t.Run("UpdateByPK_*", func(t *testing.T) {

			uc, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			uc.FirstName = "Jane"
			uc.Age = 19
			now := time.Now()
			uc.UpdatedAt = &now

			if _, err := tbl.UpdateByPK(ctx, dbwPgx, uc, "*"); err != nil {
				t.Fatalf("failed to update customer: %v", err)
			}

			cUpd, err := tbl.GetByPK(ctx, dbwPgx, uc.ID)
			if err != nil {
				t.Fatalf("failed to select customer: %v", err)
			}

			uc.RowVersion++ // to make it equal to the updated row version
			if !uc.Equal(&cUpd.Customer) {
				t.Fatalf("\nexp:%#v\ngot:%#v", uc, cUpd)
			}
		})

		t.Run("UpdateReturningByPK_ssn_*", func(t *testing.T) {

			uc, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			uc.FirstName = "Rob"
			uc.SSN = &[]string{"123-45-6789"}[0]
			now := time.Now()
			uc.UpdatedAt = &now

			cUpd, err := tbl.UpdateReturningByPK(ctx, dbwPgx, uc, "ssn", "*")
			if err != nil {
				t.Fatalf("failed to update customer: %v", err)
			}

			if cUpd.SSN == nil {
				t.Fatalf("expected SSN to be set, got nil")
			}

			if cUpd.FirstName == uc.FirstName {
				t.Fatalf("expected only SSN to be updated, got %#v", *cUpd)
			}

			uc.RowVersion++ // to make it equal to the updated row version

			if cUpd.ID != uc.ID ||
				cUpd.RowVersion != uc.RowVersion ||
				cUpd.Age != uc.Age ||
				*cUpd.SSN != *uc.SSN ||
				!((cUpd.UpdatedAt).Equal(*uc.UpdatedAt)) {
				t.Fatalf("\nexp:%#v\ngot:%#v", uc, cUpd)
			}

		})
		t.Run("UpdateReturningByPK_!ssn_ssn_age", func(t *testing.T) {

			uc, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			uc.FirstName = "Rob"
			uc.Age = 20
			uc.SSN = &[]string{"987-65-4321"}[0]
			now := time.Now()
			uc.UpdatedAt = &now

			cUpd, err := tbl.UpdateReturningByPK(ctx, dbwPgx, uc, "!ssn", "age,ssn")
			if err != nil {
				t.Fatalf("failed to update customer: %v", err)
			}

			if cUpd.SSN != nil || cUpd.FirstName != "" || cUpd.LastName != "" {
				t.Fatalf("expected SSN and Age to be returned, got nil")
			}

			if cUpd.Age != uc.Age {
				t.Fatalf("expected Age to be updated and returned, got %#v", *cUpd)
			}

			if equalPtr(cUpd.SSN, uc.SSN) {
				t.Fatalf("expected SSN to be ignored, got %#v", *cUpd)
			}

		})
		t.Run("SoftDeleteByPK", func(t *testing.T) {
			uc, err := tbl.InsertReturning(ctx, dbwPgx, &c, "*", "*")
			if err != nil {
				t.Fatalf("failed to insert customer: %v", err)
			}

			now := time.Now()
			uc.DeletedAt = &now
			uc.DeletedBy = new(int)
			*uc.DeletedBy = 101

			err = dbwPgx.InTx(ctx, func(tx velum.Transaction) error {
				_, err := tbl.SoftDeleteByPK(ctx, tx, uc)
				return err
			})

			if err != nil {
				t.Fatalf("failed to delete customer: %v", err)
			}

			dc, err := tbl.GetByPK(ctx, dbwPgx, uc.ID)
			if err != nil {
				t.Fatalf("failed to select customer: %v", err)
			}

			if !uc.Equal(&dc.Customer) {
				t.Fatalf("\nexp:%#v\ngot:%#v", customer, dc.Customer)
			}
		})
	})

}

// func TestTable_Update(t *testing.T) {
// 	ctx := context.Background()

// 	initConnections(t)

// 	tbl := velum.NewTable[Customer]("customers")

// 	customer, err := tbl.SelectByPK(ctx, dbwPgx, 1)
// 	if err != nil {
// 		t.Fatalf("failed to select customer: %v", err)
// 	}

// 	customer.SSN = &[]string{"123-45-6789"}[0]
// 	customer.UpdatedAt.SetNow()

// 	updateCmd := tbl.Update(velum.WithScope("ssn"), velum.WithWhere("id = $1"))
// 	if _, err := updateCmd.Exec(ctx, dbwPgx, customer, 2); err != nil {
// 		t.Fatalf("failed to update customer: %v", err)
// 	}

// 	updated, err := tbl.SelectByPK(ctx, dbwPgx, 2)
// 	if err != nil {
// 		t.Fatalf("failed to select customer: %v", err)
// 	}

// 	if *customer.SSN != *updated.SSN {
// 		t.Fatalf("expected SSN %q, got %q", *customer.SSN, *updated.SSN)
// 	}

// }

// func TestTable_UpdateMultiple(t *testing.T) {
// 	ctx := context.Background()

// 	initConnections(t)

// 	tbl := velum.NewTable[Customer]("customers")

// 	customer, err := tbl.SelectByPK(ctx, dbwPgx, 1)
// 	if err != nil {
// 		t.Fatalf("failed to select customer: %v", err)
// 	}

// 	customer.SSN = &[]string{"123-45-6789"}[0]
// 	customer.UpdatedAt.SetNow()

// 	updateCmd := tbl.Update(velum.WithScope("ssn")).Returning("*")
// 	customers, err := updateCmd.Query(ctx, dbwPgx, customer)
// 	if err != nil {
// 		t.Fatalf("failed to update customer: %v", err)
// 	}

// 	for _, updated := range customers {
// 		if *customer.SSN != *updated.SSN {
// 			t.Fatalf("expected SSN %q, got %q", *customer.SSN, *updated.SSN)
// 		}
// 	}
// }

// func TestTable_Select(t *testing.T) {
// 	ctx := context.Background()

// 	initConnections(t)

// 	tbl := velum.NewTable[Customer]("customers")

// 	customers, err := tbl.Select(velum.WithWhere("id > $1")).Query(ctx, dbwPgx, 1)
// 	if err != nil {
// 		t.Fatalf("failed to select customers: %v", err)
// 	}

// 	for _, c := range customers {
// 		if c.ID <= 1 {
// 			t.Fatalf("expected ID > 1, got %d", c.ID)
// 		}
// 	}
// }

// func TestPregenerateSQL(t *testing.T) {

// }
// func TestTable_FieldAddrs(t *testing.T) {

// 	tbl := strup.NewTable[Customer]("customers")

// 	customer := &Customer{
// 		ID:        1,
// 		FirstName: "John",
// 		LastName:  "Doe",
// 		BirthDate: time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC),
// 	}

// 	fields := []any{
// 		&customer.ID,
// 		&customer.FirstName,
// 		&customer.LastName,
// 		&customer.BirthDate,
// 		&customer.SSN,
// 		&customer.RowVersion,
// 		&customer.DeletedAt,
// 		&customer.DeletedBy,
// 	}

// 	fieldAddrs := tbl.ScopeFieldAddrs("", customer)

// 	if cnt := len(fieldAddrs); cnt != len(fields) {
// 		t.Fatalf("expected %d field addresses, got %d", len(fields), cnt)
// 	}

// 	for i, ptr := range fieldAddrs {
// 		if !reflect.DeepEqual(ptr, fields[i]) {
// 			t.Fatalf("expected %v, got %v, field %d", fields[i], ptr, i)
// 		}
// 	}
// }

// func Benchmark_SelectByPK(b *testing.B) {
// 	ctx := context.Background()

// 	initConnections(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	b.ReportAllocs()
// 	b.ResetTimer()

// 	b.Run("pgx/raw/SelectByPK", func(b *testing.B) {
// 		b.ReportAllocs()
// 		b.ResetTimer()
// 		for b.Loop() {
// 			row := dbPgx.QueryRow(ctx, `SELECT id, first_name, last_name, birth_date,
// 	                              ssn, row_version, deleted_at, deleted_by
// 							FROM customers WHERE id = $1`, rand.Intn(9999)+1)
// 			var c Customer
// 			err := row.Scan(&c.ID, &c.FirstName,
// 				&c.LastName, &c.BirthDate,
// 				&c.SSN, &c.RowVersion, &c.DeletedAt, &c.DeletedBy)
// 			if err != nil {
// 				b.Fatalf("failed to scan customer: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("pgx/tbl/SelectByPK", func(b *testing.B) {
// 		b.ReportAllocs()
// 		b.ResetTimer()
// 		for b.Loop() {
// 			_, err := tbl.SelectByPK(ctx, dbwPgx, rand.Intn(9999)+1)
// 			if err != nil {
// 				b.Fatalf("failed to select customer: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("pgx/tbl/SelectByPK/into", func(b *testing.B) {
// 		b.ReportAllocs()
// 		b.ResetTimer()

// 		for b.Loop() {
// 			var c Customer
// 			err := tbl.SelectByPkInto(ctx, dbwPgx, rand.Intn(9999)+1, &c)
// 			if err != nil {
// 				b.Fatalf("failed to select customer: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/raw/SelectByPK", func(b *testing.B) {
// 		for b.Loop() {
// 			row := dbSql.QueryRow(`SELECT id, first_name, last_name, birth_date,
// 	                              ssn, row_version, deleted_at
// 							FROM customers WHERE id = $1`, rand.Intn(9999)+1)
// 			var c Customer
// 			err := row.Scan(&c.ID, &c.FirstName,
// 				&c.LastName, &c.BirthDate,
// 				&c.SSN, &c.RowVersion, &c.DeletedAt)
// 			if err != nil {
// 				b.Fatalf("failed to scan customer: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/tbl/SelectByPK", func(b *testing.B) {
// 		for b.Loop() {
// 			_, err := tbl.SelectByPK(ctx, dbwSql, rand.Intn(9999)+1)
// 			if err != nil {
// 				b.Fatalf("failed to select customer: %v", err)
// 			}
// 		}
// 	})
// }

// b.Run("pgx/tbl/Run", func(b *testing.B) {

// 	for b.Loop() {
// 		_, err := q.Run(ctx, dbwPgx)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })

// b.Run("pgx/tbl/Range", func(b *testing.B) {
// 	for b.Loop() {
// 		err := q.Range(ctx, dbwPgx, func(c Customer) { _ = 0 })
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })

// b.Run("pgx/tbl/RunX", func(b *testing.B) {
// 	var v []Customer
// 	for b.Loop() {
// 		err := q.RunX(ctx, &v, dbwPgx)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })

// b.Run("sql/tbl/Run", func(b *testing.B) {
// 	for b.Loop() {
// 		_, err := q.Run(ctx, dbwSql)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })

// b.Run("sql/tbl/Range", func(b *testing.B) {
// 	for b.Loop() {
// 		err := q.Range(ctx, dbwSql, func(c Customer) { _ = 0 })
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })

// b.Run("sql/tbl/RunX", func(b *testing.B) {
// 	var v []Customer
// 	for b.Loop() {
// 		err := q.RunX(ctx, &v, dbwSql)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// })
//}

// func Benchmark_Select10000(b *testing.B) {
// 	ctx := context.Background()

// 	initConnections(b)

// 	tbl := strup.NewTable[Customer]("customers")
// 	q := tbl.Select("", strup.OrderBy("id"))

// 	b.Run("pgx/raw", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			rows, err := dbPgx.Query(ctx, `SELECT id, first_name, last_name, birth_date,
// 	                              ssn, row_version, deleted_at
// 							FROM customers ORDER BY id`)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 			defer rows.Close()

// 			var customers []Customer
// 			var row Customer

// 			for rows.Next() {
// 				err = rows.Scan(&row.ID, &row.FirstName,
// 					&row.LastName, &row.BirthDate,
// 					&row.SSN, &row.RowVersion, &row.DeletedAt)
// 				if err != nil {
// 					b.Fatalf("failed to scan customer: %v", err)
// 				}
// 				customers = append(customers, row)
// 			}

// 			_ = customers

// 			if err := rows.Err(); err != nil {
// 				b.Fatalf("failed to iterate customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("pgx/Table.Run", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			v, err := q.Run(ctx, dbwPgx)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 			_ = v
// 		}
// 	})

// 	b.Run("pgx/Table.Range", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			err := q.Range(ctx, dbwPgx, func(c Customer) { _ = 0 })
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("pgx/Table.RunX", func(b *testing.B) {
// 		var v []Customer
// 		for i := 0; i < b.N; i++ {
// 			err := q.RunX(ctx, &v, dbwPgx)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/raw", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			rows, err := dbSql.Query(`SELECT id, first_name, last_name, birth_date,
// 	                              ssn, row_version, deleted_at
// 							FROM customers ORDER BY id`)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 			defer rows.Close()

// 			var customers []Customer
// 			var row Customer

// 			for rows.Next() {
// 				err = rows.Scan(&row.ID, &row.FirstName,
// 					&row.LastName, &row.BirthDate,
// 					&row.SSN, &row.RowVersion, &row.DeletedAt)
// 				if err != nil {
// 					b.Fatalf("failed to scan customer: %v", err)
// 				}
// 				customers = append(customers, row)
// 			}

// 			_ = customers

// 			if err := rows.Err(); err != nil {
// 				b.Fatalf("failed to iterate customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/Table.Run", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			_, err := q.Run(ctx, dbwSql)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/Table.Range", func(b *testing.B) {
// 		for i := 0; i < b.N; i++ {
// 			err := q.Range(ctx, dbwSql, func(c Customer) { _ = 0 })
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 		}
// 	})

// 	b.Run("sql/Table.RunX", func(b *testing.B) {
// 		var v []Customer
// 		for i := 0; i < b.N; i++ {
// 			err := q.RunX(ctx, &v, dbwSql)
// 			if err != nil {
// 				b.Fatalf("failed to select customers: %v", err)
// 			}
// 		}
// 	})
// }

// func BenchmarkTable_Pgx_SelectAllRows(b *testing.B) {
// 	ctx := context.Background()

// 	initDatabaseClients(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	q := tbl.Select("", strup.OrderBy("id"))
// 	for i := 0; i < b.N; i++ {
// 		_, err := q.Run(ctx, dbwPgx)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_Pgx_SelectAllRowsX(b *testing.B) {
// 	ctx := context.Background()

// 	initDatabaseClients(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	q := tbl.Select("", strup.OrderBy("id"))
// 	v := make([]Customer, 0, 10000)
// 	for i := 0; i < b.N; i++ {
// 		err := q.RunX(ctx, &v, dbwPgx)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_Sql_SelectAllRows(b *testing.B) {
// 	ctx := context.Background()

// 	initDatabaseClients(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	q := tbl.Select("", strup.OrderBy("id"))
// 	for i := 0; i < b.N; i++ {
// 		_, err := q.Run(ctx, dbwSql)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_RawSql_SelectAllRows(b *testing.B) {
// 	//ctx := context.Background()

// 	initDatabaseClients(b)

// 	//tbl := strup.NewTable[Customer]("customers")

// 	qry := "SELECT id, first_name, last_name, birth_date, ssn, row_version, deleted_at FROM customers ORDER BY id"
// 	for i := 0; i < b.N; i++ {
// 		rows, err := dbSql.Query(qry)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 		defer rows.Close()

// 		var customers []Customer
// 		var row Customer

// 		for rows.Next() {
// 			err = rows.Scan(&row.ID, &row.FirstName,
// 				&row.LastName, &row.BirthDate,
// 				&row.SSN, &row.RowVersion, &row.DeletedAt)
// 			if err != nil {
// 				b.Fatalf("failed to scan customer: %v", err)
// 			}
// 			customers = append(customers, row)
// 		}

// 		_ = customers

// 		if err := rows.Err(); err != nil {
// 			b.Fatalf("failed to iterate customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_RawPgx_SelectAllRows(b *testing.B) {

// 	initDatabaseClients(b)

// 	qry := "SELECT id, first_name, last_name, birth_date, ssn, row_version, deleted_at FROM customers ORDER BY id"
// 	for i := 0; i < b.N; i++ {
// 		rows, err := dbPgx.Query(context.Background(), qry)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 		defer rows.Close()

// 		var customers []Customer
// 		var row Customer

// 		for rows.Next() {
// 			err = rows.Scan(&row.ID, &row.FirstName,
// 				&row.LastName, &row.BirthDate,
// 				&row.SSN, &row.RowVersion, &row.DeletedAt)
// 			if err != nil {
// 				b.Fatalf("failed to scan customer: %v", err)
// 			}
// 			customers = append(customers, row)
// 		}
// 		_ = customers

// 		if err := rows.Err(); err != nil {
// 			b.Fatalf("failed to iterate customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_Pgx_RangeAllRows(b *testing.B) {
// 	b.ReportAllocs()
// 	ctx := context.Background()

// 	initDatabaseClients(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	q := tbl.Select("", strup.OrderBy("id"))

// 	for i := 0; i < b.N; i++ {
// 		err := q.Range(ctx, dbwPgx, func(c *Customer) { _ = 0 })
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// }

// func BenchmarkTable_Sql_RangeAllRows(b *testing.B) {
// 	ctx := context.Background()

// 	initDatabaseClients(b)

// 	tbl := strup.NewTable[Customer]("customers")

// 	q := tbl.Select("", strup.OrderBy("id"))

// 	for i := 0; i < b.N; i++ {
// 		err := q.Range(ctx, dbwSql, func(c *Customer) { _ = 0 })
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 	}
// }

// b.Run("sql/raw", func(b *testing.B) {
// 	for b.Loop() {
// 		rows, err := dbSql.Query(`SELECT id, first_name, last_name, birth_date,
// 							  ssn, row_version,created_at, updated_at, deleted_at, deleted_by
// 						FROM customers ORDER BY id LIMIT 1`)
// 		if err != nil {
// 			b.Fatalf("failed to select customers: %v", err)
// 		}
// 		defer rows.Close()

// 		var customers []Customer
// 		var row Customer

// 		for rows.Next() {
// 			err = rows.Scan(&row.ID, &row.FirstName,
// 				&row.LastName, &row.BirthDate,
// 				&row.SSN, &row.RowVersion, &row.DeletedAt)
// 			if err != nil {
// 				b.Fatalf("failed to scan customer: %v", err)
// 			}
// 			customers = append(customers, row)
// 		}

// 		if err := rows.Err(); err != nil {
// 			b.Fatalf("failed to iterate customers: %v", err)
// 		}
// 	}
// })
