package velum_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axkit/velum"
)

// Minimal fake implementations of the velum DB interfaces.
// These allow Table and Dataset methods to be tested without a real database.

type fakeExecResult struct{}

func (fakeExecResult) RowsAffected() (int64, error) { return 1, nil }

type fakeDBRow struct{ err error }

func (r *fakeDBRow) Scan(_ ...any) error { return r.err }
func (r *fakeDBRow) Err() error          { return r.err }

type fakeDBRows struct{ remaining int }

func (r *fakeDBRows) Next() bool          { r.remaining--; return r.remaining >= 0 }
func (r *fakeDBRows) Close() error        { return nil }
func (r *fakeDBRows) Err() error          { return nil }
func (r *fakeDBRows) Scan(_ ...any) error { return nil }

var errFakeNotFound = errors.New("not found")

type fakeTestDB struct {
	rowErr  error
	rowsN   int
	execErr error
}

func (db *fakeTestDB) ExecContext(_ context.Context, _ string, _ ...any) (velum.Result, error) {
	if db.execErr != nil {
		return nil, db.execErr
	}
	return fakeExecResult{}, nil
}

func (db *fakeTestDB) QueryRowContext(_ context.Context, _ string, _ ...any) velum.Row {
	return &fakeDBRow{err: db.rowErr}
}

func (db *fakeTestDB) QueryContext(_ context.Context, _ string, _ ...any) (velum.Rows, error) {
	return &fakeDBRows{remaining: db.rowsN}, nil
}

func (db *fakeTestDB) IsNotFound(err error) bool { return errors.Is(err, errFakeNotFound) }

func (db *fakeTestDB) Begin(_ context.Context) (velum.Transaction, error) {
	return &fakeTestTx{db: db}, nil
}

func (db *fakeTestDB) InTx(ctx context.Context, fn func(velum.Transaction) error) error {
	tx, _ := db.Begin(ctx)
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

type fakeTestTx struct{ db *fakeTestDB }

func (t *fakeTestTx) ExecContext(ctx context.Context, q string, args ...any) (velum.Result, error) {
	return t.db.ExecContext(ctx, q, args...)
}
func (t *fakeTestTx) QueryRowContext(ctx context.Context, q string, args ...any) velum.Row {
	return t.db.QueryRowContext(ctx, q, args...)
}
func (t *fakeTestTx) QueryContext(ctx context.Context, q string, args ...any) (velum.Rows, error) {
	return t.db.QueryContext(ctx, q, args...)
}
func (t *fakeTestTx) Commit(_ context.Context) error   { return nil }
func (t *fakeTestTx) Rollback(_ context.Context) error { return nil }

// Table method tests.

func TestTable_Get_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	_, err := tbl.Get(context.Background(), &fakeTestDB{}, "age", "WHERE id=$1", 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
}

func TestTable_GetTo_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var id int
	err := tbl.GetByPKTo(context.Background(), &fakeTestDB{}, []any{&id}, 1)
	if err != nil {
		t.Fatalf("GetTo: %v", err)
	}
}

func TestTable_Select_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	rows, err := tbl.Select(context.Background(), &fakeTestDB{rowsN: 3}, "age", "WHERE age > $1", 0)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("Select: got %d rows, want 3", len(rows))
	}
}

func TestTable_SelectAll_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	rows, err := tbl.SelectAll(context.Background(), &fakeTestDB{rowsN: 2})
	if err != nil {
		t.Fatalf("SelectAll: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("SelectAll: got %d rows, want 2", len(rows))
	}
}

func TestTable_Insert_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{Customer: Customer{FirstName: "Test", LastName: "User"}}
	if err := tbl.Insert(context.Background(), &fakeTestDB{}, c); err != nil {
		t.Fatalf("Insert: %v", err)
	}
}

func TestTable_InsertScope_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{Customer: Customer{Age: 25}}
	if _, err := tbl.InsertScope(context.Background(), &fakeTestDB{}, c, "age"); err != nil {
		t.Fatalf("InsertScope: %v", err)
	}
}

func TestTable_Update_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{ID: 1, Customer: Customer{Age: 30}}
	if _, err := tbl.Update(context.Background(), &fakeTestDB{}, c, "age", "WHERE id=$1"); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func TestTable_UpdateReturning_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{ID: 1, Customer: Customer{Age: 30}}
	if _, err := tbl.UpdateReturning(context.Background(), &fakeTestDB{}, c, "age", velum.FullScope, "WHERE id=$1"); err != nil {
		t.Fatalf("UpdateReturning: %v", err)
	}
}

func TestTable_DeleteByPK_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	if _, err := tbl.DeleteByPK(context.Background(), &fakeTestDB{}, 42); err != nil {
		t.Fatalf("DeleteByPK: %v", err)
	}
}

func TestTable_DeleteReturningByPK_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{ID: 42}
	if _, err := tbl.DeleteReturningByPK(context.Background(), &fakeTestDB{}, c); err != nil {
		t.Fatalf("DeleteReturningByPK: %v", err)
	}
}

func TestTable_Delete_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	if _, err := tbl.Delete(context.Background(), &fakeTestDB{}, "WHERE id=$1", 42); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func TestTable_DeleteReturning_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{ID: 42}
	if _, err := tbl.DeleteReturning(context.Background(), &fakeTestDB{}, c, "WHERE id=$1"); err != nil {
		t.Fatalf("DeleteReturning: %v", err)
	}
}

func TestTable_SoftDeleteReturningByPK_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	c := &CustomerSerial{ID: 1}
	if _, err := tbl.SoftDeleteReturningByPK(context.Background(), &fakeTestDB{}, c); err != nil {
		t.Fatalf("SoftDeleteReturningByPK: %v", err)
	}
}

func TestTable_Exist_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	if _, err := tbl.Exist(context.Background(), &fakeTestDB{}, "WHERE id=$1", 1); err != nil {
		t.Fatalf("Exist: %v", err)
	}
}

func TestTable_ExistByPK_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	if _, err := tbl.ExistByPK(context.Background(), &fakeTestDB{}, 1); err != nil {
		t.Fatalf("ExistByPK: %v", err)
	}
}

func TestTable_Count_FakeDB(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	if _, err := tbl.Count(context.Background(), &fakeTestDB{}, "WHERE age > $1", 18); err != nil {
		t.Fatalf("Count: %v", err)
	}
}

// Dataset method tests.

func TestDataset_Get_FakeDB(t *testing.T) {
	ds := velum.NewDataset[customerSimpleRow]("SELECT id, first_name, age FROM t")
	if _, err := ds.Get(context.Background(), &fakeTestDB{}, 1); err != nil {
		t.Fatalf("Dataset.Get: %v", err)
	}
}

func TestDataset_Select_FakeDB(t *testing.T) {
	ds := velum.NewDataset[customerSimpleRow]("SELECT id, first_name, age FROM t")
	rows, err := ds.Select(context.Background(), &fakeTestDB{rowsN: 2})
	if err != nil {
		t.Fatalf("Dataset.Select: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Dataset.Select: got %d rows, want 2", len(rows))
	}
}

// TestDatasetQuery_WithTailClause_FakeDB executes a DatasetQuery built via
// WithTailClause. This exercises the DatasetTailClause loop branch inside
// render() and the startsWithWhitespace helper.
func TestDatasetQuery_WithTailClause_FakeDB(t *testing.T) {
	ds := velum.NewDataset[customerSimpleRow]("SELECT id, first_name, age FROM t")
	rows, err := ds.WithTailClause("ORDER BY id DESC LIMIT 1").Select(context.Background(), &fakeTestDB{rowsN: 1})
	if err != nil {
		t.Fatalf("DatasetQuery.Select with tail clause: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}
