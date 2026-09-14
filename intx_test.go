package velum_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axkit/velum"
)

var errInTx = errors.New("boom")

// countInTxRows returns how many rows of the given marker survived in intx_probe.
func countInTxRows(t *testing.T, marker string) int {
	t.Helper()
	var n int
	if err := dbPgx.QueryRow(context.Background(),
		`SELECT count(*) FROM intx_probe WHERE marker = $1`, marker).Scan(&n); err != nil {
		t.Fatalf("count failed: %v", err)
	}
	return n
}

func setupInTxProbe(t *testing.T) {
	t.Helper()
	initConnections(t)
	if _, err := dbPgx.Exec(context.Background(),
		`CREATE TABLE IF NOT EXISTS intx_probe (marker TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
}

// InTx must roll back and return fn's error when fn fails.
func TestInTx_RollbackOnError_Pgx(t *testing.T) {
	setupInTxProbe(t)
	const marker = "pgx-rollback"

	err := dbwPgx.InTx(context.Background(), func(tx velum.Transaction) error {
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO intx_probe (marker) VALUES ($1)`, marker); err != nil {
			return err
		}
		return errInTx
	})

	if !errors.Is(err, errInTx) {
		t.Errorf("InTx returned %v, want %v", err, errInTx)
	}
	if n := countInTxRows(t, marker); n != 0 {
		t.Errorf("row was committed despite fn error: found %d rows, want 0", n)
	}
}

func TestInTx_RollbackOnError_Sql(t *testing.T) {
	setupInTxProbe(t)
	const marker = "sql-rollback"

	err := dbwSql.InTx(context.Background(), func(tx velum.Transaction) error {
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO intx_probe (marker) VALUES ($1)`, marker); err != nil {
			return err
		}
		return errInTx
	})

	if !errors.Is(err, errInTx) {
		t.Errorf("InTx returned %v, want %v", err, errInTx)
	}
	if n := countInTxRows(t, marker); n != 0 {
		t.Errorf("row was committed despite fn error: found %d rows, want 0", n)
	}
}

// InTx must commit and return nil when fn succeeds.
func TestInTx_CommitOnSuccess_Pgx(t *testing.T) {
	setupInTxProbe(t)
	const marker = "pgx-commit"

	err := dbwPgx.InTx(context.Background(), func(tx velum.Transaction) error {
		_, err := tx.ExecContext(context.Background(),
			`INSERT INTO intx_probe (marker) VALUES ($1)`, marker)
		return err
	})
	if err != nil {
		t.Fatalf("InTx returned %v, want nil", err)
	}
	if n := countInTxRows(t, marker); n != 1 {
		t.Errorf("row was not committed: found %d rows, want 1", n)
	}
}

// A failing commit must surface as an error, not be swallowed.
func TestInTx_CommitErrorReported_Pgx(t *testing.T) {
	setupInTxProbe(t)

	err := dbwPgx.InTx(context.Background(), func(tx velum.Transaction) error {
		// Deferred constraint fires at COMMIT time.
		if _, err := tx.ExecContext(context.Background(),
			`CREATE TABLE intx_deferred (id INT PRIMARY KEY DEFERRABLE INITIALLY DEFERRED)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO intx_deferred (id) VALUES (1), (1)`); err != nil {
			return err
		}
		return nil
	})
	if err == nil {
		t.Error("InTx returned nil, want the COMMIT error")
	}
}
