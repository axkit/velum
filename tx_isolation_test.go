package velum_test

import (
	"context"
	"errors"
	"testing"

	"github.com/axkit/velum"
)

func setupTxProbe(t *testing.T) {
	t.Helper()
	initConnections(t)
	ctx := context.Background()
	if _, err := dbPgx.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS tx_probe (id INT PRIMARY KEY, n INT NOT NULL)`); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
}

func sqlState(err error) string {
	var se interface{ SQLState() string }
	if errors.As(err, &se) {
		return se.SQLState()
	}
	return ""
}

// The level asked for must be the level the server actually runs with.
func TestTxOptions_IsolationApplied(t *testing.T) {
	setupTxProbe(t)

	levels := []struct {
		opt  velum.IsoLevel
		want string
	}{
		{velum.IsoDefault, "read committed"},
		{velum.ReadCommitted, "read committed"},
		{velum.RepeatableRead, "repeatable read"},
		{velum.Serializable, "serializable"},
	}

	wrappers := map[string]velum.DatabaseWrapper{"pgx": dbwPgx, "sql": dbwSql}
	for name, db := range wrappers {
		for _, lv := range levels {
			t.Run(name+"/"+lv.opt.String(), func(t *testing.T) {
				var got string
				err := velum.InTxWith(context.Background(), db, velum.TxOptions{IsoLevel: lv.opt},
					func(tx velum.Transaction) error {
						return tx.QueryRowContext(context.Background(), `SHOW transaction_isolation`).Scan(&got)
					})
				if err != nil {
					t.Fatalf("InTxWith failed: %v", err)
				}
				if got != lv.want {
					t.Errorf("isolation is %q, want %q", got, lv.want)
				}
			})
		}
	}
}

// ReadOnly must reach the server, so a write inside the transaction fails.
func TestTxOptions_ReadOnly(t *testing.T) {
	setupTxProbe(t)

	wrappers := map[string]velum.DatabaseWrapper{"pgx": dbwPgx, "sql": dbwSql}
	for name, db := range wrappers {
		t.Run(name, func(t *testing.T) {
			err := velum.InTxWith(context.Background(), db,
				velum.TxOptions{IsoLevel: velum.RepeatableRead, ReadOnly: true},
				func(tx velum.Transaction) error {
					_, err := tx.ExecContext(context.Background(),
						`INSERT INTO tx_probe (id, n) VALUES (100, 1)`)
					return err
				})
			if err == nil {
				t.Fatal("write in a read-only transaction succeeded, want failure")
			}
			if got := sqlState(err); got != "25006" {
				t.Errorf("SQLSTATE is %q, want 25006 (read_only_sql_transaction)", got)
			}
		})
	}
}

// Two REPEATABLE READ transactions writing the same row: the second one is
// aborted with 40001 and must be reported as retryable.
func TestTxOptions_SerializationConflictIsRetryable(t *testing.T) {
	setupTxProbe(t)
	ctx := context.Background()

	wrappers := map[string]velum.DatabaseWrapper{"pgx": dbwPgx, "sql": dbwSql}
	for name, db := range wrappers {
		t.Run(name, func(t *testing.T) {
			id := 1
			if name == "sql" {
				id = 2
			}
			if _, err := dbPgx.Exec(ctx,
				`INSERT INTO tx_probe (id, n) VALUES ($1, 0)
				 ON CONFLICT (id) DO UPDATE SET n = 0`, id); err != nil {
				t.Fatalf("seed failed: %v", err)
			}

			tx1, err := velum.BeginTx(ctx, db, velum.TxOptions{IsoLevel: velum.RepeatableRead})
			if err != nil {
				t.Fatalf("begin tx1: %v", err)
			}
			tx2, err := velum.BeginTx(ctx, db, velum.TxOptions{IsoLevel: velum.RepeatableRead})
			if err != nil {
				t.Fatalf("begin tx2: %v", err)
			}
			defer tx2.Rollback(ctx)

			// Pin tx2's snapshot before tx1 writes: under REPEATABLE READ the
			// snapshot is taken by the first statement, not by BEGIN. Doing it
			// here makes the conflict deterministic instead of a race between
			// a blocked UPDATE and the COMMIT.
			var n int
			if err := tx2.QueryRowContext(ctx, `SELECT n FROM tx_probe WHERE id = $1`, id).Scan(&n); err != nil {
				t.Fatalf("tx2 snapshot read: %v", err)
			}

			if _, err := tx1.ExecContext(ctx, `UPDATE tx_probe SET n = n + 1 WHERE id = $1`, id); err != nil {
				t.Fatalf("tx1 update: %v", err)
			}
			if err := tx1.Commit(ctx); err != nil {
				t.Fatalf("tx1 commit: %v", err)
			}

			// tx2 now tries to write a row that changed after its snapshot.
			_, tx2Err := tx2.ExecContext(ctx, `UPDATE tx_probe SET n = n + 100 WHERE id = $1`, id)

			if tx2Err == nil {
				t.Fatal("tx2 succeeded, want serialization failure")
			}
			if got := sqlState(tx2Err); got != "40001" {
				t.Errorf("SQLSTATE is %q, want 40001 (serialization_failure)", got)
			}
			if !velum.IsRetryable(db, tx2Err) {
				t.Errorf("IsRetryable(%v) = false, want true", tx2Err)
			}
		})
	}
}

// Ordinary errors must not be mistaken for transient conflicts.
func TestIsRetryable_OrdinaryErrorIsNotRetryable(t *testing.T) {
	setupTxProbe(t)
	ctx := context.Background()

	wrappers := map[string]velum.DatabaseWrapper{"pgx": dbwPgx, "sql": dbwSql}
	for name, db := range wrappers {
		t.Run(name, func(t *testing.T) {
			err := velum.InTxWith(ctx, db, velum.TxOptions{}, func(tx velum.Transaction) error {
				_, err := tx.ExecContext(ctx, `SELECT * FROM no_such_table`)
				return err
			})
			if err == nil {
				t.Fatal("query on a missing table succeeded")
			}
			if velum.IsRetryable(db, err) {
				t.Errorf("IsRetryable(%v) = true, want false", err)
			}
		})
	}
}
