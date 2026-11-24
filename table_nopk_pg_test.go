package velum_test

import (
	"context"
	"testing"

	"github.com/axkit/velum"
)

// TestNoPK_CompositeKeyOnPostgres runs the clause-based methods against a
// real table whose key is (user_id, hint_key), through both drivers.
func TestNoPK_CompositeKeyOnPostgres(t *testing.T) {
	ctx := context.Background()
	initConnections(t)

	for _, w := range []struct {
		name string
		db   velum.DatabaseWrapper
	}{
		{"pgx", dbwPgx},
		{"sql", dbwSql},
	} {
		t.Run(w.name, func(t *testing.T) {
			name := "seen_hints_" + w.name
			if _, err := w.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
				t.Fatal(err)
			}
			if _, err := w.db.ExecContext(ctx, "CREATE TABLE "+name+
				" (user_id int NOT NULL, hint_key text NOT NULL, note text NOT NULL, PRIMARY KEY (user_id, hint_key))"); err != nil {
				t.Fatal(err)
			}
			tbl := velum.NewTable[SeenHint](name)

			for _, h := range []SeenHint{{1, "a", "x"}, {1, "b", "y"}, {2, "a", "z"}} {
				if err := tbl.Insert(ctx, w.db, &h); err != nil {
					t.Fatalf("Insert: %v", err)
				}
			}

			key := "WHERE user_id=$1 AND hint_key=$2"
			res, err := tbl.Update(ctx, w.db, &SeenHint{Note: "changed"}, "note", key, 1, "b")
			if err != nil {
				t.Fatalf("Update: %v", err)
			}
			if n, _ := res.RowsAffected(); n != 1 {
				t.Fatalf("Update affected %d rows, want 1", n)
			}

			got, err := tbl.Get(ctx, w.db, "note", key, 1, "b")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Note != "changed" {
				t.Fatalf("note = %q, want changed", got.Note)
			}

			upd, err := tbl.UpdateReturning(ctx, w.db, &SeenHint{Note: "again"}, "note", velum.FullScope, key, 2, "a")
			if err != nil {
				t.Fatalf("UpdateReturning: %v", err)
			}
			if *upd != (SeenHint{2, "a", "again"}) {
				t.Fatalf("UpdateReturning = %+v", *upd)
			}

			rows, err := tbl.Select(ctx, w.db, "key", "WHERE user_id=$1 ORDER BY hint_key", 1)
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			if len(rows) != 2 || rows[0].HintKey != "a" || rows[1].HintKey != "b" || rows[0].Note != "" {
				t.Fatalf("Select = %+v", rows)
			}

			if _, err := tbl.Delete(ctx, w.db, key, 1, "a"); err != nil {
				t.Fatalf("Delete: %v", err)
			}
			if n, err := tbl.Count(ctx, w.db, ""); err != nil || n != 2 {
				t.Fatalf("Count = %d, %v; want 2", n, err)
			}
		})
	}
}
