package velum_test

import (
	"context"
	"testing"
	"time"

	"github.com/axkit/velum"
)

// TestUpsert_OnPostgres runs the upsert forms against PostgreSQL through both
// drivers.
func TestUpsert_OnPostgres(t *testing.T) {
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
			exec := func(sql string) {
				t.Helper()
				if _, err := w.db.ExecContext(ctx, sql); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
			}
			sfx := "_" + w.name

			t.Run("DoUpdate outcomes and system columns", func(t *testing.T) {
				name := "upsert_prices" + sfx
				exec("DROP TABLE IF EXISTS " + name)
				exec("CREATE TABLE " + name + ` (sku text PRIMARY KEY, price bigint NOT NULL, currency text NOT NULL,
					note text NOT NULL DEFAULT '', row_version bigint NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz)`)
				prices := velum.NewTable[ProductPrice](name)
				cmd := prices.MustUpsertReturningCommand("price", velum.FullScope, velum.OnConflict{
					Target:      "(sku)",
					Action:      velum.DoUpdate,
					UpdateScope: "price",
					UpdateWhere: "WHERE t.price <> EXCLUDED.price",
				})

				created := time.Now().Add(-time.Hour).Truncate(time.Microsecond)
				row := ProductPrice{SKU: "a", Price: 10, Currency: "EUR", RowVersion: 1, CreatedAt: created}
				got, outcome, err := cmd.QueryRow(ctx, w.db, &row)
				if err != nil || outcome != velum.UpsertInserted {
					t.Fatalf("first upsert: %v, %v", outcome, err)
				}
				if got.RowVersion != 1 || got.UpdatedAt != nil || !got.CreatedAt.Equal(created) {
					t.Fatalf("inserted row = %+v", got)
				}

				now := time.Now().Truncate(time.Microsecond)
				row.CreatedAt, row.UpdatedAt = now, &now
				got, outcome, err = cmd.QueryRow(ctx, w.db, &row)
				if err != nil || outcome != velum.UpsertSkipped || got != nil {
					t.Fatalf("same price: %v, %v, %+v; want skipped", outcome, err, got)
				}

				row.Price = 20
				got, outcome, err = cmd.QueryRow(ctx, w.db, &row)
				if err != nil || outcome != velum.UpsertUpdated {
					t.Fatalf("new price: %v, %v", outcome, err)
				}
				if got.Price != 20 || got.RowVersion != 2 || got.UpdatedAt == nil || !got.UpdatedAt.Equal(now) {
					t.Fatalf("updated row = %+v", got)
				}
				if !got.CreatedAt.Equal(created) {
					t.Fatalf("created_at changed: %v, want %v", got.CreatedAt, created)
				}

				nothing := prices.MustUpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)"})
				for _, tc := range []struct {
					sku  string
					want int64
				}{{"a", 0}, {"b", 1}} {
					r := ProductPrice{SKU: tc.sku, Price: 1, Currency: "EUR", RowVersion: 1, CreatedAt: now}
					res, err := nothing.Exec(ctx, w.db, &r)
					if err != nil {
						t.Fatal(err)
					}
					if n, _ := res.RowsAffected(); n != tc.want {
						t.Fatalf("DO NOTHING %s: %d rows, want %d", tc.sku, n, tc.want)
					}
				}
			})

			t.Run("partial index target", func(t *testing.T) {
				name := "upsert_audit" + sfx
				exec("DROP TABLE IF EXISTS " + name)
				exec("CREATE TABLE " + name + ` (id serial PRIMARY KEY, type text NOT NULL, payload text NOT NULL,
					occurred_at timestamptz NOT NULL, source_event_id text)`)
				exec("CREATE UNIQUE INDEX ON " + name + " (source_event_id) WHERE source_event_id IS NOT NULL")
				cmd := velum.NewTable[AuditEvent](name).MustUpsertCommand(velum.FullScope,
					velum.OnConflict{Target: "(source_event_id) WHERE source_event_id IS NOT NULL"})

				src := "e1"
				for i, tc := range []struct {
					src  *string
					want int64
				}{{&src, 1}, {&src, 0}, {nil, 1}, {nil, 1}} {
					res, err := cmd.Exec(ctx, w.db, &AuditEvent{Type: "x", OccurredAt: time.Now(), SourceEventID: tc.src})
					if err != nil {
						t.Fatal(err)
					}
					if n, _ := res.RowsAffected(); n != tc.want {
						t.Fatalf("insert %d: %d rows, want %d", i, n, tc.want)
					}
				}
			})

			t.Run("expression target", func(t *testing.T) {
				name := "upsert_env" + sfx
				exec("DROP TABLE IF EXISTS " + name)
				exec("CREATE TABLE " + name + ` (id text PRIMARY KEY, workspace_id int NOT NULL, name text NOT NULL,
					color text NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL)`)
				exec("CREATE UNIQUE INDEX ON " + name + " (workspace_id, lower(name))")
				cmd := velum.NewTable[Environment](name).MustUpsertReturningCommand(velum.FullScope, velum.EmptyScope, velum.OnConflict{
					Target:      "(workspace_id, lower(name))",
					Action:      velum.DoUpdate,
					UpdateScope: "color",
				})
				now := time.Now()
				first, outcome, err := cmd.QueryRow(ctx, w.db, &Environment{ID: "id1", WorkspaceID: 1, Name: "Prod", Color: "red", CreatedAt: now, UpdatedAt: now})
				if err != nil || outcome != velum.UpsertInserted {
					t.Fatalf("insert: %v, %v", outcome, err)
				}
				second, outcome, err := cmd.QueryRow(ctx, w.db, &Environment{ID: "id2", WorkspaceID: 1, Name: "prod", Color: "blue", CreatedAt: now, UpdatedAt: now})
				if err != nil || outcome != velum.UpsertUpdated {
					t.Fatalf("update: %v, %v", outcome, err)
				}
				if first.ID != "id1" || second.ID != "id1" {
					t.Fatalf("ids = %q, %q; want id1 twice", first.ID, second.ID)
				}
			})

			t.Run("ON CONSTRAINT, table without PK, in a transaction", func(t *testing.T) {
				name := "upsert_ctx" + sfx
				exec("DROP TABLE IF EXISTS " + name)
				exec("CREATE TABLE " + name + ` (workspace_id int, user_id int, document_id text, connection_id int NOT NULL,
					schema_name text NOT NULL, updated_at timestamptz NOT NULL,
					CONSTRAINT pk_` + name + ` PRIMARY KEY (workspace_id, user_id, document_id))`)
				cmd := velum.NewTable[DocumentContext](name).MustUpsertReturningCommand(velum.FullScope, velum.FullScope,
					velum.OnConflict{Target: "ON CONSTRAINT pk_" + name, Action: velum.DoUpdate, UpdateScope: "ctx"})

				err := w.db.InTx(ctx, func(tx velum.Transaction) error {
					for i, want := range []velum.UpsertOutcome{velum.UpsertInserted, velum.UpsertUpdated} {
						row := DocumentContext{WorkspaceID: 1, UserID: 2, DocumentID: "d", ConnectionID: 10 + i, SchemaName: "public", UpdatedAt: time.Now()}
						got, outcome, err := cmd.QueryRow(ctx, tx, &row)
						if err != nil {
							return err
						}
						if outcome != want || got.ConnectionID != 10+i {
							t.Errorf("upsert %d: %v, %+v", i, outcome, got)
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			})

			t.Run("UpdateExpr with args", func(t *testing.T) {
				name := "upsert_rate" + sfx
				exec("DROP TABLE IF EXISTS " + name)
				exec("CREATE TABLE " + name + ` (kind text, key text, count int NOT NULL, expires_at timestamptz NOT NULL,
					PRIMARY KEY (kind, key))`)
				cmd := velum.NewTable[RateCounter](name).MustUpsertReturningCommand(velum.FullScope, "counter", velum.OnConflict{
					Target: "(kind, key)",
					Action: velum.DoUpdate,
					UpdateExpr: "count = CASE WHEN t.expires_at <= $1 THEN 1 ELSE t.count + 1 END, " +
						"expires_at = CASE WHEN t.expires_at <= $1 THEN EXCLUDED.expires_at ELSE t.expires_at END",
				})
				now := time.Now()
				for want := 1; want <= 3; want++ {
					got, _, err := cmd.QueryRow(ctx, w.db, &RateCounter{Kind: "ip", Key: "k", Count: 1, ExpiresAt: now.Add(time.Minute)}, now)
					if err != nil {
						t.Fatal(err)
					}
					if got.Count != want {
						t.Fatalf("count = %d, want %d", got.Count, want)
					}
				}
			})
		})
	}
}
