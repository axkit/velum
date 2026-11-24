package velum_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/axkit/velum"
)

// Structs of the examples in docs/proposals/upsert.md, section 6.

type ProductPrice struct {
	SKU        string `dbw:"pk,gen=no"`
	Price      int64  `dbw:"price"`
	Currency   string `dbw:"price"`
	Note       string
	RowVersion int64      `dbw:"version"`
	CreatedAt  time.Time  `dbw:"insert"`
	UpdatedAt  *time.Time `dbw:"update"`
}

type Permission struct {
	ID     string `dbw:"gen=no"`
	Name   string
	BitPos int `dbw:"bits"`
}

type AuditEvent struct {
	ID            int64 `dbw:"gen=serial"`
	Type          string
	Payload       string
	OccurredAt    time.Time
	SourceEventID *string
}

type Environment struct {
	ID          string `dbw:"gen=no"`
	WorkspaceID int
	Name        string
	Color       string    `dbw:"color"`
	CreatedAt   time.Time `dbw:"insert"`
	UpdatedAt   time.Time `dbw:"update"`
}

type DocumentContext struct {
	WorkspaceID  int
	UserID       int
	DocumentID   string
	ConnectionID int       `dbw:"ctx"`
	SchemaName   string    `dbw:"ctx"`
	UpdatedAt    time.Time `dbw:"update"`
}

type RateCounter struct {
	Kind      string
	Key       string
	Count     int       `dbw:"counter"`
	ExpiresAt time.Time `dbw:"counter"`
}

type ImportReport struct {
	DocumentID string `dbw:"pk,gen=no"`
	ImportID   string `dbw:"report"`
	Report     string `dbw:"report"`
}

type RepoWorkspace struct {
	WorkspaceID    int `dbw:"pk,gen=no"`
	OrganizationID int
}

func TestUpsertCommand_SQL(t *testing.T) {
	prices := velum.NewTable[ProductPrice]("product_prices")
	hints := velum.NewTable[SeenHint]("user_seen_hints")
	audit := velum.NewTable[AuditEvent]("audit_events")
	reports := velum.NewTable[ImportReport]("edt.import_reports")

	cases := []struct {
		name string
		cmd  velum.Command[ProductPrice]
		sql  string
	}{
		{
			"6.1 update price on conflict",
			prices.MustUpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateScope: "price"}),
			"INSERT INTO product_prices AS t (sku,price,currency,note,row_version,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7) " +
				"ON CONFLICT (sku) DO UPDATE SET price=EXCLUDED.price,currency=EXCLUDED.currency,row_version=t.row_version+1,updated_at=$8",
		},
		{
			"named insert scope adds version and insert columns",
			prices.MustUpsertCommand("price", velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateScope: "price"}),
			"INSERT INTO product_prices AS t (sku,price,currency,row_version,created_at) VALUES ($1,$2,$3,$4,$5) " +
				"ON CONFLICT (sku) DO UPDATE SET price=EXCLUDED.price,currency=EXCLUDED.currency,row_version=t.row_version+1,updated_at=$6",
		},
		{
			"touch: only system columns",
			prices.MustUpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate}),
			"INSERT INTO product_prices AS t (sku,price,currency,note,row_version,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7) " +
				"ON CONFLICT (sku) DO UPDATE SET row_version=t.row_version+1,updated_at=$8",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &recordingDB{}
			if _, err := tc.cmd.Exec(context.Background(), db, &ProductPrice{}); err != nil {
				t.Fatal(err)
			}
			if got := db.last(t).sql; got != tc.sql {
				t.Errorf("sql\n got: %s\nwant: %s", got, tc.sql)
			}
		})
	}

	execSQL := func(t *testing.T, exec func(db *recordingDB) error) string {
		t.Helper()
		db := &recordingDB{}
		if err := exec(db); err != nil {
			t.Fatal(err)
		}
		return db.last(t).sql
	}
	ctx := context.Background()

	t.Run("6.2 target columns, DO NOTHING", func(t *testing.T) {
		cmd := hints.MustUpsertCommand(velum.FullScope, velum.OnConflict{Target: "(user_id, hint_key)"})
		got := execSQL(t, func(db *recordingDB) error { _, err := cmd.Exec(ctx, db, &SeenHint{}); return err })
		want := "INSERT INTO user_seen_hints AS t (user_id,hint_key,note) VALUES ($1,$2,$3) ON CONFLICT (user_id, hint_key) DO NOTHING"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.3 no target, DO NOTHING", func(t *testing.T) {
		cmd := hints.MustUpsertCommand("key", velum.OnConflictDoNothing)
		got := execSQL(t, func(db *recordingDB) error { _, err := cmd.Exec(ctx, db, &SeenHint{}); return err })
		want := "INSERT INTO user_seen_hints AS t (user_id,hint_key) VALUES ($1,$2) ON CONFLICT DO NOTHING"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.5 partial index target", func(t *testing.T) {
		cmd := audit.MustUpsertCommand(velum.FullScope, velum.OnConflict{Target: "(source_event_id) WHERE source_event_id IS NOT NULL"})
		got := execSQL(t, func(db *recordingDB) error { _, err := cmd.Exec(ctx, db, &AuditEvent{}); return err })
		want := "INSERT INTO audit_events AS t (id,type,payload,occurred_at,source_event_id) VALUES (DEFAULT,$1,$2,$3,$4) " +
			"ON CONFLICT (source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.9 constants in SET and UpdateWhere", func(t *testing.T) {
		cmd := reports.MustUpsertCommand(velum.FullScope, velum.OnConflict{
			Target:      "(document_id)",
			Action:      velum.DoUpdate,
			UpdateScope: "report",
			UpdateExpr:  "checked = '{}', imported_at = now()",
			UpdateWhere: "WHERE t.import_id <> EXCLUDED.import_id",
		})
		got := execSQL(t, func(db *recordingDB) error { _, err := cmd.Exec(ctx, db, &ImportReport{}); return err })
		want := "INSERT INTO edt.import_reports AS t (document_id,import_id,report) VALUES ($1,$2,$3) " +
			"ON CONFLICT (document_id) DO UPDATE SET import_id=EXCLUDED.import_id,report=EXCLUDED.report,checked = '{}', imported_at = now() " +
			"WHERE t.import_id <> EXCLUDED.import_id"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})
}

func TestUpsertReturningCommand_SQL(t *testing.T) {
	ctx := context.Background()
	query := func(t *testing.T, run func(db *recordingDB) error) recordedCall {
		t.Helper()
		db := &recordingDB{}
		if err := run(db); err != nil {
			t.Fatal(err)
		}
		return db.last(t)
	}

	t.Run("6.4 DO NOTHING RETURNING", func(t *testing.T) {
		cmd := velum.NewTable[Permission]("permissions").
			MustUpsertReturningCommand(velum.FullScope, "bits", velum.OnConflict{Target: "(id)"})
		got := query(t, func(db *recordingDB) error { _, _, err := cmd.QueryRow(ctx, db, &Permission{}); return err }).sql
		want := "INSERT INTO permissions AS t (id,name,bit_pos) VALUES ($1,$2,$3) ON CONFLICT (id) DO NOTHING RETURNING id,bit_pos,(t.xmax = 0)"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.6 expression target, UpdateWhere, EmptyScope returns PK", func(t *testing.T) {
		cmd := velum.NewTable[Environment]("pgp.environments").MustUpsertReturningCommand(velum.FullScope, velum.EmptyScope, velum.OnConflict{
			Target:      "(workspace_id, lower(name))",
			Action:      velum.DoUpdate,
			UpdateScope: "color",
			UpdateWhere: "WHERE t.color <> EXCLUDED.color",
		})
		got := query(t, func(db *recordingDB) error { _, _, err := cmd.QueryRow(ctx, db, &Environment{}); return err }).sql
		want := "INSERT INTO pgp.environments AS t (id,workspace_id,name,color,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6) " +
			"ON CONFLICT (workspace_id, lower(name)) DO UPDATE SET color=EXCLUDED.color,updated_at=$7 " +
			"WHERE t.color <> EXCLUDED.color RETURNING id,(t.xmax = 0)"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.7 ON CONSTRAINT, table without PK", func(t *testing.T) {
		cmd := velum.NewTable[DocumentContext]("pgp.user_document_contexts").MustUpsertReturningCommand(velum.FullScope, velum.UpdateScope,
			velum.OnConflict{Target: "ON CONSTRAINT pk_user_document_contexts", Action: velum.DoUpdate, UpdateScope: "ctx"})
		got := query(t, func(db *recordingDB) error { _, _, err := cmd.QueryRow(ctx, db, &DocumentContext{}); return err }).sql
		want := "INSERT INTO pgp.user_document_contexts AS t (workspace_id,user_id,document_id,connection_id,schema_name,updated_at) VALUES ($1,$2,$3,$4,$5,$6) " +
			"ON CONFLICT ON CONSTRAINT pk_user_document_contexts DO UPDATE SET connection_id=EXCLUDED.connection_id,schema_name=EXCLUDED.schema_name,updated_at=$7 " +
			"RETURNING updated_at,(t.xmax = 0)"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})

	t.Run("6.8 UpdateExpr with args", func(t *testing.T) {
		cmd := velum.NewTable[RateCounter]("rate_limit_counters").MustUpsertReturningCommand(velum.FullScope, "counter", velum.OnConflict{
			Target: "(kind, key)",
			Action: velum.DoUpdate,
			UpdateExpr: "count = CASE WHEN t.expires_at <= $1 THEN 1 ELSE t.count + 1 END, " +
				"expires_at = CASE WHEN t.expires_at <= $1 THEN EXCLUDED.expires_at ELSE t.expires_at END",
		})
		now := time.Now()
		call := query(t, func(db *recordingDB) error {
			_, _, err := cmd.QueryRow(ctx, db, &RateCounter{Kind: "ip", Key: "k", Count: 1}, now)
			return err
		})
		want := "INSERT INTO rate_limit_counters AS t (kind,key,count,expires_at) VALUES ($1,$2,$3,$4) " +
			"ON CONFLICT (kind, key) DO UPDATE SET count = CASE WHEN t.expires_at <= $5 THEN 1 ELSE t.count + 1 END, " +
			"expires_at = CASE WHEN t.expires_at <= $5 THEN EXCLUDED.expires_at ELSE t.expires_at END " +
			"RETURNING count,expires_at,(t.xmax = 0)"
		if call.sql != want {
			t.Errorf("sql\n got: %s\nwant: %s", call.sql, want)
		}
		if len(call.args) != 5 || call.args[4] != now {
			t.Errorf("args = %v, want 4 row values and now", call.args)
		}
	})

	t.Run("6.10 DO NOTHING RETURNING FullScope", func(t *testing.T) {
		cmd := velum.NewTable[RepoWorkspace]("repo_workspaces").
			MustUpsertReturningCommand(velum.FullScope, velum.FullScope, velum.OnConflict{Target: "(workspace_id)"})
		got := query(t, func(db *recordingDB) error { _, _, err := cmd.QueryRow(ctx, db, &RepoWorkspace{}); return err }).sql
		want := "INSERT INTO repo_workspaces AS t (workspace_id,organization_id) VALUES ($1,$2) " +
			"ON CONFLICT (workspace_id) DO NOTHING RETURNING workspace_id,organization_id,(t.xmax = 0)"
		if got != want {
			t.Errorf("sql\n got: %s\nwant: %s", got, want)
		}
	})
}

func TestUpsertCommand_ArgsOrder(t *testing.T) {
	prices := velum.NewTable[ProductPrice]("product_prices")
	cmd := prices.MustUpsertCommand("price", velum.OnConflict{
		Target:      "(sku)",
		Action:      velum.DoUpdate,
		UpdateScope: "price",
		UpdateWhere: "WHERE t.price < $1",
	})
	now := time.Now()
	row := &ProductPrice{SKU: "a", Price: 10, Currency: "EUR", RowVersion: 1, CreatedAt: now, UpdatedAt: &now}
	db := &recordingDB{}
	if _, err := cmd.Exec(context.Background(), db, row, int64(100)); err != nil {
		t.Fatal(err)
	}
	call := db.last(t)
	if !strings.HasSuffix(call.sql, "updated_at=$6 WHERE t.price < $7") {
		t.Errorf("sql: %s", call.sql)
	}
	// $1..$5 insert values, $6 updated_at, $7 the caller's arg.
	if len(call.args) != 7 {
		t.Fatalf("args = %d, want 7", len(call.args))
	}
	if p, ok := call.args[0].(*string); !ok || *p != "a" {
		t.Errorf("args[0] = %v, want *string a", call.args[0])
	}
	if p, ok := call.args[5].(**time.Time); !ok || *p != &now {
		t.Errorf("args[5] = %v, want &row.UpdatedAt", call.args[5])
	}
	if call.args[6] != int64(100) {
		t.Errorf("args[6] = %v, want 100", call.args[6])
	}
}

// upsertRowsDB answers QueryContext with zero or one row; the last scan
// destination (the xmax = 0 flag) gets inserted.
type upsertRowsDB struct {
	recordingDB
	hasRow   bool
	inserted bool
	scanErr  error
}

type upsertRows struct {
	db   *upsertRowsDB
	done bool
}

func (r *upsertRows) Next() bool {
	if r.done || !r.db.hasRow {
		return false
	}
	r.done = true
	return true
}
func (r *upsertRows) Close() error { return nil }
func (r *upsertRows) Err() error   { return nil }
func (r *upsertRows) Scan(dest ...any) error {
	if r.db.scanErr != nil {
		return r.db.scanErr
	}
	*dest[len(dest)-1].(*bool) = r.db.inserted
	return nil
}

func (db *upsertRowsDB) QueryContext(_ context.Context, sql string, args ...any) (velum.Rows, error) {
	db.record(sql, args)
	return &upsertRows{db: db}, nil
}

func TestUpsertCommand_QueryRowOutcome(t *testing.T) {
	cmd := velum.NewTable[RepoWorkspace]("repo_workspaces").
		MustUpsertReturningCommand(velum.FullScope, velum.FullScope, velum.OnConflict{Target: "(workspace_id)"})
	ctx := context.Background()

	cases := []struct {
		name    string
		db      *upsertRowsDB
		outcome velum.UpsertOutcome
		row     bool
	}{
		{"inserted", &upsertRowsDB{hasRow: true, inserted: true}, velum.UpsertInserted, true},
		{"updated", &upsertRowsDB{hasRow: true, inserted: false}, velum.UpsertUpdated, true},
		{"skipped", &upsertRowsDB{hasRow: false}, velum.UpsertSkipped, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, outcome, err := cmd.QueryRow(ctx, tc.db, &RepoWorkspace{WorkspaceID: 1, OrganizationID: 2})
			if err != nil {
				t.Fatal(err)
			}
			if outcome != tc.outcome {
				t.Errorf("outcome = %v, want %v", outcome, tc.outcome)
			}
			if (got != nil) != tc.row {
				t.Errorf("row = %v, want row %v", got, tc.row)
			}
		})
	}

	t.Run("scan error", func(t *testing.T) {
		scanErr := errors.New("scan failed")
		_, outcome, err := cmd.QueryRow(ctx, &upsertRowsDB{hasRow: true, scanErr: scanErr}, &RepoWorkspace{})
		if !errors.Is(err, scanErr) || outcome != velum.UpsertSkipped {
			t.Errorf("err = %v, outcome = %v", err, outcome)
		}
	})
}

func TestUpsertCommand_ConfigErrors(t *testing.T) {
	prices := velum.NewTable[ProductPrice]("product_prices")
	ws := velum.NewTable[RepoWorkspace]("repo_workspaces")

	cases := []struct {
		name   string
		build  func() error
		reason string
	}{
		{"DoUpdate without Target", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Action: velum.DoUpdate, UpdateScope: "price"})
			return err
		}, "DoUpdate needs a Target"},
		{"DoNothing with UpdateScope", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", UpdateScope: "price"})
			return err
		}, "DoNothing with UpdateScope"},
		{"nothing to set", func() error {
			_, err := ws.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(workspace_id)", Action: velum.DoUpdate})
			return err
		}, "nothing to set"},
		{"UpdateScope wider than insert scope", func() error {
			_, err := prices.UpsertCommand("price", velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateScope: velum.FullScope})
			return err
		}, `column note is not inserted by scope "price"`},
		{"UpdateExpr assigns update column", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateExpr: "updated_at = now()"})
			return err
		}, "UpdateExpr assigns updated_at"},
		{"UpdateExpr assigns version column in a row form", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateExpr: `(note, "row_version") = ('x', 1)`})
			return err
		}, "UpdateExpr assigns row_version"},
		{"UpdateExpr assigns UpdateScope column", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateScope: "price", UpdateExpr: "PRICE = t.price + 1"})
			return err
		}, "UpdateExpr assigns price, which UpdateScope"},
		{"unknown action", func() error {
			_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(sku)", Action: 7})
			return err
		}, "unknown ConflictAction 7"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build()
			if !errors.Is(err, velum.ErrInvalidUpsert) {
				t.Fatalf("err = %v, want ErrInvalidUpsert", err)
			}
			var ce *velum.UpsertConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("err %T is not *UpsertConfigError", err)
			}
			if !strings.Contains(ce.Reason, tc.reason) {
				t.Errorf("reason = %q, want it to contain %q", ce.Reason, tc.reason)
			}
			if !strings.HasPrefix(ce.SQL, "INSERT INTO ") {
				t.Errorf("SQL = %q, want the statement", ce.SQL)
			}
			if !strings.Contains(ce.Caller, "upsert_test.go:") {
				t.Errorf("Caller = %q, want this test file", ce.Caller)
			}
			if !strings.Contains(err.Error(), "; sql: INSERT INTO ") {
				t.Errorf("Error() = %q, want the SQL in it", err.Error())
			}
		})
	}
}

func TestUpsertCommand_ValidExpressionsAreNotTargets(t *testing.T) {
	prices := velum.NewTable[ProductPrice]("product_prices")
	_, err := prices.UpsertCommand(velum.FullScope, velum.OnConflict{
		Target: "(sku)",
		Action: velum.DoUpdate,
		// "=" and "," inside CASE, strings and function calls are not assignments.
		UpdateExpr: "note = CASE WHEN t.price <= EXCLUDED.price THEN 'a=b, updated_at=1' ELSE coalesce(t.note, 'x') END",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMustUpsertCommand_Panics(t *testing.T) {
	prices := velum.NewTable[ProductPrice]("product_prices")
	defer func() {
		r := recover()
		err, ok := r.(error)
		if !ok || !errors.Is(err, velum.ErrInvalidUpsert) {
			t.Fatalf("panic = %v, want *UpsertConfigError", r)
		}
		var ce *velum.UpsertConfigError
		if errors.As(err, &ce) && !strings.Contains(ce.Caller, "upsert_test.go:") {
			t.Errorf("Caller = %q, want this test file", ce.Caller)
		}
	}()
	prices.MustUpsertReturningCommand(velum.FullScope, velum.FullScope, velum.OnConflict{Action: velum.DoUpdate, UpdateScope: "price"})
}
