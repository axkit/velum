package velum_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/axkit/velum"
)

// recordingDB records every statement sent to it. Rows and Row are empty, so
// read methods return no data; the tests only look at the SQL and arguments.
type recordingDB struct {
	calls []recordedCall
}

type recordedCall struct {
	sql  string
	args []any
}

func (db *recordingDB) record(sql string, args []any) {
	db.calls = append(db.calls, recordedCall{sql: sql, args: append([]any(nil), args...)})
}

func (db *recordingDB) ExecContext(_ context.Context, sql string, args ...any) (velum.Result, error) {
	db.record(sql, args)
	return fakeExecResult{}, nil
}

func (db *recordingDB) QueryRowContext(_ context.Context, sql string, args ...any) velum.Row {
	db.record(sql, args)
	return &fakeDBRow{}
}

func (db *recordingDB) QueryContext(_ context.Context, sql string, args ...any) (velum.Rows, error) {
	db.record(sql, args)
	return &fakeDBRows{}, nil
}

func (db *recordingDB) last(t *testing.T) recordedCall {
	t.Helper()
	if len(db.calls) == 0 {
		t.Fatal("no statement was executed")
	}
	return db.calls[len(db.calls)-1]
}

// SeenHint has a composite key (user_id, hint_key): no "id" field and no
// dbw:"pk" tag, so the table has no primary key.
type SeenHint struct {
	UserID  int    `dbw:"key"`
	HintKey string `dbw:"key"`
	Note    string `dbw:"note"`
}

func TestNoPK_ClauseMethods(t *testing.T) {
	tbl := velum.NewTable[SeenHint]("user_seen_hints")
	ctx := context.Background()
	row := &SeenHint{UserID: 1, HintKey: "k", Note: "n"}

	cases := []struct {
		name string
		call func(db *recordingDB) error
		sql  string
	}{
		{"Select note", func(db *recordingDB) error {
			_, err := tbl.Select(ctx, db, "note", "WHERE user_id=$1", 1)
			return err
		}, "SELECT t.note FROM user_seen_hints t WHERE user_id=$1"},
		{"Select *", func(db *recordingDB) error {
			_, err := tbl.Select(ctx, db, velum.FullScope, "WHERE user_id=$1", 1)
			return err
		}, "SELECT t.user_id,t.hint_key,t.note FROM user_seen_hints t WHERE user_id=$1"},
		{"Get note", func(db *recordingDB) error {
			_, err := tbl.Get(ctx, db, "note", "WHERE user_id=$1 AND hint_key=$2", 1, "k")
			return err
		}, "SELECT t.note FROM user_seen_hints t WHERE user_id=$1 AND hint_key=$2"},
		{"Insert", func(db *recordingDB) error {
			return tbl.Insert(ctx, db, row)
		}, "INSERT INTO user_seen_hints (user_id,hint_key,note) VALUES ($1,$2,$3) RETURNING user_id,hint_key,note"},
		{"InsertScope key", func(db *recordingDB) error {
			_, err := tbl.InsertScope(ctx, db, row, "key")
			return err
		}, "INSERT INTO user_seen_hints (user_id,hint_key) VALUES ($1,$2)"},
		{"Update note", func(db *recordingDB) error {
			_, err := tbl.Update(ctx, db, row, "note", "WHERE user_id=$1 AND hint_key=$2", 1, "k")
			return err
		}, "UPDATE user_seen_hints SET note=$1 WHERE user_id=$2 AND hint_key=$3"},
		{"UpdateReturning note", func(db *recordingDB) error {
			_, err := tbl.UpdateReturning(ctx, db, row, "note", "note", "WHERE user_id=$1 AND hint_key=$2", 1, "k")
			return err
		}, "UPDATE user_seen_hints SET note=$1 WHERE user_id=$2 AND hint_key=$3 RETURNING note"},
		{"Delete", func(db *recordingDB) error {
			_, err := tbl.Delete(ctx, db, "WHERE user_id=$1", 1)
			return err
		}, "DELETE FROM user_seen_hints WHERE user_id=$1"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &recordingDB{}
			// fakeDBRow scans nothing and returns nil, so read errors are not expected either.
			if err := tc.call(db); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := db.last(t).sql; got != tc.sql {
				t.Errorf("sql\n got: %s\nwant: %s", got, tc.sql)
			}
		})
	}
}

func TestNoPK_ByPKMethodsReturnErrNoPrimaryKey(t *testing.T) {
	tbl := velum.NewTable[SeenHint]("user_seen_hints")
	ctx := context.Background()
	row := &SeenHint{UserID: 1, HintKey: "k"}

	cases := []struct {
		name string
		call func(db *recordingDB) error
	}{
		{"GetByPK", func(db *recordingDB) error { _, err := tbl.GetByPK(ctx, db, 1); return err }},
		{"GetByPKTo", func(db *recordingDB) error { return tbl.GetByPKTo(ctx, db, nil, 1) }},
		{"ExistByPK", func(db *recordingDB) error { _, err := tbl.ExistByPK(ctx, db, 1); return err }},
		{"DeleteByPK", func(db *recordingDB) error { _, err := tbl.DeleteByPK(ctx, db, 1); return err }},
		{"DeleteReturningByPK", func(db *recordingDB) error { _, err := tbl.DeleteReturningByPK(ctx, db, row); return err }},
		{"UpdateByPK *", func(db *recordingDB) error { _, err := tbl.UpdateByPK(ctx, db, row, velum.FullScope); return err }},
		{"UpdateByPK note", func(db *recordingDB) error { _, err := tbl.UpdateByPK(ctx, db, row, "note"); return err }},
		{"UpdateReturningByPK", func(db *recordingDB) error {
			_, err := tbl.UpdateReturningByPK(ctx, db, row, velum.FullScope, velum.FullScope)
			return err
		}},
		{"SoftDeleteByPK", func(db *recordingDB) error { _, err := tbl.SoftDeleteByPK(ctx, db, row); return err }},
		{"SoftDeleteReturningByPK", func(db *recordingDB) error { _, err := tbl.SoftDeleteReturningByPK(ctx, db, row); return err }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &recordingDB{}
			if err := tc.call(db); !errors.Is(err, velum.ErrNoPrimaryKey) {
				t.Fatalf("err = %v, want ErrNoPrimaryKey", err)
			}
			// UpdateByPK without a PK used to send "UPDATE … SET …" without WHERE,
			// overwriting the whole table: nothing may reach the database.
			if len(db.calls) != 0 {
				t.Fatalf("statement executed: %q", db.calls[0].sql)
			}
		})
	}
}

func TestUpdate_ClauseArgsFollowSetArgs(t *testing.T) {
	tbl := velum.NewTable[CustomerFriendlySeq]("customers")
	db := &recordingDB{}
	c := &CustomerFriendlySeq{ID: 7, Customer: Customer{Age: 30}}

	if _, err := tbl.Update(context.Background(), db, c, "age", "WHERE first_name=$1 AND last_name=$2", "A", "B"); err != nil {
		t.Fatal(err)
	}
	call := db.last(t)
	// Customer has row_version (version, no arg) and updated_at (update, one arg).
	want := "UPDATE customers SET age=$1,row_version=row_version+1,updated_at=$2 WHERE first_name=$3 AND last_name=$4"
	if call.sql != want {
		t.Errorf("sql\n got: %s\nwant: %s", call.sql, want)
	}
	if len(call.args) != 4 || call.args[2] != "A" || call.args[3] != "B" {
		t.Errorf("args = %v, want [age updated_at A B]", call.args)
	}
}

func TestUpdateByPK_ClauseUnchanged(t *testing.T) {
	tbl := velum.NewTable[CustomerFriendlySeq]("customers")
	db := &recordingDB{}
	if _, err := tbl.UpdateByPK(context.Background(), db, &CustomerFriendlySeq{ID: 7}, "age"); err != nil {
		t.Fatal(err)
	}
	if got := db.last(t).sql; !strings.HasSuffix(got, " WHERE id=$1") {
		t.Errorf("by-PK clause must keep $1 for the PK: %s", got)
	}
}

type TwoPK struct {
	A int `dbw:"pk,gen=no"`
	B int `dbw:"pk,gen=no"`
}

func TestNewTable_PanicsOnSeveralPK(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("NewTable did not panic")
		}
		if msg, _ := r.(string); !strings.Contains(msg, "composite primary keys are not supported") {
			t.Fatalf("panic = %v", r)
		}
	}()
	velum.NewTable[TwoPK]("two_pk")
}
