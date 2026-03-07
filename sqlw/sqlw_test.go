package sqlw_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"testing"

	"github.com/axkit/velum"
	"github.com/axkit/velum/sqlw"
)

// fakeDriver is a minimal database/sql driver used only in tests.
// It does not talk to any real database; connections are created instantly.
type fakeDriver struct{}
type fakeConn struct{}
type fakeTx struct{}
type fakeStmt struct{}

func (fakeDriver) Open(_ string) (driver.Conn, error)             { return fakeConn{}, nil }
func (fakeConn) Prepare(_ string) (driver.Stmt, error)            { return fakeStmt{}, nil }
func (fakeConn) Close() error                                     { return nil }
func (fakeConn) Begin() (driver.Tx, error)                        { return fakeTx{}, nil }
func (fakeTx) Commit() error                                      { return nil }
func (fakeTx) Rollback() error                                    { return nil }
func (fakeStmt) Close() error                                     { return nil }
func (fakeStmt) NumInput() int                                    { return -1 }
func (fakeStmt) Exec(_ []driver.Value) (driver.Result, error)     { return nil, nil }
func (fakeStmt) Query(_ []driver.Value) (driver.Rows, error)      { return nil, nil }

var registerOnce sync.Once

func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	registerOnce.Do(func() {
		sql.Register("velum_sqlw_test", fakeDriver{})
	})
	db, err := sql.Open("velum_sqlw_test", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestDatabaseWrapper_Begin_RespectsContext verifies that Begin propagates
// context cancellation to the underlying BeginTx call.
//
// Before the fix, Begin called db.Begin() which internally always used
// context.Background(), so a cancelled caller context was silently ignored
// and Begin would succeed instead of returning context.Canceled.
func TestDatabaseWrapper_Begin_RespectsContext(t *testing.T) {
	db := newTestDB(t)
	w := sqlw.NewDatabaseWrapper(db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before calling Begin

	_, err := w.Begin(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Begin with cancelled context: got %v, want context.Canceled", err)
	}
}

// TestDatabaseWrapper_InTx_RespectsContext verifies that InTx also propagates
// a cancelled context (InTx delegates to Begin internally).
func TestDatabaseWrapper_InTx_RespectsContext(t *testing.T) {
	db := newTestDB(t)
	w := sqlw.NewDatabaseWrapper(db)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	called := false
	err := w.InTx(ctx, func(_ velum.Transaction) error {
		called = true
		return nil
	})

	if called {
		t.Error("fn should not have been called when Begin fails")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("InTx with cancelled context: got %v, want context.Canceled", err)
	}
}
