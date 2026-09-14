package logw_test

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/axkit/velum"
	"github.com/axkit/velum/logw"
)

// recHandler is a slog.Handler that captures every log record for assertion.
type recHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *recHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r.Clone())
	h.mu.Unlock()
	return nil
}

func (h *recHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *recHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *recHandler) at(i int) slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[i]
}

func (h *recHandler) last() slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[len(h.records)-1]
}

// flatAttrs flattens all slog attrs in r to a map[string]slog.Value using dot
// notation for nested groups, e.g. "args.$1", "row1.id".
func flatAttrs(r slog.Record) map[string]slog.Value {
	m := make(map[string]slog.Value)
	var walk func(prefix string, attrs []slog.Attr)
	walk = func(prefix string, attrs []slog.Attr) {
		for _, a := range attrs {
			if a.Value.Kind() == slog.KindGroup {
				walk(prefix+a.Key+".", a.Value.Group())
			} else {
				m[prefix+a.Key] = a.Value
			}
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindGroup {
			walk(a.Key+".", a.Value.Group())
		} else {
			m[a.Key] = a.Value
		}
		return true
	})
	return m
}

type fakeResult struct{ n int64 }

func (r fakeResult) RowsAffected() (int64, error) { return r.n, nil }

// fakeRow implements velum.Row by scanning pre-set values into dest pointers.
type fakeRow struct {
	vals []any
	err  error
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		if i >= len(r.vals) {
			break
		}
		v := reflect.ValueOf(d)
		if v.Kind() == reflect.Ptr && !v.IsNil() {
			v.Elem().Set(reflect.ValueOf(r.vals[i]))
		}
	}
	return nil
}

func (r *fakeRow) Err() error { return r.err }

// fakeRows implements velum.Rows and the optional columnsNamer interface.
type fakeRows struct {
	cols []string
	data [][]any
	pos  int
}

func (r *fakeRows) Next() bool   { r.pos++; return r.pos <= len(r.data) }
func (r *fakeRows) Close() error { return nil }
func (r *fakeRows) Err() error   { return nil }

func (r *fakeRows) Scan(dest ...any) error {
	row := r.data[r.pos-1]
	for i, d := range dest {
		if i >= len(row) {
			break
		}
		v := reflect.ValueOf(d)
		if v.Kind() == reflect.Ptr && !v.IsNil() {
			v.Elem().Set(reflect.ValueOf(row[i]))
		}
	}
	return nil
}

func (r *fakeRows) Columns() ([]string, error) { return r.cols, nil }

// fakeTx implements velum.Transaction.
type fakeTx struct {
	commitErr   error
	rollbackErr error
	queryCtxErr error
}

func (t *fakeTx) ExecContext(_ context.Context, _ string, _ ...any) (velum.Result, error) {
	return fakeResult{1}, nil
}
func (t *fakeTx) QueryRowContext(_ context.Context, _ string, _ ...any) velum.Row {
	return &fakeRow{vals: []any{42}}
}
func (t *fakeTx) QueryContext(_ context.Context, _ string, _ ...any) (velum.Rows, error) {
	if t.queryCtxErr != nil {
		return nil, t.queryCtxErr
	}
	return &fakeRows{data: [][]any{{1}}}, nil
}
func (t *fakeTx) Commit(_ context.Context) error   { return t.commitErr }
func (t *fakeTx) Rollback(_ context.Context) error { return t.rollbackErr }

// fakeRowsPlain implements velum.Rows but NOT columnsNamer, so column names
// fall back to positional placeholders ($1, $2, …) in the log.
type fakeRowsPlain struct {
	data [][]any
	pos  int
}

func (r *fakeRowsPlain) Next() bool   { r.pos++; return r.pos <= len(r.data) }
func (r *fakeRowsPlain) Close() error { return nil }
func (r *fakeRowsPlain) Err() error   { return nil }

func (r *fakeRowsPlain) Scan(dest ...any) error {
	row := r.data[r.pos-1]
	for i, d := range dest {
		if i >= len(row) {
			break
		}
		v := reflect.ValueOf(d)
		if v.Kind() == reflect.Ptr && !v.IsNil() {
			v.Elem().Set(reflect.ValueOf(row[i]))
		}
	}
	return nil
}

// fakeDB implements velum.DatabaseWrapper.
type fakeDB struct {
	result    velum.Result
	row       *fakeRow
	rows      *fakeRows
	tx        velum.Transaction
	execDelay time.Duration // added to ExecContext to simulate slow queries
	execErr   error         // returned by ExecContext
	rowsErr   error         // returned by QueryContext
	beginErr  error         // returned by Begin
}

func defaultDB() *fakeDB {
	return &fakeDB{
		result: fakeResult{3},
		row:    &fakeRow{vals: []any{1, "Alice"}},
		rows: &fakeRows{
			cols: []string{"id", "name"},
			data: [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Carol"}},
		},
		tx: &fakeTx{},
	}
}

func (db *fakeDB) ExecContext(_ context.Context, _ string, _ ...any) (velum.Result, error) {
	if db.execDelay > 0 {
		time.Sleep(db.execDelay)
	}
	if db.execErr != nil {
		return nil, db.execErr
	}
	return db.result, nil
}
func (db *fakeDB) QueryRowContext(_ context.Context, _ string, _ ...any) velum.Row {
	return db.row
}
func (db *fakeDB) QueryContext(_ context.Context, _ string, _ ...any) (velum.Rows, error) {
	if db.rowsErr != nil {
		return nil, db.rowsErr
	}
	return db.rows, nil
}
func (db *fakeDB) IsNotFound(err error) bool { return errors.Is(err, errNotFound) }
func (db *fakeDB) Begin(_ context.Context) (velum.Transaction, error) {
	if db.beginErr != nil {
		return nil, db.beginErr
	}
	return db.tx, nil
}
func (db *fakeDB) InTx(ctx context.Context, fn func(velum.Transaction) error) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

var errNotFound = errors.New("not found")

func newLogger(h *recHandler) *slog.Logger { return slog.New(h) }

func assertStr(t *testing.T, key, want string, m map[string]slog.Value) {
	t.Helper()
	got := m[key].String()
	if got != want {
		t.Errorf("%s: got %q, want %q", key, got, want)
	}
}

func assertInt64(t *testing.T, key string, want int64, m map[string]slog.Value) {
	t.Helper()
	if got := m[key].Int64(); got != want {
		t.Errorf("%s: got %d, want %d", key, got, want)
	}
}

func assertPresent(t *testing.T, m map[string]slog.Value, key string) {
	t.Helper()
	if _, ok := m[key]; !ok {
		t.Errorf("expected key %q in log attrs", key)
	}
}

func assertAbsent(t *testing.T, m map[string]slog.Value, key string) {
	t.Helper()
	if _, ok := m[key]; ok {
		t.Errorf("expected key %q to be absent in log attrs", key)
	}
}

// drainRows iterates and optionally scans all rows, then closes.
func drainRows(t *testing.T, rows velum.Rows, scanFn func(velum.Rows)) {
	t.Helper()
	for rows.Next() {
		if scanFn != nil {
			scanFn(rows)
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("rows.Close: %v", err)
	}
}

func TestExecContext_BasicFields(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))

	_, _ = w.ExecContext(context.Background(), "DELETE FROM t")

	if h.count() != 1 {
		t.Fatalf("want 1 record, got %d", h.count())
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "exec", m)
	assertStr(t, "sql", "DELETE FROM t", m)
	assertPresent(t, m, "dur")
	assertInt64(t, "rows_affected", 3, m)
	assertAbsent(t, m, "args_count") // no args passed
}

func TestExecContext_ArgsCount(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))

	_, _ = w.ExecContext(context.Background(), "DELETE FROM t WHERE id=$1", 7)

	m := flatAttrs(h.last())
	assertInt64(t, "args_count", 1, m)
	assertAbsent(t, m, "args.$1") // values not logged when logArgs=false
}

func TestExecContext_LogArgs(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)), logw.WithLogArgs(true))

	_, _ = w.ExecContext(context.Background(), "UPDATE t SET x=$1 WHERE id=$2", 42, 99)

	m := flatAttrs(h.last())
	assertInt64(t, "args_count", 2, m)
	assertPresent(t, m, "args.$1")
	assertPresent(t, m, "args.$2")
	if m["args.$1"].Int64() != 42 {
		t.Errorf("args.$1: got %v, want 42", m["args.$1"])
	}
	if m["args.$2"].Int64() != 99 {
		t.Errorf("args.$2: got %v, want 99", m["args.$2"])
	}
}

func TestExecContext_NoArgsSkipsArgsCount(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)), logw.WithLogArgs(true))

	_, _ = w.ExecContext(context.Background(), "SELECT 1")

	assertAbsent(t, flatAttrs(h.last()), "args_count")
}

func TestExecContext_SlowQuery_WarnLevel(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.execDelay = 2 * time.Millisecond // make the operation reliably slow
	w := logw.New(db,
		logw.WithBaseLogger(newLogger(h)),
		logw.WithSlowQueryThreshold(1*time.Millisecond),
	)
	_, _ = w.ExecContext(context.Background(), "SELECT 1")

	if h.last().Level != slog.LevelWarn {
		t.Errorf("level=%v, want WARN", h.last().Level)
	}
}

func TestExecContext_FastQuery_DebugLevel(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(),
		logw.WithBaseLogger(newLogger(h)),
		logw.WithSlowQueryThreshold(1000*time.Second), // never exceeded
	)
	_, _ = w.ExecContext(context.Background(), "SELECT 1")

	if h.last().Level != slog.LevelDebug {
		t.Errorf("level=%v, want DEBUG", h.last().Level)
	}
}

func TestQueryRowContext_BasicFields(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))

	row := w.QueryRowContext(context.Background(), "SELECT id, name FROM t WHERE id=$1", 1)
	var id int
	var name string
	_ = row.Scan(&id, &name)

	if h.count() != 1 {
		t.Fatalf("want 1 record, got %d", h.count())
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "query_row", m)
	assertPresent(t, m, "sql")
	assertPresent(t, m, "query_dur")
	assertPresent(t, m, "scan_dur")
	assertAbsent(t, m, "result.$1") // logRows=0 → no result values
}

func TestQueryRowContext_LogRows(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(1))

	row := w.QueryRowContext(context.Background(), "SELECT id, name FROM t WHERE id=$1", 1)
	var id int
	var name string
	_ = row.Scan(&id, &name)

	m := flatAttrs(h.last())
	assertPresent(t, m, "result.$1")
	assertPresent(t, m, "result.$2")
	if m["result.$1"].Int64() != 1 {
		t.Errorf("result.$1: got %v, want 1", m["result.$1"])
	}
	if m["result.$2"].String() != "Alice" {
		t.Errorf("result.$2: got %v, want Alice", m["result.$2"])
	}
}

func TestQueryContext_BasicFields(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))

	rows, err := w.QueryContext(context.Background(), "SELECT id, name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	drainRows(t, rows, nil)

	if h.count() != 1 {
		t.Fatalf("want 1 record, got %d", h.count())
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "query", m)
	assertPresent(t, m, "first_row_dur")
	assertPresent(t, m, "scan_dur")
	assertInt64(t, "rows", 3, m)
	assertAbsent(t, m, "row1.id") // logRows=0
}

func TestQueryContext_RowSampling_WithColumnNames(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(2))

	rows, err := w.QueryContext(context.Background(), "SELECT id, name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	var id int
	var name string
	drainRows(t, rows, func(r velum.Rows) { _ = r.Scan(&id, &name) })

	m := flatAttrs(h.last())
	assertInt64(t, "rows", 3, m)
	// rows_sampled present because 2 captured < 3 total
	assertInt64(t, "rows_sampled", 2, m)
	// first two rows captured with column names from columnsNamer
	assertPresent(t, m, "row1.id")
	assertPresent(t, m, "row1.name")
	assertPresent(t, m, "row2.id")
	assertPresent(t, m, "row2.name")
	assertAbsent(t, m, "row3.id")
	if m["row1.id"].Int64() != 1 {
		t.Errorf("row1.id: got %v, want 1", m["row1.id"])
	}
	if m["row1.name"].String() != "Alice" {
		t.Errorf("row1.name: got %v, want Alice", m["row1.name"])
	}
	if m["row2.id"].Int64() != 2 {
		t.Errorf("row2.id: got %v, want 2", m["row2.id"])
	}
}

func TestQueryContext_NoRowsSampledWhenAllCaptured(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(5))

	rows, err := w.QueryContext(context.Background(), "SELECT id, name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	var id int
	var name string
	drainRows(t, rows, func(r velum.Rows) { _ = r.Scan(&id, &name) })

	m := flatAttrs(h.last())
	assertInt64(t, "rows", 3, m)
	assertAbsent(t, m, "rows_sampled") // all 3 rows captured, no truncation
	assertPresent(t, m, "row1.id")
	assertPresent(t, m, "row2.id")
	assertPresent(t, m, "row3.id")
}

func TestQueryContext_EmptyResult(t *testing.T) {
	db := &fakeDB{
		result: fakeResult{0},
		row:    &fakeRow{},
		rows:   &fakeRows{cols: []string{"id", "name"}, data: nil},
		tx:     &fakeTx{},
	}
	h := &recHandler{}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(5))

	rows, err := w.QueryContext(context.Background(), "SELECT id, name FROM t WHERE 1=0")
	if err != nil {
		t.Fatal(err)
	}
	drainRows(t, rows, nil)

	m := flatAttrs(h.last())
	assertStr(t, "op", "query", m)
	assertInt64(t, "rows", 0, m)
	assertAbsent(t, m, "row1.id")
	assertAbsent(t, m, "rows_sampled")
}

func TestTransaction_Commit(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tx.ExecContext(ctx, "INSERT INTO t VALUES($1)", 1)
	_, _ = tx.ExecContext(ctx, "UPDATE t SET x=$1 WHERE id=$2", 10, 1)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// 2 exec entries + 1 tx summary
	if h.count() != 3 {
		t.Fatalf("want 3 records, got %d", h.count())
	}

	m0 := flatAttrs(h.at(0))
	m1 := flatAttrs(h.at(1))
	m2 := flatAttrs(h.at(2))

	txID := m0["tx_id"].Uint64()
	if txID == 0 {
		t.Error("tx_id should be non-zero")
	}
	if m1["tx_id"].Uint64() != txID {
		t.Error("tx_id must be consistent across exec entries")
	}

	assertStr(t, "op", "tx", m2)
	assertStr(t, "outcome", "committed", m2)
	assertInt64(t, "queries", 2, m2)
	if m2["tx_id"].Uint64() != txID {
		t.Error("summary tx_id must match exec tx_id")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)

	m := flatAttrs(h.last())
	assertStr(t, "op", "tx", m)
	assertStr(t, "outcome", "rolled_back", m)
}

func TestLoggerResolution_BaseLogger(t *testing.T) {
	base := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(base)))

	_, _ = w.ExecContext(context.Background(), "SELECT 1")

	if base.count() != 1 {
		t.Errorf("base logger: want 1 record, got %d", base.count())
	}
}

func TestLoggerResolution_WithLoggerContext(t *testing.T) {
	base := &recHandler{}
	ctxH := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(base)))

	ctx := logw.WithLogger(context.Background(), newLogger(ctxH))
	_, _ = w.ExecContext(ctx, "SELECT 1")

	if base.count() != 0 {
		t.Error("base logger must not receive when context logger is set")
	}
	if ctxH.count() != 1 {
		t.Errorf("context logger: want 1 record, got %d", ctxH.count())
	}
}

func TestLoggerResolution_WithLoggerFromContext(t *testing.T) {
	base := &recHandler{}
	fnH := &recHandler{}
	withLoggerH := &recHandler{}

	type myKey struct{}

	w := logw.New(defaultDB(),
		logw.WithBaseLogger(newLogger(base)),
		logw.WithLoggerFromContext(func(ctx context.Context) *slog.Logger {
			l, _ := ctx.Value(myKey{}).(*slog.Logger)
			return l
		}),
	)

	// Both logw.WithLogger and the custom key are set; fn-key must win.
	ctx := logw.WithLogger(context.Background(), newLogger(withLoggerH))
	ctx = context.WithValue(ctx, myKey{}, newLogger(fnH))
	_, _ = w.ExecContext(ctx, "SELECT 1")

	if fnH.count() != 1 {
		t.Errorf("fn logger: want 1 record, got %d", fnH.count())
	}
	if withLoggerH.count() != 0 {
		t.Error("WithLogger logger must not receive when fn returns non-nil")
	}
	if base.count() != 0 {
		t.Error("base logger must not receive")
	}
}

func TestLoggerResolution_NilFnFallsBack(t *testing.T) {
	base := &recHandler{}
	w := logw.New(defaultDB(),
		logw.WithBaseLogger(newLogger(base)),
		logw.WithLoggerFromContext(func(ctx context.Context) *slog.Logger {
			return nil // always nil → fall back to next in chain
		}),
	)

	_, _ = w.ExecContext(context.Background(), "SELECT 1")

	if base.count() != 1 {
		t.Errorf("base logger: want 1 record when fn returns nil, got %d", base.count())
	}
}

func TestWrapper_IsNotFound(t *testing.T) {
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(&recHandler{})))

	if w.IsNotFound(nil) {
		t.Error("nil error: want false")
	}
	if w.IsNotFound(errors.New("other")) {
		t.Error("unrelated error: want false")
	}
	if !w.IsNotFound(errNotFound) {
		t.Error("sentinel error: want true")
	}
}

func TestWrapper_Begin_Error(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.beginErr = errors.New("connection refused")
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	_, err := w.Begin(context.Background())
	if err == nil {
		t.Fatal("expected error from Begin")
	}
	// No log entry should be emitted for a failed Begin.
	if h.count() != 0 {
		t.Errorf("want 0 records on Begin failure, got %d", h.count())
	}
}

func TestWrapper_InTx_Commit(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	err := w.InTx(ctx, func(tx velum.Transaction) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO t VALUES($1)", 1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// 1 exec entry + 1 tx summary.
	if h.count() != 2 {
		t.Fatalf("want 2 records, got %d", h.count())
	}
	assertStr(t, "outcome", "committed", flatAttrs(h.last()))
}

func TestWrapper_InTx_RollbackOnError(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	fnErr := errors.New("boom")
	err := w.InTx(ctx, func(tx velum.Transaction) error { return fnErr })
	if !errors.Is(err, fnErr) {
		t.Fatalf("want fnErr, got %v", err)
	}
	// Only the tx summary (0 queries, rolled_back).
	if h.count() != 1 {
		t.Fatalf("want 1 record, got %d", h.count())
	}
	assertStr(t, "outcome", "rolled_back", flatAttrs(h.last()))
}

// fakeTxDB adds transaction options support on top of fakeDB.
type fakeTxDB struct {
	*fakeDB
	gotOpts   velum.TxOptions
	retryable bool
}

func (db *fakeTxDB) BeginTx(ctx context.Context, opts velum.TxOptions) (velum.Transaction, error) {
	db.gotOpts = opts
	return db.Begin(ctx)
}

func (db *fakeTxDB) InTxWith(ctx context.Context, opts velum.TxOptions, fn func(velum.Transaction) error) error {
	db.gotOpts = opts
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (db *fakeTxDB) IsRetryable(error) bool { return db.retryable }

// Options must reach the underlying wrapper and the level must be logged.
func TestWrapper_InTxWith_Forwards(t *testing.T) {
	h := &recHandler{}
	db := &fakeTxDB{fakeDB: defaultDB()}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	opts := velum.TxOptions{IsoLevel: velum.RepeatableRead, ReadOnly: true}
	err := w.InTxWith(ctx, opts, func(tx velum.Transaction) error { return nil })
	if err != nil {
		t.Fatalf("InTxWith returned %v", err)
	}
	if db.gotOpts != opts {
		t.Errorf("underlying wrapper got %+v, want %+v", db.gotOpts, opts)
	}
	assertStr(t, "isolation", "repeatable read", flatAttrs(h.last()))
}

// A wrapper without options support must fail loudly, never silently downgrade.
func TestWrapper_InTxWith_Unsupported(t *testing.T) {
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(&recHandler{})))

	err := w.InTxWith(context.Background(), velum.TxOptions{IsoLevel: velum.RepeatableRead},
		func(tx velum.Transaction) error { return nil })
	if !errors.Is(err, velum.ErrTxOptionsUnsupported) {
		t.Fatalf("want ErrTxOptionsUnsupported, got %v", err)
	}

	if _, err := w.BeginTx(context.Background(), velum.TxOptions{IsoLevel: velum.Serializable}); !errors.Is(err, velum.ErrTxOptionsUnsupported) {
		t.Fatalf("BeginTx: want ErrTxOptionsUnsupported, got %v", err)
	}
}

// Plain InTx keeps working over a wrapper without options support.
func TestWrapper_InTx_WorksWithoutOptionsSupport(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))

	err := w.InTx(context.Background(), func(tx velum.Transaction) error { return nil })
	if err != nil {
		t.Fatalf("InTx returned %v", err)
	}
	if got, ok := flatAttrs(h.last())["isolation"]; ok {
		t.Errorf("isolation logged as %v for a default transaction, want nothing", got)
	}
}

func TestWrapper_IsRetryable(t *testing.T) {
	w := logw.New(&fakeTxDB{fakeDB: defaultDB(), retryable: true},
		logw.WithBaseLogger(newLogger(&recHandler{})))
	if !w.IsRetryable(errors.New("conflict")) {
		t.Error("IsRetryable = false, want true")
	}

	// fakeDB alone cannot classify errors.
	plain := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(&recHandler{})))
	if plain.IsRetryable(errors.New("conflict")) {
		t.Error("IsRetryable = true for a wrapper without support, want false")
	}
}

func TestWrapper_InTx_CommitError(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	commitErr := errors.New("commit failed")
	db.tx = &fakeTx{commitErr: commitErr}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	err := w.InTx(context.Background(), func(tx velum.Transaction) error { return nil })
	if !errors.Is(err, commitErr) {
		t.Fatalf("want commitErr, got %v", err)
	}
}

func TestExecContext_Error(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.execErr = errors.New("db error")
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	_, err := w.ExecContext(context.Background(), "DELETE FROM t")
	if err == nil {
		t.Fatal("expected error")
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "exec", m)
	assertPresent(t, m, "error")
}

func TestQueryContext_Error(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.rowsErr = errors.New("query failed")
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	_, err := w.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error")
	}
	// emitQuery is called immediately with the error.
	m := flatAttrs(h.last())
	assertStr(t, "op", "query", m)
	assertPresent(t, m, "error")
}

func TestQueryRowContext_ScanError(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.row = &fakeRow{err: errors.New("scan failed")}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	row := w.QueryRowContext(context.Background(), "SELECT id FROM t WHERE id=$1", 1)
	var id int
	err := row.Scan(&id)
	if err == nil {
		t.Fatal("expected scan error")
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "query_row", m)
	assertPresent(t, m, "error")
}

func TestQueryContext_NoColumnNames(t *testing.T) {
	h := &recHandler{}
	// fakeDBPlain returns fakeRowsPlain which does not implement columnsNamer
	// → column keys fall back to $1, $2, … instead of column names.
	noNameDB := &fakeDBPlain{
		fakeDB: defaultDB(),
		plainRows: &fakeRowsPlain{
			data: [][]any{{42, "Bob"}},
		},
	}
	w2 := logw.New(noNameDB, logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(1))

	rows, err := w2.QueryContext(context.Background(), "SELECT id, name FROM t")
	if err != nil {
		t.Fatal(err)
	}
	var id int
	var name string
	drainRows(t, rows, func(r velum.Rows) { _ = r.Scan(&id, &name) })

	m := flatAttrs(h.last())
	// Without column names, keys are positional: $1, $2.
	assertPresent(t, m, "row1.$1")
	assertPresent(t, m, "row1.$2")
	assertAbsent(t, m, "row1.id")
}

// fakeDBPlain wraps fakeDB but returns fakeRowsPlain from QueryContext.
type fakeDBPlain struct {
	*fakeDB
	plainRows *fakeRowsPlain
}

func (db *fakeDBPlain) QueryContext(_ context.Context, _ string, _ ...any) (velum.Rows, error) {
	return db.plainRows, nil
}

func TestQueryRowContext_LogRows_NonPointerDest(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	// fakeRow with a nil dest element: Scan succeeds but one dest is non-addressable.
	db.row = &fakeRow{vals: []any{7, "Carol"}}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)), logw.WithLogRows(1))

	row := w.QueryRowContext(context.Background(), "SELECT id, name FROM t")
	var id int
	// Pass nil as the second dest — captureRow must handle non-pointer gracefully.
	_ = row.Scan(&id, nil)

	m := flatAttrs(h.last())
	assertStr(t, "op", "query_row", m)
	assertPresent(t, m, "result.$1") // id captured
	assertPresent(t, m, "result.$2") // nil passed through as-is
}

func TestTransaction_QueryRowContext(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	row := tx.QueryRowContext(ctx, "SELECT id FROM t WHERE id=$1", 1)
	var id int
	_ = row.Scan(&id)
	_ = tx.Rollback(ctx)

	// 1 query_row entry + 1 tx summary.
	if h.count() != 2 {
		t.Fatalf("want 2 records, got %d", h.count())
	}
	assertStr(t, "op", "query_row", flatAttrs(h.at(0)))
	assertPresent(t, flatAttrs(h.at(0)), "tx_id")
}

func TestTransaction_QueryContext(t *testing.T) {
	h := &recHandler{}
	w := logw.New(defaultDB(), logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tx.QueryContext(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatal(err)
	}
	drainRows(t, rows, nil)
	_ = tx.Commit(ctx)

	// 1 query entry + 1 tx summary.
	if h.count() != 2 {
		t.Fatalf("want 2 records, got %d", h.count())
	}
	assertStr(t, "op", "query", flatAttrs(h.at(0)))
	assertPresent(t, flatAttrs(h.at(0)), "tx_id")
}

func TestTransaction_CommitError(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.tx = &fakeTx{commitErr: errors.New("commit failed")}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected commit error")
	}
	m := flatAttrs(h.last())
	assertStr(t, "op", "tx", m)
	assertStr(t, "outcome", "committed", m) // outcome reflects intent, not result
	assertPresent(t, m, "error")
}

func TestTransaction_QueryContext_Error(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.tx = &fakeTx{queryCtxErr: errors.New("query failed")}
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))
	ctx := context.Background()

	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.QueryContext(ctx, "SELECT id FROM t")
	if err == nil {
		t.Fatal("expected error from tx.QueryContext")
	}
	_ = tx.Rollback(ctx)

	// First record is the emitQuery for the failed query.
	m := flatAttrs(h.at(0))
	assertStr(t, "op", "query", m)
	assertPresent(t, m, "error")
	assertPresent(t, m, "tx_id")
}

func TestWrapper_InTx_BeginError(t *testing.T) {
	h := &recHandler{}
	db := defaultDB()
	db.beginErr = errors.New("connection refused")
	w := logw.New(db, logw.WithBaseLogger(newLogger(h)))

	err := w.InTx(context.Background(), func(tx velum.Transaction) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error from InTx when Begin fails")
	}
	// No log entry emitted when Begin itself fails.
	if h.count() != 0 {
		t.Errorf("want 0 records, got %d", h.count())
	}
}
