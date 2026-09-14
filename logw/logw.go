// Package logw wraps a velum.DatabaseWrapper and emits a structured
// log/slog entry for every database call. It is a drop-in replacement:
// pass the Wrapper wherever a velum.DatabaseWrapper is expected.
//
// Slow queries are logged at WARN level when their duration exceeds a
// configurable threshold; all other calls are logged at DEBUG level.
// Argument values and sampled result rows can be included optionally.
package logw

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/axkit/velum"
)

// Compile-time check that *Wrapper satisfies velum.DatabaseWrapper.
var _ velum.DatabaseWrapper = (*Wrapper)(nil)

// loggerKey is the context key used by WithLogger.
type loggerKey struct{}

// WithLogger injects a request-scoped logger into ctx. The Wrapper uses it
// instead of its base logger for any query executed with that ctx. Use it to
// attach per-request fields such as a request or trace ID:
//
//	ctx = logw.WithLogger(ctx, logger.With("request_id", reqID))
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, l)
}

// Option configures a Wrapper.
type Option func(*Wrapper)

// WithSlowQueryThreshold sets the duration above which a query is logged at
// WARN level instead of DEBUG. Zero (the default) logs everything at DEBUG.
func WithSlowQueryThreshold(d time.Duration) Option {
	return func(w *Wrapper) { w.slowThreshold = d }
}

// WithLogArgs enables logging of query argument values. Disabled by default
// because arguments may contain sensitive data such as passwords or PII.
func WithLogArgs(b bool) Option {
	return func(w *Wrapper) { w.logArgs = b }
}

// WithLogRows sets the maximum number of result rows to include in the log
// entry. For QueryRowContext the single result row is captured when n >= 1.
// For QueryContext up to n rows are captured and emitted at Close time as
// rows_sample. Zero (the default) disables result value logging.
func WithLogRows(n int) Option {
	return func(w *Wrapper) { w.logRows = n }
}

// WithBaseLogger sets the fallback logger used when no per-request logger is
// found in the context. If not provided, slog.Default() is used.
func WithBaseLogger(l *slog.Logger) Option {
	return func(w *Wrapper) { w.log = l }
}

// WithLoggerFromContext sets a function that extracts a request-scoped logger
// from the context. Use it when your middleware already stores a logger under
// its own context key (e.g. a zerolog or zap logger wrapped in slog):
//
//	lw := logw.New(dbw,
//	    logw.WithLoggerFromContext(func(ctx context.Context) *slog.Logger {
//	        l, _ := ctx.Value(myKey{}).(*slog.Logger)
//	        return l // nil → logw falls back to WithLogger key, then base logger
//	    }),
//	)
//
// Resolution order: WithLoggerFromContext → WithLogger(ctx) → WithBaseLogger → slog.Default().
func WithLoggerFromContext(fn func(context.Context) *slog.Logger) Option {
	return func(w *Wrapper) { w.loggerFromCtx = fn }
}

// Wrapper wraps a velum.DatabaseWrapper and logs every query execution.
//
// For ExecContext a single duration field is emitted. For QueryRowContext two
// fields are emitted: the time until the underlying call returned (query_dur)
// and the time the caller spent inside Scan (scan_dur). For QueryContext two
// fields are emitted at Close time: the time until the first row was available
// (first_row_dur) and the time to consume all rows (scan_dur). Transaction
// lifetime is summarised in a single entry on Commit or Rollback.
//
// Every log entry also carries a tx_id field when the query is executed
// inside a transaction, making it easy to correlate per-query entries with the
// transaction summary.
type Wrapper struct {
	dw            velum.DatabaseWrapper
	log           *slog.Logger
	slowThreshold time.Duration
	logArgs       bool
	logRows       int
	loggerFromCtx func(context.Context) *slog.Logger
}

// New wraps dw with query logging. The logger is optional: by default
// slog.Default() is used as a fallback when no per-request logger is found in
// the context. Override it with WithBaseLogger; configure per-request logger
// lookup with WithLoggerFromContext or WithLogger.
func New(dw velum.DatabaseWrapper, opts ...Option) *Wrapper {
	w := &Wrapper{dw: dw, log: slog.Default()}
	for _, o := range opts {
		o(w)
	}
	return w
}

// IsNotFound delegates to the underlying DatabaseWrapper.
func (w *Wrapper) IsNotFound(err error) bool { return w.dw.IsNotFound(err) }

// IsRetryable delegates to the underlying DatabaseWrapper. It reports false
// when that wrapper cannot classify errors.
func (w *Wrapper) IsRetryable(err error) bool { return velum.IsRetryable(w.dw, err) }

// Begin starts a transaction with the server default isolation level. The
// returned Transaction wraps each query with per-query logging and emits a
// summary entry on Commit or Rollback.
func (w *Wrapper) Begin(ctx context.Context) (velum.Transaction, error) {
	tx, err := w.dw.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return w.wrapTx(tx, velum.IsoDefault), nil
}

// BeginTx starts a transaction with opts. It returns
// velum.ErrTxOptionsUnsupported when the underlying wrapper cannot apply them,
// so that a requested isolation level is never silently dropped by logging.
func (w *Wrapper) BeginTx(ctx context.Context, opts velum.TxOptions) (velum.Transaction, error) {
	tx, err := velum.BeginTx(ctx, w.dw, opts)
	if err != nil {
		return nil, err
	}
	return w.wrapTx(tx, opts.IsoLevel), nil
}

// wrapTx decorates tx with per-query logging and a transaction summary.
func (w *Wrapper) wrapTx(tx velum.Transaction, iso velum.IsoLevel) *loggableTx {
	return &loggableTx{
		Transaction: tx,
		w:           w,
		start:       time.Now(),
		txID:        rand.Uint64(),
		iso:         iso,
	}
}

// InTx executes fn in a transaction. Delegates to Begin so the transaction
// summary is logged via loggableTx.
func (w *Wrapper) InTx(ctx context.Context, fn func(velum.Transaction) error) error {
	tx, err := w.Begin(ctx)
	if err != nil {
		return err
	}
	return runInTx(ctx, tx, fn)
}

// InTxWith executes fn in a transaction started with opts. Delegates to
// BeginTx, so an underlying wrapper without options support fails loudly.
func (w *Wrapper) InTxWith(ctx context.Context, opts velum.TxOptions, fn func(velum.Transaction) error) error {
	tx, err := w.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	return runInTx(ctx, tx, fn)
}

// runInTx commits tx when fn succeeds and rolls it back otherwise. The named
// result is what makes the deferred func see fn's error.
func runInTx(ctx context.Context, tx velum.Transaction, fn func(velum.Transaction) error) (err error) {
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()
	return fn(tx)
}

// ExecContext executes a statement and logs its duration.
func (w *Wrapper) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	start := time.Now()
	res, err := w.dw.ExecContext(ctx, sql, args...)
	w.emitExec(ctx, sql, args, 0, time.Since(start), res, err)
	return res, err
}

// QueryRowContext executes a single-row query. The returned Row logs both the
// query duration and the scan duration when its Scan method is called.
func (w *Wrapper) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	start := time.Now()
	row := w.dw.QueryRowContext(ctx, sql, args...)
	return &loggableRow{
		Row:      row,
		w:        w,
		ctx:      ctx,
		sql:      sql,
		args:     args,
		queryDur: time.Since(start),
	}
}

// QueryContext executes a multi-row query. The returned Rows emits a single
// log entry on Close containing first_row_dur, scan_dur, and the row count.
func (w *Wrapper) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	start := time.Now()
	rows, err := w.dw.QueryContext(ctx, sql, args...)
	if err != nil {
		w.emitQuery(ctx, sql, args, 0, time.Since(start), 0, 0, nil, nil, err)
		return nil, err
	}
	return &loggableRows{
		Rows:       rows,
		w:          w,
		ctx:        ctx,
		sql:        sql,
		args:       args,
		queryStart: start,
	}, nil
}

// --- emit helpers ---

func (w *Wrapper) loggerFrom(ctx context.Context) *slog.Logger {
	if w.loggerFromCtx != nil {
		if l := w.loggerFromCtx(ctx); l != nil {
			return l
		}
	}
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok {
		return l
	}
	return w.log
}

func (w *Wrapper) level(dur time.Duration) slog.Level {
	if w.slowThreshold > 0 && dur >= w.slowThreshold {
		return slog.LevelWarn
	}
	return slog.LevelDebug
}

// columnsNamer is an optional interface that a velum.Rows implementation may
// satisfy to expose column names. Both sqlw.RowsWrapper (via embedded sql.Rows)
// and pgxw.RowsWrapper (via an explicit method) satisfy it automatically.
type columnsNamer interface {
	Columns() ([]string, error)
}

// namedGroupAttr builds a slog.Group named key with one child per value keyed
// by its placeholder position ($1, $2, …).
func namedGroupAttr(key string, vals []any) slog.Attr {
	members := make([]any, len(vals))
	for i, v := range vals {
		members[i] = slog.Any(fmt.Sprintf("$%d", i+1), v)
	}
	return slog.Group(key, members...)
}

// colName returns the column name at index i, falling back to "$<i+1>" when
// cols is nil or too short.
func colName(cols []string, i int) string {
	if i < len(cols) && cols[i] != "" {
		return cols[i]
	}
	return fmt.Sprintf("$%d", i+1)
}

// rowValuesAttr builds a slog.Group named key with one child per value, using
// column names when available and positional placeholders as a fallback.
func rowValuesAttr(key string, vals []any, cols []string) slog.Attr {
	members := make([]any, len(vals))
	for i, v := range vals {
		members[i] = slog.Any(colName(cols, i), v)
	}
	return slog.Group(key, members...)
}

// appendArgsAttrs adds args_count (always, when args are present) and, when
// logArgs is true, a structured args group with per-placeholder keys.
func (w *Wrapper) appendArgsAttrs(attrs []slog.Attr, args []any) []slog.Attr {
	if len(args) == 0 {
		return attrs
	}
	attrs = append(attrs, slog.Int("args_count", len(args)))
	if w.logArgs {
		attrs = append(attrs, namedGroupAttr("args", args))
	}
	return attrs
}

// captureRow reads the current values from scan destinations by dereferencing
// each pointer. Called after a successful Scan to record what was returned.
func captureRow(dest []any) []any {
	vals := make([]any, len(dest))
	for i, d := range dest {
		v := reflect.ValueOf(d)
		if v.IsValid() && v.Kind() == reflect.Ptr && !v.IsNil() {
			vals[i] = v.Elem().Interface()
		} else {
			vals[i] = d
		}
	}
	return vals
}

func (w *Wrapper) emitExec(ctx context.Context, sql string, args []any, txID uint64, dur time.Duration, res velum.Result, err error) {
	attrs := []slog.Attr{
		slog.String("op", "exec"),
		slog.String("sql", sql),
		slog.Duration("dur", dur),
	}
	if txID != 0 {
		attrs = append(attrs, slog.Uint64("tx_id", txID))
	}
	if res != nil {
		if n, rerr := res.RowsAffected(); rerr == nil {
			attrs = append(attrs, slog.Int64("rows_affected", n))
		}
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	attrs = w.appendArgsAttrs(attrs, args)
	w.loggerFrom(ctx).LogAttrs(ctx, w.level(dur), "velum", attrs...)
}

func (w *Wrapper) emitRow(ctx context.Context, sql string, args []any, txID uint64, queryDur, scanDur time.Duration, result []any, err error) {
	attrs := []slog.Attr{
		slog.String("op", "query_row"),
		slog.String("sql", sql),
		slog.Duration("query_dur", queryDur),
		slog.Duration("scan_dur", scanDur),
	}
	if txID != 0 {
		attrs = append(attrs, slog.Uint64("tx_id", txID))
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	attrs = w.appendArgsAttrs(attrs, args)
	if len(result) > 0 {
		attrs = append(attrs, namedGroupAttr("result", result))
	}
	w.loggerFrom(ctx).LogAttrs(ctx, w.level(queryDur+scanDur), "velum", attrs...)
}

func (w *Wrapper) emitQuery(ctx context.Context, sql string, args []any, txID uint64, firstRowDur, scanDur time.Duration, rows int, rowsSample [][]any, cols []string, err error) {
	attrs := []slog.Attr{
		slog.String("op", "query"),
		slog.String("sql", sql),
		slog.Duration("first_row_dur", firstRowDur),
		slog.Duration("scan_dur", scanDur),
		slog.Int("rows", rows),
	}
	if txID != 0 {
		attrs = append(attrs, slog.Uint64("tx_id", txID))
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	attrs = w.appendArgsAttrs(attrs, args)
	if n := len(rowsSample); n > 0 {
		if n < rows {
			attrs = append(attrs, slog.Int("rows_sampled", n))
		}
		for i, row := range rowsSample {
			attrs = append(attrs, rowValuesAttr(fmt.Sprintf("row%d", i+1), row, cols))
		}
	}
	w.loggerFrom(ctx).LogAttrs(ctx, w.level(firstRowDur+scanDur), "velum", attrs...)
}

// --- loggableRow ---

type loggableRow struct {
	velum.Row
	w        *Wrapper
	ctx      context.Context
	sql      string
	args     []any
	queryDur time.Duration
	txID     uint64
}

func (r *loggableRow) Scan(dest ...any) error {
	start := time.Now()
	err := r.Row.Scan(dest...)
	scanDur := time.Since(start)
	var result []any
	if err == nil && r.w.logRows > 0 {
		result = captureRow(dest)
	}
	r.w.emitRow(r.ctx, r.sql, r.args, r.txID, r.queryDur, scanDur, result, err)
	return err
}

// --- loggableRows ---

type loggableRows struct {
	velum.Rows
	w            *Wrapper
	ctx          context.Context
	sql          string
	args         []any
	queryStart   time.Time
	firstRowAt   time.Time
	firstSeen    bool
	rowCount     int
	txID         uint64
	capturedRows [][]any  // up to w.logRows rows, nil when logRows == 0
	cols         []string // lazily populated from columnsNamer on first Scan
}

func (r *loggableRows) Next() bool {
	ok := r.Rows.Next()
	if ok {
		if !r.firstSeen {
			r.firstRowAt = time.Now()
			r.firstSeen = true
		}
		r.rowCount++
	}
	return ok
}

func (r *loggableRows) Scan(dest ...any) error {
	err := r.Rows.Scan(dest...)
	if err == nil && r.w.logRows > 0 && len(r.capturedRows) < r.w.logRows {
		if r.cols == nil {
			if cn, ok := r.Rows.(columnsNamer); ok {
				r.cols, _ = cn.Columns()
			}
		}
		r.capturedRows = append(r.capturedRows, captureRow(dest))
	}
	return err
}

func (r *loggableRows) Close() error {
	closeAt := time.Now()
	iterErr := r.Rows.Err() // capture any mid-iteration error before Close
	closeErr := r.Rows.Close()

	err := iterErr
	if err == nil {
		err = closeErr
	}

	var firstRowDur, scanDur time.Duration
	if r.firstSeen {
		firstRowDur = r.firstRowAt.Sub(r.queryStart)
		scanDur = closeAt.Sub(r.firstRowAt)
	} else {
		// no rows returned: the full wait is "time to (non-existent) first row"
		firstRowDur = closeAt.Sub(r.queryStart)
	}

	r.w.emitQuery(r.ctx, r.sql, r.args, r.txID, firstRowDur, scanDur, r.rowCount, r.capturedRows, r.cols, err)
	return closeErr
}

// --- loggableTx ---

type loggableTx struct {
	velum.Transaction
	w      *Wrapper
	start  time.Time
	txID   uint64
	nQuery int
	iso    velum.IsoLevel // logged in the summary when not velum.IsoDefault
}

func (t *loggableTx) ExecContext(ctx context.Context, sql string, args ...any) (velum.Result, error) {
	t.nQuery++
	start := time.Now()
	res, err := t.Transaction.ExecContext(ctx, sql, args...)
	t.w.emitExec(ctx, sql, args, t.txID, time.Since(start), res, err)
	return res, err
}

func (t *loggableTx) QueryRowContext(ctx context.Context, sql string, args ...any) velum.Row {
	t.nQuery++
	start := time.Now()
	row := t.Transaction.QueryRowContext(ctx, sql, args...)
	return &loggableRow{
		Row:      row,
		w:        t.w,
		ctx:      ctx,
		sql:      sql,
		args:     args,
		queryDur: time.Since(start),
		txID:     t.txID,
	}
}

func (t *loggableTx) QueryContext(ctx context.Context, sql string, args ...any) (velum.Rows, error) {
	t.nQuery++
	start := time.Now()
	rows, err := t.Transaction.QueryContext(ctx, sql, args...)
	if err != nil {
		t.w.emitQuery(ctx, sql, args, t.txID, time.Since(start), 0, 0, nil, nil, err)
		return nil, err
	}
	return &loggableRows{
		Rows:       rows,
		w:          t.w,
		ctx:        ctx,
		sql:        sql,
		args:       args,
		queryStart: start,
		txID:       t.txID,
	}, nil
}

func (t *loggableTx) Commit(ctx context.Context) error {
	err := t.Transaction.Commit(ctx)
	t.emitSummary(ctx, "committed", err)
	return err
}

func (t *loggableTx) Rollback(ctx context.Context) error {
	err := t.Transaction.Rollback(ctx)
	t.emitSummary(ctx, "rolled_back", err)
	return err
}

func (t *loggableTx) emitSummary(ctx context.Context, outcome string, err error) {
	dur := time.Since(t.start)
	attrs := []slog.Attr{
		slog.String("op", "tx"),
		slog.Uint64("tx_id", t.txID),
		slog.String("outcome", outcome),
		slog.Duration("total_dur", dur),
		slog.Int("queries", t.nQuery),
	}
	if t.iso != velum.IsoDefault {
		attrs = append(attrs, slog.String("isolation", t.iso.String()))
	}
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	t.w.loggerFrom(ctx).LogAttrs(ctx, t.w.level(dur), "velum", attrs...)
}
