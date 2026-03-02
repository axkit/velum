package logw_test

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/axkit/velum"
	"github.com/axkit/velum/logw"
)

// ExampleNew demonstrates Wrapper setup. Replace the dw variable with
// pgxw.NewDatabaseWrapper(pool) or sqlw.NewDatabaseWrapper(db) and pass
// lw wherever a velum.DatabaseWrapper is expected.
func ExampleNew() {
	var dw velum.DatabaseWrapper // e.g. pgxw.NewDatabaseWrapper(pool)

	lw := logw.New(dw,
		logw.WithSlowQueryThreshold(100*time.Millisecond), // WARN when exceeded
		logw.WithLogRows(5),                               // sample up to 5 result rows
	)

	// lw satisfies velum.DatabaseWrapper — pass it to tbl / ds methods.
	_ = lw
}

// ExampleWithLogger shows how HTTP middleware can attach a request-scoped
// logger so that every query executed within the request carries common
// fields such as request_id or trace_id.
func ExampleWithLogger() {
	var dw velum.DatabaseWrapper
	lw := logw.New(dw)

	mux := http.NewServeMux()
	mux.Handle("/", loggingMiddleware(lw,
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// ctx already contains the enriched logger — pass it to tbl / ds.
			_ = r.Context()
		}),
	))
	_ = mux
}

func loggingMiddleware(lw *logw.Wrapper, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqLogger := slog.Default().With(
			"request_id", r.Header.Get("X-Request-ID"),
			"trace_id", r.Header.Get("X-Trace-ID"),
		)
		ctx := logw.WithLogger(r.Context(), reqLogger)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ExampleWithLoggerFromContext shows how to reuse a logger that your own
// middleware already placed in the context under a custom key. Returning nil
// from the function falls back to the logw.WithLogger key and then to
// WithBaseLogger.
func ExampleWithLoggerFromContext() {
	type ctxKey struct{}

	var dw velum.DatabaseWrapper

	lw := logw.New(dw,
		logw.WithLoggerFromContext(func(ctx context.Context) *slog.Logger {
			l, _ := ctx.Value(ctxKey{}).(*slog.Logger)
			return l
		}),
	)
	_ = lw
}
