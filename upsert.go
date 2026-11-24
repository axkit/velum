package velum

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// ConflictAction is what an upsert does when the inserted row conflicts with
// an existing one.
type ConflictAction uint8

const (
	// DoNothing generates ON CONFLICT … DO NOTHING.
	DoNothing ConflictAction = iota
	// DoUpdate generates ON CONFLICT … DO UPDATE SET ….
	DoUpdate
)

// OnConflict describes the ON CONFLICT part of an upsert. Every field is a
// fixed SQL constant of the caller; values go only through $N arguments.
// The struct is comparable and is part of the command cache key, so build it
// once (a package-level variable), never with fmt.Sprintf.
//
// In UpdateExpr and UpdateWhere the existing row is t (the table is always
// aliased as t) and the proposed row is EXCLUDED.
type OnConflict struct {
	// Target is the text after ON CONFLICT, verbatim:
	//   "(user_id, hint_key)"
	//   "(workspace_id, lower(name))"
	//   "(source_event_id) WHERE source_event_id IS NOT NULL"
	//   "ON CONSTRAINT pk_user_document_contexts"
	// Empty means no target, which PostgreSQL allows only with DoNothing.
	Target string

	// Action is DoNothing (the zero value) or DoUpdate.
	Action ConflictAction

	// UpdateScope (DoUpdate): every column of this scope gets
	// col=EXCLUDED.col. The primary key and system columns never do. Every
	// column of the scope must be in the insert scope.
	UpdateScope Scope

	// UpdateExpr (DoUpdate): extra assignments "col = expr, …" appended after
	// UpdateScope, for what EXCLUDED.col cannot express: CASE, now(),
	// constants, t.count + 1. It must not assign the version and update
	// system columns (velum sets them) nor a column of UpdateScope. $1… in
	// it are the caller's args.
	UpdateExpr string

	// UpdateWhere (DoUpdate): the condition of the update with the keyword,
	// e.g. "WHERE t.import_id <> EXCLUDED.import_id". It shares $N numbering
	// with UpdateExpr. When it is false the row is not changed at all.
	UpdateWhere string
}

// OnConflictDoNothing is "ON CONFLICT DO NOTHING" without a target.
var OnConflictDoNothing = OnConflict{}

// UpsertOutcome tells what an upsert with RETURNING did to the row.
type UpsertOutcome uint8

const (
	// UpsertSkipped: no row was written — DO NOTHING on a conflict, or
	// UpdateWhere was false.
	UpsertSkipped UpsertOutcome = iota
	// UpsertInserted: a new row was inserted.
	UpsertInserted
	// UpsertUpdated: the existing row was updated.
	UpsertUpdated
)

// String returns the outcome name.
func (o UpsertOutcome) String() string {
	switch o {
	case UpsertSkipped:
		return "skipped"
	case UpsertInserted:
		return "inserted"
	case UpsertUpdated:
		return "updated"
	}
	return "UpsertOutcome(" + strconv.Itoa(int(o)) + ")"
}

// ErrInvalidUpsert is wrapped by every *UpsertConfigError.
var ErrInvalidUpsert = errors.New("velum: invalid upsert")

// UpsertConfigError reports an OnConflict or a scope that cannot make a valid
// upsert. It is returned by Table.UpsertCommand and
// Table.UpsertReturningCommand; the Must variants panic with it.
type UpsertConfigError struct {
	Table  string
	Caller string // file:line that asked for the command
	Reason string
	// SQL is the statement the command would have run. It holds only $N
	// placeholders, no values, so it is safe to log.
	SQL      string
	Scope    Scope
	RetScope Scope
	Conflict OnConflict
}

// Error returns a one-line description: place, table, reason and SQL.
func (e *UpsertConfigError) Error() string {
	return "velum: invalid upsert at " + e.Caller + ": table " + e.Table + ": " + e.Reason + "; sql: " + e.SQL
}

// Unwrap returns ErrInvalidUpsert.
func (e *UpsertConfigError) Unwrap() error { return ErrInvalidUpsert }

// UpsertCommand is a prepared INSERT … ON CONFLICT … RETURNING statement.
type UpsertCommand[T any] struct {
	Command[T]
	rets []int
}

// QueryRow executes the upsert for row and returns the RETURNING columns as a
// newly allocated *T together with the outcome. args are bound to the $N
// placeholders of UpdateExpr and UpdateWhere.
//
// When no row is written it returns (nil, UpsertSkipped, nil): a missing row
// is not an error. It needs a QueryExecuter (not QueryRowExecuter) to tell a
// missing row apart without a driver-specific "no rows" error.
func (c *UpsertCommand[T]) QueryRow(ctx context.Context, q QueryExecuter, row *T, args ...any) (*T, UpsertOutcome, error) {
	ptrs := c.sfpe.StructFieldPtrs(row, c.cpos)
	defer c.sfpe.Release(ptrs)

	joinedPtrs := *ptrs
	if len(args) > 0 {
		joinedPtrs = append(joinedPtrs, args...)
	}

	rows, err := q.QueryContext(ctx, c.sql, joinedPtrs...)
	if err != nil {
		return nil, UpsertSkipped, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, UpsertSkipped, rows.Err()
	}

	var (
		res      T
		inserted bool
	)
	rets := c.sfpe.StructFieldPtrs(&res, c.rets)
	defer c.sfpe.Release(rets)

	dst := append(*rets, &inserted)
	if err := rows.Scan(dst...); err != nil {
		return nil, UpsertSkipped, err
	}
	if err := rows.Close(); err != nil {
		return nil, UpsertSkipped, err
	}
	if err := rows.Err(); err != nil {
		return nil, UpsertSkipped, err
	}

	if inserted {
		return &res, UpsertInserted, nil
	}
	return &res, UpsertUpdated, nil
}

// UpsertCommand returns the prepared INSERT … ON CONFLICT statement without
// RETURNING for the columns of scope. Build it once at startup and run it with
// Exec: RowsAffected is 1 when the row was inserted or updated, 0 when it was
// skipped.
//
//	var hintOnce, err = hints.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(user_id, hint_key)"})
//	// INSERT INTO user_seen_hints AS t (user_id,hint_key) VALUES ($1,$2) ON CONFLICT (user_id, hint_key) DO NOTHING
func (t *Table[T]) UpsertCommand(scope Scope, oc OnConflict) (Command[T], error) {
	cmd, err := t.cc.upsert(scope, EmptyScope, oc, false)
	return cmd.Command, err
}

// MustUpsertCommand is like UpsertCommand but panics with the
// *UpsertConfigError. Use it for package-level variables.
func (t *Table[T]) MustUpsertCommand(scope Scope, oc OnConflict) Command[T] {
	cmd, err := t.cc.upsert(scope, EmptyScope, oc, false)
	if err != nil {
		panic(err)
	}
	return cmd.Command
}

// UpsertReturningCommand returns the prepared INSERT … ON CONFLICT …
// RETURNING statement: scope selects the inserted columns, retScope the
// returned ones (the primary key always comes first when there is one).
// Build it once at startup and run it with QueryRow.
func (t *Table[T]) UpsertReturningCommand(scope, retScope Scope, oc OnConflict) (UpsertCommand[T], error) {
	return t.cc.upsert(scope, retScope, oc, true)
}

// MustUpsertReturningCommand is like UpsertReturningCommand but panics with
// the *UpsertConfigError. Use it for package-level variables.
func (t *Table[T]) MustUpsertReturningCommand(scope, retScope Scope, oc OnConflict) UpsertCommand[T] {
	cmd, err := t.cc.upsert(scope, retScope, oc, true)
	if err != nil {
		panic(err)
	}
	return cmd
}

// callerLine returns file:line of the function skip frames above it.
func callerLine(skip int) string {
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		return "unknown"
	}
	return file + ":" + strconv.Itoa(line)
}

type upsertKey struct {
	scope     Scope
	retScope  Scope
	oc        OnConflict
	returning bool
}

// upsert returns the cached command or builds it. It must be called directly
// by the exported Table methods: on error the caller's file:line is taken
// three frames up (callerLine, upsert, the Table method).
func (cc *CommandContanier[T]) upsert(scope, retScope Scope, oc OnConflict, returning bool) (UpsertCommand[T], error) {
	key := upsertKey{scope: scope, retScope: retScope, oc: oc, returning: returning}
	cc.mux.RLock()
	cmd, ok := cc.ups[key]
	cc.mux.RUnlock()
	if ok {
		return cmd, nil
	}

	cmd, reason := buildUpsert(cc.t, scope, retScope, oc, returning)
	if reason != "" {
		return UpsertCommand[T]{}, &UpsertConfigError{
			Table:    cc.t.Name(),
			Caller:   callerLine(3),
			Reason:   reason,
			SQL:      cmd.sql,
			Scope:    scope,
			RetScope: retScope,
			Conflict: oc,
		}
	}

	cc.mux.Lock()
	cc.ups[key] = cmd
	cc.mux.Unlock()
	return cmd, nil
}

// buildUpsert builds the statement and checks it. The SQL text is built even
// when the configuration is invalid, so that the error can show it; reason is
// empty when the command is valid.
func buildUpsert[T any](t *Table[T], scope, retScope Scope, oc OnConflict, returning bool) (cmd UpsertCommand[T], reason string) {
	var reasons []string
	fail := func(format string, a ...any) {
		reasons = append(reasons, fmt.Sprintf(format, a...))
	}

	ins := parseUserScopes(scope, VersionField, InsertScope)
	cols := newClause(ctColsCSV, t, ins)
	vals := newClause(ctArgsInsert, t, ins)

	inserted := make(map[int]bool, len(cols.cpos))
	for _, p := range cols.cpos {
		inserted[p] = true
	}

	sql := "INSERT INTO " + t.Name() + " AS t (" + cols.text + ") VALUES (" + vals.text + ") ON CONFLICT"
	if oc.Target != "" {
		sql += " " + oc.Target
	}

	cpos := append([]int(nil), vals.cpos...)

	switch oc.Action {
	case DoNothing:
		sql += " DO NOTHING"
		if oc.UpdateScope != EmptyScope || oc.UpdateExpr != "" || oc.UpdateWhere != "" {
			fail("DoNothing with UpdateScope, UpdateExpr or UpdateWhere: set Action to DoUpdate or clear them")
		}
	case DoUpdate:
		if oc.Target == "" {
			fail("DoUpdate needs a Target: PostgreSQL requires a conflict target or constraint name for DO UPDATE")
		}

		var set []string
		updated := make(map[string]bool) // columns of UpdateScope

		if oc.UpdateScope != EmptyScope {
			us := parseUserScopes(oc.UpdateScope)
			columns := t.Columns()
			for i := range columns {
				col := &columns[i]
				if (t.pk != nil && i == t.pk.Pos) || col.IsSystem() {
					continue
				}
				if !us.all && !isColumnInScopes(col, us) {
					continue
				}
				if !inserted[i] {
					fail("UpdateScope %q: column %s is not inserted by scope %q (inserts %s), EXCLUDED.%s would be NULL or the default",
						oc.UpdateScope, col.Name, scope, cols.text, col.Name)
				}
				set = append(set, col.Name+"=EXCLUDED."+col.Name)
				updated[strings.ToLower(col.Name)] = true
			}
		}

		system := make(map[string]bool) // columns velum sets itself
		if v := t.sysCols.version; v != nil {
			set = append(set, v.Name+"=t."+v.Name+"+1")
			system[strings.ToLower(v.Name)] = true
		}
		next := len(vals.cpos) + 1
		for _, u := range t.sysCols.updated {
			if system[strings.ToLower(u.Name)] {
				continue
			}
			set = append(set, u.Name+"="+t.FormatArg(next))
			cpos = append(cpos, u.Pos)
			system[strings.ToLower(u.Name)] = true
			next++
		}

		if oc.UpdateExpr != "" {
			for _, target := range assignmentTargets(oc.UpdateExpr) {
				switch {
				case system[target]:
					fail("UpdateExpr assigns %s: velum sets the version and update system columns itself", target)
				case updated[target]:
					fail("UpdateExpr assigns %s, which UpdateScope %q already sets to EXCLUDED.%s", target, oc.UpdateScope, target)
				}
			}
			set = append(set, ShiftParamPositions(oc.UpdateExpr, next))
		}

		if len(set) == 0 {
			fail("DoUpdate has nothing to set: give UpdateScope or UpdateExpr")
		}
		sql += " DO UPDATE SET " + strings.Join(set, ",")
		if oc.UpdateWhere != "" {
			sql += " " + ShiftParamPositions(oc.UpdateWhere, next)
		}
	default:
		fail("unknown ConflictAction %d", oc.Action)
	}

	cmd.cpos = cpos
	cmd.sfpe = t.cc.sfpe

	if returning {
		rets := newClause(ctColsCSV, t, parseUserScopes(retScope))
		sql += " RETURNING " + csvConcat(rets.text, "(t.xmax = 0)")
		cmd.rets = rets.cpos
	}

	cmd.sql = sql
	return cmd, strings.Join(reasons, "; ")
}

// assignmentTargets returns the column names assigned by a SET list such as
// `a = 1, "B" = f(x, y), (c, d) = (1, 2)`: lower-cased unless quoted. Commas
// and "=" inside parentheses and quotes are not separators.
func assignmentTargets(set string) []string {
	var targets []string
	for _, item := range splitTopLevel(set, ',') {
		left, _, ok := cutTopLevel(item, '=')
		if !ok {
			continue
		}
		left = strings.TrimSpace(left)
		if strings.HasPrefix(left, "(") && strings.HasSuffix(left, ")") {
			for _, name := range splitTopLevel(left[1:len(left)-1], ',') {
				targets = append(targets, columnName(name))
			}
			continue
		}
		targets = append(targets, columnName(left))
	}
	return targets
}

// columnName normalizes a SET target: drops a subscript or field selector
// (col[1], col.f) and folds an unquoted name to lower case.
func columnName(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, `"`) {
		if end := strings.Index(s[1:], `"`); end >= 0 {
			return s[1 : end+1]
		}
		return s
	}
	if i := strings.IndexAny(s, "[. \t\n"); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(s)
}

// splitTopLevel splits s at sep outside parentheses and quotes.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	for {
		before, after, ok := cutTopLevel(s, sep)
		parts = append(parts, before)
		if !ok {
			return parts
		}
		s = after
	}
}

// cutTopLevel cuts s at the first sep outside parentheses and single or
// double quotes.
func cutTopLevel(s string, sep byte) (before, after string, found bool) {
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(' || c == '[':
			depth++
		case c == ')' || c == ']':
			depth--
		case c == sep && depth == 0:
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}
