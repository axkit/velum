package velum

import (
	"strings"
	"sync"
)

// CommandTypeEnum identifies the DML statement type stored in a CommandContanier
// cache slot.
type CommandTypeEnum int

const (
	Insert CommandTypeEnum = iota
	Update
	Delete
	// CommandTypeEnumMax_ is a sentinel used to size the cache arrays; it is
	// not a valid statement type.
	CommandTypeEnumMax_
)

// SingleScopeKey is the cache key for statements that take one scope and one
// clause string (SELECT, INSERT, plain UPDATE, plain DELETE).
type SingleScopeKey struct {
	scope   Scope
	clauses string
}

// DoubleScopeKey is the cache key for statements that take two scopes (argument
// scope and return scope) plus a clause string (INSERT/UPDATE/DELETE RETURNING).
type DoubleScopeKey struct {
	argScope Scope
	retScope Scope
	clauses  string
}

// FunctionalCommandEnum identifies the type of scalar-result query cached in
// CommandContanier.fn.
type FunctionalCommandEnum int

const (
	// Exist caches a SELECT EXISTS(...) query.
	Exist FunctionalCommandEnum = iota
	// ExistByPK caches a SELECT EXISTS(... WHERE pk=$1) query.
	ExistByPK
	// Count caches a SELECT COUNT(*) query.
	Count
)

// FuncCommandKey is the cache key for scalar-result queries (EXISTS, COUNT).
type FuncCommandKey struct {
	typ     FunctionalCommandEnum
	clauses string
}

// CommandContanier builds and caches SQL commands for a single Table.
// Each unique (scope, clause) combination is built once and stored in the
// appropriate map; subsequent calls with the same key return the cached command.
//
// The cache is safe for concurrent use via an internal sync.RWMutex.
//
// Important: the caches grow by one entry per unique clause string and are
// never evicted. Always use fixed, parameterized clause strings to keep the
// cache bounded. See the README for details.
type CommandContanier[T any] struct {
	t            *Table[T]
	pkWhereCause string
	sfpe         StructFieldPtrExtractor[T]
	clause       map[scopeKey]clause
	mux          sync.RWMutex
	fn           map[FuncCommandKey]FunctionalCommand[T]
	sel          map[SingleScopeKey]SelectCommand[T]
	cmd          [CommandTypeEnumMax_]map[SingleScopeKey]Command[T]
	retCmd       [CommandTypeEnumMax_]map[DoubleScopeKey]ReturningCommand[T]
}

// NewCommandContainer creates a CommandContanier for the given table. It is
// called automatically by NewTable and rarely needs to be invoked directly.
func NewCommandContainer[T any](
	t *Table[T],
	sfpe StructFieldPtrExtractor[T],
	sc map[scopeKey]clause,
	anb ArgFormatter,
) *CommandContanier[T] {

	cc := CommandContanier[T]{
		t:      t,
		clause: sc,
		sfpe:   sfpe,
		sel:    make(map[SingleScopeKey]SelectCommand[T]),
		fn:     make(map[FuncCommandKey]FunctionalCommand[T]),
	}

	for i := range CommandTypeEnumMax_ {
		cc.cmd[i] = make(map[SingleScopeKey]Command[T])
		cc.retCmd[i] = make(map[DoubleScopeKey]ReturningCommand[T])
	}

	if pk := cc.t.PK(); pk != nil {
		cc.pkWhereCause = " WHERE " + pk.Name + "=" + anb(1)
	}

	return &cc
}

// Select returns a cached SelectCommand for the given scope and WHERE/ORDER/LIMIT
// clause string. The command is built on the first call and reused thereafter.
func (cc *CommandContanier[T]) Select(scope Scope, clauses string) SelectCommand[T] {
	key := SingleScopeKey{scope: scope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.sel[key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildSelect(cc.t, scope, clauses)

	cc.mux.Lock()
	cc.sel[key] = cmd
	cc.mux.Unlock()
	return cmd
}

func buildSelect[T any](t *Table[T], scope Scope, clauses string) SelectCommand[T] {
	scopes := parseUserScopes(scope)
	cols := newClause(ctColsPrefixedCSV, t, scopes)
	sql := "SELECT " + cols.text + " FROM " + t.Name() + " t " + clauses
	cmd := SelectCommand[T]{
		sql:  sql,
		cpos: cols.cpos,
		sfpe: t.cc.sfpe,
	}
	return cmd
}

// Insert returns a cached Command for an INSERT statement that covers the
// given scope. The statement does not include a RETURNING clause.
func (cc *CommandContanier[T]) Insert(scope Scope) Command[T] {
	key := SingleScopeKey{scope: scope, clauses: ""}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Insert][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildInsert(cc.t, scope)

	cc.mux.Lock()
	cc.cmd[Insert][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func buildInsert[T any](t *Table[T], scope Scope) Command[T] {
	scopes := parseUserScopes(scope)
	cols := newClause(ctColsCSV, t, scopes)
	vals := newClause(ctArgsInsert, t, scopes)
	sql := "INSERT INTO " + t.Name() +
		" (" + cols.text + ") VALUES (" + vals.text + ")"
	return Command[T]{
		sql:  sql,
		cpos: vals.cpos,
		sfpe: t.cc.sfpe,
	}
}

// InsertReturning returns a cached ReturningCommand for an INSERT … RETURNING
// statement. argScope selects the columns to write; retScope selects the
// columns to read back from RETURNING.
func (cc *CommandContanier[T]) InsertReturning(argScope, retScope Scope) ReturningCommand[T] {
	key := DoubleScopeKey{argScope: argScope, retScope: retScope, clauses: ""}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Insert][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildInsertReturning(cc.t, argScope, retScope)
	cc.mux.Lock()
	cc.retCmd[Insert][key] = cmd
	cc.mux.Unlock()
	return cmd
}

// parseUserScopes converts a user-supplied scope CSV expression and optional
// additional system scopes into a scopeSet ready for clause building.
func parseUserScopes(userScopeCSV Scope, add ...Scope) scopeSet {

	var result scopeSet

	m := make(map[Scope]struct{})

	for s := range strings.SplitSeq(string(userScopeCSV), ",") {
		scope := Scope(strings.TrimSpace(s))
		if scope == FullScope {
			return scopeSet{all: true}
		}
		if _, ok := m[scope]; ok {
			continue
		}
		m[scope] = struct{}{}

		if scope == SystemScope {
			for _, sc := range SystemScopes {
				m[*sc] = struct{}{}
				result.system = append(result.system, *sc)
			}
			continue
		}
		negated := strings.HasPrefix(string(scope), "!")
		if negated {
			scope = scope[1:]
			if !IsSystemScope(scope) {
				result.negated = append(result.negated, scope)
			}
			continue
		}
		if IsSystemScope(scope) {
			result.system = append(result.system, scope)
			continue
		}
		result.direct = append(result.direct, scope)
	}

	for _, a := range add {
		if _, ok := m[a]; ok {
			continue
		}
		if IsSystemScope(a) {
			result.system = append(result.system, a)
			continue
		}
		if strings.HasPrefix(string(a), "!") {
			result.negated = append(result.negated, a[1:])
			continue
		}
		result.direct = append(result.direct, a)
	}

	return result
}

// scopeSet is the parsed, internal representation of a Scope expression used
// when building clause text and column-position lists.
type scopeSet struct {
	all     bool    // true when FullScope ("*") was requested
	direct  []Scope // user-defined scope names to include
	negated []Scope // user-defined scope names to exclude
	system  []Scope // system scope names to include
}

func buildInsertReturning[T any](t *Table[T], argScope, retScope Scope) ReturningCommand[T] {

	as := parseUserScopes(argScope, VersionField, InsertScope)
	rs := as
	if retScope != EmptyScope || retScope != argScope {
		rs = parseUserScopes(retScope, VersionField, InsertScope)
	}

	cols := newClause(ctColsCSV, t, as)
	vals := newClause(ctArgsInsert, t, as)
	rets := newClause(ctColsCSV, t, rs)

	sql := "INSERT INTO " + t.Name() +
		" (" + cols.text + ") VALUES (" + vals.text + ") RETURNING " + rets.text

	return ReturningCommand[T]{
		Command: Command[T]{
			sql:  sql,
			cpos: vals.cpos,
			sfpe: t.cc.sfpe,
		},
		rets: rets.cpos,
	}
}

func buildUpdate[T any](t *Table[T], scope Scope, ct clauseType, endingClause string) Command[T] {

	scopes := parseUserScopes(scope, VersionField, UpdateScope)
	cols := newClause(ct, t, scopes)

	sql := "UPDATE " + t.Name() + " SET " + cols.text
	if endingClause != "" {
		sql += " " + endingClause
	}

	return Command[T]{
		sql:  sql,
		cpos: cols.cpos,
		sfpe: t.cc.sfpe,
	}
}

// Update returns a cached Command for an UPDATE … SET statement. condition
// determines the WHERE clause: use ByPK() to filter by primary key, or
// ByClauses(sql) to provide an arbitrary clause string.
func (cc *CommandContanier[T]) Update(scope Scope, condition UpdateByOption) Command[T] {

	ct := ctColsUpdate
	baseClause := condition()
	if baseClause == clauseByPK {
		baseClause = cc.pkWhereCause
		ct = ctColsUpdateByPK
	}

	key := SingleScopeKey{scope: scope, clauses: baseClause}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Update][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildUpdate(cc.t, scope, ct, baseClause)
	cc.mux.Lock()
	cc.cmd[Update][key] = cmd
	cc.mux.Unlock()
	return cmd
}

// clauseByPK is a sentinel returned by ByPK() to signal that the WHERE clause
// should be generated from the table's primary key column.
const clauseByPK = "#$@"

// UpdateByOption is a function that returns the WHERE clause string for an
// UPDATE statement. The two standard implementations are ByPK and ByClauses.
type UpdateByOption func() string

// ByClauses returns an UpdateByOption that uses the given SQL string as the
// WHERE clause of an UPDATE statement. The string must be a fixed,
// parameterized SQL fragment — do not inline argument values.
func ByClauses(clauses string) UpdateByOption {
	return func() string {
		return clauses
	}
}

// ByPK returns an UpdateByOption that generates a WHERE clause filtering by
// the table's primary key (e.g. "WHERE id=$1"). The primary key value is
// extracted from the struct passed to the executing method.
func ByPK() UpdateByOption {
	return func() string {
		return clauseByPK
	}
}

func buildUpdateReturning[T any](t *Table[T], argScope, retScope Scope, ct clauseType, endingClause string) ReturningCommand[T] {
	as := parseUserScopes(argScope, VersionField, UpdateScope)
	rs := as
	if retScope == EmptyScope {
		rs = parseUserScopes(retScope, VersionField, UpdateScope)
	} else {
		if retScope != argScope {
			rs = parseUserScopes(retScope, VersionField, UpdateScope)
		}
	}

	cols := newClause(ct, t, as)
	rets := newClause(ctColsCSV, t, rs)

	sql := "UPDATE " + t.Name() + " SET " + cols.text
	if endingClause != "" {
		sql += " " + endingClause
	}

	sql += " RETURNING " + rets.text

	cmd := ReturningCommand[T]{
		Command: Command[T]{
			sql:  sql,
			cpos: cols.cpos,
			sfpe: t.cc.sfpe,
		},
		rets: rets.cpos,
	}
	return cmd
}

// UpdateReturning returns a cached ReturningCommand for an UPDATE … RETURNING
// statement. argScope selects the columns to write; retScope selects the
// columns to read back; condition supplies the WHERE clause.
func (cc *CommandContanier[T]) UpdateReturning(argScope, retScope Scope, condition UpdateByOption) ReturningCommand[T] {
	ct := ctColsUpdate
	clauses := condition()
	if clauses == clauseByPK {
		clauses = cc.pkWhereCause
		ct = ctColsUpdateByPK
	}

	key := DoubleScopeKey{argScope: argScope, retScope: retScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Update][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildUpdateReturning(cc.t, argScope, retScope, ct, clauses)
	cc.mux.Lock()
	cc.retCmd[Update][key] = cmd
	cc.mux.Unlock()
	return cmd
}

// Delete returns a cached Command for a DELETE statement with the given WHERE
// clause. Use parameterized placeholders; do not inline argument values.
func (cc *CommandContanier[T]) Delete(clauses string) Command[T] {
	key := SingleScopeKey{scope: EmptyScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Delete][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildDelete(cc.t, clauses)
	cc.mux.Lock()
	cc.cmd[Delete][key] = cmd
	cc.mux.Unlock()
	return cmd

}

func buildDelete[T any](t *Table[T], clauses string) Command[T] {
	cmd := Command[T]{
		sql:  "DELETE FROM " + t.Name() + " " + clauses,
		cpos: nil,
		sfpe: t.cc.sfpe,
	}
	return cmd
}

// DeleteReturning returns a cached ReturningCommand for a DELETE … RETURNING
// statement. retScope selects the columns to read back from RETURNING; clauses
// is the WHERE clause (parameterized). The primary key value is extracted from
// the struct passed to the executing method and bound to $1.
func (cc *CommandContanier[T]) DeleteReturning(retScope Scope, clauses string) ReturningCommand[T] {
	key := DoubleScopeKey{argScope: EmptyScope, retScope: retScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Delete][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildDeleteReturning(cc.t, retScope, clauses)
	cc.mux.Lock()
	cc.retCmd[Delete][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func buildDeleteReturning[T any](t *Table[T], retScope Scope, clauses string) ReturningCommand[T] {
	rs := parseUserScopes(retScope)
	rets := newClause(ctColsCSV, t, rs)

	// cpos holds the PK position so QueryRow can extract it from the caller's
	// struct and bind it to $1 in the WHERE clause (e.g. "WHERE id=$1").
	var cpos []int
	if pk := t.PK(); pk != nil {
		cpos = []int{pk.Pos}
	}

	return ReturningCommand[T]{
		Command: Command[T]{
			sql:  "DELETE FROM " + t.Name() + " " + clauses + " RETURNING " + rets.text,
			cpos: cpos,
			sfpe: t.cc.sfpe,
		},
		rets: rets.cpos,
	}
}

// Func returns a cached FunctionalCommand for a scalar query of the given type
// (Exist, ExistByPK, Count) with the supplied WHERE clause. Use parameterized
// placeholders; do not inline argument values.
func (cc *CommandContanier[T]) Func(typ FunctionalCommandEnum, clauses string) FunctionalCommand[T] {
	key := FuncCommandKey{typ: typ, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.fn[key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = buildFunctionalCommand(cc.t, typ, clauses)
	cc.mux.Lock()
	cc.fn[key] = cmd
	cc.mux.Unlock()
	return cmd

}

func buildFunctionalCommand[T any](t *Table[T], typ FunctionalCommandEnum, clauses string) FunctionalCommand[T] {
	var sql string
	switch typ {
	case Exist:
		sql = "SELECT EXISTS(SELECT 1 FROM " + t.Name() + " t " + clauses + ")"
	case ExistByPK:
		if pk := t.PK(); pk != nil {
			sql = "SELECT EXISTS(SELECT 1 FROM " + t.Name() + " WHERE " + pk.Name + "=" + t.FormatArg(1) + ")"
		}
	case Count:
		sql = "SELECT COUNT(*) FROM " + t.Name() + " t " + clauses
	}

	return FunctionalCommand[T]{sql: sql}
}
