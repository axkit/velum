package velum

import (
	"sync"
)

type StaticSqlTextEnum int

const (
	StaticExistByPK StaticSqlTextEnum = iota
	StaticDeleteByPK
	StaticExist
	StaticCount
	StaticTruncate
)

type CommandTypeEnum int

const (
	Insert CommandTypeEnum = iota
	Update
	Delete
	Exist
	Count
	CommandTypeEnumMax_
)

type SingleScopeKey struct {
	scope   Scope
	clauses string
}

type DoubleScopeKey struct {
	argScope Scope
	retScope Scope
	clauses  string
}

type CommandContanier[T any] struct {
	tablename    string
	pkWhereCause string
	sfpe         StructFieldPtrExtractor[T]
	sc           map[ScopeKey]Clause

	//staticSQL [CommandTypeEnumMax_]string
	mux    sync.RWMutex
	sel    map[SingleScopeKey]SelectCommand[T]
	cmd    [CommandTypeEnumMax_]map[SingleScopeKey]Command[T]
	retCmd [CommandTypeEnumMax_]map[DoubleScopeKey]ReturningCommand[T]
}

func NewCommandContainer[T any](
	tablename string,
	pk *SystemColumn,
	sfpe StructFieldPtrExtractor[T],
	sc map[ScopeKey]Clause,
	anb ArgNumberBuilder,
) *CommandContanier[T] {

	cc := CommandContanier[T]{
		tablename: tablename,
		sc:        sc,
		sfpe:      sfpe,
		sel:       make(map[SingleScopeKey]SelectCommand[T]),
	}

	for i := range CommandTypeEnumMax_ {
		cc.cmd[i] = make(map[SingleScopeKey]Command[T])
		cc.retCmd[i] = make(map[DoubleScopeKey]ReturningCommand[T])
	}

	if pk != nil {
		cc.pkWhereCause = " WHERE " + pk.Name + "=" + anb(1)
	}

	return &cc
}

func (cc *CommandContanier[T]) Select(scope Scope, clauses string) SelectCommand[T] {
	key := SingleScopeKey{scope: scope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.sel[key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cols, ok := cc.sc[ScopeKey{ct: ctColsPrefixedCSV, scope: scope}]
	cmd = SelectCommand[T]{
		sql:  "SELECT " + cols.text + " FROM " + cc.tablename + " t " + clauses,
		args: cols.args,
		sfpe: cc.sfpe,
	}

	cc.mux.Lock()
	cc.sel[key] = cmd
	cc.mux.Unlock()
	return cmd
}

func (cc *CommandContanier[T]) Insert(scope Scope) Command[T] {
	key := SingleScopeKey{scope: scope, clauses: ""}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Insert][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cols := cc.sc[ScopeKey{ct: ctColsCSV, scope: scope}]
	vals := cc.sc[ScopeKey{ct: ctArgsInsert, scope: scope}]
	cmd = Command[T]{
		sql:  "INSERT INTO " + cc.tablename + " (" + cols.text + ") VALUES (" + vals.text + ")",
		args: vals.args,
		sfpe: cc.sfpe,
	}

	cc.mux.Lock()
	cc.cmd[Insert][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func (cc *CommandContanier[T]) InsertReturning(argScope, retScope Scope) ReturningCommand[T] {
	key := DoubleScopeKey{argScope: argScope, retScope: retScope, clauses: ""}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Insert][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cols := cc.sc[ScopeKey{ct: ctColsCSV, scope: argScope}]
	vals := cc.sc[ScopeKey{ct: ctArgsInsert, scope: argScope}]

	rets := cols
	if argScope != retScope {
		rets = cc.sc[ScopeKey{ct: ctColsCSV, scope: retScope}]
	}

	cmd = ReturningCommand[T]{
		Command: Command[T]{
			sql: "INSERT INTO " + cc.tablename + " (" + cols.text + ") VALUES (" + vals.text +
				") RETURNING " + rets.text,
			args: vals.args,
			sfpe: cc.sfpe,
		},
		rets: rets.args,
	}

	cc.mux.Lock()
	cc.retCmd[Insert][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func (cc *CommandContanier[T]) Update(scope Scope, condition UpdateByOption) Command[T] {

	ct := ctUpdateByPK

	clauses := condition()
	if clauses == clauseByPK {
		clauses = cc.pkWhereCause
	} else {
		ct = ctUpdate
		if clauses != "" {
			clauses = " WHERE " + clauses
		}
	}

	key := SingleScopeKey{scope: scope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Update][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	upd := cc.sc[ScopeKey{ct: ct, scope: scope}]
	cmd = Command[T]{
		sql:  "UPDATE " + cc.tablename + " SET " + upd.text + " " + clauses,
		args: upd.args,
		sfpe: cc.sfpe,
	}
	cc.mux.Lock()
	cc.cmd[Update][key] = cmd
	cc.mux.Unlock()
	return cmd
}

const clauseByPK = "#$@"

type UpdateByOption func() string

func ByClauses(clauses string) UpdateByOption {
	return func() string {
		return clauses
	}
}
func ByPK() UpdateByOption {
	return func() string {
		return clauseByPK
	}
}

func (cc *CommandContanier[T]) UpdateReturning(argScope, retScope Scope, condition UpdateByOption) ReturningCommand[T] {

	ct := ctUpdateByPK

	clauses := condition()
	if clauses == clauseByPK {
		clauses = cc.pkWhereCause
	} else {
		ct = ctUpdate
		if clauses != "" {
			clauses = " WHERE " + clauses
		}
	}

	key := DoubleScopeKey{argScope: argScope, retScope: retScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Update][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	fvs := cc.sc[ScopeKey{ct: ct, scope: argScope}]
	rets := cc.sc[ScopeKey{ct: ctColsCSV, withSysCols: true, scope: retScope}]

	cmd = ReturningCommand[T]{
		Command: Command[T]{
			sql:  "UPDATE " + cc.tablename + " SET " + fvs.text + " " + clauses + " RETURNING " + rets.text,
			args: fvs.args,
			sfpe: cc.sfpe,
		},
		rets: rets.args,
	}

	cc.mux.Lock()
	cc.retCmd[Update][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func (cc *CommandContanier[T]) Delete(clauses string) Command[T] {

	key := SingleScopeKey{scope: EmptyScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.cmd[Delete][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	cmd = Command[T]{
		sql:  "DELETE FROM " + cc.tablename + " " + clauses,
		args: nil,
	}

	cc.mux.Lock()
	cc.cmd[Delete][key] = cmd
	cc.mux.Unlock()
	return cmd
}

func (cc *CommandContanier[T]) DeleteReturning(retScope Scope, clauses string) ReturningCommand[T] {

	key := DoubleScopeKey{argScope: DeleteScope, retScope: retScope, clauses: clauses}
	cc.mux.RLock()
	cmd, ok := cc.retCmd[Delete][key]
	cc.mux.RUnlock()
	if ok {
		return cmd
	}

	ret := cc.sc[ScopeKey{ct: ctColsCSV, scope: retScope}]
	cmd = ReturningCommand[T]{
		Command: Command[T]{
			sql:  "DELETE FROM " + cc.tablename + " " + clauses + " RETURNING " + ret.text,
			args: nil,
			sfpe: cc.sfpe,
		},
		rets: ret.args,
	}
	cc.mux.Lock()
	cc.retCmd[Delete][key] = cmd
	cc.mux.Unlock()
	return cmd
}
