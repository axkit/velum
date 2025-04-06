package velum

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/axkit/velum/reflectx"
)

var (
	ErrNoPrimaryKey     = errors.New("no primary key defined")
	ErrInvalidScopePair = errors.New("invalid scope pair")
)

// Table is a struct that represents a database table.
type Table[T any] struct {
	columns          []Column
	pk               *SystemColumn
	name             string
	friendlySequence string

	cfg TableConfig

	zero            T
	uniqueScopeName map[Scope]struct{}
	scope           map[scopeKey]clause

	sysCols struct {
		created []SystemColumn
		updated []SystemColumn
		deleted []SystemColumn
		version *SystemColumn
	}

	pool    *reflectx.PointerSlicePool[T]
	ObjPool *sync.Pool

	cc            *CommandContanier[T]
	wherePkClause string
	freqCmd       struct {
		selectAllFieldsByPK SelectCommand[T]
		updateAllFieldsByPK ReturningCommand[T]
		insertAllFields     ReturningCommand[T]
		softDeleteByPK      ReturningCommand[T]
		touchByPK           ReturningCommand[T]
		deleteByPK          string
		deleteRetAllByPK    ReturningCommand[T]
	}
}

func NewTable[T any](tablename string, opts ...TableOption) *Table[T] {
	cfg := TableConfig{
		tag:            DefaultFieldTag,
		argFormatter:   DefaultParamPlaceholderBuilder,
		colNameBuilder: DefaultColumnNameBuilder,
		seqNameBuilder: DefaultFriendlySequenceNameBuilder,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	t := Table[T]{
		name:             tablename,
		cfg:              cfg,
		friendlySequence: cfg.seqNameBuilder(tablename),
		scope:            make(map[scopeKey]clause),
		ObjPool: &sync.Pool{
			New: func() any {
				var v T
				return &v
			},
		},
	}

	if err := t.init(); err != nil {
		panic(err)
	}

	return &t
}

func (t *Table[T]) CommandContainer() *CommandContanier[T] {
	return t.cc
}

func (t *Table[T]) Object(scope Scope) (*T, *[]any) {
	c := t.ObjPool.Get().(*T)
	return c, t.pool.StructFieldPtrs(c, t.freqCmd.selectAllFieldsByPK.cpos)
}
func (t *Table[T]) ObjectPut(v *T, ptrs *[]any) {
	t.ObjPool.Put(v)
	t.pool.Release(ptrs)
}

// Name returns the table name.
func (t *Table[T]) Name() string {
	return t.name
}

func (t *Table[T]) Validate(ctx context.Context) error {
	return nil
}

func (t *Table[T]) Columns() []Column {
	return t.columns
}

func (t *Table[T]) Created() []SystemColumn {
	return t.sysCols.created
}

func (t *Table[T]) Updated() []SystemColumn {
	return t.sysCols.updated
}

func (t *Table[T]) Deleted() []SystemColumn {
	return t.sysCols.deleted
}

func (t *Table[T]) Version() *SystemColumn {
	return t.sysCols.version
}

func (t *Table[T]) PK() *SystemColumn {
	return t.pk
}

func (t *Table[T]) FriendlySequence() string {
	return t.friendlySequence
}

func (t *Table[T]) ArgNumerator() ArgFormatter {
	return t.cfg.argFormatter
}

func (t *Table[T]) FormatArg(pos int) string {
	return t.cfg.argFormatter(pos)
}

func (t *Table[T]) ScopeContainer() map[scopeKey]clause {
	return t.scope
}

func (t *Table[T]) init() error {

	structFields := reflectx.ExtractStructFields(&t.zero, t.cfg.tag)

	t.initColumns(structFields)
	t.initPrimaryKeyColumn()
	t.initColumnValueGenerationRules()
	t.initSystemColumns()
	t.initUniqueScopeNames()
	t.initPool()
	t.cc = NewCommandContainer(t, t.pool, t.scope, t.cfg.argFormatter)
	t.initFrequentCommands()
	return nil
}

func (t *Table[T]) initPool() {
	fic := reflectx.NewFieldIndexContainer(len(t.columns) + 2)
	for i := range t.columns {
		fic.Add(t.columns[i].Path)
	}
	t.pool = reflectx.NewPointerSlicePool[T](fic)
}

func (t *Table[T]) initFrequentCommands() {
	if t.pk != nil {
		t.wherePkClause = "WHERE " + t.pk.Name + "=" + t.cfg.argFormatter(1)
		t.freqCmd.selectAllFieldsByPK = t.cc.Select(FullScope, t.wherePkClause)
		t.freqCmd.updateAllFieldsByPK = t.cc.UpdateReturning(FullScope, FullScope, ByPK())
		t.freqCmd.deleteByPK = "DELETE FROM " + t.name + " " + t.wherePkClause
		t.freqCmd.softDeleteByPK = t.cc.UpdateReturning(DeleteScope, SystemScope, ByPK())
	}
}

func (t *Table[T]) initColumns(structFields []reflectx.StructField) {

	t.columns = make([]Column, len(structFields))
	for i, sf := range structFields {

		ptag := reflectx.ParseTagPairs(sf.Tag, scopeTagKey)
		ptag.Add(scopeTagKey, string(FullScope))

		t.columns[i] = Column{
			Path: sf.Path,
			Name: t.cfg.colNameBuilder(sf.Name, sf.Tag),
			Tag:  ptag,
		}
	}
}

func (t *Table[T]) initPrimaryKeyColumn() {

	pos := -1
	// find the tag value "pk"
	for i := range t.columns {
		if t.columns[i].Tag.PairExist(scopeTagKey, PrimaryKeyTagOption) {
			pos = i
			break
		}
	}

	// if not found, try to find the pk column by default name.
	if pos == -1 && StandardPrimaryKeyCol != "" {
		for i := range t.columns {
			if t.columns[i].Name == StandardPrimaryKeyCol {
				pos = i
				break
			}
		}
	}

	if pos == -1 {
		return
	}

	t.pk = &SystemColumn{
		Column: &t.columns[pos],
		Pos:    pos,
	}

	t.pk.ValueGenerationMethod,
		t.pk.ValueGenerator = pkColValueGenMethod(t.pk.Tag.Value("gen"), t.friendlySequence)
}

func (t *Table[T]) initSystemColumns() {
	for i := range t.columns {
		col := &t.columns[i]

		for _, name := range col.Tag.Get(scopeTagKey) {

			sysCol := SystemColumn{
				Column: col,
				Pos:    i,
			}
			// add column to system scopes
			switch Scope(name) {
			case InsertScope:
				t.sysCols.created = append(t.sysCols.created, sysCol)
			case UpdateScope:
				t.sysCols.updated = append(t.sysCols.updated, sysCol)
			case DeleteScope:
				t.sysCols.deleted = append(t.sysCols.deleted, sysCol)
			case VersionField:
				t.sysCols.version = &sysCol
			}
		}
	}
}

func (t *Table[T]) initColumnValueGenerationRules() {
	for i := range t.columns {
		if t.pk != nil && i == t.pk.Pos {
			continue
		}
		c := &t.columns[i]
		c.ValueGenerationMethod, c.ValueGenerator = colValueGenMethod(c.Tag.Value("gen"))
	}
}

func (t *Table[T]) initUniqueScopeNames() {
	t.uniqueScopeName = make(map[Scope]struct{})
	for _, c := range t.columns {
		for _, s := range c.Tag.Get(scopeTagKey) {
			scope := Scope(s)
			if IsSystemScope(scope) {
				continue
			}

			t.uniqueScopeName[scope] = struct{}{}

			// add negated scope
			if scope != "*" && s != PrimaryKeyTagOption {
				t.uniqueScopeName["!"+scope] = struct{}{}
			}
		}
	}
}

// Scope returns a scope by its name. If the scope is not found, it panics.
// This function is used to validate the scope by the caller.
func (t *Table[T]) Scope(s string) Scope {
	for g := range strings.SplitSeq(s, ",") {
		if _, ok := t.uniqueScopeName[Scope(g)]; ok {
			return Scope(s)
		}
	}

	panic("invalid scope: " + s)
}

func (t *Table[T]) GetByPK(ctx context.Context, q QueryRowExecuter, pk any) (*T, error) {
	return t.freqCmd.selectAllFieldsByPK.Get(ctx, q, pk)
}

func (t *Table[T]) GetTo(ctx context.Context, q QueryRowExecuter, dst []any, pk any) error {
	return t.freqCmd.selectAllFieldsByPK.GetToPtr(ctx, q, dst, pk)
}

func (t *Table[T]) Get(ctx context.Context, q QueryRowExecuter, scope Scope, clauses string, clausArgs ...any) (*T, error) {
	cmd := t.cc.Select(scope, clauses)
	return cmd.Get(ctx, q, clausArgs...)
}

func (t *Table[T]) Select(ctx context.Context, q QueryExecuter, scope Scope, clauses string, args ...any) ([]T, error) {
	cmd := t.cc.Select(scope, clauses)
	return cmd.GetMany(ctx, q, args...)
}

func (t *Table[T]) Insert(ctx context.Context, q Executer, row *T, scope Scope) (Result, error) {
	cmd := t.cc.Insert(scope)
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) InsertReturning(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope) (*T, error) {
	cmd := t.cc.InsertReturning(scope, retScope)
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) Update(ctx context.Context, q Executer, row *T, scope Scope, clauses string) (Result, error) {
	cmd := t.cc.Update(scope, ByClauses(clauses))
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) UpdateByPK(ctx context.Context, q Executer, row *T, scope Scope) (Result, error) {
	cmd := t.cc.Update(scope, ByPK())
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) UpdateReturningByPK(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByPK())
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) UpdateReturning(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope, clauses string) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByClauses(clauses))
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) DeleteByPK(ctx context.Context, q Executer, pk any) (Result, error) {
	return q.ExecContext(ctx, t.freqCmd.deleteByPK, pk)
}

func (t *Table[T]) DeleteReturningByPK(ctx context.Context, q QueryRowExecuter, row *T) (*T, error) {
	cmd := t.cc.DeleteReturning(FullScope, t.wherePkClause)
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) Delete(ctx context.Context, q Executer, clauses string, args ...any) (Result, error) {
	cmd := t.cc.Delete(clauses)
	return q.ExecContext(ctx, cmd.sql, args...)
}

func (t *Table[T]) DeleteReturning(ctx context.Context, q QueryRowExecuter, row *T, clauses string) (*T, error) {
	cmd := t.cc.DeleteReturning(FullScope, clauses)
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) SoftDeleteByPK(ctx context.Context, q Executer, row *T) (Result, error) {
	cmd := t.cc.Update(DeleteScope, ByPK())
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) SoftDeleteReturningByPK(ctx context.Context, q QueryRowExecuter, row *T) (*T, error) {
	cmd := t.cc.UpdateReturning(DeleteScope, SystemScope, ByPK())
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) TouchByPK(ctx context.Context, q Executer, row *T) (Result, error) {
	cmd := t.cc.Update(UpdateScope, ByPK())
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) Exist(ctx context.Context, q QueryRowExecuter, clauses string, args ...any) (bool, error) {
	return t.exist(ctx, q, Exist, clauses, args...)
}

func (t *Table[T]) ExistByPK(ctx context.Context, q QueryRowExecuter, pk any) (bool, error) {
	return t.exist(ctx, q, ExistByPK, t.wherePkClause, pk)
}

func (t *Table[T]) exist(ctx context.Context, q QueryRowExecuter, typ FunctionalCommandEnum, sql string, args ...any) (bool, error) {
	var result bool
	cmd := t.cc.Func(typ, sql)
	err := cmd.Call(ctx, q, &result, args...)
	return result, err
}

func (t *Table[T]) Count(ctx context.Context, q QueryRowExecuter, clauses string, args ...any) (int, error) {
	var result int
	cmd := t.cc.Func(Count, clauses)
	err := cmd.Call(ctx, q, &result, args...)
	return result, err
}
