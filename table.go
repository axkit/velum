package velum

import (
	"context"
	"errors"

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
	scope           map[ScopeKey]Clause

	sysCols struct {
		created []SystemColumn
		updated []SystemColumn
		deleted []SystemColumn
		version *SystemColumn
	}

	pool *PointerSlicePool[T]

	cc            *CommandContanier[T]
	wherePkClause string
	freqCmd       struct {
		selectAllFieldsByPK SelectCommand[T]
		updateAllFieldsByPK ReturningCommand[T]
		insertAllFields     ReturningCommand[T]
		softDeleteByPK      ReturningCommand[T]
		touchByPK           ReturningCommand[T]
		deleteByPK          ReturningCommand[T]
	}
}

func NewTable[T any](tablename string, opts ...TableOption) *Table[T] {
	cfg := TableConfig{
		tag:            DefaultFieldTag,
		argNumerator:   DefaultParamPlaceholderBuilder,
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
		scope:            make(map[ScopeKey]Clause),
	}

	if err := t.init(); err != nil {
		panic(err)
	}

	if t.pk != nil {
		t.wherePkClause = "WHERE " + t.pk.Name + "=" + t.cfg.argNumerator(1)
		t.freqCmd.selectAllFieldsByPK = t.cc.Select(FullScope, t.wherePkClause)
		// t.prepared.updateAllFieldsByPK = buildUpdateReturning(&t, t.pool, FullScope, FullScope, t.wherePkClause)
	}

	return &t
}

func (t *Table[T]) C() *CommandContanier[T] {
	return t.cc
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

func (t *Table[T]) ArgNumerator() func(int) string {
	return t.cfg.argNumerator
}

func (t *Table[T]) Arg(pos int) string {
	return t.cfg.argNumerator(pos)
}

func (t *Table[T]) ScopeContainer() map[ScopeKey]Clause {
	return t.scope
}

func (t *Table[T]) init() error {

	structFields := reflectx.ExtractStructFields(&t.zero, t.cfg.tag)

	t.initColumns(structFields)
	t.initPrimaryKeyColumn()
	t.initColumnValueGenerationRules()
	t.initSystemColumns()
	t.initUniqueScopeNames()
	t.initScopes()
	t.initPool()
	t.cc = NewCommandContainer(t.name, t.pk, t.pool, t.scope, t.cfg.argNumerator)
	t.initFrequentCommands()
	return nil
}

func (t *Table[T]) initPool() {
	fic := reflectx.NewFieldIndexContainer(len(t.columns))
	for i := range t.columns {
		fic.Add(t.columns[i].Path)
	}
	t.pool = NewPointerSlicePool[T](fic)
}

func (t *Table[T]) initFrequentCommands() {
	if t.pk != nil {
		t.freqCmd.selectAllFieldsByPK = t.cc.Select(FullScope, t.wherePkClause)
		t.freqCmd.updateAllFieldsByPK = t.cc.UpdateReturning(FullScope, FullScope, ByPK())
		// t.frequentCommand.insertAllFields = t.cc.InsertReturning(FullScope, FullScope)
		// t.frequentCommand.softDeleteByPK = t.cc.UpdateReturning(DeleteScope, DeleteScope, t.wherePkClause)
		// t.frequentCommand.touchByPK = t.cc.UpdateReturning(UpdateScope, UpdateScope, t.wherePkClause)
		// t.frequentCommand.deleteByPK = t.cc.DeleteReturning(FullScope, t.wherePkClause)
	}
}

func (t *Table[T]) initPrimaryKeyColumn() {

	pos := -1
	// find the tag value "pk"
	for i := range t.columns {
		if t.columns[i].Tag.Exist("pk") {
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
	t.pk.Tag.Add("scope", string(PrimaryKeyTagOption))
}

func (t *Table[T]) initColumns(structFields []reflectx.StructField) {

	t.columns = make([]Column, len(structFields))
	for i, sf := range structFields {

		ptag := reflectx.ParseTagPairs(sf.Tag)
		ptag.Add("scope", "*")

		t.columns[i] = Column{
			Path: sf.Path,
			Name: t.cfg.colNameBuilder(sf.Name, sf.Tag),
			Tag:  ptag,
		}
	}
}

func (t *Table[T]) initScopes() {

	t.scope = make(map[ScopeKey]Clause, len(t.uniqueScopeName)*int(ctMax_))

	for scope := range t.uniqueScopeName {
		vals := ExtractScopeClauses(t.columns, scope, t.pk, t.cfg.argNumerator)
		for i := range vals {
			t.scope[ScopeKey{scope: scope, ct: clauseType(i)}] = vals[i]
		}
	}

	t.addSystemColumnsToScopes()
}

func (t *Table[T]) addSystemColumnsToScopes() {

	// add system columns to the user scope
	for key, clause := range t.scope {
		if key.scope == FullScope || key.scope == "pk" {
			continue
		}

		if IsSystemScope(key.scope) {
			continue
		}

		text := ""
		if t.sysCols.version != nil {
			name := t.sysCols.version.Name
			text = name + "=" + name + "+1"

			if key.ct == ctUpdateByPK || key.ct == ctUpdate {
				clause.join(text, t.sysCols.version.Pos, nil)
			} else if key.ct == ctColsCSV || key.ct == ctColsPrefixedCSV {
				clause.join(name, t.sysCols.version.Pos, t.sysCols.version.Path)
			}
			t.scope[key] = clause
		}
		// if !(key.ct == ctColsCSV || key.ct == ctUpdateByPK || key.ct == ctUpdate) {
		// 	continue
		// }

		// text := ""
		// if t.sysCols.version != nil {
		// 	name := t.sysCols.version.Name
		// 	text = name + "=" + name + "+1"
		// 	clause.join(text, t.sysCols.version.Pos, t.sysCols.version.Path)
		// }

		// for _, uCol := range t.sysCols.updated {
		// 	name := uCol.Name
		// 	if text != "" {
		// 		text += ","
		// 	}
		// 	clause.join(name, uCol.Pos, uCol.Path)
		// }
	}
}

func (t *Table[T]) initSystemColumns() {
	for i := range t.columns {
		col := &t.columns[i]

		for _, name := range col.Tag.Get("scope") {

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
		for _, s := range c.Tag.Get("scope") {
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
	if _, ok := t.uniqueScopeName[Scope(s)]; ok {
		return Scope(s)
	}
	panic("invalid scope" + s)
}

func (t *Table[T]) GetByPK(ctx context.Context, q QueryRowExecuter, pk any) (*T, error) {
	return t.freqCmd.selectAllFieldsByPK.GetOne(ctx, q, pk)
}

func (t *Table[T]) GetOne(ctx context.Context, q QueryRowExecuter, scope Scope, clauses string, args ...any) (*T, error) {
	cmd := t.cc.Select(scope, clauses)
	return cmd.GetOne(ctx, q, args...)
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

func (t *Table[T]) Update(ctx context.Context, q Executer, scope Scope, clauses string, row *T) (Result, error) {
	cmd := t.cc.Update(scope, ByClauses(clauses))
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) UpdateByPK(ctx context.Context, q Executer, scope Scope, row *T) (Result, error) {
	cmd := t.cc.Update(scope, ByPK())
	return cmd.Exec(ctx, q, row)
}

func (t *Table[T]) UpdateReturningByPK(ctx context.Context, q QueryRowExecuter, scope, retScope Scope, row *T) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByPK())
	return cmd.QueryRow(ctx, q, row)
}

func (t *Table[T]) UpdateReturning(ctx context.Context, q QueryRowExecuter, scope, retScope Scope, clauses string, row *T) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByClauses(clauses))
	return cmd.QueryRow(ctx, q, row)
}
