package velum

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/axkit/velum/reflectx"
)

var (
	// ErrNoPrimaryKey is returned by Table methods that require a primary key
	// (GetByPK, DeleteByPK, ExistByPK, etc.) when the struct T has no field
	// named "id" and no field tagged with dbw:"pk".
	ErrNoPrimaryKey = errors.New("no primary key defined")

	// ErrInvalidScopePair is returned when the combination of SET scope and
	// RETURNING scope produces an empty column list (e.g. both are EmptyScope
	// when at least one must select columns).
	ErrInvalidScopePair = errors.New("invalid scope pair")
)

// Table is the central descriptor for a single database table. It is created
// once at application startup with NewTable and is safe for concurrent use
// from multiple goroutines.
//
// Table pre-builds SQL strings and a pool of field-pointer slices at
// construction time so that every CRUD method incurs zero allocations in the
// steady state. The type parameter T must be a struct whose exported fields
// correspond to database columns; fields are discovered via the struct tag
// configured by WithTag (default "dbw").
type Table[T any] struct {
	columns          []Column
	pk               *SystemColumn
	name             string
	friendlySequence string

	cfg TableConfig

	uniqueScopeName map[Scope]struct{}
	scope           map[scopeKey]clause

	sysCols struct {
		created []SystemColumn
		updated []SystemColumn
		deleted []SystemColumn
		version *SystemColumn
	}

	pool *reflectx.PointerSlicePool[T]
	// ObjPool is a sync.Pool of *T values. It is exposed for advanced callers
	// that want to borrow and return struct instances manually (see Object and
	// ObjectPut). In normal usage, CRUD methods manage the pool internally.
	ObjPool *sync.Pool

	cc            *CommandContanier[T]
	wherePkClause string
	freqCmd       struct {
		selectAllFieldsByPK SelectCommand[T]
		updateAllFieldsByPK ReturningCommand[T]
		insertAllFields     ReturningCommand[T]
		softDeleteByPK      ReturningCommand[T] // used by SoftDeleteReturningByPK
		deleteRetAllByPK    ReturningCommand[T]
		deleteByPK          string
	}
}

// NewTable creates a Table descriptor for the database table named tablename.
// It reads column metadata from the struct T using the configured struct tag,
// builds SQL clause fragments for every scope, and pre-warms a pointer pool
// and the most frequently used command strings.
//
// NewTable panics if T is not a struct or if column metadata cannot be built
// (e.g. conflicting tag options). It is intended to be called once at program
// startup and stored in a package-level or struct-level variable.
//
// opts are applied in order; later options override earlier ones.
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
				return new(T)
			},
		},
	}

	if err := t.init(); err != nil {
		panic(err)
	}

	return &t
}

// CommandContainer returns the underlying CommandContanier that lazily builds
// and caches SQL commands for this table. Use it for advanced scenarios where
// you want to pre-build a command once and execute it many times without going
// through the Table convenience methods.
func (t *Table[T]) CommandContainer() *CommandContanier[T] {
	return t.cc
}

// Object borrows a *T from ObjPool together with a pre-built *[]any of field
// pointers covering the full-scope column set. The caller must pass both
// values to ObjectPut when done to return them to their respective pools.
//
// This low-level method is intended for hot paths where callers want to
// control scanning manually without additional allocations. The scope
// parameter is accepted for forward compatibility but is currently unused;
// the returned pointer slice always covers FullScope columns.
func (t *Table[T]) Object(scope Scope) (*T, *[]any) {
	c := t.ObjPool.Get().(*T)
	return c, t.pool.StructFieldPtrs(c, t.freqCmd.selectAllFieldsByPK.cpos)
}

// ObjectPut returns v and ptrs to their respective pools. It must be called
// exactly once for every Object call to avoid memory leaks.
func (t *Table[T]) ObjectPut(v *T, ptrs *[]any) {
	t.ObjPool.Put(v)
	t.pool.Release(ptrs)
}

// Name returns the table name.
func (t *Table[T]) Name() string {
	return t.name
}

// Validate is a no-op placeholder reserved for future use (e.g. verifying
// that the table's columns exist in the live database schema at startup).
func (t *Table[T]) Validate(ctx context.Context) error {
	return nil
}

// Columns returns the slice of all columns derived from T. The slice is built
// once at NewTable time and is read-only; callers must not modify it.
func (t *Table[T]) Columns() []Column {
	return t.columns
}

// Created returns the system columns tagged with the "insert" scope
// (e.g. created_at). These columns are included in INSERT statements only.
func (t *Table[T]) Created() []SystemColumn {
	return t.sysCols.created
}

// Updated returns the system columns tagged with the "update" scope
// (e.g. updated_at). These columns are included in UPDATE statements only.
func (t *Table[T]) Updated() []SystemColumn {
	return t.sysCols.updated
}

// Deleted returns the system columns tagged with the "delete" scope
// (e.g. deleted_at, deleted_by). These columns are included in soft-delete
// UPDATE statements only.
func (t *Table[T]) Deleted() []SystemColumn {
	return t.sysCols.deleted
}

// Version returns the single system column tagged with the "version" scope
// (e.g. row_version), or nil if no such column exists. On every UPDATE this
// column is auto-incremented as col=col+1, providing optimistic locking.
func (t *Table[T]) Version() *SystemColumn {
	return t.sysCols.version
}

// PK returns the primary-key column descriptor, or nil if the struct T has
// no field named "id" and no field explicitly tagged with dbw:"pk".
func (t *Table[T]) PK() *SystemColumn {
	return t.pk
}

// FriendlySequence returns the database sequence name derived from the table
// name by the configured sequence-name builder (default: tablename + "_seq").
// This sequence is used when the PK generation method is FriendlySequence
// (the default when no gen= tag option is provided for the PK field).
func (t *Table[T]) FriendlySequence() string {
	return t.friendlySequence
}

// ArgNumerator returns the ArgFormatter configured for this table. The
// formatter converts a 1-based argument position to the driver-specific
// placeholder string (e.g. "$1" for PostgreSQL).
func (t *Table[T]) ArgNumerator() ArgFormatter {
	return t.cfg.argFormatter
}

// FormatArg formats argument position pos (1-based) as a driver-specific
// placeholder string using the configured ArgFormatter.
func (t *Table[T]) FormatArg(pos int) string {
	return t.cfg.argFormatter(pos)
}

// ScopeContainer returns the internal scope-to-clause map built at NewTable
// time. It is exposed for advanced introspection (e.g. inspecting pre-built
// SQL fragments); callers must not modify the returned map.
func (t *Table[T]) ScopeContainer() map[scopeKey]clause {
	return t.scope
}

func (t *Table[T]) init() error {

	var zero T

	structFields := reflectx.ExtractStructFields(&zero, t.cfg.tag)

	t.columns = buildColumnsFromFields(structFields, t.cfg.colNameBuilder)
	t.initPrimaryKeyColumn()
	t.initColumnValueGenerationRules()
	t.initSystemColumns()
	t.initUniqueScopeNames()
	t.pool = newPointerSlicePool[T](t.columns)
	t.cc = NewCommandContainer(t, t.pool, t.scope, t.cfg.argFormatter)
	t.initFrequentCommands()
	return nil
}

func (t *Table[T]) initFrequentCommands() {
	if t.pk != nil {
		t.wherePkClause = "WHERE " + t.pk.Name + "=" + t.cfg.argFormatter(1)
		t.freqCmd.insertAllFields = t.cc.InsertReturning(FullScope, FullScope)
		t.freqCmd.selectAllFieldsByPK = t.cc.Select(FullScope, t.wherePkClause)
		t.freqCmd.updateAllFieldsByPK = t.cc.UpdateReturning(FullScope, FullScope, ByPK())
		t.freqCmd.deleteByPK = "DELETE FROM " + t.name + " " + t.wherePkClause
		t.freqCmd.softDeleteByPK = t.cc.UpdateReturning(DeleteScope, SystemScope, ByPK())
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

// Scope validates that s names a scope known to this table and returns it as
// a typed Scope value. If s contains multiple comma-separated names, at least
// one must match a registered scope. Panics if no component of s is a valid
// scope for this table.
//
// Use this method at application startup to catch scope typos early rather
// than discovering them at query time.
func (t *Table[T]) Scope(s string) Scope {
	for g := range strings.SplitSeq(s, ",") {
		if _, ok := t.uniqueScopeName[Scope(g)]; ok {
			return Scope(s)
		}
	}

	panic("invalid scope: " + s)
}

// GetByPK executes a SELECT with FullScope and returns the single row whose
// primary key equals pk. It returns sql.ErrNoRows (or the driver equivalent)
// when no row is found.
//
// Generated SQL (example):
//
//	SELECT t.id, t.first_name, t.last_name, ... FROM customers t WHERE id=$1
func (t *Table[T]) GetByPK(ctx context.Context, q QueryRowExecuter, pk any) (*T, error) {
	return t.freqCmd.selectAllFieldsByPK.Get(ctx, q, pk)
}

// GetTo executes a full-scope SELECT by primary key and scans the result row
// into the caller-provided dst slice of pointers. dst must contain one pointer
// per column in FullScope, in column order.
//
// Use this when you want to control scan destinations without allocating a new
// struct (e.g. when reusing pooled result objects externally).
func (t *Table[T]) GetTo(ctx context.Context, q QueryRowExecuter, dst []any, pk any) error {
	return t.freqCmd.selectAllFieldsByPK.GetToPtr(ctx, q, dst, pk)
}

// Get executes a SELECT for the given scope with an arbitrary SQL clause and
// returns the single matching row as a newly allocated *T. clausArgs are
// bound to the placeholders in clauses.
//
//	tbl.Get(ctx, dbw, "profile", "WHERE email=$1", email)
//	// SELECT t.id, t.first_name, t.last_name FROM customers t WHERE email=$1
//
// It returns sql.ErrNoRows (or the driver equivalent) when no row is found.
func (t *Table[T]) Get(ctx context.Context, q QueryRowExecuter, scope Scope, clauses string, clausArgs ...any) (*T, error) {
	cmd := t.cc.Select(scope, clauses)
	return cmd.Get(ctx, q, clausArgs...)
}

// Select executes a SELECT for the given scope with an arbitrary SQL clause
// and returns all matching rows. args are bound to the placeholders in
// clauses. It returns a nil slice (not an error) when no rows are found.
//
//	tbl.Select(ctx, dbw, "price,stock", "WHERE active=$1 ORDER BY name", true)
//	// SELECT t.id, t.price, t.stock FROM products t WHERE active=$1 ORDER BY name
func (t *Table[T]) Select(ctx context.Context, q QueryExecuter, scope Scope, clauses string, args ...any) ([]T, error) {
	cmd := t.cc.Select(scope, clauses)
	return cmd.GetMany(ctx, q, args...)
}

// SelectAll executes a SELECT with FullScope and no WHERE clause, returning
// every row in the table. It returns a nil slice (not an error) when the
// table is empty.
func (t *Table[T]) SelectAll(ctx context.Context, q QueryExecuter) ([]T, error) {
	cmd := t.cc.Select(FullScope, "")
	return cmd.GetMany(ctx, q)
}

// Insert inserts row into the database using all columns, then scans the
// RETURNING clause back into row in place. This populates database-generated
// fields such as the primary key and created_at without a separate round-trip.
//
// Generated SQL (example):
//
//	INSERT INTO customers (id, first_name, ...) VALUES (DEFAULT, $1, ...)
//	RETURNING id, first_name, ...
func (t *Table[T]) Insert(ctx context.Context, q QueryRowExecuter, row *T) error {
	return t.cc.t.freqCmd.insertAllFields.QueryRowTo(ctx, q, row)
}

// InsertScope inserts row using only the columns selected by scope. Unlike
// Insert it does not use a RETURNING clause; the returned Result carries the
// number of rows affected. Use this to skip columns that have database-side
// defaults and do not need to be read back immediately.
func (t *Table[T]) InsertScope(ctx context.Context, q Executer, row *T, scope Scope) (Result, error) {
	cmd := t.cc.Insert(scope)
	return cmd.Exec(ctx, q, row)
}

// InsertReturning inserts row using the columns selected by scope and returns
// the columns listed in retScope as a newly allocated *T. It is useful when
// you want to receive only a subset of columns after an insert (e.g. just
// the system columns, not the full row).
//
//	inserted, err := tbl.InsertReturning(ctx, dbw, &c, velum.FullScope, velum.FullScope)
//	fmt.Println("new id:", inserted.ID)
func (t *Table[T]) InsertReturning(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope) (*T, error) {
	cmd := t.cc.InsertReturning(scope, retScope)
	return cmd.QueryRow(ctx, q, row)
}

// Update executes an UPDATE for the columns selected by scope, filtered by
// the provided SQL clause. System columns (version, update) are appended
// automatically. The primary key is not automatically used in the WHERE
// condition; pass it as part of clauses (e.g. "WHERE sku=$1").
//
//	tbl.Update(ctx, dbw, &p, "stock", "WHERE sku=$1", sku)
//	// UPDATE products SET stock=$2, row_version=row_version+1, updated_at=$3 WHERE sku=$1
func (t *Table[T]) Update(ctx context.Context, q Executer, row *T, scope Scope, clauses string) (Result, error) {
	cmd := t.cc.Update(scope, ByClauses(clauses))
	return cmd.Exec(ctx, q, row)
}

// UpdateByPK executes an UPDATE for the columns selected by scope, using the
// primary key as the WHERE condition. System columns (version, update) are
// appended automatically.
//
//	tbl.UpdateByPK(ctx, dbw, &p, "price,stock")
//	// UPDATE products SET price=$2, stock=$3, row_version=row_version+1, updated_at=$4 WHERE id=$1
func (t *Table[T]) UpdateByPK(ctx context.Context, q Executer, row *T, scope Scope) (Result, error) {
	cmd := t.cc.Update(scope, ByPK())
	return cmd.Exec(ctx, q, row)
}

// UpdateReturningByPK executes an UPDATE by primary key for the columns
// selected by scope and returns the columns listed in retScope as a newly
// allocated *T.
//
//	updated, err := tbl.UpdateReturningByPK(ctx, dbw, &p, "price", velum.FullScope)
func (t *Table[T]) UpdateReturningByPK(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByPK())
	return cmd.QueryRow(ctx, q, row)
}

// UpdateReturning executes an UPDATE for the columns selected by scope,
// filtered by the provided SQL clause, and returns the columns listed in
// retScope as a newly allocated *T.
func (t *Table[T]) UpdateReturning(ctx context.Context, q QueryRowExecuter, row *T, scope, retScope Scope, clauses string) (*T, error) {
	cmd := t.cc.UpdateReturning(scope, retScope, ByClauses(clauses))
	return cmd.QueryRow(ctx, q, row)
}

// DeleteByPK executes a hard DELETE for the row with the given primary key.
// It does not use a RETURNING clause.
//
//	tbl.DeleteByPK(ctx, dbw, id)
//	// DELETE FROM tablename WHERE id=$1
func (t *Table[T]) DeleteByPK(ctx context.Context, q Executer, pk any) (Result, error) {
	return q.ExecContext(ctx, t.freqCmd.deleteByPK, pk)
}

// DeleteReturningByPK executes a hard DELETE by primary key and returns the
// deleted row via RETURNING FullScope as a newly allocated *T.
func (t *Table[T]) DeleteReturningByPK(ctx context.Context, q QueryRowExecuter, row *T) (*T, error) {
	cmd := t.cc.DeleteReturning(FullScope, t.wherePkClause)
	return cmd.QueryRow(ctx, q, row)
}

// Delete executes a hard DELETE filtered by the provided SQL clause.
// args are bound to the placeholders in clauses.
//
//	tbl.Delete(ctx, dbw, "WHERE expired_at < $1", time.Now())
//	// DELETE FROM tablename WHERE expired_at < $1
func (t *Table[T]) Delete(ctx context.Context, q Executer, clauses string, args ...any) (Result, error) {
	cmd := t.cc.Delete(clauses)
	return q.ExecContext(ctx, cmd.sql, args...)
}

// DeleteReturning executes a hard DELETE filtered by the provided SQL clause
// and returns the deleted row via RETURNING FullScope as a newly allocated *T.
func (t *Table[T]) DeleteReturning(ctx context.Context, q QueryRowExecuter, row *T, clauses string) (*T, error) {
	cmd := t.cc.DeleteReturning(FullScope, clauses)
	return cmd.QueryRow(ctx, q, row)
}

// SoftDeleteByPK sets the columns in the "delete" scope (e.g. deleted_at,
// deleted_by) using the values already in row, using the primary key as the
// WHERE condition. System "update" and "version" columns are also updated
// automatically (e.g. updated_at, row_version).
func (t *Table[T]) SoftDeleteByPK(ctx context.Context, q Executer, row *T) (Result, error) {
	cmd := t.cc.Update(DeleteScope, ByPK())
	return cmd.Exec(ctx, q, row)
}

// SoftDeleteReturningByPK performs a soft delete by primary key and returns
// all system-scope columns (version, insert, update, delete) from the updated
// row as a newly allocated *T.
func (t *Table[T]) SoftDeleteReturningByPK(ctx context.Context, q QueryRowExecuter, row *T) (*T, error) {
	return t.freqCmd.softDeleteByPK.QueryRow(ctx, q, row)
}

// Exist reports whether at least one row matching the provided SQL clause
// exists in the table.
//
//	ok, err := tbl.Exist(ctx, dbw, "WHERE email=$1", email)
//	// SELECT EXISTS(SELECT 1 FROM tablename WHERE email=$1)
func (t *Table[T]) Exist(ctx context.Context, q QueryRowExecuter, clauses string, args ...any) (bool, error) {
	return t.exist(ctx, q, Exist, clauses, args...)
}

// ExistByPK reports whether a row with the given primary key exists.
//
//	ok, err := tbl.ExistByPK(ctx, dbw, id)
//	// SELECT EXISTS(SELECT 1 FROM tablename WHERE id=$1)
func (t *Table[T]) ExistByPK(ctx context.Context, q QueryRowExecuter, pk any) (bool, error) {
	return t.exist(ctx, q, ExistByPK, t.wherePkClause, pk)
}

func (t *Table[T]) exist(ctx context.Context, q QueryRowExecuter, typ FunctionalCommandEnum, sql string, args ...any) (bool, error) {
	var result bool
	cmd := t.cc.Func(typ, sql)
	err := cmd.Call(ctx, q, &result, args...)
	return result, err
}

// Count returns the number of rows matching the provided SQL clause.
//
//	n, err := tbl.Count(ctx, dbw, "WHERE active=$1", true)
//	// SELECT COUNT(*) FROM tablename WHERE active=$1
func (t *Table[T]) Count(ctx context.Context, q QueryRowExecuter, clauses string, args ...any) (int, error) {
	var result int
	cmd := t.cc.Func(Count, clauses)
	err := cmd.Call(ctx, q, &result, args...)
	return result, err
}
