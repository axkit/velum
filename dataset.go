package velum

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"

	"github.com/axkit/velum/reflectx"
)

// ClauseSet represents a collection of placeholder values keyed by their placeholder name.
type ClauseSet map[string]string

// DatasetTailClause is the special clause name used to append SQL after the template if not any placeholders are defined.
const DatasetTailClause = "query_tail_clause"

type datasetTemplate struct {
	// statement is the original SQL template.
	statement string
	// segments holds the SQL parts between the placeholders.
	segments []string
	// placeholders holds the names of the placeholders in order of appearance.
	placeholders []string
	// baseMaxArg is the highest argument index found in the original statement.
	baseMaxArg int
}

type datasetConfig struct {
	// tag is the struct tag used to extract column metadata.
	tag string
	// colNameBuilder is the function used to map struct fields to column names.
	colNameBuilder func(string, string) string
}

// DatasetOption configures Dataset creation.
type DatasetOption func(*datasetConfig)

// WithDatasetTag overrides the struct tag used to extract column metadata.
func WithDatasetTag(tag string) DatasetOption {
	return func(cfg *datasetConfig) {
		cfg.tag = tag
	}
}

// WithDatasetColumnNameBuilder overrides the function used to map struct fields to column names.
func WithDatasetColumnNameBuilder(fn func(string, string) string) DatasetOption {
	return func(cfg *datasetConfig) {
		cfg.colNameBuilder = fn
	}
}

// Dataset allows executing arbitrary SELECT statements (joins, aggregates, etc.)
// while keeping the ability to tweak SQL clauses identified by placeholders.
type Dataset[T any] struct {
	template datasetTemplate
	sfpe     StructFieldPtrExtractor[T]
	cpos     []int

	defaults ClauseSet

	cmd SelectCommand[T]
}

// DatasetQuery represents a compiled dataset statement bound to a ClauseSet.
// It can be executed multiple times with different arguments without rebuilding SQL.
type DatasetQuery[T any] struct {
	cmd SelectCommand[T]
}

// NewDataset creates a Dataset bound to the provided SELECT statement.
func NewDataset[T any](statement string, opts ...DatasetOption) *Dataset[T] {
	cfg := datasetConfig{
		tag:            DefaultFieldTag,
		colNameBuilder: DefaultColumnNameBuilder,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	var zero T
	structFields := reflectx.ExtractStructFields(&zero, cfg.tag)
	columns := buildColumnsFromFields(structFields, cfg.colNameBuilder)

	cpos, err := datasetColumnPositions(columns)
	if err != nil {
		panic(err)
	}

	template := newDatasetTemplate(strings.TrimSpace(statement))

	return &Dataset[T]{
		template: template,
		sfpe:     newPointerSlicePool[T](columns),
		cpos:     cpos,
		defaults: make(ClauseSet, 0),
	}
}

// Statement returns the original template statement.
func (ds *Dataset[T]) Statement() string {
	return ds.template.statement
}

// CommandWithClauses builds (or reuses) a SelectCommand for the provided ClauseSet.
// Unknown clause names panic.
func (ds *Dataset[T]) CommandWithClauses(cs ClauseSet) SelectCommand[T] {

	sql := ds.template.render(cs)
	return SelectCommand[T]{
		sql:  sql,
		cpos: ds.cpos,
		sfpe: ds.sfpe,
	}

}

// Command keeps backward compatibility by appending the provided clauses after the statement.
func (ds *Dataset[T]) Command(tailClause string) SelectCommand[T] {
	return ds.CommandWithClauses(ClauseSet{DatasetTailClause: tailClause})
}

// Get executes using the default clause set (if any).
func (ds *Dataset[T]) Get(ctx context.Context, q QueryRowExecuter, args ...any) (*T, error) {
	cmd := ds.CommandWithClauses(nil)
	return cmd.Get(ctx, q, args...)
}

// Select executes using the default clause set (if any) and returns all rows.
func (ds *Dataset[T]) Select(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {
	cmd := ds.CommandWithClauses(nil)
	return cmd.GetMany(ctx, q, args...)
}

// WithClauses creates a reusable DatasetQuery bound to the provided ClauseSet.
func (ds *Dataset[T]) WithClauses(clauses ClauseSet) DatasetQuery[T] {
	return DatasetQuery[T]{cmd: ds.CommandWithClauses(clauses)}
}

// WithNamedClause creates a reusable DatasetQuery for a single placeholder.
func (ds *Dataset[T]) WithNamedClause(name, clause string) DatasetQuery[T] {
	return ds.WithClauses(ClauseSet{name: clause})
}

// WithTailClause creates a reusable DatasetQuery for a single placeholder.
func (ds *Dataset[T]) WithTailClause(clause string) DatasetQuery[T] {
	return ds.WithClauses(ClauseSet{DatasetTailClause: clause})
}

// Get executes the compiled query.
func (dq DatasetQuery[T]) Get(ctx context.Context, q QueryRowExecuter, args ...any) (*T, error) {
	return dq.cmd.Get(ctx, q, args...)
}

// Select executes the compiled query and returns all rows.
func (dq DatasetQuery[T]) Select(ctx context.Context, q QueryExecuter, args ...any) ([]T, error) {
	return dq.cmd.GetMany(ctx, q, args...)
}

// mergeClauses combines defaults with overrides and validates placeholder names.
func (ds *Dataset[T]) mergeClauses(overrides ClauseSet) ClauseSet {
	if len(overrides) == 0 {
		return ds.defaults
	}

	result := make(ClauseSet, len(ds.defaults)+len(overrides))
	maps.Copy(result, ds.defaults)

	for k, v := range overrides {
		if !ds.template.isValidPlaceholder(k) && k != DatasetTailClause {
			panic(fmt.Sprintf("dataset: unknown clause %q", k))
		}
		result[k] = v
	}
	return result
}

func (ds *Dataset[T]) clauseKey(clauses ClauseSet) string {
	var b strings.Builder
	for _, name := range ds.template.placeholders {
		b.WriteString(name)
		b.WriteByte('=')
		if val, ok := clauses[name]; ok {
			b.WriteString(val)
		}
		b.WriteByte(0)
	}
	b.WriteString(DatasetTailClause)
	b.WriteByte('=')
	if val, ok := clauses[DatasetTailClause]; ok {
		b.WriteString(val)
	}
	return b.String()
}

func (tpl *datasetTemplate) render(clauses ClauseSet) string {
	var b strings.Builder
	b.WriteString(tpl.segments[0])

	nextArg := tpl.baseMaxArg + 1

	for i, name := range tpl.placeholders {
		if name == DatasetTailClause {
			b.WriteByte(' ')

			continue
		}
		clause := strings.TrimSpace(clauses[name])
		adjusted, consumed := normalizeClause(clause, nextArg)
		b.WriteString(adjusted)
		nextArg += consumed
		b.WriteString(tpl.segments[i+1])
	}

	if tail := strings.TrimSpace(clauses[DatasetTailClause]); tail != "" {
		if b.Len() > 0 && !startsWithWhitespace(tail) {
			b.WriteByte(' ')
		}
		adjusted, _ := normalizeClause(tail, nextArg)
		b.WriteString(adjusted)
	}
	return b.String()
}

func (tpl *datasetTemplate) isValidPlaceholder(name string) bool {
	for _, placeholder := range tpl.placeholders {
		if placeholder == name {
			return true
		}
	}
	return false
}

func newDatasetTemplate(statement string) datasetTemplate {
	prefix, suffix := `/*`, `*/`
	var segments []string
	var placeholders []string
	var current strings.Builder

	remaining := statement
	for {
		idx := strings.Index(remaining, prefix)
		if idx == -1 {
			current.WriteString(remaining)
			break
		}

		current.WriteString(remaining[:idx])
		remaining = remaining[idx+len(prefix):]

		end := strings.Index(remaining, suffix)
		if end == -1 {
			panic("velum: unmatched placeholder in statement")
		}

		raw := remaining[:end]
		name := strings.TrimSpace(raw)
		if name == "" {
			panic("velum: empty placeholder name")
		}

		if !isPlaceholderToken(name) {
			current.WriteString(prefix)
			current.WriteString(raw)
			current.WriteString(suffix)
			remaining = remaining[end+len(suffix):]
			continue
		}

		segments = append(segments, current.String())
		current.Reset()
		placeholders = append(placeholders, name)
		remaining = remaining[end+len(suffix):]
	}

	segments = append(segments, current.String())

	if len(placeholders) == 0 {
		// no placeholders found, use tail clause only
		placeholders = []string{DatasetTailClause}
		segments = []string{statement, ""}
	}

	return datasetTemplate{
		statement:    statement,
		segments:     segments,
		placeholders: placeholders,
		baseMaxArg:   maxPlaceholderIndex(statement),
	}
}

func datasetColumnPositions(columns []Column) ([]int, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("dataset: struct must have at least one exported field")
	}

	m := make(map[string]int, len(columns))
	for i, col := range columns {
		m[col.Name] = i
	}

	cpos := make([]int, len(columns))
	for i, col := range columns {
		pos, ok := m[col.Name]
		if !ok {
			return nil, fmt.Errorf("dataset: column %q not found in struct definition", col)
		}
		cpos[i] = pos
	}
	return cpos, nil
}

var placeholderRegexp = regexp.MustCompile(`\$(\d+)`)

func maxPlaceholderIndex(sql string) int {
	matches := placeholderRegexp.FindAllStringSubmatch(sql, -1)
	max := 0
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		val, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		if val > max {
			max = val
		}
	}
	return max
}

func normalizeClause(clause string, nextArg int) (string, int) {
	maxIdx := maxPlaceholderIndex(clause)
	if maxIdx == 0 || nextArg <= 1 {
		return clause, maxIdx
	}
	return ShiftParamPositions(clause, nextArg), maxIdx
}

func isPlaceholderToken(name string) bool {
	if name == "" {
		return false
	}
	if strings.ContainsAny(name, " \n\t\r") {
		return false
	}
	for _, r := range name {
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		return false
	}
	return true
}

func startsWithWhitespace(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case ' ', '\n', '\t', '\r':
		return true
	default:
		return false
	}
}
