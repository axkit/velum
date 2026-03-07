package velum

import (
	"github.com/axkit/velum/reflectx"
)

// Column describes a single database column derived from an exported struct field.
type Column struct {
	// Name is the column name as it appears in SQL statements.
	Name string
	// Path is the index path to the field within the struct hierarchy.
	// For a top-level field it contains a single index; for an embedded struct
	// field it contains the parent index followed by the field index.
	Path []int
	// Tag holds the parsed key-value pairs from the struct's dbw tag.
	Tag reflectx.TagPairs
	// ValueGenerationMethod describes how the column's value is produced on
	// INSERT (relevant only for primary key columns).
	ValueGenerationMethod PkColumnValueGenMethod
	// ValueGenerator holds the sequence name or function expression used when
	// ValueGenerationMethod is FriendlySequence or CustomSequence.
	ValueGenerator string
}

// SystemColumn wraps a Column with its position in the parent columns slice.
// It is used for primary keys and for columns assigned to system scopes
// (version, insert, update, delete).
type SystemColumn struct {
	*Column
	// Pos is the zero-based index of the column in the Table's columns slice.
	Pos int
}

// IsValueGeneratedByDB reports whether the column's value is assigned by the
// database on INSERT (SERIAL, UUID, friendly sequence, or custom sequence).
// When true, the column is omitted from the INSERT argument list.
func (c *Column) IsValueGeneratedByDB() bool {
	v := c.ValueGenerationMethod
	return v == SerialFieldType || v == UuidFieldType || v == FriendlySequence || v == CustomSequence
}

// IsSystem reports whether the column belongs to at least one system scope
// (version, insert, update, or delete). System columns are managed by Velum
// automatically and follow special inclusion rules per statement type.
func (c *Column) IsSystem() bool {
	for _, s := range c.Tag.Get(scopeTagKey) {
		if IsSystemScope(Scope(s)) {
			return true
		}
	}
	return false
}

// InsertArgument returns the SQL fragment used for this column in the VALUES
// clause of an INSERT statement. For database-generated values it returns the
// appropriate expression (DEFAULT, gen_random_uuid(), nextval(...)); for
// application-provided values it returns regularParam (e.g. "$1").
func InsertArgument(genMethod PkColumnValueGenMethod, valueGenerator string, regularParam string) (sqlParam string) {
	switch genMethod {
	case SerialFieldType:
		return "DEFAULT"
	case UuidFieldType:
		return "gen_random_uuid()"
	case NoSequence:
		return regularParam
	}
	// CustomSequence and FriendlySequence both use nextval.
	return "nextval('" + valueGenerator + "')"
}

func pkColValueGenMethod(genOptVal string, friendlySequence string) (method PkColumnValueGenMethod, value string) {
	if genOptVal == "" {
		return FriendlySequence, friendlySequence
	}
	return colValueGenMethod(genOptVal)
}

func colValueGenMethod(genOptVal string) (method PkColumnValueGenMethod, value string) {
	switch PkColumnValueGenMethod(genOptVal) {
	case SerialFieldType:
		return SerialFieldType, "DEFAULT"
	case UuidFieldType:
		return UuidFieldType, "gen_random_uuid()"
	case NoSequence:
		return NoSequence, ""
	case "":
		return NoSequence, ""
	}
	return CustomSequence, genOptVal
}

// buildColumnsFromFields converts the extracted struct field descriptors into
// Column values, applying the column-name builder and ensuring every column
// implicitly belongs to FullScope ("*").
func buildColumnsFromFields(structFields []reflectx.StructField, colNameBuilder func(string, string) string) []Column {
	columns := make([]Column, len(structFields))
	for i, sf := range structFields {
		ptag := reflectx.ParseTagPairs(sf.Tag, scopeTagKey)
		ptag.Add(scopeTagKey, string(FullScope))

		columns[i] = Column{
			Path: sf.Path,
			Name: colNameBuilder(sf.Name, sf.Tag),
			Tag:  ptag,
		}
	}
	return columns
}

// newPointerSlicePool builds a PointerSlicePool sized to hold a pointer for
// every column, with a small extra capacity for typical system-column overhead.
func newPointerSlicePool[T any](columns []Column) *reflectx.PointerSlicePool[T] {
	fic := reflectx.NewFieldIndexContainer(len(columns) + 2)
	for i := range columns {
		fic.Add(columns[i].Path)
	}
	return reflectx.NewPointerSlicePool[T](fic)
}
