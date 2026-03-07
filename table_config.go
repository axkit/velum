package velum

// TableConfig holds the configuration options for a Table. It is populated
// by applying TableOption functions passed to NewTable.
type TableConfig struct {
	tag            string
	argFormatter   ArgFormatter
	colNameBuilder func(attr, tag string) string
	seqNameBuilder func(string) string
}

// TableOption is a functional option that modifies a TableConfig.
// Pass options to NewTable to customise column-name derivation, struct-tag
// keys, SQL placeholder format, and sequence naming.
type TableOption func(*TableConfig)

// WithTag changes the struct tag key used to read column metadata from T.
// The default key is DefaultFieldTag ("dbw").
//
//	tbl := velum.NewTable[Customer]("customers", velum.WithTag("db"))
func WithTag(tag string) TableOption {
	return func(o *TableConfig) {
		o.tag = tag
	}
}

// WithArgFormatter sets the function that renders positional SQL parameter
// placeholders. The default is ArgAsNumber, which produces "$1", "$2", etc.
// Use ArgAsQuestionMark for drivers that accept "?" placeholders (MySQL-style).
//
//	tbl := velum.NewTable[T]("t", velum.WithArgFormatter(velum.ArgAsQuestionMark))
func WithArgFormatter(f ArgFormatter) TableOption {
	return func(o *TableConfig) {
		o.argFormatter = f
	}
}

// WithColumnNameBuilder sets the function that derives a database column name
// from a struct field name and its raw tag string. The default is ToSnakeCase,
// which converts CamelCase field names to snake_case and honours the
// "name=col_name" tag option.
//
//	tbl := velum.NewTable[T]("t", velum.WithColumnNameBuilder(myNamer))
func WithColumnNameBuilder(f func(attr string, tag string) string) TableOption {
	return func(o *TableConfig) {
		o.colNameBuilder = f
	}
}

// WithSequenceNameBuilder sets the function that derives a PostgreSQL sequence
// name from the table name. The default is TableWithSeqSuffix, which appends
// "_seq" to the table name. Override this when your sequences follow a
// different naming convention.
//
//	tbl := velum.NewTable[T]("orders", velum.WithSequenceNameBuilder(func(t string) string {
//	    return t + "_id_seq"
//	}))
func WithSequenceNameBuilder(f func(string) string) TableOption {
	return func(o *TableConfig) {
		o.seqNameBuilder = f
	}
}
