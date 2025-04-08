package velum

import (
	"context"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

var DefaultFieldTag = "dbw"

// DefaultColumnNameBuilder refers to the function that converts the
// field name to the column name.
var DefaultColumnNameBuilder = ToSnakeCase

// DefaultParamPlaceholderBuilder refers to the function that converts the
// argument position to the placeholder in the SQL query.
var DefaultParamPlaceholderBuilder = ArgAsNumber

// DefaultFriendlySequenceNameBuilder refers to the function that converts the
// table name to the sequence name.
//
// Usually the sequence naming convention is <table_name>_seq, etc.
var DefaultFriendlySequenceNameBuilder = TableWithSeqSuffix

// Scope defines the group of the fields in the struct to be
// used in the query operation.
type Scope string

const (
	EmptyScope Scope = ""

	// FullScope is the scope that includes all fields, including system fields.
	FullScope Scope = "*"

	scopeTagKey string = "scope"
)

var (
	TagKey                      = "dbw"
	VersionField          Scope = "version"
	InsertScope           Scope = "insert"
	UpdateScope           Scope = "update"
	DeleteScope           Scope = "delete"
	PrimaryKeyTagOption         = "pk"
	StandardPrimaryKeyCol       = "id"
	SystemScope           Scope = "system"
)

// SystemScopes holds the system scopes.
var SystemScopes = []*Scope{&VersionField, &InsertScope, &UpdateScope, &DeleteScope}

// IsSystemScope returns true if the scope is a system scope.
func IsSystemScope(s Scope) bool {
	for _, sys := range SystemScopes {
		if *sys == s {
			return true
		}
	}
	return false
}

// ColumnValueGenMethod defines the method to generate the value of a field.
type ColumnValueGenMethod string

const (
	// Default sequence generation method. It is used when no sequence generation method is defined.
	// The function SequenceNameBuilder is used to generate sequence name by table name.
	FriendlySequence ColumnValueGenMethod = ""

	// SerialFieleType says the field is serial and it is generated by database.
	SerialFieleType ColumnValueGenMethod = "serial"

	// UuidFileType says the field is uuid and it is generated by database.
	UuidFileType ColumnValueGenMethod = "uuid"

	// NoSequence says the field value is generated and assigned by application.
	NoSequence ColumnValueGenMethod = "no"

	// CustomSequece holds the sequence name to be used for the field in insert operation.
	CustomSequece ColumnValueGenMethod = "customseq"
)

type Rows interface {
	// Close closes the rows, making the connection ready for use again. It is safe
	// to call Close after rows is already closed.
	Close() error

	// Err returns any error that occurred while reading. Err must only be called after the Rows is closed (either by
	// calling Close or by Next returning false). If it is called early it may return nil even if there was an error
	// executing the query.
	Err() error

	// Next prepares the next row for reading. It returns true if there is another
	// row and false if no more rows are available or a fatal error has occurred.
	// It automatically closes rows when all rows are read.
	//
	// Callers should check rows.Err() after rows.Next() returns false to detect
	// whether result-set reading ended prematurely due to an error. See
	// Conn.Query for details.
	//
	// For simpler error handling, consider using the higher-level pgx v5
	// CollectRows() and ForEachRow() helpers instead.
	Next() bool

	// Scan reads the values from the current row into dest values positionally.
	// dest can include pointers to core types, values implementing the Scanner
	// interface, and nil. nil will skip the value entirely. It is an error to
	// call Scan without first calling Next() and checking that it returned true.
	Scan(dest ...any) error
}

type Row interface {
	Scan(args ...any) error
	Err() error
}

type Result interface {
	// LastInsertId returns the integer generated by the database
	// in response to a command. Typically this will be from an
	// "auto increment" column when inserting a new row. Not all
	// databases support this feature, and the syntax of such
	// statements varies.
	//LastInsertId() (int64, error)

	// RowsAffected returns the number of rows affected by an
	// update, insert, or delete. Not every database or database
	// driver may support this.
	RowsAffected() (int64, error)
}

type QueryExecuter interface {
	QueryContext(ctx context.Context, sql string, args ...any) (Rows, error)
}

type QueryRowExecuter interface {
	QueryRowContext(ctx context.Context, sql string, args ...any) Row
}

type Executer interface {
	ExecContext(ctx context.Context, sql string, args ...any) (Result, error)
}

// IsolationLevel is the transaction isolation level used in TxOptions.
type IsolationLevel int

const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// ToSnakeCase takes 'customer_id' if attribute tag is `dbw:"name=customer_id"`, otherwise
// it converts the attribute name to snake case: CustomerID int -> customer_id.
func ToSnakeCase(attr, tag string) string {
	if tag != "" {
		from := strings.Index(tag, "name=")
		if from != -1 {
			from += 5
			to := strings.Index(tag[from:], ",")
			if to != -1 {
				return tag[from : from+to]
			}
			return tag[from:]
		}
	}
	snake := matchFirstCap.ReplaceAllString(attr, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type ArgFormatter func(i int) string

func ArgAsNumber(i int) string {
	return "$" + strconv.Itoa(i)
}

func ArgAsQuestionMark(i int) string {
	return "?"
}

func TableWithSeqSuffix(tablename string) string {
	return tablename + "_seq"
}

// ShiftParamPositions replaces "id = $1 OR id > $1 AND name < $2"
// with "id = $10 OR id > $10 AND name < $11" if fromIndex is 10.
func ShiftParamPositions(sqlWhere string, fromIndex int) string {
	// Regular expression to match placeholders like $1, $2, etc.
	re := regexp.MustCompile(`\$(\d+)`)

	// Replace each placeholder with the incremented index
	result := re.ReplaceAllStringFunc(sqlWhere, func(match string) string {
		// Extract the number from the placeholder
		num, _ := strconv.Atoi(strings.TrimPrefix(match, "$"))
		// Increment the number by the fromIndex
		return "$" + strconv.Itoa(num+fromIndex-1)
	})

	return result
}

// StructPluralName converts the struct name to the table name.
// By default, it converts the struct name to snake case and
// pluralizes it.
func StructPluralName(v any) string {
	name := reflect.TypeOf(v).Name()
	snake := matchFirstCap.ReplaceAllString(name, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(ToPluralName(snake))
}

func ToPluralName(s string) string {
	if strings.HasSuffix(s, "z") {
		return s + "zes" // Add 'zes' for these suffixes
	}
	if strings.HasSuffix(s, "s") || strings.HasSuffix(s, "x") ||
		strings.HasSuffix(s, "ch") ||
		strings.HasSuffix(s, "sh") {
		return s + "es" // Add 'es' for these suffixes
	}
	return s + "s" // Default pluralization by adding 's'
}

// IsTagOptionExists checks if the tag contains the tagOption.
// dbw:"name=customer_id,pk" -> IsTagOptionExists("name=customer_id,pk", "pk") returns true.
// dbw:"name=customer_id,pk" -> IsTagOptionExists("name=customer_id,pk", "name") returns true.
func IsTagOptionExist(tag string, tagOption string) bool {

	hp := tagOption + ","
	hs := "," + tagOption
	co := "," + tagOption + ","

	return tag == tagOption ||
		strings.HasPrefix(tag, hp) ||
		strings.HasSuffix(tag, hs) ||
		strings.Contains(tag, co)
}

type Transaction interface {
	Executer
	QueryRowExecuter
	QueryExecuter
	Commit(context.Context) error
	Rollback(context.Context) error
}

type DatabaseWrapper interface {
	Executer
	QueryRowExecuter
	QueryExecuter
	IsNotFound(err error) bool
	Begin(context.Context) (Transaction, error)
	InTx(context.Context, func(Transaction) error) error
}
