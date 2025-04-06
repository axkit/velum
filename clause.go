package velum

// clauseType is a type for the SQL text part.
type clauseType uint8

const (
	// ctColsCSV: id, name, age
	ctColsCSV clauseType = iota
	// ctColsPrefixedCSV: t.id, t.name, t.age
	ctColsPrefixedCSV
	// ctArgsInsert: $1, $2, $3 or nextval('seq'), $2, $3, or DEFAULT, $1, $2, etc.
	ctArgsInsert
	// ctColsUpdateByPK: name=$2, age=$3 (keeps $1 for PK in the 'where cause')
	ctColsUpdateByPK
	// ctColsUpdate: name=$1, age=$2, ssn=$3 (no PK)
	ctColsUpdate
)

// String returns the string code of the clause type.
func (ct clauseType) String() string {
	switch ct {
	case ctColsCSV:
		return "ctColsCSV"
	case ctColsPrefixedCSV:
		return "ctColsPrefixedCSV"
	case ctArgsInsert:
		return "ctArgsInsert"
	case ctColsUpdateByPK:
		return "ctColsUpdateByPK"
	case ctColsUpdate:
		return "ctColsUpdate"
	default:
		return "unknown clause type"
	}
}

// scopeKey is a key for the scope map.
type scopeKey struct {
	scope Scope
	ct    clauseType
}

// clause is a value for the given scope and clause type.
// It holds the SQL text and the column positions to be used in the
// SQL statement as arguments or reading/writing the values from/to the struct.
type clause struct {
	text string
	cpos []int
	typ  clauseType
}

type Tabler interface {
	Name() string
	Columns() []Column
	PK() *SystemColumn
	FormatArg(int) string
}

// newClause builds the SQL clause for the given scopes and clause type.
// It takes primary key and system columns into account if present.
func newClause(typ clauseType, t Tabler, ss scopeSet) clause {

	pk := t.PK()
	pkPos := -1
	c := clause{typ: typ}
	if pk != nil {
		pkArg := t.FormatArg(1) // PK is always the first argument in the SQL statements.
		c = newClauseWithPK(typ, pk, pkArg)
		pkPos = pk.Pos
	}

	cols := t.Columns()

	if ss.all {
		// if scope is all (*)
		for i := range cols {
			if pkPos != i {
				c.addColumn(&cols[i], i, t.FormatArg(c.len()+1))
			}
		}
		return c
	}

	for i := range cols {
		if pk.Pos == i {
			continue
		}
		col := &cols[i]
		if isColumnInScopes(col, ss) {
			c.addColumn(col, i, t.FormatArg(c.len()+1))
		}
	}
	return c
}

func newClauseWithPK(ct clauseType, pk *SystemColumn, pkArgValue string) clause {

	switch ct {
	case ctColsCSV:
		return clause{typ: ct, text: pk.Name, cpos: []int{pk.Pos}}
	case ctColsPrefixedCSV:
		return clause{typ: ct, text: "t." + pk.Name, cpos: []int{pk.Pos}}
	case ctArgsInsert:
		c := clause{
			typ: ct,
			text: InsertArgument(
				pk.ValueGenerationMethod,
				pk.ValueGenerator,
				pkArgValue,
			)}

		if !pk.IsValueGeneratedByDB() {
			c.cpos = append(c.cpos, pk.Pos)
		}
		return c
	case ctColsUpdateByPK:
		return clause{typ: ct, cpos: []int{pk.Pos}}
	case ctColsUpdate:
		return clause{typ: ct}
	}

	panic("unknown clause type")
}

// join appends the text to the existing text.
// If colPos is not -1, it appends it to the fidx slice.
func (c *clause) join(text string, colPos int) {
	c.text = csvConcat(c.text, text)
	if colPos != -1 {
		c.cpos = append(c.cpos, colPos)
	}
}

func (c *clause) len() int {
	return len(c.cpos)
}

func (c *clause) addColumn(col *Column, colPos int, arg string) {
	switch c.typ {
	case ctColsCSV:
		c.join(col.Name, colPos)
	case ctColsPrefixedCSV:
		c.join("t."+col.Name, colPos)
	case ctArgsInsert:
		c.join(arg, colPos)
	case ctColsUpdateByPK, ctColsUpdate:
		if col.Tag.PairExist(scopeTagKey, string(VersionField)) {
			c.join(col.Name+"="+col.Name+"+1", -1)
			break
		}
		c.join(col.Name+"="+arg, colPos)
	}
}

// isColumnInScopes returns true if the column satisfies the scopes.
func isColumnInScopes(col *Column, ss scopeSet) bool {

	result := false
	for _, s := range ss.direct {
		if !result && col.Tag.PairExist(scopeTagKey, string(s)) {
			result = true
			break
		}
	}

	for _, s := range ss.system {
		if !result && col.Tag.PairExist(scopeTagKey, string(s)) {
			result = true
			break
		}
	}

	if col.IsSystem() {
		return result
	}
	for _, s := range ss.negated {

		if col.Tag.PairExist(scopeTagKey, string(s)) {
			result = false
			continue
		}
		result = true
	}

	return result
}

// csvConcat concatenates the text with a comma.
func csvConcat(csvLine, column string) string {
	if csvLine != "" {
		csvLine += ","
	}
	csvLine += column
	return csvLine
}
