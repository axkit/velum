package velum

import "strings"

// clauseType is a type for the SQL text part.
type clauseType int

const (
	// ctColsCSV: id, name, age
	ctColsCSV clauseType = iota
	// ctColsPrefixedCSV: t.id, t.name, t.age
	ctColsPrefixedCSV
	// ctArgsInsert: $1, $2, $3 or nextval('seq'), $2, $3
	ctArgsInsert
	// ctUpdateByPK: name=$2, age=$3 (keep $1 for PK in the 'where cause')
	ctUpdateByPK
	// ctUpdate: name=$1, age=$2, ssn=$3 (no PK)
	ctUpdate

	ctRetsUpdateByPK

	ctRetsUpdate
	ctMax_
)

func (ct clauseType) String() string {
	switch ct {
	case ctColsCSV:
		return "ctColsCSV"
	case ctColsPrefixedCSV:
		return "ctPrefixedColsCSV"
	case ctArgsInsert:
		return "ctInsertArgs"
	case ctUpdateByPK:
		return "ctUpdateByPK"
	case ctUpdate:
		return "ctUpdate"
	default:
		return "unknown clause type"
	}
}

const scopeTagKey string = "scope"

// ScopeKey is a key for the scope map.
type ScopeKey struct {
	ct          clauseType
	withSysCols bool
	scope       Scope
}

// Clause is a value for the given scope and clause type.
type Clause struct {
	text string
	args [][]int
	fidx []int
}

func (c *Clause) join(text string, colPos int, args []int) {
	c.text = csvConcat(c.text, text)
	if colPos != -1 && len(args) > 0 {
		c.args = append(c.args, args)
		c.fidx = append(c.fidx, colPos)
	}
}

func isColumnNotInScope(col *Column, scope Scope) bool {
	if scope != FullScope {
		if strings.HasPrefix(string(scope), "!") {
			if col.Tag.PairExist(scopeTagKey, string(scope[1:])) {
				return true
			}
		} else if !col.Tag.PairExist(scopeTagKey, string(scope)) {
			return true
		}
	}
	return false
}

// ExtractScopeClauses extracts the clauses from the columns for the given scope for every clause type.
func ExtractScopeClauses(cols []Column, scope Scope, pk *SystemColumn, argNumerator ArgNumberBuilder) [int(ctMax_)]Clause {

	var c [int(ctMax_)]Clause

	insPos := 1
	if pk != nil {
		c[ctColsCSV] = Clause{text: pk.Name, fidx: []int{pk.Pos}, args: [][]int{pk.Path}}
		c[ctColsPrefixedCSV] = Clause{text: "t." + pk.Name, fidx: []int{pk.Pos}, args: [][]int{pk.Path}}
		c[ctArgsInsert] = Clause{
			text: InsertArgument(
				pk.ValueGenerationMethod,
				pk.ValueGenerator,
				argNumerator(1),
			)}

		c[ctUpdateByPK] = Clause{fidx: []int{pk.Pos}, args: [][]int{pk.Path}}

		if !pk.IsValueGeneratedByDB() {
			c[ctArgsInsert].args = append(c[ctArgsInsert].args, pk.Path)
			c[ctArgsInsert].fidx = append(c[ctArgsInsert].fidx, pk.Pos)
			insPos++
		}
	}

	isSystemScope := IsSystemScope(scope)

	for i := range cols {
		if pk != nil && pk.Pos == i {
			continue
		}

		col := &cols[i]

		if isColumnNotInScope(col, scope) {
			continue
		}

		// isSystemCol := col.IsSystem()

		c[ctColsCSV].join(col.Name, i, col.Path)
		c[ctColsPrefixedCSV].join("t."+col.Name, i, col.Path)
		c[ctArgsInsert].join(argNumerator(insPos), i, col.Path)
		insPos++

		if !isSystemScope {
			switch {
			case col.Tag.PairExist(scopeTagKey, string(VersionField)):
				c[ctUpdateByPK].join(col.Name+"="+col.Name+"+1", -1, col.Path)
				c[ctUpdate].join(col.Name+"="+col.Name+"+1", -1, col.Path)

			case col.Tag.PairExist(scopeTagKey, string(DeleteScope)),
				col.Tag.PairExist(scopeTagKey, string(InsertScope)):
				break
			default:
				c[ctUpdateByPK].join(col.Name+"="+argNumerator(len(c[ctUpdateByPK].args)+1), i, col.Path)
				c[ctUpdate].join(col.Name+"="+argNumerator(len(c[ctUpdate].args)+1), i, col.Path)
			}
		}
	}

	return c
}

func csvConcat(csvLine, column string) string {
	if csvLine != "" {
		csvLine += ","
	}
	csvLine += column
	return csvLine
}
