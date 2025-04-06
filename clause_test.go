package velum

import (
	"reflect"
	"testing"

	"github.com/axkit/velum/reflectx"
)

func Test_newClauseWithPK(t *testing.T) {
	pkSerial := &SystemColumn{
		Column: &Column{
			Name:                  "id",
			Path:                  []int{0},
			Tag:                   reflectx.TagPairs{"scope": {"pk"}},
			ValueGenerationMethod: SerialFieleType,
		},
		Pos: 0,
	}

	pkManual := &SystemColumn{
		Column: &Column{
			Name:                  "id",
			Path:                  []int{0},
			Tag:                   reflectx.TagPairs{"scope": {"pk"}},
			ValueGenerationMethod: NoSequence,
		},
	}
	testManualPK := []struct {
		name      string
		clauseTyp clauseType
		pkArg     string
		want      clause
	}{
		{
			name:      "ctArgsInsert with manual value",
			clauseTyp: ctArgsInsert,
			pkArg:     "$1",
			want: clause{
				typ:  ctArgsInsert,
				text: "$1",
				cpos: []int{0},
			},
		},
	}

	testSerialPK := []struct {
		name      string
		clauseTyp clauseType
		pkArg     string
		want      clause
	}{
		{
			name:      "ctColsCSV",
			clauseTyp: ctColsCSV,
			pkArg:     "$1",
			want: clause{
				typ:  ctColsCSV,
				text: "id",
				cpos: []int{0},
			},
		},
		{
			name:      "ctColsPrefixedCSV",
			clauseTyp: ctColsPrefixedCSV,
			pkArg:     "$1",
			want: clause{
				typ:  ctColsPrefixedCSV,
				text: "t.id",
				cpos: []int{0},
			},
		},
		{
			name:      "ctArgsInsert with generated value",
			clauseTyp: ctArgsInsert,
			pkArg:     "$1",
			want: clause{
				typ:  ctArgsInsert,
				text: "DEFAULT",
			},
		},

		{
			name:      "ctColsUpdateByPK",
			clauseTyp: ctColsUpdateByPK,
			pkArg:     "$1",
			want: clause{
				typ:  ctColsUpdateByPK,
				cpos: []int{0},
			},
		},
		{
			name:      "ctColsUpdate",
			clauseTyp: ctColsUpdate,
			pkArg:     "$1",
			want: clause{
				typ: ctColsUpdate,
			},
		},
	}

	for _, tt := range testSerialPK {
		t.Run(tt.name, func(t *testing.T) {
			got := newClauseWithPK(tt.clauseTyp, pkSerial, tt.pkArg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newClauseWithPK() = %#v, want %#v", got, tt.want)
			}
		})
	}

	for _, tt := range testManualPK {
		t.Run(tt.name, func(t *testing.T) {
			got := newClauseWithPK(tt.clauseTyp, pkManual, tt.pkArg)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newClauseWithPK() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_clause_join(t *testing.T) {
	c := clause{}
	c.join("id", 0)
	c.join("name", 1)

	want := clause{
		text: "id,name",
		cpos: []int{0, 1},
	}

	if !reflect.DeepEqual(c, want) {
		t.Errorf("clause.join() = %#v, want %#v", c, want)
	}
}

func Test_clause_len(t *testing.T) {
	c := clause{
		cpos: []int{0, 1, 2},
	}

	if got := c.len(); got != 3 {
		t.Errorf("clause.len() = %v, want %v", got, 3)
	}
}

func Test_clause_addColumn(t *testing.T) {
	col := &Column{
		Name: "name",
		Tag:  reflectx.TagPairs{"scope": {"update"}},
	}

	tests := []struct {
		name      string
		clauseTyp clauseType
		colPos    int
		arg       string
		want      clause
	}{
		{
			name:      "ctColsCSV",
			clauseTyp: ctColsCSV,
			colPos:    1,
			arg:       "$1",
			want: clause{
				typ:  ctColsCSV,
				text: "name",
				cpos: []int{1},
			},
		},
		{
			name:      "ctColsPrefixedCSV",
			clauseTyp: ctColsPrefixedCSV,
			colPos:    1,
			arg:       "$1",
			want: clause{
				typ:  ctColsPrefixedCSV,
				text: "t.name",
				cpos: []int{1},
			},
		},
		{
			name:      "ctArgsInsert",
			clauseTyp: ctArgsInsert,
			colPos:    1,
			arg:       "$1",
			want: clause{
				typ:  ctArgsInsert,
				text: "$1",
				cpos: []int{1},
			},
		},
		{
			name:      "ctColsUpdate",
			clauseTyp: ctColsUpdate,
			colPos:    1,
			arg:       "$1",
			want: clause{
				typ:  ctColsUpdate,
				text: "name=$1",
				cpos: []int{1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := clause{typ: tt.clauseTyp}
			c.addColumn(col, tt.colPos, tt.arg)
			if !reflect.DeepEqual(c, tt.want) {
				t.Errorf("clause.addColumn() = %#v, want %#v", c, tt.want)
			}
		})
	}
}

func Test_csvConcat(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		column string
		want   string
	}{
		{
			name:   "Empty line",
			line:   "",
			column: "id",
			want:   "id",
		},
		{
			name:   "Non-empty line",
			line:   "id",
			column: "name",
			want:   "id,name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := csvConcat(tt.line, tt.column); got != tt.want {
				t.Errorf("csvConcat() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_isColumnInScopes(t *testing.T) {
	// tests := []struct {
	// 	name        string
	// 	column      *Column
	// 	scopes      []Scope
	// 	directScope bool
	// 	want        bool
	// }{
	// 	{
	// 		name: "Column matches direct scope",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"update"}},
	// 		},
	// 		scopes:      []Scope{"update"},
	// 		directScope: true,
	// 		want:        true,
	// 	},
	// 	{
	// 		name: "Column does not match direct scope",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"insert"}},
	// 		},
	// 		scopes:      []Scope{"update"},
	// 		directScope: true,
	// 		want:        false,
	// 	},
	// 	{
	// 		name: "Column matches negated scope",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"update"}},
	// 		},
	// 		scopes:      []Scope{"update"},
	// 		directScope: false,
	// 		want:        false,
	// 	},
	// 	{
	// 		name: "Column does not match negated scope",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"user"}},
	// 		},
	// 		scopes:      []Scope{"ssn"},
	// 		directScope: false,
	// 		want:        true,
	// 	},
	// 	{
	// 		name: "Column matches one of multiple scopes",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"update"}},
	// 		},
	// 		scopes:      []Scope{"insert", "update"},
	// 		directScope: true,
	// 		want:        true,
	// 	},
	// 	{
	// 		name: "Column does not match any scope",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{scopeTagKey: {"delete"}},
	// 		},
	// 		scopes:      []Scope{"insert", "update"},
	// 		directScope: true,
	// 		want:        false,
	// 	},
	// 	{
	// 		name: "Column with no scopes",
	// 		column: &Column{
	// 			Tag: reflectx.TagPairs{},
	// 		},
	// 		scopes:      []Scope{"update"},
	// 		directScope: true,
	// 		want:        false,
	// 	},
	// }

	// for _, tt := range tests {
	// 	t.Run(tt.name, func(t *testing.T) {
	// 		got := isColumnInScopes(tt.column, tt.scopes)
	// 		if got != tt.want {
	// 			t.Errorf("isColumnInScopes() = %v, want %v", got, tt.want)
	// 		}
	// 	})
	// }
}
func Test_clauseType_String(t *testing.T) {
	tests := []struct {
		name string
		ct   clauseType
		want string
	}{
		{
			name: "ctColsCSV",
			ct:   ctColsCSV,
			want: "ctColsCSV",
		},
		{
			name: "ctColsPrefixedCSV",
			ct:   ctColsPrefixedCSV,
			want: "ctColsPrefixedCSV",
		},
		{
			name: "ctArgsInsert",
			ct:   ctArgsInsert,
			want: "ctArgsInsert",
		},
		{
			name: "ctColsUpdateByPK",
			ct:   ctColsUpdateByPK,
			want: "ctColsUpdateByPK",
		},
		{
			name: "ctColsUpdate",
			ct:   ctColsUpdate,
			want: "ctColsUpdate",
		},
		{
			name: "unknown clause type",
			ct:   clauseType(255), // Invalid clauseType
			want: "unknown clause type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ct.String(); got != tt.want {
				t.Errorf("clauseType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
