package velum

import (
	"reflect"
	"slices"
	"testing"

	"github.com/axkit/velum/reflectx"
)

// Test cases for isColumnNotInScope
func Test_isColumnNotInScope(t *testing.T) {
	tests := []struct {
		name   string
		column *Column
		scope  Scope
		want   bool
	}{
		{
			name:   "Column in FullScope",
			column: &Column{Tag: reflectx.TagPairs{"scope": {"full"}}},
			scope:  FullScope,
			want:   false,
		},
		{
			name:   "Column in specific scope",
			column: &Column{Tag: reflectx.TagPairs{"scope": {"specific"}}},
			scope:  "specific",
			want:   false,
		},
		{
			name:   "Column not in specific scope",
			column: &Column{Tag: reflectx.TagPairs{"scope": {"other"}}},
			scope:  "specific",
			want:   true,
		},
		{
			name:   "Column with no scope",
			column: &Column{Tag: reflectx.TagPairs{}},
			scope:  "specific",
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isColumnNotInScope(tt.column, tt.scope); got != tt.want {
				t.Errorf("isColumnNotInScope() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test cases for ExtractScopeClauses
func Test_ExtractScopeClauses(t *testing.T) {

	pkSerial := Column{
		Name:                  "id",
		Path:                  []int{0},
		Tag:                   reflectx.TagPairs{"scope": {"*", "pk"}, "gen": {"serial"}},
		ValueGenerationMethod: SerialFieleType,
	}

	pkFriendlySeq := Column{
		Name:                  "id",
		Path:                  []int{0},
		Tag:                   reflectx.TagPairs{"scope": {"*", "pk"}},
		ValueGenerationMethod: FriendlySequence,
		ValueGenerator:        "customer_seq",
	}

	cols := []Column{
		{
			Name: "first_name",
			Path: []int{1, 0},
			Tag:  reflectx.TagPairs{"scope": {"*", "u"}},
		},
		{
			Name: "last_name",
			Path: []int{1, 1},
			Tag:  reflectx.TagPairs{"scope": {"*", "u"}},
		},
		{
			Name: "age",
			Path: []int{1, 2},
			Tag:  reflectx.TagPairs{"scope": {"*", "age"}},
		},
		{
			Name: "row_version",
			Path: []int{2, 3},
			Tag:  reflectx.TagPairs{"scope": {"*", "version"}},
		},
	}

	tests := []struct {
		name         string
		columns      []Column
		scope        Scope
		pk           *SystemColumn
		argNumerator ArgNumberBuilder
		want         [int(ctMax_)]Clause
	}{
		{
			name:         "one column in scope",
			scope:        "age",
			columns:      slices.Concat([]Column{pkSerial}, cols),
			pk:           &SystemColumn{Column: &pkSerial, Pos: 0},
			argNumerator: DefaultParamPlaceholderBuilder,
			want: [int(ctMax_)]Clause{
				ctArgsInsert:      {text: "DEFAULT,$1", args: [][]int{{1, 2}}, fidx: []int{3}},
				ctColsCSV:         {text: "id,age", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctColsPrefixedCSV: {text: "t.id,t.age", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctUpdateByPK:      {text: "age=$2", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctUpdate:          {text: "age=$1", args: [][]int{{1, 2}}, fidx: []int{3}},
			},
		},
		{
			name:         "no pk+one column in scope",
			scope:        "age",
			columns:      cols,
			pk:           nil,
			argNumerator: DefaultParamPlaceholderBuilder,
			want: [int(ctMax_)]Clause{
				ctArgsInsert:      {text: "$1", args: [][]int{{1, 2}}, fidx: []int{2}},
				ctColsCSV:         {text: "age", args: [][]int{{1, 2}}, fidx: []int{2}},
				ctColsPrefixedCSV: {text: "t.age", args: [][]int{{1, 2}}, fidx: []int{2}},
				ctUpdateByPK:      {text: "age=$1", args: [][]int{{1, 2}}, fidx: []int{2}},
				ctUpdate:          {text: "age=$1", args: [][]int{{1, 2}}, fidx: []int{2}},
			},
		},
		{
			name:         "fiendly sequence+one column in scope",
			scope:        "age",
			columns:      slices.Concat([]Column{pkFriendlySeq}, cols),
			pk:           &SystemColumn{Column: &pkFriendlySeq, Pos: 0},
			argNumerator: DefaultParamPlaceholderBuilder,
			want: [int(ctMax_)]Clause{
				ctArgsInsert:      {text: "nextval('customer_seq'),$1", args: [][]int{{1, 2}}, fidx: []int{3}},
				ctColsCSV:         {text: "id,age", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctColsPrefixedCSV: {text: "t.id,t.age", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctUpdateByPK:      {text: "age=$2", args: [][]int{{0}, {1, 2}}, fidx: []int{0, 3}},
				ctUpdate:          {text: "age=$1", args: [][]int{{1, 2}}, fidx: []int{3}},
			},
		},
		{
			name:         "no column in scope",
			scope:        "no_scope",
			columns:      slices.Concat([]Column{pkSerial}, cols),
			pk:           &SystemColumn{Column: &pkSerial, Pos: 0},
			argNumerator: DefaultParamPlaceholderBuilder,
			want: [int(ctMax_)]Clause{
				ctArgsInsert:      {text: "DEFAULT", args: nil},
				ctColsCSV:         {text: "id", args: [][]int{{0}}, fidx: []int{0}},
				ctColsPrefixedCSV: {text: "t.id", args: [][]int{{0}}, fidx: []int{0}},
				ctUpdateByPK:      {text: "", args: [][]int{{0}}, fidx: []int{0}},
				ctUpdate:          {text: "", args: nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractScopeClauses(tt.columns, tt.scope, tt.pk, tt.argNumerator)
			for i := 0; i < len(got); i++ {
				if reflect.DeepEqual(got[i], tt.want[i]) {
					continue
				}
				t.Errorf("name %s, clause_type: %s\n", tt.name, (clauseType(i)).String())
				t.Errorf("ct: %s, got[%d]: %#v\n", (clauseType(i)).String(), i, got[i])
				t.Errorf("ct: %s, wan[%d]: %#v\n", (clauseType(i)).String(), i, tt.want[i])
			}
		})
	}
}
func Test_IsValueGeneratedByDB(t *testing.T) {
	tests := []struct {
		name   string
		column Column
		want   bool
	}{
		{
			name:   "Value generated by SerialFieleType",
			column: Column{ValueGenerationMethod: SerialFieleType},
			want:   true,
		},
		{
			name:   "Value generated by UuidFileType",
			column: Column{ValueGenerationMethod: UuidFileType},
			want:   true,
		},
		{
			name:   "Value generated by FriendlySequence",
			column: Column{ValueGenerationMethod: FriendlySequence},
			want:   true,
		},
		{
			name:   "Value generated by CustomSequece",
			column: Column{ValueGenerationMethod: CustomSequece},
			want:   true,
		},
		{
			name:   "Value not generated by database",
			column: Column{ValueGenerationMethod: NoSequence},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.column.IsValueGeneratedByDB(); got != tt.want {
				t.Errorf("IsValueGeneratedByDB() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_IsSystem(t *testing.T) {
	tests := []struct {
		name   string
		column Column
		want   bool
	}{
		{
			name: "Column with system scope",
			column: Column{
				Tag: reflectx.TagPairs{scopeTagKey: {string(DeleteScope)}},
			},
			want: true,
		},
		{
			name: "Column without system scope",
			column: Column{
				Tag: reflectx.TagPairs{scopeTagKey: {"user"}},
			},
			want: false,
		},
		{
			name: "Column with multiple scopes including system",
			column: Column{
				Tag: reflectx.TagPairs{scopeTagKey: {"user", string(UpdateScope)}},
			},
			want: true,
		},
		{
			name: "Column with no scopes",
			column: Column{
				Tag: reflectx.TagPairs{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.column.IsSystem(); got != tt.want {
				t.Errorf("IsSystem() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_InsertArgument(t *testing.T) {
	tests := []struct {
		name           string
		genMethod      ColumnValueGenMethod
		valueGenerator string
		regularParam   string
		want           string
	}{
		{
			name:      "tag option 'serial' returns DEFAULT",
			genMethod: SerialFieleType,
			want:      "DEFAULT",
		},
		{
			name:      "tag option 'uuid' returns gen_random_uuid()",
			genMethod: UuidFileType,
			want:      "gen_random_uuid()",
		},
		{
			name:         "noseq returns $1",
			genMethod:    NoSequence,
			regularParam: "$1",
			want:         "$1",
		},
		{
			name:           "tag option 'seq=custom_seq' returns nextval with custom_seq",
			genMethod:      CustomSequece,
			valueGenerator: "custom_seq",
			want:           "nextval('custom_seq')",
		},
		{
			name:           "FriendlySequence returns nextval with 'friendly_sequence'",
			genMethod:      FriendlySequence,
			valueGenerator: "friendly_sequence",
			want:           "nextval('friendly_sequence')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InsertArgument(tt.genMethod, tt.valueGenerator, tt.regularParam); got != tt.want {
				t.Errorf("InsertArgument() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_colValueGenMethod(t *testing.T) {
	tests := []struct {
		name       string
		genOptVal  string
		wantMethod ColumnValueGenMethod
		wantValue  string
	}{
		{
			name:       "SerialFieleType returns DEFAULT",
			genOptVal:  string(SerialFieleType),
			wantMethod: SerialFieleType,
			wantValue:  "DEFAULT",
		},
		{
			name:       "UuidFileType returns gen_random_uuid()",
			genOptVal:  string(UuidFileType),
			wantMethod: UuidFileType,
			wantValue:  "gen_random_uuid()",
		},
		{
			name:       "NoSequence returns empty value",
			genOptVal:  string(NoSequence),
			wantMethod: NoSequence,
			wantValue:  "",
		},
		{
			name:       "Empty string returns NoSequence with empty value",
			genOptVal:  "",
			wantMethod: NoSequence,
			wantValue:  "",
		},
		{
			name:       "Custom sequence returns CustomSequece with genOptVal",
			genOptVal:  "custom_sequence",
			wantMethod: CustomSequece,
			wantValue:  "custom_sequence",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMethod, gotValue := colValueGenMethod(tt.genOptVal)
			if gotMethod != tt.wantMethod || gotValue != tt.wantValue {
				t.Errorf("colValueGenMethod(%q) = (%v, %q), want (%v, %q)", tt.genOptVal, gotMethod, gotValue, tt.wantMethod, tt.wantValue)
			}
		})
	}
}
