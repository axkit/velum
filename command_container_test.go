package velum

import (
	"reflect"
	"strings"
	"testing"
)

func Test_parseUserScopes(t *testing.T) {

	// cols := []Column{
	// 	{Name: "id", Path: []int{0}, Tag: reflectx.TagPairs{"scope": {"*", "pk"}}},
	// 	{Name: "first_name", Path: []int{1}, Tag: reflectx.TagPairs{"scope": {"*"}}},
	// 	{Name: "last_name", Path: []int{2}, Tag: reflectx.TagPairs{"scope": {"*"}}},
	// 	{Name: "age", Path: []int{3}, Tag: reflectx.TagPairs{"scope": {"age"}}},
	// 	{Name: "ssn", Path: []int{4}, Tag: reflectx.TagPairs{"scope": {"ssn"}}},
	// 	{Name: "row_version", Path: []int{5}, Tag: reflectx.TagPairs{"scope": {"version"}}},
	// 	{Name: "updated_at", Path: []int{6}, Tag: reflectx.TagPairs{"scope": {"updated"}}},
	// 	{Name: "updated_by", Path: []int{7}, Tag: reflectx.TagPairs{"scope": {"updated"}}},
	// }

	sets := []struct {
		userScopeCSV     Scope
		additionalScopes []Scope
		exp              scopeSet
	}{
		{
			userScopeCSV:     "*",
			additionalScopes: nil,
			exp: scopeSet{
				all: true,
			},
		},
		{
			userScopeCSV:     "age",
			additionalScopes: nil,
			exp: scopeSet{
				direct:  []Scope{"age"},
				negated: nil,
				system:  nil,
			},
		},
		{
			userScopeCSV:     "!ssn",
			additionalScopes: nil,
			exp: scopeSet{
				direct:  nil,
				negated: []Scope{"ssn"},
				system:  nil,
			},
		},
		{
			userScopeCSV:     "ssn, !age",
			additionalScopes: []Scope{"version", "update"},
			exp: scopeSet{
				direct:  []Scope{"ssn"},
				negated: []Scope{"age"},
				system:  []Scope{"version", "update"},
			},
		},
		{
			userScopeCSV:     " !age,ssn ",
			additionalScopes: []Scope{"version", "update"},
			exp: scopeSet{
				direct:  []Scope{"ssn"},
				negated: []Scope{"age"},
				system:  []Scope{"version", "update"},
			},
		},
		{
			userScopeCSV:     "system,ssn,!age",
			additionalScopes: []Scope{"version", "update"},
			exp: scopeSet{
				direct:  []Scope{"ssn"},
				negated: []Scope{"age"},
				system:  []Scope{"version", "insert", "update", "delete"},
			},
		},
		{
			userScopeCSV:     "!age",
			additionalScopes: []Scope{"version"},
			exp: scopeSet{
				direct:  nil,
				negated: []Scope{"age"},
				system:  []Scope{"version"},
			},
		},
	}

	for _, set := range sets {
		t.Run(string(set.userScopeCSV), func(t *testing.T) {
			got := parseUserScopes(set.userScopeCSV, set.additionalScopes...)
			if !reflect.DeepEqual(got, set.exp) {
				t.Errorf("parseUserScopes(%q, %v) = %#v, want %#v", set.userScopeCSV, set.additionalScopes, got, set.exp)
			}
		})
	}

}

// Test_buildDeleteReturning verifies that buildDeleteReturning produces a
// correct SQL statement and sets cpos/rets correctly.
// Previously it read from t.cc.clause (always-empty map) and produced
// "DELETE FROM t ... RETURNING " with no column names.
func Test_buildDeleteReturning(t *testing.T) {
	// testMLRow is defined in memory_leak_test.go (same package).
	tbl := NewTable[testMLRow]("items")

	tests := []struct {
		name          string
		retScope      Scope
		clauses       string
		wantSQLPrefix string
		wantReturning string
		wantCposLen   int // expected number of entries in Command.cpos (for WHERE args)
		wantRetsLen   int // expected number of entries in rets (scanned return columns)
	}{
		{
			name:          "FullScope returning all columns",
			retScope:      FullScope,
			clauses:       "WHERE id=$1",
			wantSQLPrefix: "DELETE FROM items WHERE id=$1 RETURNING ",
			wantReturning: "id,name,age", // all columns
			wantCposLen:   1,             // PK position for WHERE $1
			wantRetsLen:   3,             // id, name, age
		},
		{
			// newClause always prepends the PK column (consistent with
			// buildInsertReturning / buildUpdateReturning), so "age" scope
			// returns "id,age".
			name:          "age scope returns id and age",
			retScope:      "age",
			clauses:       "WHERE id=$1",
			wantSQLPrefix: "DELETE FROM items WHERE id=$1 RETURNING ",
			wantReturning: "id,age",
			wantCposLen:   1,
			wantRetsLen:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := buildDeleteReturning(tbl, tt.retScope, tt.clauses)

			wantSQL := tt.wantSQLPrefix + tt.wantReturning
			if cmd.sql != wantSQL {
				t.Errorf("sql:\n got  %q\n want %q", cmd.sql, wantSQL)
			}

			if !strings.HasSuffix(cmd.sql, tt.wantReturning) {
				t.Errorf("sql does not end with RETURNING columns %q: %q", tt.wantReturning, cmd.sql)
			}

			if len(cmd.cpos) != tt.wantCposLen {
				t.Errorf("cpos len: got %d, want %d", len(cmd.cpos), tt.wantCposLen)
			}

			if len(cmd.rets) != tt.wantRetsLen {
				t.Errorf("rets len: got %d, want %d", len(cmd.rets), tt.wantRetsLen)
			}
		})
	}
}

// Test_buildDeleteReturning_NoReturnEmpty verifies the old bug is gone:
// the RETURNING clause must not be empty.
func Test_buildDeleteReturning_NotEmpty(t *testing.T) {
	tbl := NewTable[testMLRow]("items")
	cmd := buildDeleteReturning(tbl, FullScope, "WHERE id=$1")

	const marker = "RETURNING "
	idx := strings.Index(cmd.sql, marker)
	if idx == -1 {
		t.Fatalf("sql does not contain RETURNING: %q", cmd.sql)
	}
	tail := cmd.sql[idx+len(marker):]
	if tail == "" {
		t.Errorf("RETURNING clause is empty — bug is still present: %q", cmd.sql)
	}
	t.Logf("RETURNING clause: %q", tail)
}
