package velum

import (
	"reflect"
	"strings"
	"testing"
)

// dsTestRow is a minimal struct used by dataset unit tests below.
type dsTestRow struct {
	ID   int
	Name string
}

// testSysRow has system columns so TouchByPK and SoftDeleteReturningByPK
// generate non-trivial SQL that can be inspected in tests.
type testSysRow struct {
	ID         int  `dbw:"gen=serial"`
	Name       string
	RowVersion int  `dbw:"version"`
	UpdatedInt int  `dbw:"update"` // int rather than time.Time to avoid extra imports
	DeletedInt int  `dbw:"delete"`
}

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

// --- Bug 1: buildInsertReturning used || instead of && ---

// Test_buildInsertReturning_ScopesAppliedCorrectly verifies that the INSERT
// argument columns follow argScope and the RETURNING columns follow retScope
// independently when the two scopes differ.
//
// Before the fix (|| condition), rs was always rebuilt from retScope, making
// the optimisation branch (rs = as) dead code. The most observable impact was
// that when retScope == EmptyScope the RETURNING clause wrongly used system-only
// columns instead of argScope's columns.
func Test_buildInsertReturning_ScopesAppliedCorrectly(t *testing.T) {
	tbl := NewTable[testMLRow]("items")

	tests := []struct {
		name          string
		argScope      Scope
		retScope      Scope
		wantInsertCSV string // comma-separated list inside VALUES(...)
		wantReturning string // text after RETURNING
	}{
		{
			// When both scopes are identical the optimisation reuses the
			// already-parsed scopeSet without redundant work.
			name:          "same scope: INSERT args and RETURNING both use FullScope",
			argScope:      FullScope,
			retScope:      FullScope,
			wantInsertCSV: "DEFAULT,$1,$2", // id=DEFAULT (serial), name=$1, age=$2
			wantReturning: "id,name,age",
		},
		{
			// retScope="age" → RETURNING should contain only id+age,
			// while the INSERT still writes all FullScope columns.
			name:          "different scopes: INSERT uses FullScope, RETURNING uses age scope",
			argScope:      FullScope,
			retScope:      "age",
			wantInsertCSV: "DEFAULT,$1,$2",
			wantReturning: "id,age",
		},
		{
			// argScope="age" → INSERT writes only id+age,
			// retScope=FullScope → RETURNING reads all columns.
			name:          "different scopes: INSERT uses age scope, RETURNING uses FullScope",
			argScope:      "age",
			retScope:      FullScope,
			wantInsertCSV: "DEFAULT,$1", // id=DEFAULT, age=$1
			wantReturning: "id,name,age",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := buildInsertReturning(tbl, tt.argScope, tt.retScope)

			// Verify INSERT VALUES clause.
			valStart := strings.Index(cmd.sql, "VALUES (")
			valEnd := strings.Index(cmd.sql, ") RETURNING")
			if valStart == -1 || valEnd == -1 {
				t.Fatalf("could not locate VALUES(...) in sql: %q", cmd.sql)
			}
			gotValues := cmd.sql[valStart+len("VALUES (") : valEnd]
			if gotValues != tt.wantInsertCSV {
				t.Errorf("VALUES = %q, want %q (full sql: %q)",
					gotValues, tt.wantInsertCSV, cmd.sql)
			}

			// Verify RETURNING clause.
			const marker = "RETURNING "
			idx := strings.Index(cmd.sql, marker)
			if idx == -1 {
				t.Fatalf("RETURNING not found in sql: %q", cmd.sql)
			}
			gotReturning := cmd.sql[idx+len(marker):]
			if gotReturning != tt.wantReturning {
				t.Errorf("RETURNING = %q, want %q (full sql: %q)",
					gotReturning, tt.wantReturning, cmd.sql)
			}
		})
	}
}

// --- Bug 3: freqCmd.softDeleteByPK built but not wired to SoftDeleteReturningByPK ---

// Test_freqCmd_softDeleteByPK_UsedBySoftDeleteReturningByPK verifies that the
// SQL inside freqCmd.softDeleteByPK matches what SoftDeleteReturningByPK now
// executes directly, confirming the pre-built command is actually wired in.
func Test_freqCmd_softDeleteByPK_UsedBySoftDeleteReturningByPK(t *testing.T) {
	tbl := NewTable[testSysRow]("things")

	if tbl.freqCmd.softDeleteByPK.sql == "" {
		t.Fatal("freqCmd.softDeleteByPK.sql is empty")
	}

	// The pre-built command must match cc.UpdateReturning(DeleteScope, SystemScope, ByPK()).
	want := tbl.cc.UpdateReturning(DeleteScope, SystemScope, ByPK())
	if tbl.freqCmd.softDeleteByPK.sql != want.sql {
		t.Errorf("freqCmd.softDeleteByPK.sql:\n got  %q\n want %q",
			tbl.freqCmd.softDeleteByPK.sql, want.sql)
	}
	if !reflect.DeepEqual(tbl.freqCmd.softDeleteByPK.rets, want.rets) {
		t.Errorf("freqCmd.softDeleteByPK.rets: got %v, want %v",
			tbl.freqCmd.softDeleteByPK.rets, want.rets)
	}

	// The SQL must contain RETURNING (it is a ReturningCommand, not a plain DELETE).
	if !strings.Contains(tbl.freqCmd.softDeleteByPK.sql, "RETURNING") {
		t.Errorf("expected RETURNING in softDeleteByPK sql: %q",
			tbl.freqCmd.softDeleteByPK.sql)
	}
}

func TestNewDatasetTemplate_UnmatchedPlaceholder(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unmatched /*, got none")
		}
	}()
	newDatasetTemplate("SELECT /* no closing")
}

func TestNewDatasetTemplate_EmptyPlaceholderName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty placeholder name, got none")
		}
	}()
	newDatasetTemplate("SELECT /* */ FROM t")
}

func TestIsPlaceholderToken(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"valid", "WHERE_FILTER", true},
		{"valid with digits", "FILTER_1", true},
		{"dash rejected", "my-filter", false},
		{"dot rejected", "filter.name", false},
		{"space rejected", "has space", false},
		{"empty", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPlaceholderToken(tt.in); got != tt.want {
				t.Errorf("isPlaceholderToken(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestNormalizeClause(t *testing.T) {
	tests := []struct {
		name         string
		clause       string
		nextArg      int
		wantClause   string
		wantConsumed int
	}{
		{
			name:         "no placeholders",
			clause:       "ORDER BY id",
			nextArg:      5,
			wantClause:   "ORDER BY id",
			wantConsumed: 0,
		},
		{
			name:         "nextArg is 1 - no shift needed",
			clause:       "WHERE id = $1",
			nextArg:      1,
			wantClause:   "WHERE id = $1",
			wantConsumed: 1,
		},
		{
			name:         "shift from $1 to $3",
			clause:       "AND age > $1",
			nextArg:      3,
			wantClause:   "AND age > $3",
			wantConsumed: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotClause, gotConsumed := normalizeClause(tt.clause, tt.nextArg)
			if gotClause != tt.wantClause {
				t.Errorf("clause = %q, want %q", gotClause, tt.wantClause)
			}
			if gotConsumed != tt.wantConsumed {
				t.Errorf("consumed = %d, want %d", gotConsumed, tt.wantConsumed)
			}
		})
	}
}

func TestStartsWithWhitespace(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{" text", true},
		{"\ntext", true},
		{"\ttext", true},
		{"\rtext", true},
		{"text", false},
		{"", false},
	}
	for _, tt := range tests {
		got := startsWithWhitespace(tt.in)
		if got != tt.want {
			t.Errorf("startsWithWhitespace(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestMaxPlaceholderIndex(t *testing.T) {
	tests := []struct {
		sql  string
		want int
	}{
		{"SELECT id FROM t WHERE id=$1 AND age=$2", 2},
		{"SELECT id FROM t", 0},
		{"WHERE id=$3 AND name=$1", 3},
	}
	for _, tt := range tests {
		got := maxPlaceholderIndex(tt.sql)
		if got != tt.want {
			t.Errorf("maxPlaceholderIndex(%q) = %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestDatasetColumnPositions_Empty(t *testing.T) {
	_, err := datasetColumnPositions(nil)
	if err == nil {
		t.Error("expected error for empty columns, got nil")
	}
}

func TestMergeClauses_EmptyOverrides(t *testing.T) {
	ds := NewDataset[dsTestRow]("SELECT id, name FROM t /*WHERE_FILTER*/")
	result := ds.mergeClauses(ClauseSet{})
	if result == nil {
		t.Error("mergeClauses with empty overrides returned nil")
	}
}

func TestMergeClauses_ValidOverride(t *testing.T) {
	ds := NewDataset[dsTestRow]("SELECT id, name FROM t /*WHERE_FILTER*/")
	result := ds.mergeClauses(ClauseSet{"WHERE_FILTER": "WHERE id = $1"})
	if result["WHERE_FILTER"] != "WHERE id = $1" {
		t.Errorf("mergeClauses result[WHERE_FILTER] = %q, want %q", result["WHERE_FILTER"], "WHERE id = $1")
	}
}

func TestMergeClauses_UnknownClause_Panics(t *testing.T) {
	ds := NewDataset[dsTestRow]("SELECT id, name FROM t /*WHERE_FILTER*/")
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown clause, got none")
		}
	}()
	ds.mergeClauses(ClauseSet{"UNKNOWN_CLAUSE": "ORDER BY id"})
}

func TestClauseKey(t *testing.T) {
	ds := NewDataset[dsTestRow]("SELECT id, name FROM t /*WHERE_FILTER*/")

	key1 := ds.clauseKey(ClauseSet{"WHERE_FILTER": "WHERE id = $1"})
	if key1 == "" {
		t.Error("clauseKey returned empty string")
	}

	// Include DatasetTailClause so its lookup branch is exercised.
	key2 := ds.clauseKey(ClauseSet{
		"WHERE_FILTER":    "WHERE id = $1",
		DatasetTailClause: "ORDER BY id",
	})
	if key2 == "" {
		t.Error("clauseKey with tail clause returned empty string")
	}
	if key1 == key2 {
		t.Error("clauseKey with and without tail clause should differ")
	}
}
