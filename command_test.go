package velum

import (
	"context"
	"testing"
)

type testCmdRow struct {
	ID   int `dbw:"gen=serial"`
	Name string
}

type cmdFakeRows struct{ remaining int }

func (r *cmdFakeRows) Next() bool          { r.remaining--; return r.remaining >= 0 }
func (r *cmdFakeRows) Close() error        { return nil }
func (r *cmdFakeRows) Err() error          { return nil }
func (r *cmdFakeRows) Scan(_ ...any) error { return nil }

type cmdFakeDB struct{ rowsCount int }

func (db *cmdFakeDB) QueryContext(_ context.Context, _ string, _ ...any) (Rows, error) {
	return &cmdFakeRows{remaining: db.rowsCount}, nil
}

func TestNewCommand(t *testing.T) {
	tbl := NewTable[testCmdRow]("t")
	cmd := NewCommand[testCmdRow]("SELECT id, name FROM t WHERE id=$1", tbl.cc.sfpe, []int{0, 1})
	if cmd.sql != "SELECT id, name FROM t WHERE id=$1" {
		t.Errorf("sql = %q, want SELECT id, name FROM t WHERE id=$1", cmd.sql)
	}
	if len(cmd.cpos) != 2 {
		t.Errorf("cpos len = %d, want 2", len(cmd.cpos))
	}
}

func TestReturningCommand_Query(t *testing.T) {
	tbl := NewTable[testCmdRow]("t")
	cmd := tbl.cc.UpdateReturning(FullScope, FullScope, ByPK())
	rows, err := cmd.Query(context.Background(), &cmdFakeDB{rowsCount: 2})
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestShiftParamPositions(t *testing.T) {
	tests := []struct {
		name      string
		sqlWhere  string
		fromIndex int
		expected  string
	}{
		{
			name:      "BasicReplacement",
			sqlWhere:  "id = $1 AND name = $2",
			fromIndex: 10,
			expected:  "id = $10 AND name = $11",
		},
		{
			name:      "MultipleOccurrences",
			sqlWhere:  "id = $1 OR id > $1 AND name < $2",
			fromIndex: 5,
			expected:  "id = $5 OR id > $5 AND name < $6",
		},
		{
			name:      "NoPlaceholders",
			sqlWhere:  "id = 1 AND name = 'test'",
			fromIndex: 3,
			expected:  "id = 1 AND name = 'test'",
		},
		{
			name:      "SinglePlaceholder",
			sqlWhere:  "id=$1",
			fromIndex: 7,
			expected:  "id=$7",
		},
		{
			name:      "ComplexQuery",
			sqlWhere:  "id = $1 AND (age > $2 OR age < $3) AND name = $4",
			fromIndex: 20,
			expected:  "id = $20 AND (age > $21 OR age < $22) AND name = $23",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShiftParamPositions(tt.sqlWhere, tt.fromIndex)
			if result != tt.expected {
				t.Errorf("ReplaceWherePlaceholder(%q, %d) = %q, want %q", tt.sqlWhere, tt.fromIndex, result, tt.expected)
			}
		})
	}
}
