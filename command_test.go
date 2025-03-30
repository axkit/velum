package velum

import (
	"testing"
)

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
