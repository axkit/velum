package velum

import (
	"testing"
)

func TestTableOptions(t *testing.T) {
	tests := []struct {
		name     string
		option   TableOption
		expected TableConfig
	}{
		{
			name:   "WithTag sets the tag field",
			option: WithTag("test_tag"),
			expected: TableConfig{
				tag: "test_tag",
			},
		},
		{
			name:   "WithName sets the name field",
			option: WithName("test_name"),
			expected: TableConfig{
				tag: "test_name", // Note: WithName sets the `tag` field in the current implementation
			},
		},
		{
			name: "WithArgumentNumerator sets the argNumerator function",
			option: WithArgumentNumerator(func(argPos int) string {
				return "arg_" + string(rune(argPos+'0'))
			}),
			expected: TableConfig{
				argNumerator: func(argPos int) string {
					return "arg_" + string(rune(argPos+'0'))
				},
			},
		},
		{
			name: "WithColumnNameBuilder sets the colNameBuilder function",
			option: WithColumnNameBuilder(func(attr, tag string) string {
				return attr + "_" + tag
			}),
			expected: TableConfig{
				colNameBuilder: func(attr, tag string) string {
					return attr + "_" + tag
				},
			},
		},
		{
			name: "WithSequenceNameBuilder sets the seqNameBuilder function",
			option: WithSequenceNameBuilder(func(name string) string {
				return "seq_" + name
			}),
			expected: TableConfig{
				seqNameBuilder: func(name string) string {
					return "seq_" + name
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new TableConfig and apply the option
			config := TableConfig{}
			tt.option(&config)

			// Compare the modified config with the expected config
			if tt.expected.tag != "" && config.tag != tt.expected.tag {
				t.Errorf("expected tag = %v, got %v", tt.expected.tag, config.tag)
			}

			if tt.expected.argNumerator != nil {
				expectedArg := tt.expected.argNumerator(1)
				gotArg := config.argNumerator(1)
				if expectedArg != gotArg {
					t.Errorf("expected argNumerator(1) = %v, got %v", expectedArg, gotArg)
				}
			}

			if tt.expected.colNameBuilder != nil {
				expectedCol := tt.expected.colNameBuilder("attr", "tag")
				gotCol := config.colNameBuilder("attr", "tag")
				if expectedCol != gotCol {
					t.Errorf("expected colNameBuilder(attr, tag) = %v, got %v", expectedCol, gotCol)
				}
			}

			if tt.expected.seqNameBuilder != nil {
				expectedSeq := tt.expected.seqNameBuilder("name")
				gotSeq := config.seqNameBuilder("name")
				if expectedSeq != gotSeq {
					t.Errorf("expected seqNameBuilder(name) = %v, got %v", expectedSeq, gotSeq)
				}
			}
		})
	}
}
