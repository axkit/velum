package reflectx

import (
	"reflect"
	"testing"
	"time"
)

func TestExtractStructFields(t *testing.T) {

	type CustomerType struct {
		TypeID   int `dbw:"bd"`
		TypeName string
	}

	type Address struct {
		City    string `dbw:"u"`
		State   string `dbw:"u"`
		Country string `dbw:"u"`
	}

	type Customer struct {
		*CustomerType
		ID              int
		FirstName       string    `dbw:"u"`
		LastName        string    `dbw:"u"`
		BirthDate       time.Time `dbw:"u,bd"`
		UpdateAt        *time.Time
		DeletedAt       *time.Time
		Ignored         int `dbw:"-"`
		unexportedField bool
		Address
	}

	tests := []struct {
		name     string
		input    any
		tag      string
		expected []StructField
	}{
		{
			name:  "BasicStruct",
			input: Customer{},
			tag:   "dbw",
			expected: []StructField{
				{Name: "TypeID", Tag: "bd", Path: []int{0, 0}},
				{Name: "TypeName", Tag: "", Path: []int{0, 1}},
				{Name: "ID", Tag: "", Path: []int{1}},
				{Name: "FirstName", Tag: "u", Path: []int{2}},
				{Name: "LastName", Tag: "u", Path: []int{3}},
				{Name: "BirthDate", Tag: "u,bd", Path: []int{4}},
				{Name: "UpdateAt", Tag: "", Path: []int{5}},
				{Name: "DeletedAt", Tag: "", Path: []int{6}},
				{Name: "City", Tag: "u", Path: []int{9, 0}},
				{Name: "State", Tag: "u", Path: []int{9, 1}},
				{Name: "Country", Tag: "u", Path: []int{9, 2}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := ExtractStructFields(tt.input, tt.tag)

			if len(actual) != len(tt.expected) {
				t.Fatalf("expected %d fields, got %d", len(tt.expected), len(actual))
			}

			for i, field := range actual {
				if field.Name != tt.expected[i].Name ||
					field.Tag != tt.expected[i].Tag ||
					!reflect.DeepEqual(field.Path, tt.expected[i].Path) {
					t.Errorf("field %d mismatch: got %+v, want %+v", i, field, tt.expected[i])
				}
			}
		})
	}
}
func TestMustBeStruct(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		shouldPanic bool
	}{
		{
			name:        "StructInput",
			input:       struct{}{},
			shouldPanic: false,
		},
		{
			name:        "PointerToStructInput",
			input:       &struct{}{},
			shouldPanic: false,
		},
		{
			name:        "NonStructInputInt",
			input:       42,
			shouldPanic: true,
		},
		{
			name:        "NonStructInputString",
			input:       "not a struct",
			shouldPanic: true,
		},
		{
			name:        "NilInput",
			input:       nil,
			shouldPanic: true,
		},
		{
			name:        "SliceInput",
			input:       []int{1, 2, 3},
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.shouldPanic {
						t.Errorf("unexpected panic for input: %v", tt.input)
					}
				} else if tt.shouldPanic {
					t.Errorf("expected panic but did not panic for input: %v", tt.input)
				}
			}()
			MustBeStruct(tt.input)
		})
	}
}
