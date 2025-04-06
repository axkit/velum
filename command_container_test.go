package velum

import (
	"reflect"
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
