package reflectx

import (
	"reflect"
	"testing"
)

func TestTagPairs(t *testing.T) {
	tests := []struct {
		name          string
		tag           string
		key           string
		getExpected   []string
		valueExpected string
	}{
		{"SinglePair", "key=value", "key", []string{"value"}, "value"},
		{"MultiPairs", "key1=value1,key2=value2", "key2", []string{"value2"}, "value2"},
		{"JustScope", "pwd", "scope", []string{"pwd"}, "pwd"},
		{"EmptyTag", "", "scope", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := ParseTagPairs(tt.tag, "scope")
			if got := tp.Get(tt.key); !reflect.DeepEqual(got, tt.getExpected) {
				t.Errorf("Get(%s) = %v, want %v", tt.key, got, tt.getExpected)
			}
			if got := tp.Value(tt.key); got != tt.valueExpected {
				t.Errorf("Value(%s) = %v, want %v", tt.key, got, tt.valueExpected)
			}
		})
	}
}

func TestTagPairs_Value(t *testing.T) {

	tests := []struct {
		name     string
		tag      string
		key      string
		expected string
	}{
		{"SinglePair", "key=value", "key", "value"},
		{"MultiPairs", "key1=value1,key2=value2", "key2", "value2"},
		{"JustScope", "pwd", "scope", "pwd"},
		{"EmptyTag", "", "scope", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tp := ParseTagPairs(tt.tag, "scope")
			if got := tp.Value(tt.key); got != tt.expected {
				t.Errorf("Value(%s) = %v, want %v", tt.key, got, tt.expected)
			}
		})
	}
}

func TestTagPairs_AddScope(t *testing.T) {
	tests := []struct {
		name             string
		key              string
		value            string
		expectedScope    []string
		expectedScopePwd bool
	}{
		{"SinglePair", "key", "value", nil, false},
		{"MultiPairs", "key2", "value2", nil, false},
		{"EmptyTag", "scope", "*", []string{"*"}, false},
		{"JustScope", "scope", "pwd", []string{"pwd"}, true},
		{"RepeatedValue", "scope", "pwd", []string{"pwd"}, true},
	}

	for _, tt := range tests {
		tp := NewTagPairs()
		t.Run(tt.name, func(t *testing.T) {
			tp.Add(tt.key, tt.value)
			if got := tp.Get("scope"); !reflect.DeepEqual(got, tt.expectedScope) {
				t.Errorf("Get(%s) = %v, want %v", tt.key, got, tt.expectedScope)
			}
			if got := tp.PairExist("scope", "pwd"); got != tt.expectedScopePwd {
				t.Errorf("PairExist(%s, pwd) = %v, want %v", tt.key, got, tt.expectedScopePwd)
			}
		})
	}
}
