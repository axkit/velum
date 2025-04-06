package reflectx

import (
	"slices"
	"strings"
)

// TagPairs represents a map of key-value pairs.
type TagPairs map[string][]string

// NewTagPairs creates a new TagPairs instance.
func NewTagPairs() TagPairs {
	return make(TagPairs)
}

// Get retrieves all values associated with the given key.
func (tp *TagPairs) Get(key string) []string {
	return (*tp)[key]
}

// Value retrieves the first value associated with the given key.
func (tp *TagPairs) Value(key string) string {
	if val := tp.Get(key); len(val) > 0 {
		return val[0]
	}
	return ""
}

// Add adds a new key-value pair to the TagPairsX.
func (tp *TagPairs) Add(key, value string) {
	pair := tp.Get(key)
	if len(pair) == 0 {
		(*tp)[key] = []string{value}
		return
	}
	for _, v := range pair {
		if v == value {
			return
		}
	}
	(*tp)[key] = append((*tp)[key], value)
}

// Exist checks if the given key exists in the TagPairsX.
func (tp TagPairs) Exist(key string) bool {
	_, exists := tp[key]
	return exists
}

// PairExist checks if the given key-value pair exists in the TagPairsX.
func (tp TagPairs) PairExist(key, val string) bool {
	values, exists := tp[key]
	if !exists {
		return false
	}

	return slices.Contains(values, val)
}

// ParseTagPairs parses a tag string into TagPairsX.
func ParseTagPairs(tag string, scopeTagKey string) TagPairs {
	tp := make(TagPairs)

	if len(tag) == 0 {
		return tp
	}

	from := 0
	var s string
	for {
		to := strings.Index(tag[from:], ",")
		if to != -1 {
			s = tag[from : from+to]
			from = from + to + 1
		} else {
			s = tag[from:]
		}

		if s != "" {
			kv := strings.Split(s, "=")
			if len(kv) == 2 {
				tp.Add(kv[0], kv[1])
			} else {
				tp.Add(scopeTagKey, s)
			}
		}

		if to == -1 {
			break
		}
	}
	return tp
}
