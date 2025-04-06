package reflectx

import (
	"reflect"
)

// StructField represents a field in a struct.
type StructField struct {
	// Path is the path to the field in the struct hierarchy.
	// It is represented as a slice of integers, where each integer is the index
	// of the field in the parent struct.
	Path []int
	// Name is the name of the field.
	Name string
	// Tag is the value of the struct tag.
	Tag string
}

// ExtractStructFields extracts fields from a struct or a pointer to a struct.
//
// It returns a slice of StructField, which contains the field name, tag value,
// and the path to the field in the struct hierarchy.
// The tag parameter specifies the struct tag to look for.
// The function panics if the input is not a struct or a pointer to a struct.
// The function does not include unexported fields and fields with a tag value of "-".
// It also handles embedded structs and nested structs.
// The path is represented as a slice of integers, where each integer is the index
// of the field in the parent struct.
func ExtractStructFields(ptrToStruct any, tag string) []StructField {

	MustBeStruct(ptrToStruct)
	v := reflect.ValueOf(ptrToStruct)
	t := v.Type()

	return extractStructFields(t, tag, nil)
}

func extractStructFields(t reflect.Type, tag string, parent *StructField) (fields []StructField) {

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := range t.NumField() {
		field := t.Field(i) // struct field

		if !field.IsExported() {
			continue
		}

		sf := StructField{
			Name: field.Name,
			Tag:  field.Tag.Get(tag),
		}

		if sf.Tag == "-" {
			continue
		}

		if parent != nil {
			sf.Path = append(sf.Path, parent.Path...)
		}
		sf.Path = append(sf.Path, i)

		if field.Anonymous { // embedded struct
			embeddedFields := extractStructFields(field.Type, tag, &sf)
			fields = append(fields, embeddedFields...)
		} else {
			fields = append(fields, sf)
		}
	}
	return fields
}

// MustBeStruct panics if the given value is a struct or a pointer to a struct.
func MustBeStruct(s any) {
	v := reflect.ValueOf(s)
	t := v.Type()

	// Accept direct struct
	if t.Kind() == reflect.Struct {
		return
	}

	// Accept pointer to struct
	if t.Kind() == reflect.Ptr && t.Elem().Kind() == reflect.Struct {
		return
	}
	panic("MustBeStruct: expected struct or pointer to struct, got " + t.Kind().String())
}
