package reflectx

import (
	"reflect"
	"strings"
)

type StructField struct {
	Path []int
	Name string
	Tag  string
}

func ExtractStructFields(ptrToStruct any, tag string) []StructField {

	t := reflect.TypeOf(ptrToStruct)
	if t.Kind() == reflect.Ptr {
		t = t.Elem() // dereference pointer
	}

	if t.Kind() != reflect.Struct {
		panic("parseStruct: input must be a struct or a pointer to a struct")
	}

	return extractStructFields(t, tag, nil)
}

func extractStructFields(t reflect.Type, tag string, parent *StructField) (fields []StructField) {

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for i := range t.NumField() {
		sf := t.Field(i) // struct field

		if !sf.IsExported() {
			continue
		}

		f := StructField{
			Name: sf.Name,
			Tag:  sf.Tag.Get(tag),
		}

		if f.Tag == "-" {
			continue
		}

		if parent != nil {
			f.Path = append(f.Path, parent.Path...)
		}
		f.Path = append(f.Path, i)

		if sf.Anonymous { // embedded struct
			embeddedFields := extractStructFields(sf.Type, tag, &f)
			fields = append(fields, embeddedFields...)
		} else {
			fields = append(fields, f)
		}
	}
	return fields
}

func FieldAddress(ptrToStruct any, fieldPaths [][]int, dst []any) {
	v := reflect.ValueOf(ptrToStruct)
	dst = dst[:len(fieldPaths)]
	for i, path := range fieldPaths {
		v = v.FieldByIndex(path)
		dst[i] = v.Addr().Interface()
	}
}

func FirstFieldWithTagValue(fields []StructField, tagOption string) (int, bool) {

	hp := tagOption + ","
	hs := "," + tagOption
	co := "," + tagOption + ","

	for i := range fields {
		tag := fields[i].Tag
		if tag == tagOption ||
			strings.HasPrefix(tag, hp) ||
			strings.HasSuffix(tag, hs) ||
			strings.Contains(tag, co) {
			return i, true
		}
	}
	return -1, false
}
