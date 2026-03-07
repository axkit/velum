package reflectx

import (
	"reflect"
	"sync"
)

// PointerSlicePool is a pool of *[]any slices used to scan query result rows
// into a struct T without allocating a new slice per call.
//
// Each element of the pooled slice is a pointer to a field of a T value,
// ordered according to the column positions passed to StructFieldPtrs.
// Callers must call Release exactly once per StructFieldPtrs call to return
// the slice to the pool.
type PointerSlicePool[T any] struct {
	fic  FieldIndexContainer
	pool sync.Pool
}

// NewPointerSlicePool creates a PointerSlicePool for struct T. fic must
// describe the index paths to every field that might be scanned; its Cap()
// determines the pre-allocated capacity of each pooled slice.
//
// NewPointerSlicePool panics if T is not a struct.
func NewPointerSlicePool[T any](fic FieldIndexContainer) *PointerSlicePool[T] {

	var zero T
	MustBeStruct(zero)

	return &PointerSlicePool[T]{
		fic: fic,
		pool: sync.Pool{New: func() any {
			slice := make([]any, 0, fic.Cap())
			return &slice
		}},
	}
}

func (p *PointerSlicePool[T]) ptrs() *[]any {
	return p.pool.Get().(*[]any)
}

// Release returns s to the pool. It must be called exactly once per
// StructFieldPtrs call, typically via defer.
func (p *PointerSlicePool[T]) Release(s *[]any) {
	*s = (*s)[:0]
	p.pool.Put(s)
}

// StructFieldPtrs borrows a *[]any from the pool and appends a pointer to
// each field of v whose column index appears in scopeColIndexes. The slice
// is returned in column-index order, ready to be passed directly to
// rows.Scan.
//
// For fields accessed through a pointer-to-struct embedding, StructFieldPtrs
// allocates the intermediate struct on the heap if the pointer is nil.
//
// Callers must call Release when the slice is no longer needed.
func (p *PointerSlicePool[T]) StructFieldPtrs(v *T, scopeColIndexes []int) *[]any {

	s := reflect.ValueOf(v).Elem()
	ptrs := p.ptrs()

	p.fic.RangeByFieldPath(scopeColIndexes, func(fieldPath []uint16) {
		n := len(fieldPath)
		if n == 1 {
			field := s.Field(int(fieldPath[0]))
			*ptrs = append(*ptrs, field.Addr().Interface())
			return
		}

		ss := s
		for j := range n {
			if j > 0 {
				if ss.Kind() == reflect.Pointer && ss.Type().Elem().Kind() == reflect.Struct {
					if ss.IsNil() {
						ss.Set(reflect.New(ss.Type().Elem()))
					}
					ss = ss.Elem()
				}
			}
			ss = ss.Field(int(fieldPath[j]))
		}
		*ptrs = append(*ptrs, ss.Addr().Interface())
	})
	return ptrs
}
