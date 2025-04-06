package reflectx

import (
	"reflect"
	"sync"
)

type PointerSlicePool[T any] struct {
	fic  FieldIndexContainer
	pool sync.Pool
}

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

func (p *PointerSlicePool[T]) Release(s *[]any) {
	*s = (*s)[:0]
	p.pool.Put(s)
}

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
