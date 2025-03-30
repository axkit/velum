package velum

import (
	"reflect"
	"sync"

	"github.com/axkit/velum/reflectx"
)

type PointerSlicePool[T any] struct {
	fic  reflectx.FieldIndexContainer
	pool sync.Pool
}

func NewPointerSlicePool[T any](fic reflectx.FieldIndexContainer) *PointerSlicePool[T] {
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

func (p *PointerSlicePool[T]) StructFieldPtrs(v *T, index [][]int) []any {

	mustBeStruct(v)

	s := reflect.ValueOf(v).Elem()

	ptrs := p.ptrs()
	n := len(index)

	for i := range n {
		idx := index[i]
		m := len(idx)
		if m == 1 {
			field := s.Field(idx[0])
			*ptrs = append(*ptrs, field.Addr().Interface())
			continue
		}

		ss := s
		for j := range m {
			if j > 0 {
				if ss.Kind() == reflect.Pointer && ss.Type().Elem().Kind() == reflect.Struct {
					if ss.IsNil() {
						panic("reflect: indirection through nil pointer to embedded struct")
					}
					ss = ss.Elem()
				}
			}
			ss = ss.Field(idx[j])
		}
		*ptrs = append(*ptrs, ss.Addr().Interface())
	}
	return *ptrs
}

func (p *PointerSlicePool[T]) StructFieldPtrsXXX(v *T, scopeColIndexes []int) []any {

	mustBeStruct(v)

	s := reflect.ValueOf(v).Elem()
	ptrs := p.ptrs()

	p.fic.RangeByPosX(scopeColIndexes, func(fieldIndex []uint16) {
		m := len(fieldIndex)
		if m == 1 {
			field := s.Field(int(fieldIndex[0]))
			*ptrs = append(*ptrs, field.Addr().Interface())
			return
		}

		ss := s
		for j := range fieldIndex {
			if j > 0 {
				if ss.Kind() == reflect.Pointer && ss.Type().Elem().Kind() == reflect.Struct {
					if ss.IsNil() {
						panic("reflect: indirection through nil pointer to embedded struct")
					}
					ss = ss.Elem()
				}
			}
			ss = ss.Field(int(fieldIndex[j]))
		}
		*ptrs = append(*ptrs, ss.Addr().Interface())
	})
	return *ptrs
}
