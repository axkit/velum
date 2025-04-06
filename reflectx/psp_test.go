package reflectx

import (
	"testing"
)

type TestStruct struct {
	ID   int
	Name string
	*EmbeddedStruct
	CreatedAt string
}

type EmbeddedStruct struct {
	Age int
}

func TestPointerSlicePool(t *testing.T) {

	fields := ExtractStructFields(&TestStruct{}, "dbw")
	fic := NewFieldIndexContainer(len(fields))
	for _, fp := range fields {
		fic.Add(fp.Path)
	}

	pool := NewPointerSlicePool[TestStruct](fic)

	t.Run("StructFieldPtrs", func(t *testing.T) {
		testStruct := &TestStruct{
			ID:   1,
			Name: "Test",
			EmbeddedStruct: &EmbeddedStruct{
				Age: 30,
			},
			CreatedAt: "2025-01-01",
		}
		scopeColIndexes := []int{0, 1, 2, 3}
		ptrs := pool.StructFieldPtrs(testStruct, scopeColIndexes)
		if len(*ptrs) != len(scopeColIndexes) {
			t.Errorf("PointerSlicePool.StructFieldPtrs() returned %d pointers, want %d", len(*ptrs), len(scopeColIndexes))
		}
		if *(*ptrs)[0].(*int) != testStruct.ID {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[0] = %v, want %v", *(*ptrs)[0].(*int), testStruct.ID)
		}
		if *(*ptrs)[1].(*string) != testStruct.Name {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[1] = %v, want %v", *(*ptrs)[1].(*string), testStruct.Name)
		}
		if *(*ptrs)[2].(*int) != testStruct.Age {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[2] = %v, want %v", *(*ptrs)[2].(*int), testStruct.Age)
		}
		if *(*ptrs)[3].(*string) != testStruct.CreatedAt {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[3] = %v, want %v", *(*ptrs)[3].(*string), testStruct.CreatedAt)
		}

		pool.Release(ptrs)
		if len(*ptrs) != 0 {
			t.Errorf("PointerSlicePool.Release() did not reset the slice, got length = %d", len(*ptrs))
		}
	})

}

func TestPointerSlicePool_Embedded_Nil(t *testing.T) {

	fields := ExtractStructFields(&TestStruct{}, "dbw")
	fic := NewFieldIndexContainer(len(fields))
	for _, fp := range fields {
		fic.Add(fp.Path)
	}

	pool := NewPointerSlicePool[TestStruct](fic)

	t.Run("StructFieldPtrs", func(t *testing.T) {
		testStruct := &TestStruct{
			ID:             1,
			Name:           "Test",
			EmbeddedStruct: nil,
			CreatedAt:      "2025-01-01",
		}
		scopeColIndexes := []int{0, 1, 2, 3}
		ptrs := pool.StructFieldPtrs(testStruct, scopeColIndexes)
		if len(*ptrs) != len(scopeColIndexes) {
			t.Errorf("PointerSlicePool.StructFieldPtrs() returned %d pointers, want %d", len(*ptrs), len(scopeColIndexes))
		}
		if *(*ptrs)[0].(*int) != testStruct.ID {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[0] = %v, want %v", *(*ptrs)[0].(*int), testStruct.ID)
		}
		if *(*ptrs)[1].(*string) != testStruct.Name {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[1] = %v, want %v", *(*ptrs)[1].(*string), testStruct.Name)
		}
		if *(*ptrs)[2].(*int) != testStruct.Age {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[2] = %v, want %v", *(*ptrs)[2].(*int), testStruct.Age)
		}
		if *(*ptrs)[3].(*string) != testStruct.CreatedAt {
			t.Errorf("PointerSlicePool.StructFieldPtrs() ptr[3] = %v, want %v", *(*ptrs)[3].(*string), testStruct.CreatedAt)
		}

		pool.Release(ptrs)
		if len(*ptrs) != 0 {
			t.Errorf("PointerSlicePool.Release() did not reset the slice, got length = %d", len(*ptrs))
		}
	})

}
