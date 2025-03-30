package reflectx

import (
	"reflect"
	"testing"
)

func TestIndex_Add(t *testing.T) {
	idx := &Index{}
	idx.Add(1, 2, 3)

	expected := Index{1, 2, 3, 0, 0, 0, 0, 3}
	if *idx != expected {
		t.Errorf("Index.Add() = %v, want %v", *idx, expected)
	}
}

func TestIndex_Len(t *testing.T) {
	idx := &Index{}
	idx.Add(1, 2, 3)

	if idx.Len() != 3 {
		t.Errorf("Index.Len() = %v, want %v", idx.Len(), 3)
	}
}

func TestIndex_Value(t *testing.T) {
	idx := &Index{}
	idx.Add(1, 2, 3)

	dest := make([]int, 3)
	idx.Value(dest)

	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(dest, expected) {
		t.Errorf("Index.Value() = %v, want %v", dest, expected)
	}
}

func TestIndex_Empty(t *testing.T) {
	idx := &Index{}

	if !idx.Empty() {
		t.Errorf("Index.Empty() = false, want true")
	}

	idx.Add(1)
	if idx.Empty() {
		t.Errorf("Index.Empty() = true, want false")
	}
}

func TestNewFieldIndexContainer(t *testing.T) {
	container := NewFieldIndexContainer(2)

	if container.Cap() != 2 {
		t.Errorf("NewFieldIndexContainer.Cap() = %v, want %v", container.Cap(), 2)
	}
	if container.Len() != 0 {
		t.Errorf("NewFieldIndexContainer.Len() = %v, want %v", container.Len(), 0)
	}
}

func TestFieldIndexContainer_Add(t *testing.T) {
	container := NewFieldIndexContainer(2)
	container.Add([]int{1, 2, 3})

	expected := FieldIndexContainer{2, 1, 6, 9, 0, 0, 1, 2, 3}
	if !reflect.DeepEqual(container, expected) {
		t.Errorf("FieldIndexContainer.Add() = %v, want %v", container, expected)
	}

}

func TestFieldIndexContainer_Range(t *testing.T) {
	container := NewFieldIndexContainer(3)
	container.Add([]int{0})
	container.Add([]int{1})
	container.Add([]int{2, 0})

	i := 0
	container.Range(func(fieldIndex []uint16) {
		if i == 0 {
			if !reflect.DeepEqual(fieldIndex, []uint16{0}) {
				t.Errorf("FieldIndexContainer.Range() = %v, want %v", fieldIndex, []int{0})
			}
		}
		if i == 1 {
			if !reflect.DeepEqual(fieldIndex, []uint16{1}) {
				t.Errorf("FieldIndexContainer.Range() = %v, want %v", fieldIndex, []int{1})
			}
		}
		if i == 2 {
			if !reflect.DeepEqual(fieldIndex, []uint16{2, 0}) {
				t.Errorf("FieldIndexContainer.Range() = %v, want %v", fieldIndex, []int{2, 0})
			}
		}
		i++
	})
}

func BenchmarkFieldIndexContainer_Range(b *testing.B) {
	container := NewFieldIndexContainer(10)
	for i := 0; i < 10; i++ {
		container.Add([]int{1})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sum := 0
		container.Range(func(fieldIndex []uint16) {
			for _, v := range fieldIndex {
				sum += int(v)
			}
		})
		_ = sum
	}
}

func BenchmarkFieldIndexContainer_RangeByOne(b *testing.B) {
	container := NewFieldIndexContainer(10)
	for i := 0; i < 10; i++ {
		container.Add([]int{1})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sum := 0
		container.RangeByOne(func(i int, fieldIndex int) {
			sum += fieldIndex
		})
		_ = sum
	}
}

func BenchmarkFieldIndexContainer_RangeByPos(b *testing.B) {
	container := NewFieldIndexContainer(10)
	for i := 0; i < 10; i++ {
		container.Add([]int{1})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sum := 0
		container.RangeByPos([]int{1, 2, 3}, func(fieldIndex int) {
			sum += fieldIndex
		})
		_ = sum
	}
}
