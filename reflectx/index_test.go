package reflectx

import (
	"reflect"
	"testing"
)

func TestNewFieldIndexContainer(t *testing.T) {
	container := NewFieldIndexContainer(6)

	t.Run("Cap", func(t *testing.T) {
		if got := container.Cap(); got != 6 {
			t.Errorf("FieldIndexContainer.Cap() = %v, want %v", got, 6)
		}
	})

	t.Run("Len", func(t *testing.T) {
		if got := container.Len(); got != 0 {
			t.Errorf("FieldIndexContainer.Len() = %v, want %v", got, 0)
		}
	})

	t.Run("Add", func(t *testing.T) {
		container.Add([]int{0})
		container.Add([]int{1})
		container.Add([]int{2})
		container.Add([]int{3, 0})
		container.Add([]int{3, 1})
		container.Add([]int{4})
		if got := container.Len(); got != 6 {
			t.Errorf("FieldIndexContainer.Len() after Add = %v, want %v", got, 6)
		}
		if n := container.Len(); n != 6 {
			t.Errorf("FieldIndexContainer.Len() = %v, want %v", n, 6)
		}

		expected := FieldIndexContainer{6, 6, 14, 15, 15, 16, 16, 17, 17, 19, 19, 21, 21, 22, 0, 1, 2, 3, 0, 3, 1, 4}
		if !reflect.DeepEqual(container, expected) {
			t.Errorf("FieldIndexContainer = %v, want %v", container, expected)
		}
	})
}

// func TestFieldIndexContainer_Add(t *testing.T) {
// 	container := NewFieldIndexContainer(2)
// 	container.Add([]int{1, 2, 3})

// 	expected := FieldIndexContainer{2, 1, 6, 9, 0, 0, 1, 2, 3}
// 	if !reflect.DeepEqual(container, expected) {
// 		t.Errorf("FieldIndexContainer.Add() = %v, want %v", container, expected)
// 	}

// }

func BenchmarkFieldIndexContainer_RangeByFieldPath(b *testing.B) {
	container := NewFieldIndexContainer(10)
	for i := 0; i < 10; i++ {
		container.Add([]int{1})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		sum := 0
		container.RangeByFieldPath([]int{1, 2, 3}, func(fieldPath []uint16) {
			for _, v := range fieldPath {
				sum += int(v)
			}
		})
		_ = sum
	}
}
