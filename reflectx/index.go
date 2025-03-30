package reflectx

type Index [8]int

const LengthPos = 7

func (idx *Index) Add(vals ...int) {
	for _, v := range vals {
		idx[idx[LengthPos]] = v
		idx[LengthPos]++
	}
}

func (idx *Index) Len() int {
	return idx[LengthPos]
}

func (idx *Index) Value(dest []int) {
	copy(dest, idx[:len(dest)])
}

func (idx *Index) Empty() bool {
	return idx[LengthPos] == 0
}

// FieldIndexContainer is a container for field indexes.
// [0] - cap
// [1] - len
// [2] - field1: from
// [3] - field1: to
// [4] - field2: from
// [5] - field2: to
// ...
// [n*2+2] - field1: field index in the struct (1 item)
// [n*2+3] - field2: field index in the struct (2 items)
// [n*2+5] - field3: field index in the struct (2 items)

type FieldIndexContainer []uint16

const (
	capPos     = 0
	lenPos     = 1
	firstIndex = 2
)

func NewFieldIndexContainer(n int) FieldIndexContainer {
	result := make(FieldIndexContainer, 2+n*2, 2+n*2*2)
	result[capPos] = uint16(n)
	return result
}

func (f FieldIndexContainer) Len() int {
	return int(f[lenPos])
}

func (f FieldIndexContainer) Cap() int {
	return int(f[capPos])
}

func (fic *FieldIndexContainer) Add(fieldPath []int) {

	from := len(*fic)

	for _, fp := range fieldPath {
		*fic = append(*fic, uint16(fp))
	}

	s := *fic

	to := len(s)
	pos := s[lenPos]
	offset := 2 + pos*2
	s[lenPos]++
	s[offset] = uint16(from)
	s[offset+1] = uint16(to)
}

func (f FieldIndexContainer) Range(fn func(fieldIndex []uint16)) {
	pos := int(f[lenPos])
	for i := 0; i < pos; i += 2 {
		offset := 2 + i
		from := f[offset]
		to := f[offset+1]
		fn(f[from:to])
	}
}

func (f FieldIndexContainer) RangeByOne(fn func(i int, fieldIndex int)) {
	pos := f[lenPos]
	for i := 2; i < int(pos+2); i += 2 {
		// from := f[i]
		// to := f[i+1]
		if f[i]+1 == f[i+1] {
			fn(i, int(f[i]))
			continue
		}
		for j := f[i]; j < f[i+1]; j++ {
			fn(i, int(f[j]))
		}
	}
}

func (f FieldIndexContainer) RangeByPos(fieldPos []int, fn func(fieldIndex int)) {
	for _, fp := range fieldPos {

		from, to := f.fieldIndex(fp)

		if x := f[from] + 1; x == f[to+1] {
			fn(int(f[x]))
			continue
		}
		for j := f[from]; j < f[to+1]; j++ {
			fn(int(f[j]))
		}
	}
}

func (f FieldIndexContainer) RangeByPosX(columnPos []int, fn func(fieldPath []uint16)) {
	for _, fp := range columnPos {

		from, to := f.fieldIndex(fp)
		fn(f[from:to])
		// 	continue
		// }
		// for j := f[from]; j < f[to+1]; j++ {
		// 	fn(int(f[j]))
		// }
	}
}

func (f FieldIndexContainer) fieldIndex(i int) (uint16, uint16) {
	offset := 2 + i*2
	return f[offset], f[offset+1]
}
