package reflectx

// FieldIndexContainer is a compact, flat []uint16 that maps column positions
// to their struct field index paths. It avoids per-element heap allocations by
// encoding all paths into a single contiguous slice.
//
// Layout:
//
//	[0]        cap   — maximum number of columns (set at construction)
//	[1]        len   — number of registered columns
//	[2+k*2]   from  — start index of column k's path data in the slice
//	[3+k*2]   to    — end index (exclusive) of column k's path data
//	[from:to]        the actual field index path for column k
//
// A path of length 1 refers to a top-level field; longer paths traverse
// embedded (possibly pointer-to-struct) fields.
type FieldIndexContainer []uint16

const (
	capPos     = 0
	lenPos     = 1
	firstIndex = 2
)

// NewFieldIndexContainer allocates a FieldIndexContainer sized for n columns.
// The initial slice covers the header and n (from, to) descriptor pairs;
// additional capacity is reserved for the path data appended by Add.
func NewFieldIndexContainer(n int) FieldIndexContainer {
	result := make(FieldIndexContainer, 2+n*2, 2+n*2*2)
	result[capPos] = uint16(n)
	return result
}

// Len returns the number of column paths currently registered.
func (f FieldIndexContainer) Len() int {
	return int(f[lenPos])
}

// Cap returns the maximum number of columns the container was sized for.
func (f FieldIndexContainer) Cap() int {
	return int(f[capPos])
}

// Add appends a new field path to the container. fieldPath is the sequence of
// struct field indices from the root struct to the target field (e.g. [0] for
// the first field, [2, 1] for the second field of the third embedded struct).
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

// RangeByFieldPath iterates over the field paths for the columns identified by
// columnPos (a slice of column positions) and calls fn for each path in order.
// The []uint16 slice passed to fn is a sub-slice of the container's backing
// array and must not be retained after fn returns.
func (f FieldIndexContainer) RangeByFieldPath(columnPos []int, fn func(fieldPath []uint16)) {
	for _, fp := range columnPos {
		from, to := f.fieldIndex(fp)
		fn(f[from:to])
	}
}

func (f FieldIndexContainer) fieldIndex(i int) (uint16, uint16) {
	offset := 2 + i*2
	return f[offset], f[offset+1]
}
