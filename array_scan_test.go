package velum_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/axkit/velum"
	"github.com/lib/pq"
)

// arrayRowsSQL mixes non-empty, NULL and empty arrays so that values from a
// previous row (GetMany reuses a single row T) would show up if not reset.
const arrayRowsSQL = `
SELECT id, tags, nums FROM (VALUES
	(1, ARRAY['a','b']::text[],     ARRAY[1,2]::int8[]),
	(2, ARRAY['c','d','e']::text[], ARRAY[3,4,5]::int8[]),
	(3, NULL::text[],               NULL::int8[]),
	(4, ARRAY[]::text[],            ARRAY[]::int8[]),
	(5, ARRAY['f']::text[],         ARRAY[6]::int8[]),
	(6, ARRAY[]::text[],            ARRAY[]::int8[])
) AS t(id, tags, nums) ORDER BY id`

var arrayRowsWant = []struct {
	tags []string
	nums []int64
}{
	{[]string{"a", "b"}, []int64{1, 2}},
	{[]string{"c", "d", "e"}, []int64{3, 4, 5}},
	{nil, nil},
	{[]string{}, []int64{}},
	{[]string{"f"}, []int64{6}},
	{[]string{}, []int64{}},
}

type arrRowPgx struct {
	ID   int
	Tags []string
	Nums []int64
}

type arrRowPq struct {
	ID   int
	Tags pq.StringArray
	Nums pq.Int64Array
}

func checkArrayRows(t *testing.T, tags [][]string, nums [][]int64) {
	t.Helper()
	if len(tags) != len(arrayRowsWant) {
		t.Fatalf("got %d rows, want %d", len(tags), len(arrayRowsWant))
	}
	for i, w := range arrayRowsWant {
		if (tags[i] == nil) != (w.tags == nil) || len(tags[i]) != len(w.tags) || (len(w.tags) > 0 && !reflect.DeepEqual(tags[i], w.tags)) {
			t.Errorf("row %d tags: got %#v, want %#v", i, tags[i], w.tags)
		}
		if (nums[i] == nil) != (w.nums == nil) || len(nums[i]) != len(w.nums) || (len(w.nums) > 0 && !reflect.DeepEqual(nums[i], w.nums)) {
			t.Errorf("row %d nums: got %#v, want %#v", i, nums[i], w.nums)
		}
	}

	// Appending to one row must not change any other row. Go in reverse so a
	// later row writes into a shared backing array before the earlier row
	// reallocates away from it.
	for i := len(tags) - 1; i >= 0; i-- {
		tags[i] = append(tags[i], "X")
	}
	for i, w := range arrayRowsWant {
		want := append(append([]string{}, w.tags...), "X")
		if !reflect.DeepEqual(tags[i], want) {
			t.Errorf("row %d shares backing array with another row: got %#v, want %#v", i, tags[i], want)
		}
	}
}

func TestArrayScan_Pgx_GetMany(t *testing.T) {
	initConnections(t)
	ds := velum.NewDataset[arrRowPgx](arrayRowsSQL)
	rows, err := ds.Select(context.Background(), dbwPgx)
	if err != nil {
		t.Fatal(err)
	}
	tags := make([][]string, len(rows))
	nums := make([][]int64, len(rows))
	for i, r := range rows {
		tags[i], nums[i] = r.Tags, r.Nums
	}
	checkArrayRows(t, tags, nums)
}

// lib/pq reuses the destination slice for an empty array ((*a)[:0]), so an
// empty array following a non-empty one used to alias the previous row.
func TestArrayScan_Pq_GetMany(t *testing.T) {
	initConnections(t)
	ds := velum.NewDataset[arrRowPq](arrayRowsSQL)
	rows, err := ds.Select(context.Background(), dbwSql)
	if err != nil {
		t.Fatal(err)
	}
	tags := make([][]string, len(rows))
	nums := make([][]int64, len(rows))
	for i, r := range rows {
		tags[i], nums[i] = r.Tags, r.Nums
	}
	checkArrayRows(t, tags, nums)
}

// database/sql + lib/pq can't scan an array into a plain []string;
// pq.StringArray (or another sql.Scanner) is required.
func TestArrayScan_Pq_PlainSliceNotSupported(t *testing.T) {
	initConnections(t)
	ds := velum.NewDataset[arrRowPgx](arrayRowsSQL)
	if _, err := ds.Select(context.Background(), dbwSql); err == nil {
		t.Fatal("expected scan error for []string with lib/pq, got nil")
	}
}

// pgx can't scan an array with a NULL element into []string;
// []*string or pgtype.FlatArray[pgtype.Text] is required.
func TestArrayScan_Pgx_NullElementNotSupported(t *testing.T) {
	initConnections(t)
	ds := velum.NewDataset[arrRowPgx](`SELECT 1, ARRAY['a',NULL]::text[], ARRAY[1]::int8[]`)
	if _, err := ds.Select(context.Background(), dbwPgx); err == nil {
		t.Fatal("expected scan error for NULL element in []string, got nil")
	}
}

type jsonRow struct {
	ID   int
	Meta map[string]any
}

// A map field must not carry keys over from the previous row.
func TestArrayScan_Pgx_JSONMapNotShared(t *testing.T) {
	initConnections(t)
	ds := velum.NewDataset[jsonRow](`SELECT * FROM (VALUES
		(1, '{"a":1}'::jsonb),
		(2, '{"b":2}'::jsonb)) AS t(id, meta) ORDER BY id`)
	rows, err := ds.Select(context.Background(), dbwPgx)
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"a": float64(1)}, {"b": float64(2)}}
	for i, r := range rows {
		if !reflect.DeepEqual(r.Meta, want[i]) {
			t.Errorf("row %d meta: got %v, want %v", i, r.Meta, want[i])
		}
	}
}
