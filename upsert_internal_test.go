package velum

import (
	"reflect"
	"testing"
)

func TestAssignmentTargets(t *testing.T) {
	cases := []struct {
		set  string
		want []string
	}{
		{"a = 1", []string{"a"}},
		{"a=1, B = 2", []string{"a", "b"}},
		{`"Mixed" = 1`, []string{"Mixed"}},
		{"(a, b) = (1, 2), c = f(x, y)", []string{"a", "b", "c"}},
		{"tags[1] = 'x', point.x = 1", []string{"tags", "point"}},
		{"n = CASE WHEN t.a <= $1 THEN 'p=q, r=s' ELSE 0 END", []string{"n"}},
		{"checked = '{}', imported_at = now()", []string{"checked", "imported_at"}},
	}
	for _, tc := range cases {
		if got := assignmentTargets(tc.set); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("assignmentTargets(%q) = %q, want %q", tc.set, got, tc.want)
		}
	}
}

type upsertCacheRow struct {
	ID   int `dbw:"gen=no"`
	Name string
}

func TestUpsert_CacheDeduplicates(t *testing.T) {
	tbl := NewTable[upsertCacheRow]("upsert_cache")
	oc := OnConflict{Target: "(id)", Action: DoUpdate, UpdateScope: FullScope}

	for range 3 {
		tbl.MustUpsertCommand(FullScope, oc)
		tbl.MustUpsertReturningCommand(FullScope, FullScope, oc)
	}
	if n := len(tbl.cc.ups); n != 2 {
		t.Fatalf("cache entries = %d, want 2 (one per returning flag)", n)
	}

	// An invalid configuration is not cached.
	if _, err := tbl.UpsertCommand(FullScope, OnConflict{Action: DoUpdate, UpdateScope: FullScope}); err == nil {
		t.Fatal("want an error")
	}
	if n := len(tbl.cc.ups); n != 2 {
		t.Fatalf("cache entries = %d after an error, want 2", n)
	}
}
