package velum_test

import (
	"context"
	"testing"
	"time"

	"github.com/axkit/velum"
	"github.com/lib/pq"
)

const benchManySQL = `
SELECT g, 'name_' || g, CASE WHEN g % 3 = 0 THEN NULL ELSE 'ssn' || g END, now(),
       CASE WHEN g % 5 = 0 THEN ARRAY[]::text[] ELSE ARRAY['a'||g,'b'||g,'c'||g] END,
       ARRAY[g, g+1]::int8[]
FROM generate_series(1, 1000) g`

type benchRowPgx struct {
	ID      int
	Name    string
	SSN     *string
	Created time.Time
	Tags    []string
	Nums    []int64
}

type benchRowPq struct {
	ID      int
	Name    string
	SSN     *string
	Created time.Time
	Tags    pq.StringArray
	Nums    pq.Int64Array
}

func Benchmark_GetMany1000_Pgx(b *testing.B) {
	initConnections(b)
	ds := velum.NewDataset[benchRowPgx](benchManySQL)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ds.Select(ctx, dbwPgx); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_GetMany1000_Pq(b *testing.B) {
	initConnections(b)
	ds := velum.NewDataset[benchRowPq](benchManySQL)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ds.Select(ctx, dbwSql); err != nil {
			b.Fatal(err)
		}
	}
}
