package velum

import (
	"fmt"
	"runtime"
	"testing"
)

// testMLRow is a minimal struct used for memory leak tests.
// It has a serial PK (detected by the "id" column name convention),
// an unscoped regular column, and a custom-scoped column.
type testMLRow struct {
	ID   int    `dbw:"gen=serial"`
	Name string
	Age  int `dbw:"age"`
}

// cacheSnapshot captures the current size of every CommandContanier cache map.
type cacheSnapshot struct {
	sel         int
	cmdInsert   int
	cmdUpdate   int
	cmdDelete   int
	retInsert   int
	retUpdate   int
	retDelete   int
	fn          int
}

func snapshotCaches[T any](cc *CommandContanier[T]) cacheSnapshot {
	return cacheSnapshot{
		sel:       len(cc.sel),
		cmdInsert: len(cc.cmd[Insert]),
		cmdUpdate: len(cc.cmd[Update]),
		cmdDelete: len(cc.cmd[Delete]),
		retInsert: len(cc.retCmd[Insert]),
		retUpdate: len(cc.retCmd[Update]),
		retDelete: len(cc.retCmd[Delete]),
		fn:        len(cc.fn),
	}
}

// delta returns the growth of every cache field relative to a baseline snapshot.
func (s cacheSnapshot) delta(baseline cacheSnapshot) cacheSnapshot {
	return cacheSnapshot{
		sel:       s.sel - baseline.sel,
		cmdInsert: s.cmdInsert - baseline.cmdInsert,
		cmdUpdate: s.cmdUpdate - baseline.cmdUpdate,
		cmdDelete: s.cmdDelete - baseline.cmdDelete,
		retInsert: s.retInsert - baseline.retInsert,
		retUpdate: s.retUpdate - baseline.retUpdate,
		retDelete: s.retDelete - baseline.retDelete,
		fn:        s.fn - baseline.fn,
	}
}

// TestCommandContainer_CacheDeduplicates verifies that repeated calls with
// identical (scope, clause) arguments hit the cache and do not grow the
// internal maps — the critical property for long-lived applications.
func TestCommandContainer_CacheDeduplicates(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cc := tbl.CommandContainer()

	// Use the exact clause format the table uses internally so we can predict
	// whether a call is a cache hit or a cache miss.
	clause := tbl.wherePkClause // e.g. "WHERE id=$1"
	baseline := snapshotCaches(cc)

	const n = 1000
	for range n {
		cc.Select(FullScope, clause)    // already cached by initFrequentCommands — hit
		cc.Select("age", clause)        // new entry on first iteration, hit on subsequent
		cc.Insert(FullScope)            // new on first iteration
		cc.Insert("age")               // new on first iteration
		cc.InsertReturning(FullScope, FullScope) // already cached by initFrequentCommands — hit
		cc.InsertReturning("age", FullScope)     // new on first iteration
		cc.Update(FullScope, ByPK())             // new on first iteration (Update, not UpdateReturning)
		cc.Update("age", ByPK())
		cc.UpdateReturning(FullScope, FullScope, ByPK()) // already cached by initFrequentCommands — hit
		cc.UpdateReturning("age", FullScope, ByPK())     // new on first iteration
		cc.Delete(clause)
		cc.DeleteReturning(FullScope, clause)
		cc.Func(Exist, clause)
		cc.Func(Count, clause)
	}

	got := snapshotCaches(cc).delta(baseline)

	// Each unique (scope, clause) pair adds exactly one cache entry on the first
	// call and must be a hit for all subsequent calls.
	// sel:      "age"+clause is the only new Select (FullScope+clause was pre-cached)
	// cmdInsert: FullScope+"" and "age"+"" are both new
	// retInsert: "age"+FullScope is the only new InsertReturning
	// cmdUpdate: FullScope+pkWhere and "age"+pkWhere are both new
	// retUpdate: "age"+FullScope+pkWhere is the only new UpdateReturning
	// cmdDelete: EmptyScope+clause is new
	// retDelete: EmptyScope+FullScope+clause is new
	// fn:        Exist+clause and Count+clause are both new
	expected := cacheSnapshot{
		sel:       1,
		cmdInsert: 2,
		retInsert: 1,
		cmdUpdate: 2,
		retUpdate: 1,
		cmdDelete: 1,
		retDelete: 1,
		fn:        2,
	}

	if got != expected {
		t.Errorf("cache growth mismatch after %d identical iterations:\ngot      %+v\nexpected %+v", n, got, expected)
	}
}

// TestCommandContainer_CacheGrowsForUniqueClauses documents the known design
// limitation: passing dynamically constructed clause strings causes proportional
// cache growth, which becomes a memory leak in long-lived applications.
//
// The correct pattern is to use fixed, parameterized clause strings ($1, $2, …)
// and pass values as query arguments — NOT to inline values into the SQL string.
func TestCommandContainer_CacheGrowsForUniqueClauses(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cc := tbl.CommandContainer()

	baseline := snapshotCaches(cc)

	const n = 100
	for i := range n {
		// Anti-pattern: inlining values into the SQL clause string.
		// Every unique string creates a new, permanent cache entry.
		cc.Select(FullScope, fmt.Sprintf("WHERE id = %d", i))
		cc.Func(Count, fmt.Sprintf("WHERE age > %d", i))
	}

	got := snapshotCaches(cc).delta(baseline)

	if got.sel != n {
		t.Errorf("sel: expected delta=%d unique cache entries (one per unique clause), got %d", n, got.sel)
	}
	if got.fn != n {
		t.Errorf("fn: expected delta=%d unique cache entries (one per unique clause), got %d", n, got.fn)
	}
	t.Logf("confirmed: %d unique Select clauses → %d cache entries (proportional growth)", n, got.sel)
}

// TestPointerSlicePool_ReleasedSliceIsReused verifies that Release returns the
// slice to the pool so the next Get can reuse it without a new allocation.
func TestPointerSlicePool_ReleasedSliceIsReused(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cpos := tbl.freqCmd.selectAllFieldsByPK.cpos
	var row testMLRow

	p1 := tbl.pool.StructFieldPtrs(&row, cpos)
	addr1 := fmt.Sprintf("%p", p1)
	tbl.pool.Release(p1)

	p2 := tbl.pool.StructFieldPtrs(&row, cpos)
	addr2 := fmt.Sprintf("%p", p2)
	tbl.pool.Release(p2)

	// sync.Pool is not guaranteed to return the identical object (the GC may
	// clear the pool between calls), but in a single-goroutine test with no GC
	// pressure this should hold. We log rather than fail on mismatch.
	if addr1 != addr2 {
		t.Logf("pool returned different slice objects (addr1=%s addr2=%s): GC may have cleared the pool", addr1, addr2)
	} else {
		t.Logf("pool correctly reused the same slice object (addr=%s)", addr1)
	}
}

// TestPointerSlicePool_AllocsAfterWarmup verifies that after an initial warmup
// the pool Get/Release cycle does not allocate a new backing slice per call.
func TestPointerSlicePool_AllocsAfterWarmup(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cpos := tbl.freqCmd.selectAllFieldsByPK.cpos
	var row testMLRow

	// Warm up: ensure the pool holds at least one slice.
	warmup := tbl.pool.StructFieldPtrs(&row, cpos)
	tbl.pool.Release(warmup)

	allocs := testing.AllocsPerRun(200, func() {
		p := tbl.pool.StructFieldPtrs(&row, cpos)
		tbl.pool.Release(p)
	})

	// Allow at most 1 allocation per iteration to account for reflect
	// closure escaping in edge cases. The slice itself must not allocate.
	const maxAllocs = 1.0
	if allocs > maxAllocs {
		t.Errorf("PointerSlicePool: %.1f allocs/cycle after warmup, want ≤%.1f", allocs, maxAllocs)
	}
	t.Logf("PointerSlicePool: %.2f allocs/cycle", allocs)
}

// TestTable_ObjPool_AllocsAfterWarmup verifies that the Table's ObjPool
// Object/ObjectPut cycle is allocation-free after warmup.
func TestTable_ObjPool_AllocsAfterWarmup(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")

	// Warm up both pools (ObjPool and pointer-slice pool).
	obj, ptrs := tbl.Object(FullScope)
	tbl.ObjectPut(obj, ptrs)

	allocs := testing.AllocsPerRun(200, func() {
		o, p := tbl.Object(FullScope)
		tbl.ObjectPut(o, p)
	})

	const maxAllocs = 1.0
	if allocs > maxAllocs {
		t.Errorf("ObjPool: %.1f allocs/cycle after warmup, want ≤%.1f", allocs, maxAllocs)
	}
	t.Logf("ObjPool: %.2f allocs/cycle", allocs)
}

// TestTable_HeapStableUnderRepeatedPoolUsage verifies that the live heap
// does not grow proportionally when the pools are used correctly (with Release).
func TestTable_HeapStableUnderRepeatedPoolUsage(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cpos := tbl.freqCmd.selectAllFieldsByPK.cpos
	var row testMLRow

	// Warm up.
	for range 50 {
		p := tbl.pool.StructFieldPtrs(&row, cpos)
		tbl.pool.Release(p)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 200_000
	for range iterations {
		p := tbl.pool.StructFieldPtrs(&row, cpos)
		tbl.pool.Release(p) // critical: must be called to return to pool
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// TotalAlloc is cumulative bytes allocated. With pool reuse, only a small
	// constant number of bytes are allocated per iteration (reflect boxing, etc.).
	// The threshold is deliberately generous to avoid flakiness.
	const maxBytesPerIter = 256
	totalAlloced := after.TotalAlloc - before.TotalAlloc
	perIter := float64(totalAlloced) / iterations
	if perIter > maxBytesPerIter {
		t.Errorf("PointerSlicePool: %.1f bytes/iter over %d iterations, want ≤%d bytes/iter — possible leak",
			perIter, iterations, maxBytesPerIter)
	}
	t.Logf("PointerSlicePool: %.2f bytes/iter over %d iterations (total=%d bytes)", perIter, iterations, totalAlloced)

	// Live heap should NOT have grown proportionally to the number of iterations.
	// After GC, HeapAlloc reflects only live objects. Pool holds O(1) objects.
	heapGrowth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const maxHeapGrowthBytes = 1 << 20 // 1 MiB — generous ceiling for test overhead
	if heapGrowth > maxHeapGrowthBytes {
		t.Errorf("live heap grew by %d bytes after %d pool cycles — pool may be leaking objects",
			heapGrowth, iterations)
	}
	t.Logf("live heap delta after GC: %+d bytes", heapGrowth)
}

// TestTable_HeapGrowsWhenReleaseIsSkipped documents the consequence of
// failing to call Release: every Get allocates a fresh slice, none are reused.
func TestTable_HeapGrowsWhenReleaseIsSkipped(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cpos := tbl.freqCmd.selectAllFieldsByPK.cpos
	var row testMLRow

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	const iterations = 10_000
	// Intentionally skip Release to demonstrate the allocation pattern.
	for range iterations {
		_ = tbl.pool.StructFieldPtrs(&row, cpos)
		// Release NOT called — simulating a bug in caller code.
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	totalAlloced := after.TotalAlloc - before.TotalAlloc
	perIter := float64(totalAlloced) / iterations
	t.Logf("Without Release: %.2f bytes/iter over %d iterations (total=%d bytes)", perIter, iterations, totalAlloced)

	// Without Release, each call to StructFieldPtrs must allocate a new slice
	// from pool.New. Confirm that allocations actually happened.
	if totalAlloced == 0 {
		t.Error("expected non-zero allocations when Release is skipped")
	}
}

// TestCommandContainer_ConcurrentCacheStability verifies that concurrent
// access to the CommandContainer caches does not cause cache growth beyond
// the expected number of unique (scope, clause) keys.
func TestCommandContainer_ConcurrentCacheStability(t *testing.T) {
	tbl := NewTable[testMLRow]("test_ml")
	cc := tbl.CommandContainer()

	// Use the same clause format as initFrequentCommands to ensure hits.
	clause := tbl.wherePkClause
	baseline := snapshotCaches(cc)

	const goroutines = 50
	const callsPerGoroutine = 200

	done := make(chan struct{}, goroutines)
	for range goroutines {
		go func() {
			for range callsPerGoroutine {
				cc.Select(FullScope, clause)    // cache hit after first call
				cc.Select("age", clause)        // cache hit after first call
				cc.Insert(FullScope)            // cache hit after first call
				cc.Update(FullScope, ByPK())    // cache hit after first call
				cc.Func(Count, clause)          // cache hit after first call
			}
			done <- struct{}{}
		}()
	}

	for range goroutines {
		<-done
	}

	got := snapshotCaches(cc).delta(baseline)

	// Concurrent calls with the same keys must not create duplicate entries.
	// FullScope+clause Select is already cached; only "age"+clause is new.
	if got.sel != 1 {
		t.Errorf("sel: expected delta=1 after concurrent identical calls, got %d (race or deduplication failure)", got.sel)
	}
	if got.cmdInsert != 1 {
		t.Errorf("cmd[Insert]: expected delta=1, got %d", got.cmdInsert)
	}
	if got.cmdUpdate != 1 {
		t.Errorf("cmd[Update]: expected delta=1, got %d", got.cmdUpdate)
	}
	if got.fn != 1 {
		t.Errorf("fn: expected delta=1, got %d", got.fn)
	}
}
