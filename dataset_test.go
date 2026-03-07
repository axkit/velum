package velum_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/axkit/velum"
)

type customerJoinRow struct {
	SerialID        int    `dbw:"name=serial_id"`
	SerialFirstName string `dbw:"name=serial_first_name"`
	ManualID        int64  `dbw:"name=manual_id"`
	ManualFirstName string `dbw:"name=manual_first_name"`
}

type customerJoinDefaultOrderRow struct {
	SerialID    int
	ManualFirst string
}

type customerJoinNoAliasRow struct {
	SerialID   int
	SerialName string
	ManualID   int64
	ManualName string
}

type customerSimpleRow struct {
	ID   int
	Name string
	Age  int
}

func TestDataset_SelectJoins(t *testing.T) {
	initConnections(t)

	ctx := context.Background()
	ds := velum.NewDataset[customerJoinRow](`
		SELECT 
			s.id AS serial_id,
			s.first_name AS serial_first_name,
			m.id AS manual_id,
			m.first_name AS manual_first_name
		FROM customers_pk_serial AS s
		JOIN customers_pk_manual AS m ON m.id = s.id
		/*WHERE_JOIN*/
		/*ORDER_JOIN*/
	`)

	whereRange := velum.ClauseSet{
		"WHERE_JOIN": "WHERE s.id BETWEEN $1 AND $2",
		"ORDER_JOIN": "ORDER BY s.id",
	}
	rangeQuery := ds.WithClauses(whereRange)

	rows, err := rangeQuery.Select(ctx, dbwPgx, 1, 3)
	if err != nil {
		t.Fatalf("dataset select failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		expID := i + 1
		if row.SerialID != expID || row.ManualID != int64(expID) {
			t.Fatalf("row %d mismatched ids: %#v", i, row)
		}
		serialExp := fmt.Sprintf("First%d", row.SerialID-1)
		manualExp := fmt.Sprintf("First%d", row.ManualID)
		if row.SerialFirstName != serialExp || row.ManualFirstName != manualExp {
			t.Fatalf("row %d mismatched names: got %#v want serial=%s manual=%s", i, row, serialExp, manualExp)
		}
	}

	nameClauses := velum.ClauseSet{
		"WHERE_JOIN": "WHERE s.first_name LIKE $1",
		"ORDER_JOIN": "ORDER BY s.id",
	}
	nameRows, err := ds.WithClauses(nameClauses).Select(ctx, dbwPgx, "First1%")
	if err != nil {
		t.Fatalf("dataset select by name failed: %v", err)
	}
	if len(nameRows) == 0 {
		t.Fatalf("expected rows for First1%% pattern, got 0")
	}

	altClauses := velum.ClauseSet{
		"WHERE_JOIN": "WHERE s.id = $1",
	}
	altRows, err := ds.WithClauses(altClauses).Select(ctx, dbwPgx, 10)
	if err != nil {
		t.Fatalf("dataset select with clauses failed: %v", err)
	}
	if len(altRows) != 1 || altRows[0].SerialID != 10 {
		t.Fatalf("unexpected altRows: %#v", altRows)
	}

	got, err := ds.WithNamedClause("WHERE_JOIN", "WHERE s.id = $1").Get(ctx, dbwPgx, 15)
	if err != nil {
		t.Fatalf("dataset get with clauses failed: %v", err)
	}
	if got.SerialID != 15 || got.ManualID != 15 {
		t.Fatalf("unexpected row from GetWithClauses: %#v", got)
	}

	reRows, err := rangeQuery.Select(ctx, dbwPgx, 1, 3)
	if err != nil {
		t.Fatalf("dataset query select failed: %v", err)
	}
	if len(reRows) != 3 {
		t.Fatalf("expected 3 rows from query, got %d", len(reRows))
	}

	getClauseRow, err := ds.WithNamedClause("WHERE_JOIN", "WHERE s.id = $1").Get(ctx, dbwPgx, 18)
	if err != nil {
		t.Fatalf("dataset get with clause failed: %v", err)
	}
	if getClauseRow.SerialID != 18 {
		t.Fatalf("expected SerialID=18, got %d", getClauseRow.SerialID)
	}

	singleQuery := ds.WithNamedClause("WHERE_JOIN", "WHERE s.id = $1")
	singleRow, err := singleQuery.Get(ctx, dbwPgx, 19)
	if err != nil {
		t.Fatalf("dataset single clause query failed: %v", err)
	}
	if singleRow.SerialID != 19 {
		t.Fatalf("expected SerialID=19, got %d", singleRow.SerialID)
	}
}

func TestDataset_SelectJoinsWithoutAliases(t *testing.T) {
	initConnections(t)

	ctx := context.Background()
	ds := velum.NewDataset[customerJoinNoAliasRow](`
		SELECT 
			s.id,
			s.first_name,
			m.id,
			m.first_name
		FROM customers_pk_serial AS s
		JOIN customers_pk_manual AS m ON m.id = s.id
		/*WHERE_JOIN*/
		/*ORDER_JOIN*/
	`)

	rows, err := ds.WithClauses(velum.ClauseSet{
		"WHERE_JOIN": "WHERE s.id BETWEEN $1 AND $2",
		"ORDER_JOIN": "ORDER BY s.id",
	}).Select(ctx, dbwPgx, 1, 3)
	if err != nil {
		t.Fatalf("dataset select without aliases failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, row := range rows {
		expID := i + 1
		if row.SerialID != expID || row.ManualID != int64(expID) {
			t.Fatalf("row %d mismatched ids: %#v", i, row)
		}
		if row.SerialName == "" || row.ManualName == "" {
			t.Fatalf("row %d expected names, got %#v", i, row)
		}
	}
}

func TestDataset_DefaultColumnOrder(t *testing.T) {
	initConnections(t)

	ctx := context.Background()
	ds := velum.NewDataset[customerJoinDefaultOrderRow](`
		SELECT 
			s.id AS serial_id,
			m.first_name AS manual_first_name
		FROM customers_pk_serial AS s
		JOIN customers_pk_manual AS m ON m.id = s.id
		/*WHERE_SIMPLE*/
	`)

	row, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE s.id = $1").Get(ctx, dbwPgx, 11)
	if err != nil {
		t.Fatalf("dataset default column order get failed: %v", err)
	}
	if row.SerialID != 11 {
		t.Fatalf("expected SerialID=11, got %d", row.SerialID)
	}
	expName := fmt.Sprintf("First%d", row.SerialID)
	if row.ManualFirst != expName {
		t.Fatalf("expected ManualFirst=%s, got %s", expName, row.ManualFirst)
	}

	rows, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE s.id BETWEEN $1 AND $2 ORDER BY s.id").Select(ctx, dbwPgx, 20, 22)
	if err != nil {
		t.Fatalf("dataset default column order select failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for idx, r := range rows {
		expID := 20 + idx
		if r.SerialID != expID {
			t.Fatalf("row %d expected SerialID=%d, got %d", idx, expID, r.SerialID)
		}
		exp := fmt.Sprintf("First%d", expID)
		if r.ManualFirst != exp {
			t.Fatalf("row %d expected ManualFirst=%s, got %s", idx, exp, r.ManualFirst)
		}
	}
}

func TestDataset_DefaultColumnMapping(t *testing.T) {
	initConnections(t)

	ctx := context.Background()
	ds := velum.NewDataset[customerSimpleRow](`
		SELECT id, first_name, age
		FROM customers_pk_serial
		/*WHERE_SIMPLE*/
	`)

	row, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE id = $1").Get(ctx, dbwPgx, 5)
	if err != nil {
		t.Fatalf("dataset default column mapping get failed: %v", err)
	}
	if row.ID != 5 {
		t.Fatalf("expected ID=5, got %d", row.ID)
	}
	if row.Name == "" || !strings.HasPrefix(row.Name, "First") {
		t.Fatalf("expected Name to be populated, got %s", row.Name)
	}

	rows, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE id BETWEEN $1 AND $2 ORDER BY id").Select(ctx, dbwPgx, 2, 4)
	if err != nil {
		t.Fatalf("dataset default column mapping select failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, r := range rows {
		expID := 2 + i
		if r.ID != expID {
			t.Fatalf("row %d expected ID=%d, got %d", i, expID, r.ID)
		}
	}
}

func TestDataset_IgnoresRegularComments(t *testing.T) {
	initConnections(t)

	ctx := context.Background()
	ds := velum.NewDataset[customerSimpleRow](`
		SELECT
			/* regular comment */
			id,
			first_name,
			age
		FROM customers_pk_serial
		/*WHERE_SIMPLE*/
	`)

	if _, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE id = $1").Get(ctx, dbwPgx, 7); err != nil {
		t.Fatalf("dataset regular comment get failed: %v", err)
	}
	rows, err := ds.WithNamedClause("WHERE_SIMPLE", "WHERE id BETWEEN $1 AND $2 ORDER BY id").Select(ctx, dbwPgx, 8, 9)
	if err != nil {
		t.Fatalf("dataset regular comment select failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
