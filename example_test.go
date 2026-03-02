package velum_test

import (
	"context"
	"fmt"
	"time"

	"github.com/axkit/velum"
)

// exOrderSummary is used in Dataset examples. It maps columns returned by
// a multi-table SELECT to a flat Go struct using the name= tag option.
type exOrderSummary struct {
	OrderID      int     `dbw:"name=order_id"`
	CustomerName string  `dbw:"name=customer_name"`
	Total        float64 `dbw:"name=total"`
}

// exOrderRank extends exOrderSummary with a window-function rank column.
type exOrderRank struct {
	OrderID      int     `dbw:"name=order_id"`
	CustomerName string  `dbw:"name=customer_name"`
	Total        float64 `dbw:"name=total"`
	Rank         int     `dbw:"name=rank"`
}

// --- Table ---

// ExampleNewTable_options shows the functional options available when creating
// a Table descriptor.
func ExampleNewTable_options() {
	// Default: "dbw" tag, $N placeholders, snake_case column names.
	_ = velum.NewTable[CustomerSerial]("customers")

	// Override to use a different struct tag and placeholder style.
	_ = velum.NewTable[CustomerSerial]("customers",
		velum.WithTag("db"),                             // read "db" tag instead of "dbw"
		velum.WithArgFormatter(velum.ArgAsQuestionMark), // use ? placeholders
	)
}

// ExampleTable_InsertReturning shows how to insert a row and scan the
// database-generated primary key and system columns back in one round-trip.
func ExampleTable_InsertReturning() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper // e.g. pgxw.NewDatabaseWrapper(pool)

	now := time.Now()
	c := CustomerSerial{Customer: Customer{
		FirstName: "Alice",
		LastName:  "Smith",
		Age:       30,
		SystemColumns: SystemColumns{
			CreatedAt:  now,
			RowVersion: 1,
		},
	}}

	// INSERT INTO customers (first_name, last_name, age, ssn, row_version, created_at, ...)
	// VALUES ($1, $2, $3, $4, $5, $6, ...) RETURNING id, first_name, ...
	inserted, err := tbl.InsertReturning(context.Background(), db, &c, velum.FullScope, velum.FullScope)
	if err != nil {
		return
	}
	fmt.Println("new id:", inserted.ID)
}

// ExampleTable_Get shows how to fetch a single row by an arbitrary clause.
// This is the natural approach for tables with composite keys or when
// looking up by a unique constraint other than the primary key.
func ExampleTable_Get() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper

	// SELECT t.id, t.first_name, t.last_name, t.age, ...
	// FROM customers t WHERE first_name=$1 AND last_name=$2
	c, err := tbl.Get(context.Background(), db, velum.FullScope, "WHERE first_name=$1 AND last_name=$2", "Alice", "Smith")
	if err != nil {
		return
	}
	fmt.Println(c.ID, c.FirstName, c.LastName)
}

// ExampleTable_GetByPK shows a primary-key lookup returning the full row.
func ExampleTable_GetByPK() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper

	// SELECT t.id, t.first_name, t.last_name, t.age, t.ssn, t.row_version, ...
	// FROM customers t WHERE id=$1
	c, err := tbl.GetByPK(context.Background(), db, 42)
	if err != nil {
		return
	}
	fmt.Println(c.FirstName, c.LastName)
}

// ExampleTable_Select shows scoped SELECT queries with a WHERE clause.
func ExampleTable_Select() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper
	ctx := context.Background()

	// Full row — all columns, filtered and sorted.
	// SELECT t.id, t.first_name, ... FROM customers t WHERE age > $1 ORDER BY last_name
	all, err := tbl.Select(ctx, db, velum.FullScope, "WHERE age > $1 ORDER BY last_name", 18)
	_, _ = all, err

	// Lightweight list: only the "age" scope column (plus PK).
	// SELECT t.id, t.age FROM customers t WHERE age > $1
	light, err := tbl.Select(ctx, db, "age", "WHERE age > $1", 18)
	_, _ = light, err

	// Exclude "ssn" — all non-system columns except ssn are selected.
	// SELECT t.id, t.first_name, t.last_name, t.age, ... FROM customers t WHERE age > $1
	safe, err := tbl.Select(ctx, db, "!ssn", "WHERE age > $1", 18)
	_, _ = safe, err
}

// ExampleTable_ExistByPK shows existence checks.
func ExampleTable_ExistByPK() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper
	ctx := context.Background()

	// Check by primary key.
	ok, err := tbl.ExistByPK(ctx, db, 42)
	_, _ = ok, err

	// Check by arbitrary condition.
	ok, err = tbl.Exist(ctx, db, "WHERE first_name=$1 AND last_name=$2", "Alice", "Smith")
	_, _ = ok, err

	// Count matching rows.
	n, err := tbl.Count(ctx, db, "WHERE age > $1", 18)
	_, _ = n, err
}

// ExampleTable_UpdateByPK shows scope-based partial updates by primary key.
func ExampleTable_UpdateByPK() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper
	ctx := context.Background()

	now := time.Now()
	c := &CustomerSerial{
		ID: 1,
		Customer: Customer{
			Age: 31,
			SystemColumns: SystemColumns{UpdatedAt: &now},
		},
	}

	// Update only the "age" column.
	// UPDATE customers SET age=$2, row_version=row_version+1, updated_at=$3 WHERE id=$1
	_, err := tbl.UpdateByPK(ctx, db, c, "age")
	_ = err

	// Update all non-system columns (full row update).
	// UPDATE customers SET first_name=$2, last_name=$3, age=$4, ..., row_version=row_version+1, updated_at=$5 WHERE id=$1
	_, err = tbl.UpdateByPK(ctx, db, c, velum.FullScope)
	_ = err

	// Update everything except SSN — useful when SSN is managed separately.
	// UPDATE customers SET first_name=$2, last_name=$3, age=$4, row_version=row_version+1, updated_at=$5 WHERE id=$1
	_, err = tbl.UpdateByPK(ctx, db, c, "!ssn")
	_ = err

	// Update and return the resulting row.
	// UPDATE customers SET age=$2, ... WHERE id=$1 RETURNING id, first_name, ...
	updated, err := tbl.UpdateReturningByPK(ctx, db, c, "age", velum.FullScope)
	_, _ = updated, err
}

// ExampleTable_SoftDeleteByPK shows a soft-delete operation. Fields tagged
// dbw:"delete" (deleted_at, deleted_by, …) are set; row_version is
// auto-incremented. No row is physically removed.
func ExampleTable_SoftDeleteByPK() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper

	c := &CustomerSerial{ID: 1}

	// UPDATE customers SET deleted_at=$2, deleted_by=$3, row_version=row_version+1 WHERE id=$1
	_, err := tbl.SoftDeleteByPK(context.Background(), db, c)
	_ = err

	// Same, but return the final row state.
	deleted, err := tbl.SoftDeleteReturningByPK(context.Background(), db, c)
	_, _ = deleted, err
}

// ExampleTable_DeleteByPK shows hard-delete operations.
func ExampleTable_DeleteByPK() {
	tbl := velum.NewTable[CustomerSerial]("customers")
	var db velum.DatabaseWrapper
	ctx := context.Background()

	// Delete by primary key.
	// DELETE FROM customers WHERE id=$1
	_, err := tbl.DeleteByPK(ctx, db, 42)
	_ = err

	// Delete by arbitrary clause (e.g. bulk expiry cleanup).
	// DELETE FROM customers WHERE deleted_at < $1
	_, err = tbl.Delete(ctx, db, "WHERE deleted_at < $1", time.Now().Add(-30*24*time.Hour))
	_ = err
}

// --- Dataset ---

// ExampleNewDataset shows how to define a multi-table query with named SQL
// placeholders that can be injected at call time.
func ExampleNewDataset() {
	// Placeholders are SQL block comments matching /*IDENTIFIER*/.
	// Omitted placeholders are replaced with an empty string.
	ds := velum.NewDataset[exOrderSummary](`
		SELECT
			o.id            AS order_id,
			c.first_name    AS customer_name,
			o.total         AS total
		FROM orders o
		JOIN customers c ON c.id = o.customer_id
		/*WHERE_FILTER*/
		/*ORDER_BY*/
	`)
	_ = ds
}

// ExampleDataset_WithClauses shows how to build a reusable query and how
// Velum automatically renumbers $N parameters across the base query and
// injected clauses.
//
// The base query contains a window function with two ORDER BY clauses:
//   - an internal one inside OVER (ORDER BY o.total DESC) that controls
//     ranking within each partition;
//   - an external one injected via /*ORDER_BY*/ that controls the final
//     result order.
//
// The PARTITION BY expression is also injectable via /*PARTITION_BY*/,
// so callers can group by customer, region, or any other dimension without
// changing the base query.
func ExampleDataset_WithClauses() {
	// The base query uses $1 (tenant_id). RANK() carries its own internal
	// ORDER BY (o.total DESC); both PARTITION BY and the outer ORDER BY are
	// injected at call time through named placeholders.
	ds := velum.NewDataset[exOrderRank](`
		SELECT
			o.id                                             AS order_id,
			c.first_name                                     AS customer_name,
			o.total                                          AS total,
			RANK() OVER (/*PARTITION_BY*/
			             ORDER BY o.total DESC)              AS rank
		FROM orders o
		JOIN customers c ON c.id = o.customer_id
		WHERE o.tenant_id = $1
		/*EXTRA_FILTER*/
		/*ORDER_BY*/
	`)

	var db velum.DatabaseWrapper
	ctx := context.Background()

	// Build a reusable compiled query once.
	// EXTRA_FILTER starts its own numbering at $1; Velum shifts it to $2 so
	// it follows the tenant_id argument already present in the base query.
	// PARTITION_BY and ORDER_BY carry no parameters, so no shifting is needed.
	recentTop := ds.WithClauses(velum.ClauseSet{
		"PARTITION_BY": "PARTITION BY o.customer_id",
		"EXTRA_FILTER": "AND o.paid_at > $1",           // becomes $2 at runtime
		"ORDER_BY":     "ORDER BY o.customer_id, rank", // outer sort
	})

	tenantID := 7

	// Execute with two arguments: tenantID ($1) and the cut-off time ($2).
	rows, err := recentTop.Select(ctx, db, tenantID, time.Now().Add(-24*time.Hour))
	_, _ = rows, err

	// Reuse the same compiled query with different arguments.
	rows, err = recentTop.Select(ctx, db, tenantID, time.Now().Add(-7*24*time.Hour))
	_, _ = rows, err

	// Single-row fetch — the top-ranked order for a specific customer.
	// Swap the partition dimension to o.region_id without touching the query.
	row, err := ds.WithClauses(velum.ClauseSet{
		"PARTITION_BY": "PARTITION BY o.customer_id",
		"EXTRA_FILTER": "AND o.customer_id = $1", // becomes $2
		"ORDER_BY":     "ORDER BY rank LIMIT 1",
	}).Get(ctx, db, tenantID, 99)
	_, _ = row, err

	// Append a tail clause after the template body (no named placeholder needed).
	rows, err = ds.WithTailClause("ORDER BY total DESC LIMIT 10").Select(ctx, db, tenantID)
	_, _ = rows, err
}

// --- Repository ---

// exOrder represents a row in the orders table.
type exOrder struct {
	ID         int        `dbw:"gen=serial"`
	CustomerID int
	Total      float64    `dbw:"total"` // "total" scope enables partial updates
	PaidAt     *time.Time
	RowVersion int        `dbw:"version"`
	CreatedAt  time.Time  `dbw:"insert"`
	UpdatedAt  *time.Time `dbw:"update"`
}

// OrderRepository centralises all data-access for the orders domain.
// Tables and Datasets are initialised once at startup; all methods are safe
// for concurrent use because Table and Dataset hold no mutable state.
//
// The database handle is intentionally passed per-call rather than stored in
// the struct. This enables two patterns without any extra wiring:
//   - Transactions: pass a velum.Transaction obtained from db.InTx — every
//     method participates in the same transaction transparently.
//   - Read/write splitting: pass a primary-db handle to mutating methods and
//     a replica handle to read methods at the call site.
type OrderRepository struct {
	customers *velum.Table[CustomerSerial]
	orders    *velum.Table[exOrder]
	rankView  *velum.Dataset[exOrderRank]
}

// NewOrderRepository builds the repository. Call it once in main() or a DI
// container and share the result across the application.
func NewOrderRepository() *OrderRepository {
	return &OrderRepository{
		customers: velum.NewTable[CustomerSerial]("customers"),
		orders:    velum.NewTable[exOrder]("orders"),

		// rankView ranks every order within a partition (supplied at call time)
		// by total descending. The outer ORDER BY is also injected per call so
		// the same Dataset works for different reporting endpoints.
		rankView: velum.NewDataset[exOrderRank](`
			SELECT
				o.id                                         AS order_id,
				c.first_name                                 AS customer_name,
				o.total                                      AS total,
				RANK() OVER (/*PARTITION_BY*/
				             ORDER BY o.total DESC)          AS rank
			FROM orders o
			JOIN customers c ON c.id = o.customer_id
			WHERE o.customer_id = $1
			/*EXTRA_FILTER*/
			/*ORDER_BY*/
		`),
	}
}

// GetCustomer returns a customer by primary key.
func (r *OrderRepository) GetCustomer(ctx context.Context, db velum.DatabaseWrapper, id int) (*CustomerSerial, error) {
	return r.customers.GetByPK(ctx, db, id)
}

// CreateOrder inserts a new order and returns the full row including the
// database-generated id and system columns.
func (r *OrderRepository) CreateOrder(ctx context.Context, db velum.DatabaseWrapper, o *exOrder) (*exOrder, error) {
	return r.orders.InsertReturning(ctx, db, o, velum.FullScope, velum.FullScope)
}

// UpdateTotal updates only the total column for the given order.
// row_version is auto-incremented and updated_at is set automatically.
func (r *OrderRepository) UpdateTotal(ctx context.Context, db velum.DatabaseWrapper, o *exOrder) (velum.Result, error) {
	return r.orders.UpdateByPK(ctx, db, o, "total")
}

// TopOrders returns orders for a customer ranked by total within a given
// partition, with the outer sort and optional filters injected at call time.
func (r *OrderRepository) TopOrders(ctx context.Context, db velum.DatabaseWrapper, customerID int) ([]exOrderRank, error) {
	return r.rankView.WithClauses(velum.ClauseSet{
		"PARTITION_BY": "PARTITION BY o.customer_id",
		"ORDER_BY":     "ORDER BY rank",
	}).Select(ctx, db, customerID)
}

// PlaceOrder creates an order and bumps the customer's row_version in a single
// transaction, illustrating how Table methods accept velum.Transaction
// interchangeably with velum.DatabaseWrapper.
func (r *OrderRepository) PlaceOrder(ctx context.Context, db velum.DatabaseWrapper, customerID int, total float64) (*exOrder, error) {
	var placed *exOrder
	err := db.InTx(ctx, func(tx velum.Transaction) error {
		now := time.Now()
		o, err := r.orders.InsertReturning(ctx, tx, &exOrder{
			CustomerID: customerID,
			Total:      total,
			PaidAt:     &now,
		}, velum.FullScope, velum.FullScope)
		if err != nil {
			return err
		}
		placed = o

		// Touch the customer row to update row_version and updated_at.
		c := &CustomerSerial{ID: customerID}
		_, err = r.customers.UpdateByPK(ctx, tx, c, velum.EmptyScope)
		return err
	})
	return placed, err
}

// ExampleNewOrderRepository demonstrates initialising a repository that owns
// multiple Table and Dataset descriptors and using them from a single call site.
func ExampleNewOrderRepository() {
	repo := NewOrderRepository()
	var db velum.DatabaseWrapper
	ctx := context.Background()

	// Simple primary-key lookup.
	customer, err := repo.GetCustomer(ctx, db, 42)
	_, _ = customer, err

	// Full insert with RETURNING — id and system columns are populated.
	now := time.Now()
	order, err := repo.CreateOrder(ctx, db, &exOrder{
		CustomerID: 42,
		Total:      199.99,
		PaidAt:     &now,
	})
	_, _ = order, err

	// Partial update — only total is written; row_version is auto-incremented.
	order.Total = 249.99
	_, err = repo.UpdateTotal(ctx, db, order)
	_ = err

	// Window-function dataset query — ranks orders by total for the customer.
	summaries, err := repo.TopOrders(ctx, db, 42)
	_, _ = summaries, err

	// Transactional: insert order + touch customer in one round-trip.
	placed, err := repo.PlaceOrder(ctx, db, 42, 89.90)
	_, _ = placed, err
}
