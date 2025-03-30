package velum_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"github.com/axkit/velum"
	"github.com/axkit/velum/pgxw"
	"github.com/axkit/velum/sqlw"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var testDatabaseTables = []struct {
	tablename string
	idcolumn  string
	typ       any
}{
	{"customers_pk_serial", "id SERIAL PRIMARY KEY NOT NULL", CustomerSerial{}},
	{"customers_pk_friendly_seq", "id INT8 PRIMARY KEY NOT NULL", CustomerFriendlySeq{}},
	{"customers_pk_custom_seq", "id INT8 PRIMARY KEY NOT NULL", CustomerFriendlySeq{}},
	{"customers_pk_uuid", "id UUID PRIMARY KEY NOT NULL", CustomerUIID{}},
	{"customers_pk_manual", "id INT8 PRIMARY KEY NOT NULL", CustomerManualPK{}},
	{"customers_without_pk", "id INT8 NOT NULL", CustomerWithoutPK{}},
}

var (
	pgContainer      *postgres.PostgresContainer
	connectionString string
	dbPgx            *pgxpool.Pool
	dbwPgx           *pgxw.DatabaseWrapper
	dbSql            *sql.DB
	dbwSql           *sqlw.DatabaseWrapper
)

type SystemColumns struct {
	RowVersion int64          `dbw:"version"`
	CreatedAt  time.Time      `dbw:"insert"`
	UpdatedAt  velum.NullTime `dbw:"update"`
	DeletedAt  velum.NullTime `dbw:"delete"`
	DeletedBy  *int           `dbw:"delete"`
}

type Customer struct {
	FirstName string
	LastName  string
	BirthDate time.Time `dbw:"bd"`
	SSN       *string   `dbw:"ssn"`
	SystemColumns
}

type CustomerSerial struct {
	ID int `dbw:"gen=serial"`
	Customer
}

type CustomerFriendlySeq struct {
	ID int64
	Customer
}

type CustomerCustomerSeq struct {
	ID int64 `dbw:"gen=custom_seq"`
	Customer
}

type CustomerUIID struct {
	ID string `dbw:"gen=uuid"`
	Customer
}

type CustomerManualPK struct {
	ID int64 `dbw:"pk,gen=no"`
	Customer
}

type CustomerWithoutPK struct {
	ID int64 `dbw:"pk=no"`
	Customer
}

func equalPtr[T comparable](a, b *T) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func (a *Customer) Equal(b *Customer) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.UpdatedAt.Valid() != b.UpdatedAt.Valid() {
		return false
	}
	if a.DeletedAt.Valid() != b.DeletedAt.Valid() {
		return false
	}

	return a.FirstName == b.FirstName && a.LastName == b.LastName &&
		a.BirthDate.Equal(b.BirthDate) && equalPtr(a.SSN, b.SSN) &&
		a.RowVersion == b.RowVersion && a.CreatedAt.Equal(b.CreatedAt) &&
		a.UpdatedAt.T().Equal(b.UpdatedAt.T()) && a.DeletedAt.T().Equal(b.DeletedAt.T()) &&
		equalPtr(a.DeletedBy, b.DeletedBy)
}

type Helper interface {
	Helper()
	Fatalf(format string, args ...any)
}

func startPostgresDB(t Helper) {

	var err error
	ctx := context.Background()

	if pgContainer != nil {
		return
	}

	// Start a PostgreSQL container
	pgContainer, err = postgres.Run(ctx, "postgres:13-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		t.Fatalf("failed to start PostgreSQL container: %v\n", err)
	}
	connectionString, err = pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v\n", err)
	}
}

func initConnections(t Helper) {

	t.Helper()

	startPostgresDB(t)

	firstTime := dbPgx == nil && dbSql == nil

	initConnectionPgx(t)
	initConnectionSQL(t)

	// create database objects
	if firstTime {
		createDatabaseObjects(t, context.Background(), dbPgx)
		createCustomerRows(t, context.Background(), dbPgx, 10000)
	}
}

func initConnectionSQL(t Helper) bool {

	if dbSql != nil {
		return false
	}

	t.Helper()

	var err error
	dbSql, err = sql.Open("postgres", connectionString)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	for i := 0; i < 5; i++ {
		err = dbSql.Ping()
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("failed to ping PostgreSQL: %v", err)
	}

	dbwSql = sqlw.NewDatabaseWrapper(dbSql)
	return true
}

func initConnectionPgx(t Helper) {

	t.Helper()

	if dbPgx != nil {
		return
	}

	var err error
	dbPgx, err = pgxpool.New(context.Background(), connectionString)
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}

	for i := 0; i < 5; i++ {
		err = dbPgx.Ping(context.Background())
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		t.Fatalf("failed to ping PostgreSQL: %v", err)
	}

	dbwPgx = pgxw.NewDatabaseWrapper(dbPgx)
}

func selectAllCustomers(t *testing.T, db *pgxpool.Pool) ([]CustomerSerial, error) {
	ctx := context.Background()
	var customers []CustomerSerial
	var row CustomerSerial

	rows, err := db.Query(ctx, `SELECT id, first_name, last_name, birth_date, 
	                              ssn, row_version, created_at, updated_at, deleted_at, deleted_by
							FROM customers ORDER BY id`)
	if err != nil {
		t.Fatalf("failed to select customers: %v", err)
		return nil, fmt.Errorf("failed to select customers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&row.ID, &row.FirstName,
			&row.LastName, &row.BirthDate,
			&row.SSN, &row.RowVersion, &row.UpdatedAt, &row.DeletedAt, &row.DeletedBy)
		if err != nil {
			return nil, fmt.Errorf("failed to scan customer: %w", err)
		}
		customers = append(customers, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate customers: %w", err)
	}

	return customers, nil
}

func createDatabaseTable(t Helper, ctx context.Context, tablename string, idcolumn string, db *pgxpool.Pool) {

	t.Helper()
	_, err := db.Exec(ctx, fmt.Sprintf(`
        CREATE TABLE %s (
            %s,
            first_name TEXT 		NOT NULL,
			last_name  TEXT 		NOT NULL,
			birth_date TIMESTAMP 	NOT NULL,
			ssn TEXT,
			row_version INT8 		NOT NULL 	DEFAULT 0,
			created_at TIMESTAMP 	NOT NULL	DEFAULT CURRENT_TIMESTAMP,
			updated_At TIMESTAMPTZ,
            deleted_at TIMESTAMP WITHOUT TIME ZONE,
			deleted_by INT8
        )
    `, tablename, idcolumn))
	if err != nil {
		t.Fatalf("failed to create customers table: %w", err)
	}
}

func createDatabaseObjects(t Helper, ctx context.Context, db *pgxpool.Pool) {

	t.Helper()

	for _, c := range testDatabaseTables {
		createDatabaseTable(t, ctx, c.tablename, c.idcolumn, db)
	}

	if _, err := db.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`); err != nil {
		t.Fatalf("failed to create uuid-ossp extension: %v", err)
	}

	if _, err := db.Exec(ctx, `CREATE SEQUENCE custom_seq`); err != nil {
		t.Fatalf("failed to create custom_seq: %v", err)
	}

	if _, err := db.Exec(ctx, `CREATE SEQUENCE customers_pk_friendly_seq_seq`); err != nil {
		t.Fatalf("failed to create customers_pk_friendly_seq_seq: %v", err)
	}
}

func createCustomerRows(t Helper, ctx context.Context, db *pgxpool.Pool, n int) {

	t.Helper()

	for _, c := range testDatabaseTables {
		_, err := db.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE %s`, c.tablename))
		if err != nil {
			t.Fatalf("failed to truncate customers table: %w", err)
		}
	}

	for i := 0; i < n; i++ {
		// randomize c data
		c := Customer{
			FirstName: "First" + strconv.Itoa(i),
			LastName:  "Last" + strconv.Itoa(i),
			BirthDate: time.Date(1900+i, 1, 1, 0, 0, 0, 0, time.UTC),
			SSN:       nil,
			SystemColumns: SystemColumns{
				RowVersion: 1,
				CreatedAt:  time.Now(),
			},
		}

		_, err := db.Exec(ctx, `
			INSERT INTO customers_pk_serial (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES (DEFAULT, $1, $2, $3, $4, $5, $6)`, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_pk_serial: %w", err)
		}

		_, err = db.Exec(ctx, `
			INSERT INTO customers_pk_friendly_seq (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES (nextval('customers_pk_friendly_seq_seq'), $1, $2, $3, $4, $5, $6)`, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_pk_friendly_seq: %w", err)
		}

		_, err = db.Exec(ctx, `
			INSERT INTO customers_pk_custom_seq (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES (nextval('custom_seq'), $1, $2, $3, $4, $5, $6)`, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_pk_custom_seq: %w", err)
		}
		_, err = db.Exec(ctx, `
			INSERT INTO customers_pk_uuid (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES (uuid_generate_v4(), $1, $2, $3, $4, $5, $6)`, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_pk_uuid: %w", err)
		}

		_, err = db.Exec(ctx, `
			INSERT INTO customers_pk_manual (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, i, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_pk_manual: %w", err)
		}

		_, err = db.Exec(ctx, `
			INSERT INTO customers_without_pk (id, first_name, last_name, birth_date, ssn, created_at, row_version)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`, i, c.FirstName, c.LastName, c.BirthDate, c.SSN, c.CreatedAt, c.RowVersion)
		if err != nil {
			t.Fatalf("failed to insert test customers_without_pk: %w", err)
		}
	}
}

func TestStructToTabeName(t *testing.T) {
	type Customer struct{}
	type Bus struct{}
	type Box struct{}
	type Quiz struct{}
	type Watch struct{}
	type Brush struct{}
	type Item struct{}

	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"SingularToPlural", Customer{}, "customers"},
		{"EndsWithS", Bus{}, "buses"},
		{"EndsWithX", Box{}, "boxes"},
		{"EndsWithZ", Quiz{}, "quizzes"},
		{"EndsWithCH", Watch{}, "watches"},
		{"EndsWithSH", Brush{}, "brushes"},
		{"DefaultPluralization", Item{}, "items"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snakeName := velum.ToSnakeCase(reflect.TypeOf(tt.input).Name(), "")
			result := velum.ToPluralName(snakeName)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})

		t.Run("generic", func(t *testing.T) {
			snakeName := velum.StructPluralName[Customer]()
			if snakeName != "customers" {
				t.Errorf("expected customers, got %s", snakeName)
			}
		})
	}
}
