# velum
[![Build Status](https://github.com/axkit/velum/actions/workflows/go.yml/badge.svg)](https://github.com/axkit/velum/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/axkit/velum)](https://goreportcard.com/report/github.com/axkit/velum)
[![GoDoc](https://pkg.go.dev/badge/github.com/axkit/velum)](https://pkg.go.dev/github.com/axkit/velum)
[![Coverage Status](https://coveralls.io/repos/github/axkit/velum/badge.svg?branch=main)](https://coveralls.io/github/axkit/velum?branch=master)

 




```go
type Customer struct {
	ID 			int  		`dbw:"pk,gen=serial"` 
	FirstName 	string 
	LastName 	string 
	BirthDate   date.Date   `dbw:"bd"`     
	SSN         string 		`dbw:"ssn"`
	Address 	struct {
		Line1 	string 
		Line2 	*string
		CityID 	int 
		Country int 
		Zip 	string 
	} 
	Origin string  		 	`dbw:"-"`
	RowVersion 	int64		`dbw:"version"`
	CreatedAt 	time.Time  	`dbw:"insert"`
	UpdatedAt 	*time.Time 	`dbw:"update"`
	DeletedAt 	*time.Time 	`dbw:"delete"`
	DeletedBy 	*int       	`dbw:"delete"`
}

	// once
	dbSql, err = sql.Open("postgres", connectionString)
	dbwSql = sqlw.NewDatabaseWrapper(dbSql) // database sql wrapper here, pgx supported too
	tbl := velum.NewTable[Customer]()

	// get table row by primary key value
	c, err := tbl.GetByPK(ctx, dbwSql, 42)
	
	nc := Customer{FirstName: "Robert", LastName : "Egorov"}
	err := tbl.Insert(ctx, dbSql, &nc)
	fmt.Println("new customer id: ", nc.ID)
