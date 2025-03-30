# Velum
PosrtgresSQL micro ORM


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)


```go
tbl := velum.NewTable[customer]("customers")

if err := tbl.Validate(ctx); err != nil {
    return err
}


// ...
err := tbl.Insert(ctx, q, &row)
//
ins := tbl.Ins(scope)
err := ins.Exec(ctx, q, &row)
//
ins := tbl.Ins(scope).Returning(retScope)
nrow, err := ins.QueryRow(ctx, q, &row) 
// or
err := ins.QueryRowInto(ctx, q, &row)

// ---------
err := tbl.Delete(ctx, q, id)

del := tbl.Del(where string)
err := del.Exec(ctx, q, args...)

del := tbl.Del(where string).Returning(retScope)
rows, err := del.Query(ctx, q, args...)




ins := tbl.Ins(scope).Many()
err := ins.Exec(ctx, q, rows)

ins := tbl.Ins(scope).Many().Returning(retScope)
rows, err := ins.Query(ctx, q, rows)
// or
err := ins.QueryInto(ctx, q, rows)

// -----
err := tbl.Update(ctx, q, scope, row)

upd := tbl.Upd(scope, where).Returning(retScope)
nrow := upd.QueryRow(ctx, q, row).Scan(&row)

upd := tbl.Upd(scope).Many().Returning(retScope)
rows, err := upd.Query(ctx, q, rows)
// or 
err := upd.QueryInto(ctx, q, rows)

// -----
err := tbl.Delete(ctx, q, id)

del := tbl.Del(where string).Returning(retScope)
del.Query()



```


//
	// exist       CommandTemplate
	// count       CommandTemplate
	// softDelByPK struct {
	// 	woRet   CommandTemplate
	// 	withRet CommandTemplate
	// }
	// touchByPK   CommandTemplate
	// hardDelByPK struct {
	// 	woRet   CommandTemplate
	// 	withRet CommandTemplate
	// }

	// insert struct {
	// 	woRet   InsertCommand[T]
	// 	withRet InsertCommand[T]
	// }


SELECT scope + pk RETURNING scope + pk 
SELECT * RETURNING *

INSERT * VALUES(* -/leave pk) RETURNING *
INSERT scope (VALUES scope + pk) RETURNING scope + pk


DELETE FROM RETURNING *
DELETE FROM RETURNING scope + pk


