---
worth: yes
where: command.go:68
added: 2026-11-24
---
# Ошибки выполнения не содержат текста SQL

Оценка: ≈ 1 день (код ≈ 0,5 дня, тесты, README/AGENTS.md и заметки к выпуску — ещё ≈ 0,5).

velum возвращает ошибку драйвера как есть. `*pq.Error` и `*pgconn.PgError` несут SQLSTATE, Detail,
Constraint и Position, но не текст запроса. Поэтому в логе приложения не видно, какой запрос упал,
а `Position` без текста бесполезен.

Предложение (согласовано владельцем 24.11.2026 при обсуждении upsert, docs/proposals/upsert.md):

```go
type QueryError struct {
	SQL string // только $N, значений нет — можно писать в лог
	Err error
}
func (e *QueryError) Error() string { return e.Err.Error() + "; sql: " + e.SQL }
func (e *QueryError) Unwrap() error  { return e.Err }
```

1. Заворачивать во всех путях выполнения:
   1.1. методы `Command`/`SelectCommand`/`ReturningCommand`/`FunctionalCommand` в command.go;
   1.2. прямые `q.ExecContext` в `Table.DeleteByPK` и `Table.Delete`;
   1.3. `Dataset`.
2. `errors.Is(err, sql.ErrNoRows)`, `errors.As(err, &pgErr)` и `IsNotFound` продолжают работать через `Unwrap`.
3. Изменение поведения: перестаёт работать `err == sql.ErrNoRows` (без `errors.Is`) — отметить в заметках к выпуску.
4. Цена — одно выделение памяти только на пути ошибки.
