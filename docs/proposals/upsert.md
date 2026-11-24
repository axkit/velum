# Предложение: upsert (`INSERT … ON CONFLICT`) в `Table[T]`

Статус: **согласовано владельцем** (ред. 3, 24.11.2026). Код не написан.
Источник задачи: CR122 Гостли — вся работа с PostgreSQL идёт через velum, текст запроса — константа,
значения передаются только параметрами.

Работа делается двумя коммитами:

- **коммит 1** — таблицы без PK и `args` у `Update` (раздел 10, согласовано);
- **коммит 2** — upsert (разделы 1–9).

## 1. Что покрываем

1.1. Однострочный `INSERT … VALUES (…) ON CONFLICT …` для строки `*T`: обе формы, `DO NOTHING` и `DO UPDATE`,
с `RETURNING` и без него. Это формы 1–12 из списка Гостли.

1.2. Грубый подсчёт по Гостли (`internal/`, без тестов): найдено 45 инструкций `INSERT … ON CONFLICT`, записанных одним
строковым литералом (остальные из 56 собираются склейкой, и этот поиск их не находит).
33 из 45 — `VALUES (…)` на одну строку, их покрывает предложение. 12 из 45 — `INSERT … SELECT`, их не покрывает (раздел 7).

1.3. Весь SQL из раздела 6 проверен на PostgreSQL 17: на этой версии работает Гостли.

## 2. Типы

```go
// ConflictAction is what ON CONFLICT does.
type ConflictAction uint8

const (
	DoNothing ConflictAction = iota // ON CONFLICT … DO NOTHING
	DoUpdate                        // ON CONFLICT … DO UPDATE SET …
)

// OnConflict describes the ON CONFLICT part of an upsert. Every field is a
// fixed SQL constant of the caller; values go only through $N arguments.
// The struct is comparable and is part of the command cache key.
type OnConflict struct {
	// Target is the text after ON CONFLICT, verbatim:
	//   "(user_id, hint_key)"
	//   "(workspace_id, lower(name))"
	//   "(source_event_id) WHERE source_event_id IS NOT NULL"
	//   "ON CONSTRAINT pk_user_document_contexts"
	// Empty means no target; allowed only with DoNothing.
	Target string

	Action ConflictAction

	// UpdateScope (DoUpdate): every column of this scope gets col=EXCLUDED.col.
	UpdateScope Scope

	// UpdateExpr (DoUpdate): extra assignments "col = expr, …" appended after
	// UpdateScope, for what EXCLUDED.col cannot express: CASE, now(), constants,
	// t.count + 1. $1… in it are the caller's args (see 4.3).
	UpdateExpr string

	// UpdateWhere (DoUpdate): condition of the update, with the keyword:
	// "WHERE t.import_id <> EXCLUDED.import_id". Shares $N numbering with UpdateExpr.
	UpdateWhere string
}

// OnConflictDoNothing is "ON CONFLICT DO NOTHING" without a target.
var OnConflictDoNothing = OnConflict{}

// UpsertOutcome tells what an upsert with RETURNING did to the row.
type UpsertOutcome uint8

const (
	UpsertSkipped  UpsertOutcome = iota // no row written: DO NOTHING on conflict, or UpdateWhere was false
	UpsertInserted                      // a new row was inserted
	UpsertUpdated                       // the existing row was updated
)
```

## 3. Методы: только готовые команды

3.1. Команда собирается и проверяется при создании (раздел 8). Её создают при запуске сервиса, поэтому ошибка
в `OnConflict` видна сразу. «Ленивых» `Table.Upsert(ctx, …, oc)` по образцу остальных методов нет (согласовано):
с ними ошибка вылезла бы только при первом запросе.

3.1.1. Пара функций, как у `regexp.Compile`/`MustCompile` (согласовано):
- `UpsertCommand` возвращает `error` — для сборки в функции запуска (`Init(ctx) error`);
- `MustUpsertCommand` паникует — для переменных пакета и конструкторов без `error`.

```go
// Table[T]
func (t *Table[T]) UpsertCommand(scope Scope, oc OnConflict) (Command[T], error)
func (t *Table[T]) MustUpsertCommand(scope Scope, oc OnConflict) Command[T]
func (t *Table[T]) UpsertReturningCommand(scope, retScope Scope, oc OnConflict) (UpsertCommand[T], error)
func (t *Table[T]) MustUpsertReturningCommand(scope, retScope Scope, oc OnConflict) UpsertCommand[T]

// Execution
func (c *Command[T]) Exec(ctx context.Context, q Executer, row *T, args ...any) (Result, error) // уже есть
func (c *UpsertCommand[T]) QueryRow(ctx context.Context, q QueryExecuter, row *T,
	args ...any) (*T, UpsertOutcome, error)

type UpsertCommand[T any] struct {
	Command[T]
	rets []int
}
```

3.2. Объявление и вызов:

```go
var hintOnce = seenHints.MustUpsertCommand(velum.FullScope, seenHintOnce) // panic здесь, при init пакета

res, err := hintOnce.Exec(ctx, tx, &h)
```

В примерах раздела 6 для краткости используется `UpsertCommand` без обработки ошибки — читать как `MustUpsertCommand`.

3.3. Параметры:
3.3.1. `scope` — столбцы INSERT.
3.3.2. `retScope` — столбцы RETURNING.
3.3.3. `args` при вызове — значения для `$N` в `UpdateExpr`/`UpdateWhere`.

3.4. `Exec`: `RowsAffected` = 1 — вставлено или обновлено, 0 — пропущено.

3.5. `QueryRow` при `UpsertSkipped` возвращает `(nil, UpsertSkipped, nil)`: отсутствие строки — не ошибка.

3.6. `QueryRow` принимает `QueryExecuter`, а не `QueryRowExecuter`: «строки нет» определяется через `rows.Next() == false`,
без зависимости от драйвера (`sql.ErrNoRows` у database/sql, `pgx.ErrNoRows` у pgx). `DatabaseWrapper` и `Transaction`
реализуют оба интерфейса, так что команда работает и в `InTx`, и вне транзакции.

3.7. Команды кэшируются в `CommandContanier` в новой карте `ups map[upsertKey]UpsertCommand[T]`, ключ
`{scope, retScope, oc, returning}`. Повторный вызов `UpsertCommand` с тем же ключом отдаёт ту же команду.
Команда с ошибкой в кэш не попадает.

## 4. Как строится SQL

4.1. **Шаблон:**
`INSERT INTO <table> AS t (<cols>) VALUES (<args>) ON CONFLICT [<Target>] DO NOTHING | DO UPDATE SET <set> [<UpdateWhere>] [RETURNING <rets>,(t.xmax = 0)]`

4.2. **Псевдоним `t` есть всегда** (согласовано).
4.2.1. Без префикса PostgreSQL отвергает `row_version=row_version+1` с ошибкой `column reference "row_version" is ambiguous`:
в `DO UPDATE` видны и строка таблицы, и `EXCLUDED`.
4.2.2. После `AS t` имя таблицы в `SET`/`WHERE` уже не видно (`missing FROM-clause entry for table "environments"`).
В `UpdateExpr`/`UpdateWhere` пишут `t.col` и `EXCLUDED.col`.
4.2.3. Схема в имени таблицы работает: `NewTable[T]("pgp.jobs")` → `INSERT INTO pgp.jobs AS t …`.

4.3. **Нумерация параметров** — один общий ряд:
4.3.1. `$1…$n` — значения INSERT из полей `row`, по правилам `InsertReturning`: PK первым, если его генерирует не БД;
`gen=serial` → `DEFAULT`, `gen=uuid` → `gen_random_uuid()`; PK без `gen=` → `nextval('<table>_seq')`,
поэтому у естественных ключей нужен `gen=no`.
4.3.2. `$n+1…$n+u` — столбцы системного scope `update` в `DO UPDATE SET` (раздел 5).
4.3.3. В `UpdateExpr` и `UpdateWhere` вызывающий начинает с `$1`, velum сдвигает на `n+u` (`ShiftParamPositions`, как в `Dataset`).
`$1` в обоих полях — один и тот же `args[0]`.
4.3.4. Сдвиг делается регулярным выражением и задевает `$1` внутри строковых литералов, поэтому `$` в литералах не пишут.

4.4. **INSERT-часть** — как у `InsertReturning` (согласовано): `scope` плюс системные `version` и `insert`.
`FullScope` вставляет все столбцы, включая `update` и `delete`.

4.5. **`UpdateScope`** даёт `col=EXCLUDED.col`. PK и системные столбцы через него не попадают никогда; системные — только по разделу 5.

4.6. **Три scope независимы:**
4.6.1. что вставляем — `scope`;
4.6.2. что обновляем при конфликте — `UpdateScope`;
4.6.3. что возвращаем — `retScope`.

`UpdateScope` должен входить в `scope`. `EXCLUDED` содержит только то, что пытались вставить, и столбец вне INSERT
затёр бы данные значением по умолчанию или `NULL` (проверка 8.4).

4.7. **RETURNING** — столбцы `retScope` (PK впереди, если он есть) и в конце `(t.xmax = 0)`, который читается в признак.
`xmax = 0` значит «строку вставила эта инструкция»; этот приём Гостли уже использует (bos/bl/git_settings_db.go:31).
Когда минимальной версией станет PostgreSQL 18, его можно заменить на документированный `RETURNING old`.
`retScope = EmptyScope` возвращает только PK (если он есть) и признак.

## 5. Системные столбцы при `DO UPDATE` (согласовано)

5.1. `version` → `row_version=t.row_version+1`.

5.2. `update` → `updated_at=$k`, значение из `row.UpdatedAt`, как в `UpdateByPK`; вызывающий заполняет его до вызова.
5.2.1. У столбца свой параметр, а не `EXCLUDED.updated_at`: результат не зависит от того, попал ли `updated_at` в INSERT.
5.2.2. При `scope = FullScope` новая строка тоже получит `updated_at = row.UpdatedAt`. Если новой строке нужен `NULL`,
вставляйте именованным scope: он добавляет только `version` и `insert`.

5.3. `insert` и `delete` velum при `DO UPDATE` не трогает. `UpdateExpr` может присвоить их явно, например
`deleted_at = NULL`, чтобы «воскресить» мягко удалённую строку.

5.4. **Столбцы `version` и `update` ставит только velum.** Если `UpdateExpr` присваивает такой столбец — ошибка при создании команды (8.5).
Столбцы-цели находятся один раз: `UpdateExpr` делится по запятым вне скобок и кавычек, берётся имя перед `=`;
учитываются имена в кавычках и форма `(a, b) = (…)`.

5.5. Если `UpdateWhere` ложно, строка не меняется вовсе, в том числе `row_version` и `updated_at`. Это штатный способ
обновлять только при реальном изменении (пример 6.6).

5.6. **Обновлений без изменений нет.** Каждый `DO UPDATE` поднимает `row_version` и ставит `updated_at`. Поэтому «пустое»
обновление ради `RETURNING` или блокировки строки (`SET x = t.x`) не пишут. Вместо него — `DO NOTHING RETURNING`,
а при `UpsertSkipped` — `SELECT … FOR UPDATE` (пример 6.10).
5.6.1. Почему так. Такое обновление ложно сообщает «строка изменилась»: в `updated_at`, в аудите и тем, кто держит
`row_version` для оптимистичной блокировки (они получат ложный конфликт).
5.6.2. Кроме того, даже `SET x = t.x` создаёт в PostgreSQL новую версию строки, запись в WAL и мёртвый кортеж.

5.7. «Касание» без данных (`UpdateScope` и `UpdateExpr` пусты, есть системные столбцы) — это явное намерение,
как `UpdateByPK(…, EmptyScope)`. Оно разрешено.

5.8. `DO NOTHING` системные столбцы не трогает.

## 6. Примеры: Go-код и SQL, который он сгенерирует

Общая структура для примера 6.1:

```go
type ProductPrice struct {
	SKU        string     `dbw:"pk,gen=no"`
	Price      int64      `dbw:"price"`
	Currency   string     `dbw:"price"`
	Note       string
	RowVersion int64      `dbw:"version"`
	CreatedAt  time.Time  `dbw:"insert"`
	UpdatedAt  *time.Time `dbw:"update"`
}

var prices = velum.NewTable[ProductPrice]("product_prices")
```

### 6.1. Вставляем всю строку, при конфликте обновляем только цену (форма 3)

```go
var priceUpsert = prices.UpsertCommand(velum.FullScope,
	velum.OnConflict{Target: "(sku)", Action: velum.DoUpdate, UpdateScope: "price"})

now := time.Now()
row.RowVersion, row.CreatedAt, row.UpdatedAt = 1, now, &now
_, err := priceUpsert.Exec(ctx, db, &row)
```

```sql
INSERT INTO product_prices AS t (sku,price,currency,note,row_version,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (sku) DO UPDATE SET price=EXCLUDED.price,currency=EXCLUDED.currency,row_version=t.row_version+1,updated_at=$8
-- note при конфликте не меняется; created_at остаётся от существующей строки
```

### 6.2. Цель — столбцы, `DO NOTHING` (форма 1, aaa/bl/repo_seen_hints.go:68)

```go
type SeenHint struct {
	UserID  int
	HintKey string
}

var seenHints = velum.NewTable[SeenHint]("user_seen_hints") // без PK — после коммита 1
var hintOnce = seenHints.UpsertCommand(velum.FullScope, velum.OnConflict{Target: "(user_id, hint_key)"})

res, err := hintOnce.Exec(ctx, tx, &SeenHint{UserID: userID, HintKey: key})
n, _ := res.RowsAffected() // 0 — подсказка уже была
```

```sql
INSERT INTO user_seen_hints AS t (user_id,hint_key) VALUES ($1,$2) ON CONFLICT (user_id, hint_key) DO NOTHING
```

### 6.3. Без цели, `DO NOTHING` (форма 2)

```go
var hintAny = seenHints.UpsertCommand(velum.FullScope, velum.OnConflictDoNothing)
```

```sql
INSERT INTO user_seen_hints AS t (user_id,hint_key) VALUES ($1,$2) ON CONFLICT DO NOTHING
```

### 6.4. `DO NOTHING RETURNING` и признак (форма 4)

```go
type Permission struct {
	ID     string `dbw:"gen=no"`
	Name   string
	BitPos int    `dbw:"bits"`
}

var permAdd = perms.UpsertReturningCommand(velum.FullScope, "bits", velum.OnConflict{Target: "(id)"})

got, outcome, err := permAdd.QueryRow(ctx, db, &p)
// outcome == velum.UpsertSkipped → конфликт, got == nil, err == nil
```

```sql
INSERT INTO permissions AS t (id,name,bit_pos) VALUES ($1,$2,$3) ON CONFLICT (id) DO NOTHING RETURNING id,bit_pos,(t.xmax = 0)
```

6.4.1. Сам aaa/bl/repo.go:178 считает `bit_pos` через `SELECT COALESCE(MAX(bit_pos),0)+1 FROM permissions`.
Это `INSERT … SELECT`, раздел 7.

### 6.5. Цель с предикатом частичного индекса (форма 5, aaa/bl/repo_audit.go:131)

```go
type AuditEvent struct {
	ID            int64 `dbw:"gen=serial"`
	Type          string
	Payload       string
	OccurredAt    time.Time
	SourceEventID *uuid.UUID
}

var auditOnce = audit.UpsertCommand(velum.FullScope,
	velum.OnConflict{Target: "(source_event_id) WHERE source_event_id IS NOT NULL"})
```

```sql
INSERT INTO audit_events AS t (id,type,payload,occurred_at,source_event_id) VALUES (DEFAULT,$1,$2,$3,$4)
ON CONFLICT (source_event_id) WHERE source_event_id IS NOT NULL DO NOTHING
```

6.5.1. `COALESCE($7::timestamptz, now())` в VALUES не пишется: `OccurredAt` заполняют в Go (согласовано: время с часов приложения).

### 6.6. Цель — выражение индекса; обновлять только при реальном изменении (формы 6, 9; pgp/bl/environments.go:78)

```go
type Environment struct {
	ID          uuid.UUID `dbw:"gen=no"`
	WorkspaceID int
	Name        string
	Color       string    `dbw:"color"`
	CreatedAt   time.Time `dbw:"insert"`
	UpdatedAt   time.Time `dbw:"update"`
}

var envs = velum.NewTable[Environment]("pgp.environments")
var envUpsert = envs.UpsertReturningCommand(velum.FullScope, velum.EmptyScope, velum.OnConflict{
	Target:      "(workspace_id, lower(name))",
	Action:      velum.DoUpdate,
	UpdateScope: "color",
	UpdateWhere: "WHERE t.color <> EXCLUDED.color",
})

got, outcome, err := envUpsert.QueryRow(ctx, tx, &env)
if err == nil && outcome == velum.UpsertSkipped { // цвет тот же — строка не тронута, id берём отдельно
	got, err = envs.Get(ctx, tx, velum.EmptyScope, "WHERE workspace_id=$1 AND lower(name)=lower($2)", env.WorkspaceID, env.Name)
}
```

```sql
INSERT INTO pgp.environments AS t (id,workspace_id,name,color,created_at,updated_at) VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (workspace_id, lower(name)) DO UPDATE SET color=EXCLUDED.color,updated_at=$7
WHERE t.color <> EXCLUDED.color
RETURNING id,(t.xmax = 0)
```

Вторая цель-выражение Гостли (aaa/bl/repo_style_template_choice.go:244) пишется так же:
`Target: "(scope, COALESCE(organization_id, 0), COALESCE(workspace_id, 0))"`.

### 6.7. `ON CONSTRAINT` (форма 7, pgp/bl/context.go:92)

```go
type DocumentContext struct {
	WorkspaceID  int
	UserID       int
	DocumentID   uuid.UUID
	ConnectionID int       `dbw:"ctx"`
	SchemaName   string    `dbw:"ctx"`
	UpdatedAt    time.Time `dbw:"update"`
}

var docCtxSave = docCtx.UpsertReturningCommand(velum.FullScope, velum.UpdateScope, velum.OnConflict{
	Target: "ON CONSTRAINT pk_user_document_contexts", Action: velum.DoUpdate, UpdateScope: "ctx",
})

row.UpdatedAt = time.Now()
got, _, err := docCtxSave.QueryRow(ctx, db, &row)
```

```sql
INSERT INTO pgp.user_document_contexts AS t (workspace_id,user_id,document_id,connection_id,schema_name,updated_at)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT ON CONSTRAINT pk_user_document_contexts DO UPDATE SET connection_id=EXCLUDED.connection_id,schema_name=EXCLUDED.schema_name,updated_at=$7
RETURNING updated_at,(t.xmax = 0)
-- $6 и $7 — одно значение row.UpdatedAt (5.2.1)
```

### 6.8. Выражения с параметрами в SET, `RETURNING` произвольных столбцов (формы 8, 11; aaa/bl/repo_ratelimit.go:22)

```go
type RateCounter struct {
	Kind      string
	Key       string
	Count     int       `dbw:"counter"`
	ExpiresAt time.Time `dbw:"counter"`
}

var rateTake = counters.UpsertReturningCommand(velum.FullScope, "counter", velum.OnConflict{
	Target: "(kind, key)",
	Action: velum.DoUpdate,
	UpdateExpr: `count = CASE WHEN t.expires_at <= $1 THEN 1 ELSE t.count + 1 END,
	expires_at = CASE WHEN t.expires_at <= $1 THEN EXCLUDED.expires_at ELSE t.expires_at END`,
})

row := RateCounter{Kind: c.kind, Key: c.key, Count: 1, ExpiresAt: now.Add(window)}
got, _, err := rateTake.QueryRow(ctx, tx, &row, now)
```

```sql
INSERT INTO rate_limit_counters AS t (kind,key,count,expires_at) VALUES ($1,$2,$3,$4)
ON CONFLICT (kind, key) DO UPDATE SET count = CASE WHEN t.expires_at <= $5 THEN 1 ELSE t.count + 1 END,
  expires_at = CASE WHEN t.expires_at <= $5 THEN EXCLUDED.expires_at ELSE t.expires_at END
RETURNING count,expires_at,(t.xmax = 0)
-- args: Kind, Key, Count, ExpiresAt, now   ($1 в UpdateExpr → $5)
```

### 6.9. Константы и `now()` в SET, `UpdateWhere` (формы 8, 9; edt/blocks/imports.go:150)

```go
type ImportReport struct {
	DocumentID uuid.UUID `dbw:"pk,gen=no"`
	ImportID   uuid.UUID `dbw:"report"`
	Report     string    `dbw:"report"`
}

var reportReplace = reports.UpsertCommand(velum.FullScope, velum.OnConflict{
	Target:      "(document_id)",
	Action:      velum.DoUpdate,
	UpdateScope: "report",
	UpdateExpr:  "checked = '{}', imported_at = now()", // imported_at — не системный столбец
	UpdateWhere: "WHERE t.import_id <> EXCLUDED.import_id",
})
```

```sql
INSERT INTO edt.import_reports AS t (document_id,import_id,report) VALUES ($1,$2,$3)
ON CONFLICT (document_id) DO UPDATE SET import_id=EXCLUDED.import_id,report=EXCLUDED.report,checked = '{}', imported_at = now()
WHERE t.import_id <> EXCLUDED.import_id
```

6.9.1. Условие с параметром: `UpdateWhere: "WHERE t.seen_at < EXCLUDED.seen_at - $1::interval"`, при вызове `args = "5 minutes"`.

### 6.10. «Вставлено или уже было» с блокировкой строки (форма 10, bos/bl/git_settings_db.go:31)

Вместо пустого `DO UPDATE` (5.6) — `DO NOTHING RETURNING`, а если строка уже была, `SELECT … FOR UPDATE`:

```go
type RepoWorkspace struct {
	WorkspaceID    int `dbw:"pk,gen=no"`
	OrganizationID int
}

var repoWsAdd = repoWs.UpsertReturningCommand(velum.FullScope, velum.FullScope,
	velum.OnConflict{Target: "(workspace_id)"})

got, outcome, err := repoWsAdd.QueryRow(ctx, tx, &RepoWorkspace{WorkspaceID: wsID, OrganizationID: orgID})
created := outcome == velum.UpsertInserted
if err == nil && !created {
	got, err = repoWs.Get(ctx, tx, velum.FullScope, "WHERE workspace_id=$1 FOR UPDATE", wsID)
}
```

```sql
INSERT INTO repo_workspaces AS t (workspace_id,organization_id) VALUES ($1,$2)
ON CONFLICT (workspace_id) DO NOTHING RETURNING workspace_id,organization_id,(t.xmax = 0)
-- только если строка уже была:
SELECT t.workspace_id,t.organization_id FROM repo_workspaces t WHERE workspace_id=$1 FOR UPDATE
```

6.10.1. Корректность. `DO NOTHING` ждёт завершения параллельной незафиксированной вставки того же ключа. В READ COMMITTED
следующий `SELECT` видит уже зафиксированную строку и блокирует её. Второй запрос нужен только когда строка уже была.

### 6.11. Схема и псевдоним (форма 12)

`NewTable[DocBlock]("edt.doc_blocks")` → `INSERT INTO edt.doc_blocks AS t …`; в `UpdateExpr`/`UpdateWhere` пишут `t.…` вместо `b.…`.

### 6.12. В транзакции

```go
err := db.InTx(ctx, func(tx velum.Transaction) error {
	_, outcome, err := repoWsAdd.QueryRow(ctx, tx, &ws)
	if err != nil || outcome != velum.UpsertInserted {
		return err
	}
	_, err = labelAdd.Exec(ctx, tx, &l)
	return err
})
```

## 7. Что не входит (согласовано)

7.1. **`INSERT … SELECT`** (12 из 45): unnest-массивы, `MAX(bit_pos)+1`, копирование из другой таблицы, фильтр по `users`.
Они остаются константами и выполняются через `db.ExecContext(ctx, constSQL, args...)` у `velum.DatabaseWrapper`/`velum.Transaction`.
Единственный из них с RETURNING (aaa/bl/repo.go:178) можно выполнить через `Dataset`.

7.2. **`UpsertMany` для `[]T`** — откладывается.
7.2.1. Многострочный `VALUES` даёт по команде в кэше на каждую длину среза.
7.2.2. Вариант с unnest требует типов массивов, а velum их не знает.

7.3. **Выражения в VALUES** (`now()`, `COALESCE`, приведения) — значение вычисляют в Go.

7.4. **Свой псевдоним вместо `t`** — нет.

## 8. Проверки при создании команды

Нарушение любой проверки — ошибка `*UpsertConfigError` из `UpsertCommand`/`UpsertReturningCommand`
(в `Must…` — panic с ней). Ошибки на стандартном `errors`, без `axkit/errors` (согласовано):

```go
var ErrInvalidUpsert = errors.New("velum: invalid upsert")

type UpsertConfigError struct {
	Table    string
	Caller   string     // "internal/aaa/bl/repo.go:57": откуда вызван UpsertCommand/Must… (runtime.Caller)
	Reason   string
	SQL      string     // запрос, который получился бы: velum строит текст до проверок
	Scope    Scope
	RetScope Scope
	Conflict OnConflict
}

func (e *UpsertConfigError) Error() string // одна строка: место, таблица, причина, sql
func (e *UpsertConfigError) Unwrap() error { return ErrInvalidUpsert }
```

Пример текста:

```
velum: invalid upsert at internal/aaa/bl/repo.go:57: table user_seen_hints: UpdateScope "note": column note is not inserted by scope "key" (inserts user_id,hint_key), EXCLUDED.note would be NULL; sql: INSERT INTO user_seen_hints AS t (user_id,hint_key) VALUES ($1,$2) ON CONFLICT (user_id, hint_key) DO UPDATE SET note=EXCLUDED.note
```

В тексте SQL только `$N`, значений нет, поэтому его можно писать в лог. Гостли переносит поля в атрибуты своей
ошибки через `errors.AsType[*velum.UpsertConfigError]` или просто `core.Catch(err)` (500, текст в лог).

Проверки:

8.1. `DoUpdate` с пустым `Target`.

8.2. `DoNothing` с непустыми `UpdateScope`/`UpdateExpr`/`UpdateWhere`.

8.3. `DoUpdate` с пустым итоговым SET: нет `UpdateScope`, `UpdateExpr` и системных столбцов.

8.4. Столбец `UpdateScope`, которого нет в scope вставки.

8.5. `UpdateExpr` присваивает столбец `version` или `update`.

8.6. `UpdateExpr` присваивает столбец, который уже есть в `UpdateScope`. Эту проверку делает тот же разбор из 5.4;
без неё PostgreSQL отверг бы запрос только при выполнении (`multiple assignments to same column`).

## 9. Открытые вопросы

Нет: все решения приняты 24.11.2026. Вне этой задачи, в бэклоге velum:
- docs/backlog/feature-001-query-error-with-sql.md — текст SQL в ошибках выполнения;
- docs/backlog/task-001-logw-failed-query-error-level.md — `logw` пишет упавшие запросы на уровне ERROR.

panic в `NewTable`, `Table.Scope` и `Dataset.WithClauses` не трогаем (решение владельца).

## 10. Коммит 1: таблицы без PK и `args` у `Update` (согласовано)

10.1. **Что сломано сейчас** (проверено на fake-DB, таблица без `id` и без `dbw:"pk"`):

| № | Метод | Сейчас |
|---|---|---|
| 10.1.1 | `Select`/`Get`, `InsertScope`, `Update` по условию с именованным scope | panic: nil pointer dereference |
| 10.1.2 | `Insert` | panic |
| 10.1.3 | `UpdateByPK(FullScope)`, `UpdateReturningByPK(FullScope, …)` | `UPDATE … SET …` **без WHERE — перезаписывает всю таблицу** |
| 10.1.4 | `UpdateByPK`/`SoftDeleteByPK` с именованным scope | panic |
| 10.1.5 | `DeleteByPK`, `ExistByPK` | выполняют пустую строку SQL |
| 10.1.6 | `GetByPK` | пустая строка SQL, затем panic |
| 10.1.7 | `Update(ctx, q, row, scope, clauses)` | нет `args`: обновить строку по составному ключу нельзя |

10.2. **Исправления:**
10.2.1. table_clause.go:94: `if i == pkPos` вместо `if pk.Pos == i` — так же, как уже сделано в ветке `FullScope`.
При отсутствии PK `pkPos == -1`. Для таблиц с PK поведение не меняется.
10.2.2. Девять ByPK-методов на таблице без PK возвращают `ErrNoPrimaryKey` и не выполняют SQL: `GetByPK`, `GetByPKTo`,
`ExistByPK`, `DeleteByPK`, `DeleteReturningByPK`, `UpdateByPK`, `UpdateReturningByPK`, `SoftDeleteByPK`, `SoftDeleteReturningByPK`.
Ошибка уже объявлена (table.go:16), но нигде не возвращается.
10.2.3. `Insert` работает без PK: команда `insertAllFields` строится вне `if t.pk != nil`.
10.2.4. `NewTable` паникует, если `dbw:"pk"` стоит на нескольких полях. Сейчас молча берётся первое, и `UpdateByPK`
обновил бы все строки с этим значением. Panic оставляет место для будущей поддержки составного PK без смены поведения.
10.2.5. `Update` и `UpdateReturning` получают `args ...any`. `$N` в условии вызывающий начинает с `$1`, velum сдвигает
их после SET-параметров, как в upsert:

```go
var setQty = "WHERE order_id=$1 AND sku=$2"
_, err := items.Update(ctx, db, &item, "qty", setQty, orderID, sku)
// UPDATE order_items SET qty=$1 WHERE order_id=$2 AND sku=$3
```

Вызовы без `args` компилируются как раньше. Сломается только тот, кто уже пишет в условии абсолютные `$N`,
ссылаясь на SET-параметры; это упомянем в заметках к выпуску.

10.3. **Почему составной ключ без `id` — правильная опора для upsert и не медленнее:**
10.3.1. Поиск по `(a, b)` — один спуск по btree, как по `id`.
10.3.2. Для `ON CONFLICT` уникальный индекс на `(a, b)` нужен всё равно. Суррогатный `id` добавил бы второй индекс
на каждую вставку и сжигал бы `nextval` на каждом конфликте.
10.3.3. Накладные расходы velum: `Get` по составному условию ≈ 1,1–1,3 мкс против ≈ 0,7–0,8 мкс у `GetByPK` (поиск в кэше
против готовой команды; fake-DB, машина под нагрузкой). Это меньше 0,5% даже от локального запроса к PostgreSQL.

10.4. **Тесты:**
10.4.1. Табличный тест всех методов `Table` на структуре без PK: ни одного panic, ByPK-методы возвращают `ErrNoPrimaryKey`.
10.4.2. «`UpdateByPK` без PK не вызывает `ExecContext`».
10.4.3. Сдвиг `$N` у `Update` с `args`.
10.4.4. Panic при нескольких `pk`.
10.4.5. Интеграционный тест на PostgreSQL: таблица с составным ключом, `Select`/`Update`/`Delete` по условию.

10.5. Раздел AGENTS.md о составных ключах — под настоящие сигнатуры.

## 11. Тесты и документация к коммиту 2

11.1. Unit: SQL каждого примера раздела 6, каждая проверка раздела 8, разбор `UpdateExpr`.

11.2. Fake-DB: порядок аргументов, три значения `UpsertOutcome`, `QueryRow` без строки.

11.3. Интеграционные на PostgreSQL (testcontainers, как в velum_test.go): все три outcome, частичный индекс,
`ON CONSTRAINT`, цель-выражение, `UpdateWhere` = false, `InTx`.

11.4. README и AGENTS.md:
11.4.1. раздел «Upsert»;
11.4.2. команды объявляются переменными пакета;
11.4.3. псевдоним `t`;
11.4.4. запрет системных столбцов в `UpdateExpr`;
11.4.5. «обновлений без изменений нет».

## 12. Заметки для Гостли (не velum)

12.1. `plugins.updated_at` (COMMENT: «time the manifest or the status last changed») и `environments.updated_at` несут
бизнес-смысл «когда менялось содержимое». В Go-коде Гостли их никто не читает.
12.1.1. Для environments подходит `UpdateWhere` (6.6): строка не трогается, если цвет тот же.
12.1.2. Для plugins строка меняется при каждой проверке (`checked_at`). В системном смысле `updated_at` при этом
должен двигаться. Если нужен момент изменения манифеста, это отдельный бизнес-столбец, например `manifest_changed_at`,
который ставится через `UpdateExpr`.

12.2. Форма 10 (git_settings_db.go:31) переписывается по 6.10.
