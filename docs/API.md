# DuckFrame — API Reference

> Auto-generated reference. For detailed usage and examples, see [USER_GUIDE.md](../USER_GUIDE.md).

---

## Package `duckframe`

```go
import "github.com/lserra/duckframe"
```

### Types

#### `DataFrame`

Represents a tabular dataset backed by a DuckDB temporary table.

```go
type DataFrame struct { /* unexported fields */ }
```

#### `GroupedFrame`

Represents a DataFrame grouped by one or more columns. Created by `GroupBy()`, consumed by `Agg()`.

```go
type GroupedFrame struct { /* unexported fields */ }
```

#### `ApplyFunc`

Function signature for `ParallelApply`.

```go
type ApplyFunc func(df *DataFrame) (*DataFrame, error)
```

#### `ChunkResult`

Result of each chunk from `ReadCSVChunked`.

```go
type ChunkResult struct {
    DataFrame *DataFrame
    Index     int
    Err       error
}
```

---

### Constructors

#### `New`

```go
func New(db *engine.DB, columns []string, rows []map[string]interface{}) (*DataFrame, error)
```

Creates a DataFrame from column names and row data. Types are inferred automatically.

#### `FromQuery`

```go
func FromQuery(db *engine.DB, query string) (*DataFrame, error)
```

Creates a DataFrame from a SQL query result.

#### `ReadCSV`

```go
func ReadCSV(db *engine.DB, path string) (*DataFrame, error)
```

Reads a CSV file using DuckDB's `read_csv_auto` (automatic type/delimiter detection).

#### `ReadParquet`

```go
func ReadParquet(db *engine.DB, path string) (*DataFrame, error)
```

Reads a Parquet file.

#### `ReadJSON`

```go
func ReadJSON(db *engine.DB, path string) (*DataFrame, error)
```

Reads a JSON Lines (newline-delimited JSON) file.

#### `ReadSQLite`

```go
func ReadSQLite(db *engine.DB, path string, table string) (*DataFrame, error)
```

Reads a table from a SQLite database file using DuckDB's sqlite extension.

#### `ReadPostgres`

```go
func ReadPostgres(db *engine.DB, dsn string, query string) (*DataFrame, error)
```

Reads from PostgreSQL using DuckDB's postgres extension. The `query` can be a table name or a full SELECT statement.

#### `ReadMySQL`

```go
func ReadMySQL(db *engine.DB, dsn string, query string) (*DataFrame, error)
```

Reads from MySQL using DuckDB's mysql extension.

#### `ReadFromDB`

```go
func ReadFromDB(duckDB *engine.DB, extDB *sql.DB, query string) (*DataFrame, error)
```

Generic connector — reads from any `database/sql` compatible connection. Fetches all rows into memory, then creates a DuckDB-backed DataFrame.

---

### DataFrame Methods

#### Inspection

| Method | Signature | Description |
|---|---|---|
| `Columns` | `() []string` | Returns column names |
| `Shape` | `() (rows, cols int, err error)` | Row and column count |
| `Dtypes` | `() (map[string]string, error)` | Column name → DuckDB type |
| `Describe` | `() (*DataFrame, error)` | Descriptive stats (count, mean, std, min, max) |
| `Show` | `(maxRows ...int) error` | Print formatted table (default: 50 rows) |
| `TableName` | `() string` | Internal DuckDB table name |
| `Engine` | `() *engine.DB` | Underlying DB connection |
| `Err` | `() error` | Error stored in DataFrame (for chaining) |

#### Transformation

| Method | Signature | Description |
|---|---|---|
| `Select` | `(cols ...string) (*DataFrame, error)` | Select columns |
| `Filter` | `(expr string) (*DataFrame, error)` | Filter with SQL WHERE expression |
| `Sort` | `(col string, asc bool) (*DataFrame, error)` | Sort by column |
| `Limit` | `(n int) (*DataFrame, error)` | First n rows |
| `Head` | `(n int) (*DataFrame, error)` | Alias for Limit |
| `Tail` | `(n int) (*DataFrame, error)` | Last n rows |
| `Distinct` | `() (*DataFrame, error)` | Remove duplicates |
| `Rename` | `(oldName, newName string) (*DataFrame, error)` | Rename column |
| `Drop` | `(cols ...string) (*DataFrame, error)` | Drop columns |
| `WithColumn` | `(name, expr string) (*DataFrame, error)` | Add/replace computed column |
| `Join` | `(other *DataFrame, on, how string) (*DataFrame, error)` | Join (inner/left/right/full) |
| `Union` | `(other *DataFrame) (*DataFrame, error)` | Combine (UNION ALL) |
| `Sql` | `(query string) (*DataFrame, error)` | Raw SQL with `{df}` placeholder |

#### Aggregation

| Method | Signature | Description |
|---|---|---|
| `GroupBy` | `(cols ...string) *GroupedFrame` | Group by columns |
| `Agg` | `(col, fn string) (*DataFrame, error)` | Aggregate: mean/avg, sum, count, min, max |

#### Materialization

| Method | Signature | Description |
|---|---|---|
| `Collect` | `() ([]map[string]interface{}, error)` | To slice of maps |
| `ToSlice` | `(dest interface{}) error` | To slice of structs (mapped via `df` tag) |
| `WriteCSV` | `(path string) error` | Export to CSV |
| `WriteParquet` | `(path string) error` | Export to Parquet |
| `WriteJSON` | `(path string) error` | Export to JSON |

#### Lifecycle

| Method | Signature | Description |
|---|---|---|
| `Close` | `() error` | Drop temporary table and free resources |

#### Context-Aware

| Method/Function | Signature |
|---|---|
| `FromQueryContext` | `(ctx, db, query) (*DataFrame, error)` |
| `ReadCSVContext` | `(ctx, db, path) (*DataFrame, error)` |
| `FilterContext` | `(ctx, expr) (*DataFrame, error)` |
| `SortContext` | `(ctx, col, asc) (*DataFrame, error)` |

#### Concurrency

| Function | Signature |
|---|---|
| `ParallelApply` | `(dfs []*DataFrame, fn ApplyFunc) ([]*DataFrame, error)` |
| `ReadCSVChunked` | `(ctx, db, path, chunkSize) <-chan ChunkResult` |

---

## Package `engine`

```go
import "github.com/lserra/duckframe/internal/engine"
```

### `Open`

```go
func Open(path string) (*DB, error)
```

Opens a DuckDB connection. Pass `""` for in-memory, or a file path for persistent storage. Sets `MaxOpenConns(1)` — DuckDB temp tables are connection-scoped.

### `(*DB).Conn`

```go
func (db *DB) Conn() *sql.DB
```

Returns the underlying `*sql.DB` for direct SQL access.

### `(*DB).Close`

```go
func (db *DB) Close() error
```

Closes the DuckDB connection.
