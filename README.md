# DuckFrame 🦆

[![CI](https://github.com/lserra/duckframe/actions/workflows/ci.yml/badge.svg)](https://github.com/lserra/duckframe/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/lserra/duckframe.svg)](https://pkg.go.dev/github.com/lserra/duckframe)
[![Go Report Card](https://goreportcard.com/badge/github.com/lserra/duckframe)](https://goreportcard.com/report/github.com/lserra/duckframe)
[![Coverage](https://img.shields.io/badge/coverage-80.6%25-brightgreen)](https://github.com/lserra/duckframe)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A Pandas-like DataFrame library for Go, powered by DuckDB.**

DuckFrame brings the familiar DataFrame experience to Go, using [DuckDB](https://duckdb.org/) as the execution engine. Get the simplicity of Pandas with the performance of a vectorized columnar database — all in a single binary, no Python required.

```go
db, _ := engine.Open("")
defer db.Close()

df, _ := duckframe.ReadCSV(db, "employees.csv")
defer df.Close()

result, _ := df.Filter("age > 30").
    GroupBy("country").
    Agg("salary", "mean")

result.Show()
// DataFrame [3 rows x 2 cols]
// country    mean(salary)
// ---------  ------------
// Brazil     94000.41
// Germany    82500.37
// USA        70000.00
```

---

## Features

- **Pandas-like API** — `Filter`, `Select`, `GroupBy`, `Agg`, `Sort`, `Join`, `Union`, `Describe`...
- **DuckDB-powered** — vectorized execution, automatic parallelism, zero-copy Parquet/CSV reads
- **Multi-format** — CSV, Parquet, JSON Lines (read & write)
- **External connectors** — SQLite, PostgreSQL, MySQL, any `database/sql` driver
- **Concurrency** — `ParallelApply`, `ReadCSVChunked`, context-aware operations
- **SQL escape hatch** — `df.Sql("SELECT ... FROM {df} WHERE ...")`
- **Type-safe materialization** — `ToSlice(&[]MyStruct{})` with struct tag mapping
- **Fluent error handling** — chain operations safely, check errors at the end
- **Single binary** — no runtime dependencies, CGO for DuckDB only

## Installation

```bash
go get github.com/lserra/duckframe
```

> **Requirement:** CGO must be enabled (`CGO_ENABLED=1`) and a C compiler available (gcc/clang).
>
> - macOS: `xcode-select --install`
> - Ubuntu/Debian: `sudo apt install build-essential`

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    "github.com/lserra/duckframe"
    "github.com/lserra/duckframe/internal/engine"
)

func main() {
    // Open in-memory DuckDB
    db, err := engine.Open("")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Read CSV
    df, err := duckframe.ReadCSV(db, "data/employees.csv")
    if err != nil {
        log.Fatal(err)
    }
    defer df.Close()

    // Explore
    df.Show()

    r, c, _ := df.Shape()
    fmt.Printf("Rows: %d, Cols: %d\n", r, c)

    // Filter + Select
    result, _ := df.Filter("salary > 80000")
    defer result.Close()

    selected, _ := result.Select("name", "country", "salary")
    defer selected.Close()
    selected.Show()

    // GroupBy + Agg
    grouped, _ := df.GroupBy("country").Agg("salary", "mean")
    defer grouped.Close()
    grouped.Show()

    // Raw SQL
    top, _ := df.Sql("SELECT name, salary FROM {df} ORDER BY salary DESC LIMIT 3")
    defer top.Close()
    top.Show()

    // Materialize to structs
    type Employee struct {
        Name   string  `df:"name"`
        Salary float64 `df:"salary"`
    }
    var emps []Employee
    df.ToSlice(&emps)
    for _, e := range emps {
        fmt.Printf("%s: $%.0f\n", e.Name, e.Salary)
    }
}
```

## API Overview

### Creating DataFrames

| Function | Description |
|---|---|
| `New(db, columns, rows)` | From Go data (maps) |
| `FromQuery(db, sql)` | From any SQL query |
| `ReadCSV(db, path)` | Read CSV file |
| `ReadParquet(db, path)` | Read Parquet file |
| `ReadJSON(db, path)` | Read JSON Lines file |
| `ReadSQLite(db, path, table)` | Read from SQLite |
| `ReadPostgres(db, dsn, query)` | Read from PostgreSQL |
| `ReadMySQL(db, dsn, query)` | Read from MySQL |
| `ReadFromDB(db, extDB, query)` | Read from any `database/sql` |

### Operations

| Method | Description |
|---|---|
| `Select(cols...)` | Select columns |
| `Filter(expr)` | Filter rows with SQL expression |
| `Sort(col, asc)` | Sort by column |
| `Limit(n)` | First n rows |
| `Distinct()` | Remove duplicates |
| `GroupBy(cols...).Agg(col, fn)` | Aggregate (mean, sum, count, min, max) |
| `Join(other, on, how)` | Join (inner, left, right, full) |
| `Union(other)` | Combine DataFrames |
| `Rename(old, new)` | Rename column |
| `Drop(cols...)` | Drop columns |
| `WithColumn(name, expr)` | Add/replace computed column |
| `Head(n)` / `Tail(n)` | First/last n rows |
| `Sql(query)` | Raw SQL with `{df}` placeholder |

### Inspection

| Method | Description |
|---|---|
| `Show(maxRows...)` | Print formatted table |
| `Shape()` | (rows, cols) |
| `Columns()` | Column names |
| `Dtypes()` | Column types |
| `Describe()` | Descriptive statistics |

### Materialization

| Method | Description |
|---|---|
| `Collect()` | To `[]map[string]interface{}` |
| `ToSlice(&dest)` | To slice of structs (via `df` tags) |
| `WriteCSV(path)` | Export to CSV |
| `WriteParquet(path)` | Export to Parquet |
| `WriteJSON(path)` | Export to JSON |

### Concurrency

| Function/Method | Description |
|---|---|
| `ParallelApply(dfs, fn)` | Apply function to multiple DFs in parallel |
| `ReadCSVChunked(ctx, db, path, size)` | Stream CSV in chunks |
| `FilterContext(ctx, expr)` | Context-aware filter |
| `SortContext(ctx, col, asc)` | Context-aware sort |
| `FromQueryContext(ctx, db, sql)` | Context-aware query |
| `ReadCSVContext(ctx, db, path)` | Context-aware CSV read |

## Examples

See the [`examples/`](examples/) directory for complete, runnable examples:

| Example | Description |
|---|---|
| [`basic`](examples/basic/) | Core operations: read, filter, select, group, SQL |
| [`etl`](examples/etl/) | CSV → filter → Parquet pipeline |
| [`analysis`](examples/analysis/) | Exploratory data analysis |
| [`concurrent`](examples/concurrent/) | Parallel processing & chunked reading |
| [`http-api`](examples/http-api/) | REST API serving DataFrame queries |

## Development

```bash
make build      # Build
make test       # Run tests
make coverage   # Coverage report
make lint       # golangci-lint
make vet        # go vet
make fmt        # Format code
make all        # All of the above
```

## Project Status

| Phase | Status |
|---|---|
| 0 — Setup | ✅ |
| 1 — Core | ✅ |
| 2 — MVP | ✅ |
| 3 — Fluent API | ✅ |
| 4 — Data Formats | ✅ |
| 5 — Advanced Operations | ✅ |
| 6 — Concurrency & Streaming | ✅ |
| 7 — External Connectors | ✅ |
| 8 — Quality & Tooling | ✅ |
| 9 — Docs & Examples | ✅ |
| 10 — Benchmarks & Launch | ⬜ |

See [ROADMAP.md](ROADMAP.md) for the detailed plan.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT — see [LICENSE](LICENSE) for details.

---

> **DuckFrame** — DataFrames for Go. Powered by DuckDB. Built for engineers.
