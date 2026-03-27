# Contributing to DuckFrame

Thank you for your interest in contributing! DuckFrame is an open-source project and we welcome contributions of all kinds.

## Getting Started

### Prerequisites

- **Go** >= 1.22
- **CGO enabled** (`CGO_ENABLED=1`)
- **C compiler** (gcc or clang)
  - macOS: `xcode-select --install`
  - Ubuntu/Debian: `sudo apt install build-essential`

### Setup

```bash
git clone https://github.com/lserra/duckframe.git
cd duckframe
go mod download
make build
make test
```

## Development Workflow

### 1. Create a branch

```bash
git checkout -b feature/my-feature
```

### 2. Make your changes

- Write code in `duckframe.go` (or create new files as needed)
- Add tests in `duckframe_test.go`
- Update documentation if adding new public API

### 3. Run checks

```bash
make fmt        # Format code
make vet        # Static analysis
make lint       # golangci-lint (install: brew install golangci-lint)
make test       # Run all tests
make coverage   # Check coverage (target: ≥ 80%)
```

Or run everything at once:

```bash
make all
```

### 4. Submit a pull request

- Describe what your change does and why
- Reference any related issues
- Ensure CI passes

## Code Style

- Follow standard Go conventions (`gofmt`, `go vet`)
- All exported functions **must** have doc comments (`// FuncName does ...`)
- Error messages should start with `duckframe:` for consistency
- Use `fmt.Errorf("duckframe: ... : %w", err)` for error wrapping

## Testing

- Tests live in `duckframe_test.go` alongside the main code
- Each new public function should have at least one test
- Use `openTestDB(t)` helper to create an in-memory DuckDB for tests
- Use `testdataPath(name)` to reference files in `testdata/`
- Error propagation tests: create a "bad" DataFrame and verify the error passes through

Example:

```go
func TestMyFeature(t *testing.T) {
    db := openTestDB(t)
    defer db.Close()

    df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
    if err != nil {
        t.Fatalf("ReadCSV failed: %v", err)
    }
    defer df.Close()

    // Test your feature...
}
```

### Running specific tests

```bash
CGO_ENABLED=1 go test -v -run TestMyFeature ./...
```

### Benchmarks

```bash
CGO_ENABLED=1 go test -bench=. -benchmem -run=^$ ./...
```

## Project Structure

```
duckframe/
├── duckframe.go              # Main library (all public API)
├── duckframe_test.go         # Unit tests
├── duckframe_bench_test.go   # Benchmarks
├── example_test.go           # Example tests (godoc)
├── internal/engine/          # DuckDB connection management
├── examples/                 # Runnable example programs
│   ├── basic/                # Core operations
│   ├── etl/                  # CSV → Parquet pipeline
│   ├── analysis/             # Exploratory data analysis
│   ├── concurrent/           # Parallel + chunked processing
│   └── http-api/             # REST API example
├── testdata/                 # Test data files
├── docs/                     # Additional documentation
├── .golangci.yml             # Linter configuration
├── Makefile                  # Build/test/lint commands
├── ROADMAP.md                # Project roadmap
├── USER_GUIDE.md             # User guide
└── CONTRIBUTING.md           # This file
```

## Architecture

DuckFrame is backed by DuckDB. Every `DataFrame` is a DuckDB temporary table with a unique name (`df_1`, `df_2`, ...). Operations translate to SQL queries that create new temporary tables.

Key design decisions:

- **Single connection** (`SetMaxOpenConns(1)`) — DuckDB temp tables are connection-scoped
- **Error propagation** — DataFrames carry an `err` field; all methods check it first (guard clause)
- **Owned tables** — DataFrames created by operations "own" their table and drop it on `Close()`
- **No mutations** — every operation returns a new DataFrame

## Reporting Issues

- Use [GitHub Issues](https://github.com/lserra/duckframe/issues)
- Include Go version (`go version`), OS, and DuckDB driver version
- For bugs, include a minimal reproducer

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
