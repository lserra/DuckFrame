# Benchmarks

Performance comparison of DuckFrame against other DataFrame libraries.

## Setup

### Generate test data

```bash
python3 benchmarks/scripts/generate_data.py
```

This creates:
- `benchmarks/data/bench_100k.csv` (100K rows, ~3.7MB)
- `benchmarks/data/bench_1000k.csv` (1M rows, ~38MB)
- `benchmarks/data/departments.csv` (8 rows, for join tests)

### Install Python dependencies

```bash
pip install pandas polars duckdb
```

## Run Benchmarks

### Go: DuckFrame vs Gota

```bash
make bench
# or
CGO_ENABLED=1 go run ./cmd/bench
```

### Python: DuckDB vs Pandas vs Polars

```bash
make bench-python
# or
python3 benchmarks/scripts/bench_all_python.py
```

## Methodology

- **Iterations**: 3 per operation (1 for Gota on 1M due to performance)
- **Warmup**: Each library runs one warmup iteration before timing
- **Operations tested**: ReadCSV, Filter, Sort, GroupBy+Agg, Join, Select, WriteCSV
- **Data**: Synthetic employee data with 6 columns (id, name, age, country, department, salary)
- **Environment**: macOS, Intel 8-core, Go 1.26.1, Python 3.12.6

## Results

See the [main README](../README.md#benchmarks) for the full results table.
