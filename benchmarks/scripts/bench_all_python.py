"""
Benchmark: DuckFrame (Go/DuckDB) vs DuckDB (Python) vs Pandas vs Polars
Runs common DataFrame operations and reports timing for each.
"""

import json
import os
import platform
import tempfile
import time

import duckdb
import pandas as pd
import polars as pl


def abs_path(rel):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", rel)


OPERATIONS = ["ReadCSV", "Filter", "Sort", "GroupBy+Agg", "Join", "Select", "WriteCSV"]


def bench_duckdb(csv_path, dept_path):
    results = {}
    conn = duckdb.connect()

    start = time.perf_counter()
    conn.execute(f"CREATE TABLE df AS SELECT * FROM read_csv_auto('{csv_path}')")
    results["ReadCSV"] = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute("CREATE TABLE filtered AS SELECT * FROM df WHERE salary > 100000")
    results["Filter"] = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute("CREATE TABLE sorted AS SELECT * FROM df ORDER BY salary DESC")
    results["Sort"] = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute(
        "CREATE TABLE grouped AS SELECT department, AVG(salary) AS mean_salary FROM df GROUP BY department"
    )
    results["GroupBy+Agg"] = time.perf_counter() - start

    conn.execute(f"CREATE TABLE dept AS SELECT * FROM read_csv_auto('{dept_path}')")
    start = time.perf_counter()
    conn.execute(
        "CREATE TABLE joined AS SELECT * FROM df INNER JOIN dept USING (department)"
    )
    results["Join"] = time.perf_counter() - start

    start = time.perf_counter()
    conn.execute("CREATE TABLE selected AS SELECT name, salary, department FROM df")
    results["Select"] = time.perf_counter() - start

    tmp = os.path.join(tempfile.gettempdir(), "duckdb_bench_out.csv")
    start = time.perf_counter()
    conn.execute(f"COPY df TO '{tmp}' (FORMAT CSV, HEADER)")
    results["WriteCSV"] = time.perf_counter() - start
    os.remove(tmp)

    conn.close()
    return results


def bench_pandas(csv_path, dept_path):
    results = {}

    start = time.perf_counter()
    df = pd.read_csv(csv_path)
    results["ReadCSV"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df[df["salary"] > 100000]
    results["Filter"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.sort_values("salary", ascending=False)
    results["Sort"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.groupby("department")["salary"].mean()
    results["GroupBy+Agg"] = time.perf_counter() - start

    dept = pd.read_csv(dept_path)
    start = time.perf_counter()
    _ = df.merge(dept, on="department", how="inner")
    results["Join"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df[["name", "salary", "department"]]
    results["Select"] = time.perf_counter() - start

    tmp = os.path.join(tempfile.gettempdir(), "pandas_bench_out.csv")
    start = time.perf_counter()
    df.to_csv(tmp, index=False)
    results["WriteCSV"] = time.perf_counter() - start
    os.remove(tmp)

    return results


def bench_polars(csv_path, dept_path):
    results = {}

    start = time.perf_counter()
    df = pl.read_csv(csv_path)
    results["ReadCSV"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.filter(pl.col("salary") > 100000)
    results["Filter"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.sort("salary", descending=True)
    results["Sort"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.group_by("department").agg(pl.col("salary").mean())
    results["GroupBy+Agg"] = time.perf_counter() - start

    dept = pl.read_csv(dept_path)
    start = time.perf_counter()
    _ = df.join(dept, on="department", how="inner")
    results["Join"] = time.perf_counter() - start

    start = time.perf_counter()
    _ = df.select(["name", "salary", "department"])
    results["Select"] = time.perf_counter() - start

    tmp = os.path.join(tempfile.gettempdir(), "polars_bench_out.csv")
    start = time.perf_counter()
    df.write_csv(tmp)
    results["WriteCSV"] = time.perf_counter() - start
    os.remove(tmp)

    return results


def main():
    print("=== Cross-Language DataFrame Benchmark ===")
    print(
        f"Python {platform.python_version()} | DuckDB {duckdb.__version__} | pandas {pd.__version__} | polars {pl.__version__}"
    )
    print(f"{platform.system()}/{platform.machine()} | CPUs: {os.cpu_count()}\n")

    iterations = 3
    all_results = {}

    for label, n in [("100K", "100k"), ("1M", "1000k")]:
        csv_path = abs_path(f"benchmarks/data/bench_{n}.csv")
        dept_path = abs_path("benchmarks/data/departments.csv")

        print(f"Warmup ({label})...")
        bench_duckdb(csv_path, dept_path)
        bench_pandas(csv_path, dept_path)
        bench_polars(csv_path, dept_path)

        print(f"Running {iterations} iterations with {label} rows...\n")

        duckdb_runs = [bench_duckdb(csv_path, dept_path) for _ in range(iterations)]
        pandas_runs = [bench_pandas(csv_path, dept_path) for _ in range(iterations)]
        polars_runs = [bench_polars(csv_path, dept_path) for _ in range(iterations)]

        print(f"{'Operation':<15} {'DuckDB(Py)':>12} {'Pandas':>12} {'Polars':>12}")
        print("-" * 55)

        label_results = {}
        for op in OPERATIONS:
            duckdb_avg = sum(r[op] for r in duckdb_runs) / iterations
            pandas_avg = sum(r[op] for r in pandas_runs) / iterations
            polars_avg = sum(r[op] for r in polars_runs) / iterations
            print(
                f"{op:<15} {duckdb_avg*1000:>9.1f}ms {pandas_avg*1000:>9.1f}ms {polars_avg*1000:>9.1f}ms"
            )
            label_results[op] = {
                "duckdb_py": round(duckdb_avg * 1000, 1),
                "pandas": round(pandas_avg * 1000, 1),
                "polars": round(polars_avg * 1000, 1),
            }

        all_results[label] = label_results
        print()

    # Write JSON for later aggregation
    json_path = abs_path("benchmarks/results/python_results.json")
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"Results written to {json_path}")


if __name__ == "__main__":
    main()
