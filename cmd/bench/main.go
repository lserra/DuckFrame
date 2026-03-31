package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

type benchResult struct {
	Operation string
	DuckFrame time.Duration
	Gota      time.Duration
}

func absPath(rel string) string {
	abs, _ := filepath.Abs(rel)
	return abs
}

func benchDuckFrame(csvPath, deptPath string) []benchResult {
	var results []benchResult

	db, err := engine.Open("")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	start := time.Now()
	df, err := duckframe.ReadCSV(db, csvPath)
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"ReadCSV", time.Since(start), 0})

	start = time.Now()
	filtered, err := df.Filter("salary > 100000")
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"Filter", time.Since(start), 0})

	start = time.Now()
	sorted, err := df.Sort("salary", false)
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"Sort", time.Since(start), 0})

	start = time.Now()
	grouped, err := df.GroupBy("department").Agg("salary", "mean")
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"GroupBy+Agg", time.Since(start), 0})

	deptDf, err := duckframe.ReadCSV(db, deptPath)
	if err != nil {
		panic(err)
	}
	start = time.Now()
	joined, err := df.Join(deptDf, "department", "inner")
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"Join", time.Since(start), 0})

	start = time.Now()
	selected, err := df.Select("name", "salary", "department")
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"Select", time.Since(start), 0})

	tmpFile := filepath.Join(os.TempDir(), "duckframe_bench_out.csv")
	start = time.Now()
	err = df.WriteCSV(tmpFile)
	if err != nil {
		panic(err)
	}
	results = append(results, benchResult{"WriteCSV", time.Since(start), 0})
	os.Remove(tmpFile)

	for _, d := range []*duckframe.DataFrame{df, filtered, sorted, grouped, joined, selected, deptDf} {
		if d != nil {
			d.Close()
		}
	}

	return results
}

func benchGota(csvPath, deptPath string) []benchResult {
	var results []benchResult

	start := time.Now()
	f, err := os.Open(csvPath)
	if err != nil {
		panic(err)
	}
	df := dataframe.ReadCSV(f)
	f.Close()
	results = append(results, benchResult{"ReadCSV", 0, time.Since(start)})

	start = time.Now()
	filtered := df.Filter(
		dataframe.F{Colname: "salary", Comparator: series.Greater, Comparando: 100000.0},
	)
	_ = filtered
	results = append(results, benchResult{"Filter", 0, time.Since(start)})

	start = time.Now()
	sorted := df.Arrange(dataframe.RevSort("salary"))
	_ = sorted
	results = append(results, benchResult{"Sort", 0, time.Since(start)})

	start = time.Now()
	grouped := df.GroupBy("department").Aggregation(
		[]dataframe.AggregationType{dataframe.Aggregation_MEAN},
		[]string{"salary"},
	)
	_ = grouped
	results = append(results, benchResult{"GroupBy+Agg", 0, time.Since(start)})

	f2, err := os.Open(deptPath)
	if err != nil {
		panic(err)
	}
	deptDf := dataframe.ReadCSV(f2)
	f2.Close()
	start = time.Now()
	joined := df.InnerJoin(deptDf, "department")
	_ = joined
	results = append(results, benchResult{"Join", 0, time.Since(start)})

	start = time.Now()
	selected := df.Select([]string{"name", "salary", "department"})
	_ = selected
	results = append(results, benchResult{"Select", 0, time.Since(start)})

	tmpFile := filepath.Join(os.TempDir(), "gota_bench_out.csv")
	start = time.Now()
	out, err := os.Create(tmpFile)
	if err != nil {
		panic(err)
	}
	_ = df.WriteCSV(out)
	out.Close()
	results = append(results, benchResult{"WriteCSV", 0, time.Since(start)})
	os.Remove(tmpFile)

	return results
}

func main() {
	fmt.Printf("=== DuckFrame vs Gota Benchmark ===\n")
	fmt.Printf("Go %s | %s/%s | CPUs: %d\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU())

	dataDir := absPath("benchmarks/data")

	for _, tc := range []struct {
		label string
		file  string
		iters int
	}{
		{"100K rows", "bench_100k.csv", 3},
		{"1M rows", "bench_1000k.csv", 1},
	} {
		csvPath := filepath.Join(dataDir, tc.file)
		deptPath := filepath.Join(dataDir, "departments.csv")

		fmt.Printf("\nWarmup (%s)...\n", tc.label)
		_ = benchDuckFrame(csvPath, deptPath)
		_ = benchGota(csvPath, deptPath)

		iterations := tc.iters
		fmt.Printf("Running %d iteration(s) with %s...\n\n", iterations, tc.label)

		duckResults := make([][]benchResult, iterations)
		gotaResults := make([][]benchResult, iterations)

		for i := 0; i < iterations; i++ {
			duckResults[i] = benchDuckFrame(csvPath, deptPath)
			gotaResults[i] = benchGota(csvPath, deptPath)
		}

		fmt.Printf("%-15s %15s %15s %10s\n", "Operation", "DuckFrame", "Gota", "Speedup")
		fmt.Println("--------------------------------------------------------------")

		for opIdx := range duckResults[0] {
			var duckTotal, gotaTotal time.Duration
			for i := 0; i < iterations; i++ {
				duckTotal += duckResults[i][opIdx].DuckFrame
				gotaTotal += gotaResults[i][opIdx].Gota
			}
			duckAvg := duckTotal / time.Duration(iterations)
			gotaAvg := gotaTotal / time.Duration(iterations)
			speedup := float64(gotaAvg) / float64(duckAvg)

			op := duckResults[0][opIdx].Operation
			fmt.Printf("%-15s %15s %15s %9.1fx\n", op, duckAvg, gotaAvg, speedup)
		}
	}
}
