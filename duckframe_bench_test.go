package duckframe_test

import (
	"context"
	"testing"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

// BenchmarkSequentialFilter applies a filter to 5 DataFrames sequentially.
func BenchmarkSequentialFilter(b *testing.B) {
	db, err := engine.Open("")
	if err != nil {
		b.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	for n := 0; n < b.N; n++ {
		var dfs []*duckframe.DataFrame
		for i := 0; i < 5; i++ {
			df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
			if err != nil {
				b.Fatalf("ReadCSV failed: %v", err)
			}
			dfs = append(dfs, df)
		}

		for _, df := range dfs {
			filtered, err := df.Filter("salary > 80000")
			if err != nil {
				b.Fatalf("Filter failed: %v", err)
			}
			filtered.Close()
			df.Close()
		}
	}
}

// BenchmarkParallelFilter applies a filter to 5 DataFrames concurrently.
func BenchmarkParallelFilter(b *testing.B) {
	db, err := engine.Open("")
	if err != nil {
		b.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	filterFn := func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		return df.Filter("salary > 80000")
	}

	for n := 0; n < b.N; n++ {
		var dfs []*duckframe.DataFrame
		for i := 0; i < 5; i++ {
			df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
			if err != nil {
				b.Fatalf("ReadCSV failed: %v", err)
			}
			dfs = append(dfs, df)
		}

		results, err := duckframe.ParallelApply(dfs, filterFn)
		if err != nil {
			b.Fatalf("ParallelApply failed: %v", err)
		}
		for _, r := range results {
			r.Close()
		}
		for _, df := range dfs {
			df.Close()
		}
	}
}

// BenchmarkSequentialSortLimit sorts and limits 5 DataFrames sequentially.
func BenchmarkSequentialSortLimit(b *testing.B) {
	db, err := engine.Open("")
	if err != nil {
		b.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	for n := 0; n < b.N; n++ {
		var dfs []*duckframe.DataFrame
		for i := 0; i < 5; i++ {
			df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
			if err != nil {
				b.Fatalf("ReadCSV failed: %v", err)
			}
			dfs = append(dfs, df)
		}

		for _, df := range dfs {
			sorted, err := df.Sort("salary", false)
			if err != nil {
				b.Fatalf("Sort failed: %v", err)
			}
			limited, err := sorted.Limit(3)
			if err != nil {
				b.Fatalf("Limit failed: %v", err)
			}
			limited.Close()
			sorted.Close()
			df.Close()
		}
	}
}

// BenchmarkParallelSortLimit sorts and limits 5 DataFrames concurrently.
func BenchmarkParallelSortLimit(b *testing.B) {
	db, err := engine.Open("")
	if err != nil {
		b.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	sortLimitFn := func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		sorted, err := df.Sort("salary", false)
		if err != nil {
			return nil, err
		}
		defer sorted.Close()
		return sorted.Limit(3)
	}

	for n := 0; n < b.N; n++ {
		var dfs []*duckframe.DataFrame
		for i := 0; i < 5; i++ {
			df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
			if err != nil {
				b.Fatalf("ReadCSV failed: %v", err)
			}
			dfs = append(dfs, df)
		}

		results, err := duckframe.ParallelApply(dfs, sortLimitFn)
		if err != nil {
			b.Fatalf("ParallelApply failed: %v", err)
		}
		for _, r := range results {
			r.Close()
		}
		for _, df := range dfs {
			df.Close()
		}
	}
}

// BenchmarkChunkedReading benchmarks reading a CSV in chunks.
func BenchmarkChunkedReading(b *testing.B) {
	db, err := engine.Open("")
	if err != nil {
		b.Fatalf("failed to open DB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	for n := 0; n < b.N; n++ {
		ch := duckframe.ReadCSVChunked(ctx, db, testdataPath("employees.csv"), 3)
		for chunk := range ch {
			if chunk.Err != nil {
				b.Fatalf("chunk error: %v", chunk.Err)
			}
			chunk.DataFrame.Close()
		}
	}
}
