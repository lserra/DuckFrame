// Example: Concurrent processing
//
// This example demonstrates parallel DataFrame processing and
// chunked CSV reading with DuckFrame's concurrency primitives.
//
// Usage:
//
//	cd examples/concurrent && go run main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func main() {
	db, err := engine.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	csvPath := "../../testdata/employees.csv"

	// ============================================================
	// 1. ParallelApply — process multiple DataFrames concurrently
	// ============================================================
	fmt.Println("=== ParallelApply: Multiple DataFrames ===")

	// Create several DataFrames with different filters
	var dfs []*duckframe.DataFrame
	filters := []string{
		"country = 'Brazil'",
		"country = 'USA'",
		"country = 'Germany'",
	}
	for _, f := range filters {
		df, err := duckframe.ReadCSV(db, csvPath)
		if err != nil {
			log.Fatal(err)
		}
		filtered, err := df.Filter(f)
		if err != nil {
			log.Fatal(err)
		}
		dfs = append(dfs, filtered)
		_ = df.Close()
	}

	// Apply sort+limit in parallel to all DataFrames
	start := time.Now()
	results, err := duckframe.ParallelApply(dfs, func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		return df.Sort("salary", false)
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("ParallelApply completed in %v\n\n", time.Since(start))

	for i, r := range results {
		fmt.Printf("--- %s ---\n", filters[i])
		r.Show()
		_ = r.Close()
		fmt.Println()
	}

	for _, df := range dfs {
		_ = df.Close()
	}

	// ============================================================
	// 2. ReadCSVChunked — stream large CSV in chunks
	// ============================================================
	fmt.Println("=== ReadCSVChunked: Streaming ===")

	ctx := context.Background()
	ch := duckframe.ReadCSVChunked(ctx, db, csvPath, 3)

	totalRows := 0
	for chunk := range ch {
		if chunk.Err != nil {
			log.Fatal(chunk.Err)
		}
		r, _, _ := chunk.DataFrame.Shape()
		fmt.Printf("Chunk %d: %d rows\n", chunk.Index, r)
		totalRows += r
		_ = chunk.DataFrame.Close()
	}
	fmt.Printf("Total rows across chunks: %d\n", totalRows)

	// ============================================================
	// 3. Context-aware operations — with timeout
	// ============================================================
	fmt.Println("\n=== Context-aware operations ===")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	df, err := duckframe.ReadCSVContext(ctx, db, csvPath)
	if err != nil {
		log.Fatal(err)
	}
	defer df.Close()

	filtered, err := df.FilterContext(ctx, "age > 28")
	if err != nil {
		log.Fatal(err)
	}
	defer filtered.Close()

	sorted, err := filtered.SortContext(ctx, "salary", false)
	if err != nil {
		log.Fatal(err)
	}
	defer sorted.Close()

	fmt.Println("Context-aware pipeline result:")
	sorted.Show()

	fmt.Println("\nConcurrent processing complete!")
}
