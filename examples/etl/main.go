// Example: ETL pipeline — CSV → Filter → Parquet
//
// This example reads a CSV file, applies transformations,
// and writes the result to Parquet format.
//
// Usage:
//
//	cd examples/etl && go run main.go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func main() {
	db, err := engine.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 1. Extract: read CSV
	fmt.Println("=== Extract: Reading CSV ===")
	df, err := duckframe.ReadCSV(db, "../../testdata/employees.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer df.Close()

	r, c, _ := df.Shape()
	fmt.Printf("Loaded %d rows x %d cols\n", r, c)
	df.Show()

	// 2. Transform: filter high earners and add bonus column
	fmt.Println("\n=== Transform: Filter salary > 75000 + Add bonus ===")
	filtered, err := df.Filter("salary > 75000")
	if err != nil {
		log.Fatal(err)
	}
	defer filtered.Close()

	withBonus, err := filtered.WithColumn("bonus", "salary * 0.10")
	if err != nil {
		log.Fatal(err)
	}
	defer withBonus.Close()

	withBonus.Show()

	// 3. Load: write to Parquet
	outPath := "/tmp/high_earners.parquet"
	fmt.Printf("\n=== Load: Writing to %s ===\n", outPath)
	if err := withBonus.WriteParquet(outPath); err != nil {
		log.Fatal(err)
	}

	// 4. Verify: read back from Parquet
	fmt.Println("\n=== Verify: Reading back from Parquet ===")
	verify, err := duckframe.ReadParquet(db, outPath)
	if err != nil {
		log.Fatal(err)
	}
	defer verify.Close()

	vr, vc, _ := verify.Shape()
	fmt.Printf("Parquet contains %d rows x %d cols\n", vr, vc)
	verify.Show()

	// Cleanup
	os.Remove(outPath)
	fmt.Println("\nETL pipeline complete!")
}
