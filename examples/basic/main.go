package main

import (
	"fmt"
	"log"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func main() {
	// 1. Open DuckDB in-memory
	db, err := engine.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 2. Read CSV
	fmt.Println("=== Reading CSV ===")
	df, err := duckframe.ReadCSV(db, "testdata/employees.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer df.Close()

	df.Show()

	// 3. Filter: age > 30
	fmt.Println("\n=== Filter: age > 30 ===")
	filtered, err := df.Filter("age > 30")
	if err != nil {
		log.Fatal(err)
	}
	defer filtered.Close()

	filtered.Show()

	// 4. Select specific columns
	fmt.Println("\n=== Select: name, salary ===")
	selected, err := df.Select("name", "salary")
	if err != nil {
		log.Fatal(err)
	}
	defer selected.Close()

	selected.Show()

	// 5. GroupBy country + Avg salary
	fmt.Println("\n=== GroupBy country, Avg salary ===")
	grouped, err := df.GroupBy("country").Agg("salary", "mean")
	if err != nil {
		log.Fatal(err)
	}
	defer grouped.Close()

	grouped.Show()

	// 6. Raw SQL with {df} placeholder
	fmt.Println("\n=== SQL: Top earners by country ===")
	result, err := df.Sql(`
		SELECT country, name, salary
		FROM {df}
		WHERE salary > 80000
		ORDER BY salary DESC
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()

	result.Show()

	// 7. Chained operations: Filter -> Select
	fmt.Println("\n=== Chained: Filter(age >= 30) -> Select(name, country) ===")
	chained, err := df.Filter("age >= 30")
	if err != nil {
		log.Fatal(err)
	}
	defer chained.Close()

	final, err := chained.Select("name", "country")
	if err != nil {
		log.Fatal(err)
	}
	defer final.Close()

	final.Show()

	fmt.Println("\nDone!")
}
