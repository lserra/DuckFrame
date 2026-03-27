// Example: Exploratory data analysis
//
// This example demonstrates how to explore a dataset using
// DuckFrame — inspecting shape, types, statistics, sorting,
// grouping, and custom SQL queries.
//
// Usage:
//
//	cd examples/analysis && go run main.go
package main

import (
	"fmt"
	"log"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func main() {
	db, err := engine.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Load data
	df, err := duckframe.ReadCSV(db, "../../testdata/employees.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer df.Close()

	// 1. Shape
	rows, cols, _ := df.Shape()
	fmt.Printf("=== Dataset: %d rows x %d cols ===\n", rows, cols)

	// 2. Columns and types
	fmt.Println("\n=== Columns ===")
	fmt.Println(df.Columns())

	fmt.Println("\n=== Data Types ===")
	dtypes, _ := df.Dtypes()
	for col, dtype := range dtypes {
		fmt.Printf("  %-10s %s\n", col, dtype)
	}

	// 3. Preview
	fmt.Println("\n=== Head (3 rows) ===")
	head, _ := df.Head(3)
	defer head.Close()
	head.Show()

	fmt.Println("\n=== Tail (3 rows) ===")
	tail, _ := df.Tail(3)
	defer tail.Close()
	tail.Show()

	// 4. Descriptive statistics
	fmt.Println("\n=== Describe (numeric columns) ===")
	stats, _ := df.Describe()
	defer stats.Close()
	stats.Show()

	// 5. Unique values
	fmt.Println("\n=== Distinct countries ===")
	countries, _ := df.Select("country")
	defer countries.Close()
	unique, _ := countries.Distinct()
	defer unique.Close()
	unique.Show()

	// 6. Salary analysis by country
	fmt.Println("\n=== Average salary by country ===")
	avgByCountry, _ := df.GroupBy("country").Agg("salary", "mean")
	defer avgByCountry.Close()
	sorted, _ := avgByCountry.Sort("mean(salary)", false)
	defer sorted.Close()
	sorted.Show()

	// 7. Top earners
	fmt.Println("\n=== Top 3 earners ===")
	top, _ := df.Sort("salary", false)
	defer top.Close()
	top3, _ := top.Head(3)
	defer top3.Close()
	top3.Show()

	// 8. Custom analysis with SQL
	fmt.Println("\n=== Age distribution (SQL) ===")
	ageDist, _ := df.Sql(`
		SELECT
			CASE
				WHEN age < 30 THEN 'Under 30'
				WHEN age < 35 THEN '30-34'
				ELSE '35+'
			END AS age_group,
			COUNT(*) AS count,
			ROUND(AVG(salary), 2) AS avg_salary
		FROM {df}
		GROUP BY age_group
		ORDER BY age_group
	`)
	defer ageDist.Close()
	ageDist.Show()

	fmt.Println("\nAnalysis complete!")
}
