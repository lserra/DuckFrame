package duckframe_test

import (
	"fmt"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func ExampleNew() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.New(db, []string{"name", "age"}, []map[string]interface{}{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob", "age": int64(25)},
	})
	defer df.Close()

	r, c, _ := df.Shape()
	fmt.Printf("Shape: %d rows x %d cols\n", r, c)
	// Output: Shape: 2 rows x 2 cols
}

func ExampleReadCSV() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, "testdata/employees.csv")
	defer df.Close()

	r, c, _ := df.Shape()
	fmt.Printf("Employees: %d rows x %d cols\n", r, c)
	// Output: Employees: 7 rows x 4 cols
}

func ExampleDataFrame_Filter() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, "testdata/employees.csv")
	defer df.Close()

	filtered, _ := df.Filter("salary > 90000")
	defer filtered.Close()

	r, _, _ := filtered.Shape()
	fmt.Printf("High salary: %d employees\n", r)
	// Output: High salary: 3 employees
}

func ExampleDataFrame_Sort() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.FromQuery(db,
		"SELECT 'Alice' AS name, 85000 AS salary UNION ALL SELECT 'Bob', 72000 UNION ALL SELECT 'Carol', 95000")
	defer df.Close()

	sorted, _ := df.Sort("salary", false)
	defer sorted.Close()

	rows, _ := sorted.Collect()
	for _, row := range rows {
		fmt.Printf("%s: %v\n", row["name"], row["salary"])
	}
	// Output:
	// Carol: 95000
	// Alice: 85000
	// Bob: 72000
}

func ExampleDataFrame_GroupBy() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, "testdata/employees.csv")
	defer df.Close()

	grouped, _ := df.GroupBy("country").Agg("salary", "count")
	defer grouped.Close()

	r, _, _ := grouped.Shape()
	fmt.Printf("Countries: %d\n", r)
	// Output: Countries: 3
}

func ExampleDataFrame_Join() {
	db, _ := engine.Open("")
	defer db.Close()

	employees, _ := duckframe.FromQuery(db,
		"SELECT 'Alice' AS name, 'Eng' AS dept UNION ALL SELECT 'Bob', 'Sales'")
	defer employees.Close()

	depts, _ := duckframe.FromQuery(db,
		"SELECT 'Eng' AS dept, 100000 AS budget UNION ALL SELECT 'Sales', 50000")
	defer depts.Close()

	joined, _ := employees.Join(depts, "dept", "inner")
	defer joined.Close()

	r, c, _ := joined.Shape()
	fmt.Printf("Joined: %d rows x %d cols\n", r, c)
	// Output: Joined: 2 rows x 3 cols
}

func ExampleDataFrame_Describe() {
	db, _ := engine.Open("")
	defer db.Close()

	df, _ := duckframe.FromQuery(db,
		"SELECT 10 AS val UNION ALL SELECT 20 UNION ALL SELECT 30")
	defer df.Close()

	desc, _ := df.Describe()
	defer desc.Close()

	cols := desc.Columns()
	fmt.Printf("Stats columns: %v\n", cols)
	// Output: Stats columns: [column count mean std min max]
}
