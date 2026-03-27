package duckframe_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

func openTestDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open("")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	return db
}

func TestNewDataFrame(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	columns := []string{"name", "age", "salary"}
	rows := []map[string]interface{}{
		{"name": "Alice", "age": int64(30), "salary": 85000.0},
		{"name": "Bob", "age": int64(25), "salary": 72000.0},
		{"name": "Carol", "age": int64(35), "salary": 95000.0},
	}

	df, err := duckframe.New(db, columns, rows)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	if df.TableName() == "" {
		t.Fatal("expected non-empty table name")
	}

	gotCols := df.Columns()
	if len(gotCols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(gotCols))
	}

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 3 || c != 3 {
		t.Fatalf("expected shape (3, 3), got (%d, %d)", r, c)
	}
}

func TestNewDataFrameEmpty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	columns := []string{"id", "value"}
	df, err := duckframe.New(db, columns, nil)
	if err != nil {
		t.Fatalf("failed to create empty DataFrame: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 0 || c != 2 {
		t.Fatalf("expected shape (0, 2), got (%d, %d)", r, c)
	}
}

func TestNewDataFrameNoColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := duckframe.New(db, []string{}, nil)
	if err == nil {
		t.Fatal("expected error for empty columns")
	}
}

func TestFromQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn := db.Conn()
	conn.Exec("CREATE TEMPORARY TABLE source (id INTEGER, name VARCHAR, score DOUBLE)")
	conn.Exec("INSERT INTO source VALUES (1, 'Alice', 9.5), (2, 'Bob', 8.0), (3, 'Carol', 9.8)")

	df, err := duckframe.FromQuery(db, "SELECT * FROM source WHERE score > 9.0")
	if err != nil {
		t.Fatalf("failed to create DataFrame from query: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows, got %d", r)
	}
	if c != 3 {
		t.Fatalf("expected 3 columns, got %d", c)
	}
}

func TestDataFrameColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	columns := []string{"a", "b", "c"}
	df, err := duckframe.New(db, columns, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	got := df.Columns()
	for i, col := range columns {
		if got[i] != col {
			t.Fatalf("column %d: expected %q, got %q", i, col, got[i])
		}
	}

	got[0] = "modified"
	if df.Columns()[0] == "modified" {
		t.Fatal("Columns() should return a copy")
	}
}

func TestDataFrameClose(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"x"}, []map[string]interface{}{
		{"x": int64(1)},
	})
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}

	tableName := df.TableName()

	err = df.Close()
	if err != nil {
		t.Fatalf("failed to close DataFrame: %v", err)
	}

	var count int
	err = db.Conn().QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", tableName).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query information_schema: %v", err)
	}
	if count != 0 {
		t.Fatal("expected table to be dropped after Close()")
	}
}

func TestDataFrameEngine(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"id"}, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	if df.Engine() != db {
		t.Fatal("Engine() should return the same DB instance")
	}
}

func TestDataFrameTypeInference(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	columns := []string{"int_col", "float_col", "bool_col", "str_col"}
	rows := []map[string]interface{}{
		{"int_col": int64(42), "float_col": 3.14, "bool_col": true, "str_col": "hello"},
	}

	df, err := duckframe.New(db, columns, rows)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 1 || c != 4 {
		t.Fatalf("expected shape (1, 4), got (%d, %d)", r, c)
	}
}

// ---------------------------------------------------------------------------
// Phase 2 — MVP Operations Tests
// ---------------------------------------------------------------------------

func testdataPath(name string) string {
	wd, _ := os.Getwd()
	return filepath.Join(wd, "testdata", name)
}

func TestReadCSV(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 7 {
		t.Fatalf("expected 7 rows, got %d", r)
	}
	if c != 4 {
		t.Fatalf("expected 4 columns, got %d", c)
	}

	cols := df.Columns()
	expected := []string{"name", "age", "country", "salary"}
	for i, col := range expected {
		if cols[i] != col {
			t.Fatalf("column %d: expected %q, got %q", i, col, cols[i])
		}
	}
}

func TestReadCSVNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := duckframe.ReadCSV(db, "/nonexistent/file.csv")
	if err == nil {
		t.Fatal("expected error for non-existent CSV")
	}
}

func TestSelect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	selected, err := df.Select("name", "salary")
	if err != nil {
		t.Fatalf("failed to Select: %v", err)
	}
	defer selected.Close()

	r, c, err := selected.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 7 {
		t.Fatalf("expected 7 rows, got %d", r)
	}
	if c != 2 {
		t.Fatalf("expected 2 columns, got %d", c)
	}

	cols := selected.Columns()
	if cols[0] != "name" || cols[1] != "salary" {
		t.Fatalf("expected columns [name, salary], got %v", cols)
	}
}

func TestSelectNoColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"a"}, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	_, err = df.Select()
	if err == nil {
		t.Fatal("expected error for Select with no columns")
	}
}

func TestFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("age > 30")
	if err != nil {
		t.Fatalf("failed to Filter: %v", err)
	}
	defer filtered.Close()

	r, _, err := filtered.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	// Carol(35), Eve(32), Frank(40) = 3 rows
	if r != 3 {
		t.Fatalf("expected 3 rows with age > 30, got %d", r)
	}
}

func TestFilterString(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("country = 'Brazil'")
	if err != nil {
		t.Fatalf("failed to Filter: %v", err)
	}
	defer filtered.Close()

	r, _, err := filtered.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	// Alice, Carol, Frank = 3
	if r != 3 {
		t.Fatalf("expected 3 rows for Brazil, got %d", r)
	}
}

func TestGroupByAgg(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	result, err := df.GroupBy("country").Agg("salary", "mean")
	if err != nil {
		t.Fatalf("failed to GroupBy.Agg: %v", err)
	}
	defer result.Close()

	r, c, err := result.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	// 3 countries: Brazil, USA, Germany
	if r != 3 {
		t.Fatalf("expected 3 groups, got %d", r)
	}
	if c != 2 {
		t.Fatalf("expected 2 columns, got %d", c)
	}
}

func TestGroupByAggSum(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	result, err := df.GroupBy("country").Agg("salary", "sum")
	if err != nil {
		t.Fatalf("failed to GroupBy.Agg sum: %v", err)
	}
	defer result.Close()

	r, _, err := result.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 groups, got %d", r)
	}
}

func TestGroupByAggCount(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	result, err := df.GroupBy("country").Agg("name", "count")
	if err != nil {
		t.Fatalf("failed to GroupBy.Agg count: %v", err)
	}
	defer result.Close()

	r, _, err := result.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 groups, got %d", r)
	}
}

func TestGroupByAggInvalidFn(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"a", "b"}, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	_, err = df.GroupBy("a").Agg("b", "invalid_fn")
	if err == nil {
		t.Fatal("expected error for invalid aggregation function")
	}
}

func TestShow(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Show should not return error
	err = df.Show()
	if err != nil {
		t.Fatalf("Show failed: %v", err)
	}
}

func TestShowWithLimit(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	err = df.Show(3)
	if err != nil {
		t.Fatalf("Show with limit failed: %v", err)
	}
}

func TestSql(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	result, err := df.Sql("SELECT country, AVG(salary) AS avg_salary FROM {df} GROUP BY country ORDER BY avg_salary DESC")
	if err != nil {
		t.Fatalf("Sql failed: %v", err)
	}
	defer result.Close()

	r, c, err := result.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
	if c != 2 {
		t.Fatalf("expected 2 columns, got %d", c)
	}
}

func TestChainedOperations(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Filter then Select
	filtered, err := df.Filter("age > 28")
	if err != nil {
		t.Fatalf("failed to Filter: %v", err)
	}
	defer filtered.Close()

	selected, err := filtered.Select("name", "salary")
	if err != nil {
		t.Fatalf("failed to Select: %v", err)
	}
	defer selected.Close()

	r, c, err := selected.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	// Alice(30), Carol(35), Eve(32), Frank(40) = 4
	if r != 4 {
		t.Fatalf("expected 4 rows, got %d", r)
	}
	if c != 2 {
		t.Fatalf("expected 2 columns, got %d", c)
	}
}

// ---------------------------------------------------------------------------
// Phase 3 — Collect, ToSlice, Fluent API Tests
// ---------------------------------------------------------------------------

func TestCollect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	rows, err := df.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(rows) != 7 {
		t.Fatalf("expected 7 rows, got %d", len(rows))
	}

	// Check first row has expected keys
	first := rows[0]
	for _, key := range []string{"name", "age", "country", "salary"} {
		if _, ok := first[key]; !ok {
			t.Fatalf("expected key %q in row, got %v", key, first)
		}
	}
}

func TestCollectEmpty(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"a", "b"}, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	rows, err := df.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
}

func TestCollectAfterFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("country = 'Brazil'")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	rows, err := filtered.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for Brazil, got %d", len(rows))
	}
}

type Employee struct {
	Name    string  `df:"name"`
	Age     int64   `df:"age"`
	Country string  `df:"country"`
	Salary  float64 `df:"salary"`
}

func TestToSlice(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	var employees []Employee
	err = df.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	if len(employees) != 7 {
		t.Fatalf("expected 7 employees, got %d", len(employees))
	}

	// Check first employee
	if employees[0].Name == "" {
		t.Fatal("expected non-empty name")
	}
	if employees[0].Age == 0 {
		t.Fatal("expected non-zero age")
	}
	if employees[0].Country == "" {
		t.Fatal("expected non-empty country")
	}
	if employees[0].Salary == 0 {
		t.Fatal("expected non-zero salary")
	}
}

func TestToSliceFiltered(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("age > 30")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	var employees []Employee
	err = filtered.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	if len(employees) != 3 {
		t.Fatalf("expected 3 employees with age > 30, got %d", len(employees))
	}

	for _, emp := range employees {
		if emp.Age <= 30 {
			t.Fatalf("expected age > 30, got %d for %s", emp.Age, emp.Name)
		}
	}
}

type PartialEmployee struct {
	Name   string  `df:"name"`
	Salary float64 `df:"salary"`
}

func TestToSlicePartialStruct(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// ToSlice with a struct that has fewer fields than columns
	var employees []PartialEmployee
	err = df.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice with partial struct failed: %v", err)
	}

	if len(employees) != 7 {
		t.Fatalf("expected 7 employees, got %d", len(employees))
	}

	if employees[0].Name == "" || employees[0].Salary == 0 {
		t.Fatal("expected non-empty partial fields")
	}
}

func TestToSliceInvalidDest(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.New(db, []string{"a"}, nil)
	if err != nil {
		t.Fatalf("failed to create DataFrame: %v", err)
	}
	defer df.Close()

	// Not a pointer
	var s []Employee
	err = df.ToSlice(s)
	if err == nil {
		t.Fatal("expected error for non-pointer dest")
	}

	// Pointer to non-slice
	var x int
	err = df.ToSlice(&x)
	if err == nil {
		t.Fatal("expected error for pointer to non-slice")
	}
}

func TestErrorPropagation(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Force an error by filtering with invalid expression
	badDf, _ := df.Filter("INVALID SYNTAX !!!")

	// Error should propagate through chained operations
	if badDf.Err() == nil {
		t.Fatal("expected error on bad filter")
	}

	// Collect on error DataFrame should return error
	_, err = badDf.Collect()
	if err == nil {
		t.Fatal("expected error on Collect after bad filter")
	}

	// Show on error DataFrame should return error
	err = badDf.Show()
	if err == nil {
		t.Fatal("expected error on Show after bad filter")
	}

	// Shape on error DataFrame should return error
	_, _, err = badDf.Shape()
	if err == nil {
		t.Fatal("expected error on Shape after bad filter")
	}
}

func TestFluentChain(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Chain: Filter -> Select -> Collect
	filtered, err := df.Filter("age > 28")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	selected, err := filtered.Select("name", "salary")
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	defer selected.Close()

	rows, err := selected.Collect()
	if err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Alice(30), Carol(35), Eve(32), Frank(40) = 4
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	// Should have only name and salary columns
	for _, row := range rows {
		if _, ok := row["name"]; !ok {
			t.Fatal("expected name column in result")
		}
		if _, ok := row["salary"]; !ok {
			t.Fatal("expected salary column in result")
		}
		if _, ok := row["age"]; ok {
			t.Fatal("unexpected age column in result")
		}
	}
}

func TestFluentChainToSlice(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("country = 'Germany'")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	var employees []Employee
	err = filtered.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	if len(employees) != 2 {
		t.Fatalf("expected 2 German employees, got %d", len(employees))
	}

	for _, emp := range employees {
		if emp.Country != "Germany" {
			t.Fatalf("expected country Germany, got %s", emp.Country)
		}
	}
}
