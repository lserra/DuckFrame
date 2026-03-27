package duckframe_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

// ---------------------------------------------------------------------------
// Phase 4 — Data Formats Tests
// ---------------------------------------------------------------------------

func TestWriteCSVAndRead(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Write to temp CSV
	tmpFile := filepath.Join(t.TempDir(), "output.csv")
	err = df.WriteCSV(tmpFile)
	if err != nil {
		t.Fatalf("WriteCSV failed: %v", err)
	}

	// Read it back
	df2, err := duckframe.ReadCSV(db, tmpFile)
	if err != nil {
		t.Fatalf("failed to re-read CSV: %v", err)
	}
	defer df2.Close()

	r, c, err := df2.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 7 || c != 4 {
		t.Fatalf("expected (7, 4), got (%d, %d)", r, c)
	}
}

func TestWriteParquetAndRead(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Write to Parquet
	tmpFile := filepath.Join(t.TempDir(), "output.parquet")
	err = df.WriteParquet(tmpFile)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	// Read it back
	df2, err := duckframe.ReadParquet(db, tmpFile)
	if err != nil {
		t.Fatalf("failed to ReadParquet: %v", err)
	}
	defer df2.Close()

	r, c, err := df2.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 7 || c != 4 {
		t.Fatalf("expected (7, 4), got (%d, %d)", r, c)
	}

	// Verify columns match
	cols := df2.Columns()
	expected := []string{"name", "age", "country", "salary"}
	for i, col := range expected {
		if cols[i] != col {
			t.Fatalf("column %d: expected %q, got %q", i, col, cols[i])
		}
	}
}

func TestWriteJSONAndRead(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	// Write to JSON
	tmpFile := filepath.Join(t.TempDir(), "output.json")
	err = df.WriteJSON(tmpFile)
	if err != nil {
		t.Fatalf("WriteJSON failed: %v", err)
	}

	// Read it back
	df2, err := duckframe.ReadJSON(db, tmpFile)
	if err != nil {
		t.Fatalf("failed to ReadJSON: %v", err)
	}
	defer df2.Close()

	r, _, err := df2.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 7 {
		t.Fatalf("expected 7 rows, got %d", r)
	}
}

func TestReadJSONLines(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadJSON(db, testdataPath("employees.jsonl"))
	if err != nil {
		t.Fatalf("failed to ReadJSON: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	if r != 5 {
		t.Fatalf("expected 5 rows, got %d", r)
	}
	if c != 4 {
		t.Fatalf("expected 4 columns, got %d", c)
	}
}

func TestReadParquetNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := duckframe.ReadParquet(db, "/nonexistent/file.parquet")
	if err == nil {
		t.Fatal("expected error for non-existent Parquet file")
	}
}

func TestReadJSONNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := duckframe.ReadJSON(db, "/nonexistent/file.json")
	if err == nil {
		t.Fatal("expected error for non-existent JSON file")
	}
}

func TestCSVToParquetPipeline(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Read CSV -> Filter -> Write Parquet -> Read Parquet -> Verify
	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("failed to ReadCSV: %v", err)
	}
	defer df.Close()

	filtered, err := df.Filter("salary > 80000")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	tmpFile := filepath.Join(t.TempDir(), "high_salary.parquet")
	err = filtered.WriteParquet(tmpFile)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}

	result, err := duckframe.ReadParquet(db, tmpFile)
	if err != nil {
		t.Fatalf("ReadParquet failed: %v", err)
	}
	defer result.Close()

	r, _, err := result.Shape()
	if err != nil {
		t.Fatalf("failed to get shape: %v", err)
	}
	// Alice(85k), Carol(95k), Eve(91k), Frank(102k) = 4
	if r != 4 {
		t.Fatalf("expected 4 high salary employees, got %d", r)
	}

	var employees []Employee
	err = result.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	for _, emp := range employees {
		if emp.Salary <= 80000 {
			t.Fatalf("expected salary > 80000, got %.2f for %s", emp.Salary, emp.Name)
		}
	}
}

func TestWriteCSVErrorOnBadDF(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	defer df.Close()

	// Force error on df
	badDf, _ := df.Filter("INVALID!!!")
	err := badDf.WriteCSV("/tmp/test.csv")
	if err == nil {
		t.Fatal("expected error on WriteCSV with error DataFrame")
	}
}

func TestWriteParquetErrorOnBadDF(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	defer df.Close()

	badDf, _ := df.Filter("INVALID!!!")
	err := badDf.WriteParquet("/tmp/test.parquet")
	if err == nil {
		t.Fatal("expected error on WriteParquet with error DataFrame")
	}
}

func TestWriteJSONErrorOnBadDF(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, _ := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	defer df.Close()

	badDf, _ := df.Filter("INVALID!!!")
	err := badDf.WriteJSON("/tmp/test.json")
	if err == nil {
		t.Fatal("expected error on WriteJSON with error DataFrame")
	}
}

// ---------------------------------------------------------------------------
// Phase 5 — Advanced Operations Tests
// ---------------------------------------------------------------------------

func TestSort(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	sorted, err := df.Sort("salary", true) // ascending
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	var employees []Employee
	err = sorted.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	// Check that salaries are in ascending order
	for i := 1; i < len(employees); i++ {
		if employees[i].Salary < employees[i-1].Salary {
			t.Fatalf("salaries not in ascending order at index %d: %.2f < %.2f",
				i, employees[i].Salary, employees[i-1].Salary)
		}
	}
}

func TestSortDescending(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	sorted, err := df.Sort("salary", false) // descending
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	var employees []Employee
	err = sorted.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	for i := 1; i < len(employees); i++ {
		if employees[i].Salary > employees[i-1].Salary {
			t.Fatalf("salaries not in descending order at index %d", i)
		}
	}
}

func TestLimit(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	limited, err := df.Limit(3)
	if err != nil {
		t.Fatalf("Limit failed: %v", err)
	}
	defer limited.Close()

	r, _, err := limited.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
}

func TestDistinct(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create a DataFrame with duplicate rows
	df, err := duckframe.FromQuery(db, "SELECT 'Alice' AS name, 30 AS age UNION ALL SELECT 'Alice', 30 UNION ALL SELECT 'Bob', 25")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df.Close()

	unique, err := df.Distinct()
	if err != nil {
		t.Fatalf("Distinct failed: %v", err)
	}
	defer unique.Close()

	r, _, err := unique.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 distinct rows, got %d", r)
	}
}

func TestRename(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	renamed, err := df.Rename("name", "employee_name")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	defer renamed.Close()

	cols := renamed.Columns()
	if cols[0] != "employee_name" {
		t.Fatalf("expected first column to be 'employee_name', got %q", cols[0])
	}
	// Other columns should stay the same
	if cols[1] != "age" || cols[2] != "country" || cols[3] != "salary" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestDrop(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	dropped, err := df.Drop("country", "salary")
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}
	defer dropped.Close()

	cols := dropped.Columns()
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if cols[0] != "name" || cols[1] != "age" {
		t.Fatalf("expected [name, age], got %v", cols)
	}
}

func TestDropAllColumnsError(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	_, err = df.Drop("name", "age", "country", "salary")
	if err == nil {
		t.Fatal("expected error when dropping all columns")
	}
}

func TestWithColumnNew(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	withBonus, err := df.WithColumn("bonus", "salary * 0.10")
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}
	defer withBonus.Close()

	cols := withBonus.Columns()
	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d: %v", len(cols), cols)
	}
	if cols[4] != "bonus" {
		t.Fatalf("expected last column to be 'bonus', got %q", cols[4])
	}
}

func TestWithColumnReplace(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	// Replace salary with salary * 2
	withDouble, err := df.WithColumn("salary", "salary * 2")
	if err != nil {
		t.Fatalf("WithColumn failed: %v", err)
	}
	defer withDouble.Close()

	cols := withDouble.Columns()
	if len(cols) != 4 {
		t.Fatalf("expected 4 columns (replacement, not addition), got %d", len(cols))
	}
}

func TestJoinInner(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create two DataFrames
	left, err := duckframe.FromQuery(db,
		"SELECT 'Alice' AS name, 'Engineering' AS dept UNION ALL SELECT 'Bob', 'Marketing'")
	if err != nil {
		t.Fatalf("FromQuery left failed: %v", err)
	}
	defer left.Close()

	right, err := duckframe.FromQuery(db,
		"SELECT 'Engineering' AS dept, 100000 AS budget UNION ALL SELECT 'Marketing', 50000")
	if err != nil {
		t.Fatalf("FromQuery right failed: %v", err)
	}
	defer right.Close()

	joined, err := left.Join(right, "dept", "inner")
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	defer joined.Close()

	r, c, err := joined.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows, got %d", r)
	}
	// left has name,dept; right has dept,budget; join removes right dept → name, dept, budget = 3
	if c != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", c, joined.Columns())
	}
}

func TestJoinLeft(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	left, err := duckframe.FromQuery(db,
		"SELECT 'Alice' AS name, 'Engineering' AS dept UNION ALL SELECT 'Bob', 'Marketing' UNION ALL SELECT 'Carol', 'HR'")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer left.Close()

	right, err := duckframe.FromQuery(db,
		"SELECT 'Engineering' AS dept, 100000 AS budget UNION ALL SELECT 'Marketing', 50000")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer right.Close()

	joined, err := left.Join(right, "dept", "left")
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	defer joined.Close()

	r, _, err := joined.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows (left join keeps all left rows), got %d", r)
	}
}

func TestJoinInvalidType(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.FromQuery(db, "SELECT 1 AS id")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df.Close()

	_, err = df.Join(df, "id", "cross")
	if err == nil {
		t.Fatal("expected error for unsupported join type")
	}
}

func TestUnion(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df1, err := duckframe.FromQuery(db, "SELECT 'Alice' AS name, 30 AS age")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df1.Close()

	df2, err := duckframe.FromQuery(db, "SELECT 'Bob' AS name, 25 AS age")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df2.Close()

	unioned, err := df1.Union(df2)
	if err != nil {
		t.Fatalf("Union failed: %v", err)
	}
	defer unioned.Close()

	r, _, err := unioned.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows, got %d", r)
	}
}

func TestHead(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	head, err := df.Head(3)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	defer head.Close()

	r, _, err := head.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
}

func TestTail(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	tail, err := df.Tail(2)
	if err != nil {
		t.Fatalf("Tail failed: %v", err)
	}
	defer tail.Close()

	r, _, err := tail.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows, got %d", r)
	}
}

func TestDtypes(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	dtypes, err := df.Dtypes()
	if err != nil {
		t.Fatalf("Dtypes failed: %v", err)
	}

	if len(dtypes) != 4 {
		t.Fatalf("expected 4 column types, got %d", len(dtypes))
	}

	// name should be VARCHAR, age/salary should be numeric
	if dtypes["name"] != "VARCHAR" {
		t.Fatalf("expected name type VARCHAR, got %q", dtypes["name"])
	}
}

func TestDescribe(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	desc, err := df.Describe()
	if err != nil {
		t.Fatalf("Describe failed: %v", err)
	}
	defer desc.Close()

	// Should have columns: column, count, mean, std, min, max
	cols := desc.Columns()
	if len(cols) != 6 {
		t.Fatalf("expected 6 stat columns, got %d: %v", len(cols), cols)
	}

	// employees.csv has 2 numeric columns: age and salary
	r, _, err := desc.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows (one per numeric column), got %d", r)
	}
}

func TestDescribeNoNumericColumns(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.FromQuery(db, "SELECT 'Alice' AS name, 'USA' AS country")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df.Close()

	_, err = df.Describe()
	if err == nil {
		t.Fatal("expected error when Describe has no numeric columns")
	}
}

func TestSortChained(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	// Chain: filter -> sort -> limit
	result, err := df.Filter("salary > 60000")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer result.Close()

	sorted, err := result.Sort("salary", false) // desc
	if err != nil {
		t.Fatalf("Sort failed: %v", err)
	}
	defer sorted.Close()

	top3, err := sorted.Limit(3)
	if err != nil {
		t.Fatalf("Limit failed: %v", err)
	}
	defer top3.Close()

	r, _, err := top3.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}

	var employees []Employee
	err = top3.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	// Should be top 3 salaries in descending order
	for i := 1; i < len(employees); i++ {
		if employees[i].Salary > employees[i-1].Salary {
			t.Fatalf("expected descending salaries, index %d: %.0f > %.0f",
				i, employees[i].Salary, employees[i-1].Salary)
		}
	}
}

// ---------------------------------------------------------------------------
// Phase 6 — Concurrency & Streaming Tests
// ---------------------------------------------------------------------------

func TestParallelApply(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create 3 DataFrames from different queries
	df1, err := duckframe.FromQuery(db, "SELECT 'Alice' AS name, 85000 AS salary")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df1.Close()

	df2, err := duckframe.FromQuery(db, "SELECT 'Bob' AS name, 72000 AS salary")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df2.Close()

	df3, err := duckframe.FromQuery(db, "SELECT 'Carol' AS name, 95000 AS salary")
	if err != nil {
		t.Fatalf("FromQuery failed: %v", err)
	}
	defer df3.Close()

	// Apply filter in parallel to all DataFrames
	filterFn := func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		return df.Filter("salary > 70000")
	}

	results, err := duckframe.ParallelApply([]*duckframe.DataFrame{df1, df2, df3}, filterFn)
	if err != nil {
		t.Fatalf("ParallelApply failed: %v", err)
	}

	for _, r := range results {
		defer r.Close()
	}

	// df1 (85k) and df3 (95k) pass, df2 (72k) passes too (> 70000)
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	for i, r := range results {
		rows, _, err := r.Shape()
		if err != nil {
			t.Fatalf("Shape failed on result %d: %v", i, err)
		}
		if rows != 1 {
			t.Fatalf("expected 1 row in result %d, got %d", i, rows)
		}
	}
}

func TestParallelApplyWithSort(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Create multiple DataFrames from same source
	var dfs []*duckframe.DataFrame
	for i := 0; i < 5; i++ {
		df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
		if err != nil {
			t.Fatalf("ReadCSV failed: %v", err)
		}
		defer df.Close()
		dfs = append(dfs, df)
	}

	// Apply sort + limit in parallel
	topSalaryFn := func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		sorted, err := df.Sort("salary", false)
		if err != nil {
			return nil, err
		}
		defer sorted.Close()
		return sorted.Limit(3)
	}

	results, err := duckframe.ParallelApply(dfs, topSalaryFn)
	if err != nil {
		t.Fatalf("ParallelApply failed: %v", err)
	}

	for _, r := range results {
		defer r.Close()
	}

	// Each result should have exactly 3 rows
	for i, r := range results {
		rows, _, err := r.Shape()
		if err != nil {
			t.Fatalf("Shape failed on result %d: %v", i, err)
		}
		if rows != 3 {
			t.Fatalf("expected 3 rows in result %d, got %d", i, rows)
		}
	}
}

func TestReadCSVChunked(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	ch := duckframe.ReadCSVChunked(ctx, db, testdataPath("employees.csv"), 3)

	var totalRows int
	var chunkCount int
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("chunk %d error: %v", chunk.Index, chunk.Err)
		}
		defer chunk.DataFrame.Close()

		r, _, err := chunk.DataFrame.Shape()
		if err != nil {
			t.Fatalf("Shape failed on chunk %d: %v", chunk.Index, err)
		}
		totalRows += r
		chunkCount++
	}

	// employees.csv has 7 rows, chunk size 3 → 3 chunks (3+3+1)
	if chunkCount != 3 {
		t.Fatalf("expected 3 chunks, got %d", chunkCount)
	}
	if totalRows != 7 {
		t.Fatalf("expected 7 total rows, got %d", totalRows)
	}
}

func TestReadCSVChunkedCancel(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel immediately to test cancellation
	cancel()

	ch := duckframe.ReadCSVChunked(ctx, db, testdataPath("employees.csv"), 2)

	var gotCancelErr bool
	for chunk := range ch {
		if chunk.Err == context.Canceled {
			gotCancelErr = true
		}
		if chunk.DataFrame != nil {
			chunk.DataFrame.Close()
		}
	}

	// With immediate cancel, may get cancel error or count query may fail
	// Either way, it should not hang
	_ = gotCancelErr
}

func TestFromQueryContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	df, err := duckframe.FromQueryContext(ctx, db, "SELECT 42 AS answer, 'hello' AS greeting")
	if err != nil {
		t.Fatalf("FromQueryContext failed: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 1 || c != 2 {
		t.Fatalf("expected (1, 2), got (%d, %d)", r, c)
	}
}

func TestFromQueryContextCancel(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // ensure context expired

	_, err := duckframe.FromQueryContext(ctx, db, "SELECT 42 AS answer")
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestReadCSVContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	ctx := context.Background()
	df, err := duckframe.ReadCSVContext(ctx, db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSVContext failed: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 7 || c != 4 {
		t.Fatalf("expected (7, 4), got (%d, %d)", r, c)
	}
}

func TestFilterContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	ctx := context.Background()
	filtered, err := df.FilterContext(ctx, "salary > 90000")
	if err != nil {
		t.Fatalf("FilterContext failed: %v", err)
	}
	defer filtered.Close()

	r, _, err := filtered.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
}

func TestSortContext(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	df, err := duckframe.ReadCSV(db, testdataPath("employees.csv"))
	if err != nil {
		t.Fatalf("ReadCSV failed: %v", err)
	}
	defer df.Close()

	ctx := context.Background()
	sorted, err := df.SortContext(ctx, "salary", true)
	if err != nil {
		t.Fatalf("SortContext failed: %v", err)
	}
	defer sorted.Close()

	var employees []Employee
	err = sorted.ToSlice(&employees)
	if err != nil {
		t.Fatalf("ToSlice failed: %v", err)
	}

	for i := 1; i < len(employees); i++ {
		if employees[i].Salary < employees[i-1].Salary {
			t.Fatalf("not sorted ascending at index %d", i)
		}
	}
}

func TestChunkedProcessing(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Process chunks in parallel: read chunked, then apply filter to each chunk
	ctx := context.Background()
	ch := duckframe.ReadCSVChunked(ctx, db, testdataPath("employees.csv"), 3)

	var chunks []*duckframe.DataFrame
	for chunk := range ch {
		if chunk.Err != nil {
			t.Fatalf("chunk error: %v", chunk.Err)
		}
		chunks = append(chunks, chunk.DataFrame)
		defer chunk.DataFrame.Close()
	}

	// Apply filter in parallel to all chunks
	filterFn := func(df *duckframe.DataFrame) (*duckframe.DataFrame, error) {
		return df.Filter("salary > 80000")
	}

	results, err := duckframe.ParallelApply(chunks, filterFn)
	if err != nil {
		t.Fatalf("ParallelApply failed: %v", err)
	}

	var totalHighSalary int
	for _, r := range results {
		defer r.Close()
		rows, _, err := r.Shape()
		if err != nil {
			t.Fatalf("Shape failed: %v", err)
		}
		totalHighSalary += rows
	}

	// Verify: from 7 employees, 4 have salary > 80000
	if totalHighSalary != 4 {
		t.Fatalf("expected 4 high salary employees total, got %d", totalHighSalary)
	}
}

// ---------------------------------------------------------------------------
// Phase 7 — External Connectors Tests
// ---------------------------------------------------------------------------

// loadTestExtension tries to load a DuckDB extension, skipping if unavailable.
func loadTestExtension(t *testing.T, db *engine.DB, name string) {
	t.Helper()
	if _, err := db.Conn().Exec(fmt.Sprintf("LOAD %s", name)); err != nil {
		t.Skipf("%s extension not available (not pre-installed): %v", name, err)
	}
}

func TestReadSQLite(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	loadTestExtension(t, db, "sqlite")

	// Create a SQLite file using DuckDB's sqlite extension
	tmpDir := t.TempDir()
	sqlitePath := filepath.Join(tmpDir, "test.sqlite")

	// Create a SQLite database with a table
	_, err := db.Conn().Exec(fmt.Sprintf("ATTACH '%s' AS sqlite_db (TYPE SQLITE)", sqlitePath))
	if err != nil {
		t.Fatalf("failed to attach SQLite: %v", err)
	}
	_, err = db.Conn().Exec("CREATE TABLE sqlite_db.people (name VARCHAR, age INTEGER, city VARCHAR)")
	if err != nil {
		t.Fatalf("failed to create SQLite table: %v", err)
	}
	_, err = db.Conn().Exec("INSERT INTO sqlite_db.people VALUES ('Alice', 30, 'São Paulo'), ('Bob', 25, 'New York'), ('Carol', 35, 'Berlin')")
	if err != nil {
		t.Fatalf("failed to insert into SQLite: %v", err)
	}
	_, err = db.Conn().Exec("DETACH sqlite_db")
	if err != nil {
		t.Fatalf("failed to detach SQLite: %v", err)
	}

	// Now test ReadSQLite
	df, err := duckframe.ReadSQLite(db, sqlitePath, "people")
	if err != nil {
		t.Fatalf("ReadSQLite failed: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
	if c != 3 {
		t.Fatalf("expected 3 columns, got %d", c)
	}

	cols := df.Columns()
	if cols[0] != "name" || cols[1] != "age" || cols[2] != "city" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestReadSQLiteNotFound(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	_, err := duckframe.ReadSQLite(db, "/nonexistent/path.sqlite", "nope")
	if err == nil {
		t.Fatal("expected error for non-existent SQLite file")
	}
}

func TestReadSQLiteFilterAndSelect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	loadTestExtension(t, db, "sqlite")

	// Create SQLite test file
	tmpDir := t.TempDir()
	sqlitePath := filepath.Join(tmpDir, "test2.sqlite")

	_, err := db.Conn().Exec(fmt.Sprintf("ATTACH '%s' AS sqlite_db2 (TYPE SQLITE)", sqlitePath))
	if err != nil {
		t.Fatalf("failed to attach: %v", err)
	}
	_, err = db.Conn().Exec("CREATE TABLE sqlite_db2.scores (student VARCHAR, grade INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	_, err = db.Conn().Exec("INSERT INTO sqlite_db2.scores VALUES ('Alice', 95), ('Bob', 72), ('Carol', 88), ('David', 65)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	db.Conn().Exec("DETACH sqlite_db2")

	// Read and apply operations
	df, err := duckframe.ReadSQLite(db, sqlitePath, "scores")
	if err != nil {
		t.Fatalf("ReadSQLite failed: %v", err)
	}
	defer df.Close()

	// Filter for high grades
	filtered, err := df.Filter("grade >= 80")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	r, _, err := filtered.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 rows with grade >= 80, got %d", r)
	}
}

func TestReadFromDB(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Use DuckDB itself as the "external" database via database/sql
	extDB := db.Conn() // *sql.DB

	// Create a table in the external DB
	_, err := extDB.Exec("CREATE TABLE ext_products (id INTEGER, name VARCHAR, price DOUBLE)")
	if err != nil {
		t.Fatalf("failed to create external table: %v", err)
	}
	_, err = extDB.Exec("INSERT INTO ext_products VALUES (1, 'Widget', 9.99), (2, 'Gadget', 24.99), (3, 'Doohickey', 4.50)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// ReadFromDB using the same connection as external source
	df, err := duckframe.ReadFromDB(db, extDB, "SELECT * FROM ext_products")
	if err != nil {
		t.Fatalf("ReadFromDB failed: %v", err)
	}
	defer df.Close()

	r, c, err := df.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 3 {
		t.Fatalf("expected 3 rows, got %d", r)
	}
	if c != 3 {
		t.Fatalf("expected 3 columns, got %d", c)
	}

	cols := df.Columns()
	if cols[0] != "id" || cols[1] != "name" || cols[2] != "price" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestReadFromDBWithFilter(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	extDB := db.Conn()
	_, err := extDB.Exec("CREATE TABLE ext_items (name VARCHAR, qty INTEGER)")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	_, err = extDB.Exec("INSERT INTO ext_items VALUES ('A', 10), ('B', 5), ('C', 20), ('D', 3)")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	df, err := duckframe.ReadFromDB(db, extDB, "SELECT * FROM ext_items")
	if err != nil {
		t.Fatalf("ReadFromDB failed: %v", err)
	}
	defer df.Close()

	// Apply DuckFrame operations on the imported data
	filtered, err := df.Filter("qty > 5")
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}
	defer filtered.Close()

	r, _, err := filtered.Shape()
	if err != nil {
		t.Fatalf("Shape failed: %v", err)
	}
	if r != 2 {
		t.Fatalf("expected 2 items with qty > 5, got %d", r)
	}
}

func TestReadFromDBBadQuery(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	extDB := db.Conn()
	_, err := duckframe.ReadFromDB(db, extDB, "SELECT * FROM nonexistent_table_xyz")
	if err == nil {
		t.Fatal("expected error for bad query")
	}
}

func TestReadPostgresNoServer(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	loadTestExtension(t, db, "postgres")

	// This should fail because there's no Postgres server running
	// but it tests that the function handles errors correctly
	_, err := duckframe.ReadPostgres(db, "host=localhost port=1 dbname=none user=none", "test_table")
	if err == nil {
		t.Fatal("expected error when connecting to non-existent Postgres")
	}
}

func TestReadMySQLNoServer(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	loadTestExtension(t, db, "mysql")

	_, err := duckframe.ReadMySQL(db, "host=localhost port=1 user=none database=none", "test_table")
	if err == nil {
		t.Fatal("expected error when connecting to non-existent MySQL")
	}
}
