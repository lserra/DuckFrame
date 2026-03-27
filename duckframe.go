// Package duckframe provides a Pandas-like DataFrame API for Go,
// powered by DuckDB as the execution engine.
package duckframe

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"

	"github.com/lserra/duckframe/internal/engine"
)

// tableCounter generates unique table names for each DataFrame.
var tableCounter atomic.Uint64

func nextTableName() string {
	id := tableCounter.Add(1)
	return fmt.Sprintf("df_%d", id)
}

// DataFrame represents a tabular dataset backed by a DuckDB table or view.
type DataFrame struct {
	db        *engine.DB
	tableName string
	columns   []string
	owned     bool  // whether this DataFrame owns (and should drop) its table
	err       error // carries error for fluent chaining
}

// Err returns the error stored in the DataFrame, if any.
// Use this after a chain of operations to check for errors.
func (df *DataFrame) Err() error {
	return df.err
}

// errDF creates a DataFrame that carries an error (for fluent chaining).
func errDF(db *engine.DB, err error) *DataFrame {
	return &DataFrame{db: db, err: err}
}

// New creates a new DataFrame from column names and row data.
// Each row is a map of column name to value.
func New(db *engine.DB, columns []string, rows []map[string]interface{}) (*DataFrame, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("duckframe: columns cannot be empty")
	}

	tableName := nextTableName()
	conn := db.Conn()

	// Infer column types from first row (default to VARCHAR)
	colDefs := make([]string, len(columns))
	for i, col := range columns {
		colDefs[i] = fmt.Sprintf("%q VARCHAR", col)
		if len(rows) > 0 {
			if val, ok := rows[0][col]; ok {
				colDefs[i] = fmt.Sprintf("%q %s", col, inferType(val))
			}
		}
	}

	createSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", tableName, strings.Join(colDefs, ", "))
	if _, err := conn.Exec(createSQL); err != nil {
		return nil, fmt.Errorf("duckframe: failed to create table: %w", err)
	}

	// Insert rows
	if len(rows) > 0 {
		placeholders := make([]string, len(columns))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, strings.Join(placeholders, ", "))

		stmt, err := conn.Prepare(insertSQL)
		if err != nil {
			return nil, fmt.Errorf("duckframe: failed to prepare insert: %w", err)
		}
		defer stmt.Close()

		for _, row := range rows {
			vals := make([]interface{}, len(columns))
			for i, col := range columns {
				vals[i] = row[col]
			}
			if _, err := stmt.Exec(vals...); err != nil {
				return nil, fmt.Errorf("duckframe: failed to insert row: %w", err)
			}
		}
	}

	return &DataFrame{
		db:        db,
		tableName: tableName,
		columns:   columns,
		owned:     true,
	}, nil
}

// FromQuery creates a DataFrame from a SQL query result.
func FromQuery(db *engine.DB, query string) (*DataFrame, error) {
	tableName := nextTableName()
	conn := db.Conn()

	createSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tableName, query)
	if _, err := conn.Exec(createSQL); err != nil {
		return nil, fmt.Errorf("duckframe: failed to create table from query: %w", err)
	}

	cols, err := queryColumns(conn, tableName)
	if err != nil {
		return nil, err
	}

	return &DataFrame{
		db:        db,
		tableName: tableName,
		columns:   cols,
		owned:     true,
	}, nil
}

// fromTable wraps an existing table/view name as a DataFrame (does not own it).
func fromTable(db *engine.DB, tableName string) (*DataFrame, error) {
	cols, err := queryColumns(db.Conn(), tableName)
	if err != nil {
		return nil, err
	}

	return &DataFrame{
		db:        db,
		tableName: tableName,
		columns:   cols,
		owned:     false,
	}, nil
}

// TableName returns the internal DuckDB table name backing this DataFrame.
func (df *DataFrame) TableName() string {
	return df.tableName
}

// Columns returns the column names of the DataFrame.
func (df *DataFrame) Columns() []string {
	dst := make([]string, len(df.columns))
	copy(dst, df.columns)
	return dst
}

// Engine returns the underlying engine.DB connection.
func (df *DataFrame) Engine() *engine.DB {
	return df.db
}

// Close drops the underlying temporary table if this DataFrame owns it.
func (df *DataFrame) Close() error {
	if df.err != nil {
		return nil // nothing to close on an error DataFrame
	}
	if df.owned && df.tableName != "" {
		_, err := df.db.Conn().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", df.tableName))
		df.tableName = ""
		return err
	}
	return nil
}

// Shape returns the number of rows and columns in the DataFrame.
func (df *DataFrame) Shape() (rows int, cols int, err error) {
	if df.err != nil {
		return 0, 0, df.err
	}
	var count int
	err = df.db.Conn().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", df.tableName)).Scan(&count)
	if err != nil {
		return 0, 0, fmt.Errorf("duckframe: failed to count rows: %w", err)
	}
	return count, len(df.columns), nil
}

// queryColumns retrieves column names from a DuckDB table.
func queryColumns(conn *sql.DB, tableName string) ([]string, error) {
	rows, err := conn.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
	if err != nil {
		return nil, fmt.Errorf("duckframe: failed to query columns: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("duckframe: failed to get columns: %w", err)
	}
	return cols, nil
}

// inferType maps Go types to DuckDB SQL types.
func inferType(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return "BIGINT"
	case float32, float64:
		return "DOUBLE"
	case bool:
		return "BOOLEAN"
	default:
		return "VARCHAR"
	}
}

// ---------------------------------------------------------------------------
// MVP Operations
// ---------------------------------------------------------------------------

// ReadCSV reads a CSV file and returns a DataFrame.
func ReadCSV(db *engine.DB, path string) (*DataFrame, error) {
	tableName := nextTableName()
	query := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM read_csv_auto('%s')", tableName, path)

	if _, err := db.Conn().Exec(query); err != nil {
		return nil, fmt.Errorf("duckframe: failed to read CSV %q: %w", path, err)
	}

	cols, err := queryColumns(db.Conn(), tableName)
	if err != nil {
		return nil, err
	}

	return &DataFrame{
		db:        db,
		tableName: tableName,
		columns:   cols,
		owned:     true,
	}, nil
}

// Select returns a new DataFrame with only the specified columns.
func (df *DataFrame) Select(cols ...string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("duckframe: Select requires at least one column")
	}

	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf("%q", c)
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(quoted, ", "), df.tableName)

	return FromQuery(df.db, query)
}

// Filter returns a new DataFrame with rows matching the given SQL expression.
func (df *DataFrame) Filter(expr string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", df.tableName, expr)
	result, err := FromQuery(df.db, query)
	if err != nil {
		return errDF(df.db, err), err
	}
	return result, nil
}

// GroupedFrame represents a DataFrame grouped by one or more columns.
type GroupedFrame struct {
	df      *DataFrame
	groupBy []string
}

// GroupBy groups the DataFrame by the specified columns.
func (df *DataFrame) GroupBy(cols ...string) *GroupedFrame {
	return &GroupedFrame{
		df:      df,
		groupBy: cols,
	}
}

// Agg performs an aggregation on the grouped DataFrame.
// fn can be: "mean"/"avg", "sum", "count", "min", "max".
func (gf *GroupedFrame) Agg(col string, fn string) (*DataFrame, error) {
	if gf.df.err != nil {
		return errDF(gf.df.db, gf.df.err), gf.df.err
	}
	sqlFn := strings.ToUpper(fn)
	if sqlFn == "MEAN" {
		sqlFn = "AVG"
	}

	validFns := map[string]bool{"AVG": true, "SUM": true, "COUNT": true, "MIN": true, "MAX": true}
	if !validFns[sqlFn] {
		return nil, fmt.Errorf("duckframe: unsupported aggregation function %q", fn)
	}

	quotedGroups := make([]string, len(gf.groupBy))
	for i, g := range gf.groupBy {
		quotedGroups[i] = fmt.Sprintf("%q", g)
	}
	groupClause := strings.Join(quotedGroups, ", ")

	query := fmt.Sprintf(
		"SELECT %s, %s(%q) AS %s_%s FROM %s GROUP BY %s",
		groupClause, sqlFn, col, strings.ToLower(fn), col,
		gf.df.tableName, groupClause,
	)

	return FromQuery(gf.df.db, query)
}

// Show prints the DataFrame contents as a formatted table to stdout.
// It displays up to maxRows rows (0 = all rows).
func (df *DataFrame) Show(maxRows ...int) error {
	if df.err != nil {
		return df.err
	}
	limit := 50
	if len(maxRows) > 0 && maxRows[0] > 0 {
		limit = maxRows[0]
	}

	// Get shape before opening the query cursor
	totalRows, _, _ := df.Shape()

	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", df.tableName, limit)
	rows, err := df.db.Conn().Query(query)
	if err != nil {
		return fmt.Errorf("duckframe: failed to query for Show: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("duckframe: failed to get columns: %w", err)
	}

	fmt.Printf("DataFrame [%d rows x %d cols]\n", totalRows, len(cols))

	// Use tabwriter for aligned output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Header
	fmt.Fprintln(w, strings.Join(cols, "\t"))

	// Separator
	seps := make([]string, len(cols))
	for i, col := range cols {
		seps[i] = strings.Repeat("-", len(col)+2)
	}
	fmt.Fprintln(w, strings.Join(seps, "\t"))

	// Rows
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	printed := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("duckframe: failed to scan row: %w", err)
		}
		strs := make([]string, len(cols))
		for i, v := range values {
			strs[i] = fmt.Sprintf("%v", v)
		}
		fmt.Fprintln(w, strings.Join(strs, "\t"))
		printed++
	}

	w.Flush()

	if totalRows > printed {
		fmt.Printf("... (%d more rows)\n", totalRows-printed)
	}

	return nil
}

// Sql executes a raw SQL query and returns the result as a new DataFrame.
// Use the placeholder "{df}" in the query to reference this DataFrame's table.
func (df *DataFrame) Sql(query string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	resolved := strings.ReplaceAll(query, "{df}", df.tableName)
	return FromQuery(df.db, resolved)
}

// ---------------------------------------------------------------------------
// Phase 3 — Collect, ToSlice, Fluent API
// ---------------------------------------------------------------------------

// Collect materializes the DataFrame into a slice of maps.
// Each map represents a row with column names as keys.
func (df *DataFrame) Collect() ([]map[string]interface{}, error) {
	if df.err != nil {
		return nil, df.err
	}

	query := fmt.Sprintf("SELECT * FROM %s", df.tableName)
	rows, err := df.db.Conn().Query(query)
	if err != nil {
		return nil, fmt.Errorf("duckframe: Collect query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("duckframe: Collect failed to get columns: %w", err)
	}

	var result []map[string]interface{}
	values := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range values {
		ptrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("duckframe: Collect failed to scan row: %w", err)
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("duckframe: Collect iteration error: %w", err)
	}

	return result, nil
}

// ToSlice materializes the DataFrame into a slice of structs.
// dest must be a pointer to a slice of structs. Struct fields are matched
// to columns by the "df" tag, or by field name (case-insensitive).
//
// Example:
//
//	type Employee struct {
//	    Name    string  `df:"name"`
//	    Age     int64   `df:"age"`
//	    Salary  float64 `df:"salary"`
//	}
//	var employees []Employee
//	err := df.ToSlice(&employees)
func (df *DataFrame) ToSlice(dest interface{}) error {
	if df.err != nil {
		return df.err
	}

	// Validate dest is *[]Struct
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("duckframe: ToSlice requires a non-nil pointer to a slice")
	}
	sliceVal := rv.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("duckframe: ToSlice requires a pointer to a slice, got pointer to %s", sliceVal.Kind())
	}
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("duckframe: ToSlice requires a slice of structs, got slice of %s", elemType.Kind())
	}

	// Build column-to-field mapping
	fieldMap := buildFieldMap(elemType)

	query := fmt.Sprintf("SELECT * FROM %s", df.tableName)
	rows, err := df.db.Conn().Query(query)
	if err != nil {
		return fmt.Errorf("duckframe: ToSlice query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("duckframe: ToSlice failed to get columns: %w", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("duckframe: ToSlice failed to scan row: %w", err)
		}

		elem := reflect.New(elemType).Elem()
		for i, col := range cols {
			fieldIdx, ok := fieldMap[strings.ToLower(col)]
			if !ok {
				continue
			}
			field := elem.Field(fieldIdx)
			if values[i] == nil {
				continue
			}
			if err := setField(field, values[i]); err != nil {
				return fmt.Errorf("duckframe: ToSlice failed to set field %q: %w", col, err)
			}
		}
		sliceVal = reflect.Append(sliceVal, elem)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("duckframe: ToSlice iteration error: %w", err)
	}

	rv.Elem().Set(sliceVal)
	return nil
}

// buildFieldMap creates a mapping from lowercase column name to struct field index.
// It checks for "df" tags first, then falls back to field name.
func buildFieldMap(t reflect.Type) map[string]int {
	m := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("df")
		if tag != "" && tag != "-" {
			m[strings.ToLower(tag)] = i
		} else {
			m[strings.ToLower(f.Name)] = i
		}
	}
	return m
}

// setField converts a database value to the appropriate Go type and sets the struct field.
func setField(field reflect.Value, value interface{}) error {
	v := reflect.ValueOf(value)
	fieldType := field.Type()

	// Direct assignable
	if v.Type().AssignableTo(fieldType) {
		field.Set(v)
		return nil
	}

	// Convertible
	if v.Type().ConvertibleTo(fieldType) {
		field.Set(v.Convert(fieldType))
		return nil
	}

	// Handle common numeric conversions from database
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch val := value.(type) {
		case int64:
			field.SetInt(val)
			return nil
		case int32:
			field.SetInt(int64(val))
			return nil
		case float64:
			field.SetInt(int64(val))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch val := value.(type) {
		case float64:
			field.SetFloat(val)
			return nil
		case int64:
			field.SetFloat(float64(val))
			return nil
		}
	case reflect.String:
		field.SetString(fmt.Sprintf("%v", value))
		return nil
	case reflect.Bool:
		if b, ok := value.(bool); ok {
			field.SetBool(b)
			return nil
		}
	}

	return fmt.Errorf("cannot convert %T to %s", value, fieldType)
}

// ---------------------------------------------------------------------------
// Phase 4 — Data Formats (Read & Write)
// ---------------------------------------------------------------------------

// ReadParquet reads a Parquet file and returns a DataFrame.
func ReadParquet(db *engine.DB, path string) (*DataFrame, error) {
	tableName := nextTableName()
	query := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, path)

	if _, err := db.Conn().Exec(query); err != nil {
		return nil, fmt.Errorf("duckframe: failed to read Parquet %q: %w", path, err)
	}

	cols, err := queryColumns(db.Conn(), tableName)
	if err != nil {
		return nil, err
	}

	return &DataFrame{db: db, tableName: tableName, columns: cols, owned: true}, nil
}

// ReadJSON reads a JSON Lines (newline-delimited JSON) file and returns a DataFrame.
func ReadJSON(db *engine.DB, path string) (*DataFrame, error) {
	tableName := nextTableName()
	query := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS SELECT * FROM read_json_auto('%s')", tableName, path)

	if _, err := db.Conn().Exec(query); err != nil {
		return nil, fmt.Errorf("duckframe: failed to read JSON %q: %w", path, err)
	}

	cols, err := queryColumns(db.Conn(), tableName)
	if err != nil {
		return nil, err
	}

	return &DataFrame{db: db, tableName: tableName, columns: cols, owned: true}, nil
}

// WriteCSV writes the DataFrame contents to a CSV file.
func (df *DataFrame) WriteCSV(path string) error {
	if df.err != nil {
		return df.err
	}
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT CSV, HEADER)", df.tableName, path)
	if _, err := df.db.Conn().Exec(query); err != nil {
		return fmt.Errorf("duckframe: failed to write CSV %q: %w", path, err)
	}
	return nil
}

// WriteParquet writes the DataFrame contents to a Parquet file.
func (df *DataFrame) WriteParquet(path string) error {
	if df.err != nil {
		return df.err
	}
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET)", df.tableName, path)
	if _, err := df.db.Conn().Exec(query); err != nil {
		return fmt.Errorf("duckframe: failed to write Parquet %q: %w", path, err)
	}
	return nil
}

// WriteJSON writes the DataFrame contents to a JSON Lines file.
func (df *DataFrame) WriteJSON(path string) error {
	if df.err != nil {
		return df.err
	}
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT JSON)", df.tableName, path)
	if _, err := df.db.Conn().Exec(query); err != nil {
		return fmt.Errorf("duckframe: failed to write JSON %q: %w", path, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Phase 5 — Advanced Operations
// ---------------------------------------------------------------------------

// Sort returns a new DataFrame sorted by the given column.
// If asc is true, sorts ascending; otherwise descending.
func (df *DataFrame) Sort(col string, asc bool) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	order := "ASC"
	if !asc {
		order = "DESC"
	}
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %q %s", df.tableName, col, order)
	return FromQuery(df.db, query)
}

// Limit returns a new DataFrame with at most n rows.
func (df *DataFrame) Limit(n int) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d", df.tableName, n)
	return FromQuery(df.db, query)
}

// Distinct returns a new DataFrame with duplicate rows removed.
func (df *DataFrame) Distinct() (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	query := fmt.Sprintf("SELECT DISTINCT * FROM %s", df.tableName)
	return FromQuery(df.db, query)
}

// Rename returns a new DataFrame with a column renamed.
func (df *DataFrame) Rename(oldName, newName string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	parts := make([]string, len(df.columns))
	for i, col := range df.columns {
		if col == oldName {
			parts[i] = fmt.Sprintf("%q AS %q", col, newName)
		} else {
			parts[i] = fmt.Sprintf("%q", col)
		}
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(parts, ", "), df.tableName)
	return FromQuery(df.db, query)
}

// Drop returns a new DataFrame without the specified columns.
func (df *DataFrame) Drop(cols ...string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	dropSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		dropSet[c] = true
	}
	var kept []string
	for _, col := range df.columns {
		if !dropSet[col] {
			kept = append(kept, fmt.Sprintf("%q", col))
		}
	}
	if len(kept) == 0 {
		return nil, fmt.Errorf("duckframe: Drop would remove all columns")
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(kept, ", "), df.tableName)
	return FromQuery(df.db, query)
}

// WithColumn returns a new DataFrame with an added or replaced column
// defined by a SQL expression.
func (df *DataFrame) WithColumn(name string, expr string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	parts := make([]string, 0, len(df.columns)+1)
	replaced := false
	for _, col := range df.columns {
		if col == name {
			parts = append(parts, fmt.Sprintf("(%s) AS %q", expr, name))
			replaced = true
		} else {
			parts = append(parts, fmt.Sprintf("%q", col))
		}
	}
	if !replaced {
		parts = append(parts, fmt.Sprintf("(%s) AS %q", expr, name))
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(parts, ", "), df.tableName)
	return FromQuery(df.db, query)
}

// Join joins this DataFrame with another on the specified column.
// how can be: "inner", "left", "right", "full".
func (df *DataFrame) Join(other *DataFrame, on string, how string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	if other.err != nil {
		return errDF(df.db, other.err), other.err
	}

	howUpper := strings.ToUpper(how)
	validJoins := map[string]bool{"INNER": true, "LEFT": true, "RIGHT": true, "FULL": true}
	if !validJoins[howUpper] {
		return nil, fmt.Errorf("duckframe: unsupported join type %q", how)
	}

	// Build column list avoiding ambiguity: qualify all columns
	var selectCols []string
	otherColSet := make(map[string]bool)
	for _, col := range other.columns {
		otherColSet[col] = true
	}

	for _, col := range df.columns {
		selectCols = append(selectCols, fmt.Sprintf("a.%q", col))
	}
	for _, col := range other.columns {
		if col == on {
			continue // skip the join key from the right side
		}
		alias := col
		// If column name conflicts, prefix with right table
		for _, lcol := range df.columns {
			if lcol == col {
				alias = "right_" + col
				break
			}
		}
		selectCols = append(selectCols, fmt.Sprintf("b.%q AS %q", col, alias))
	}

	query := fmt.Sprintf("SELECT %s FROM %s a %s JOIN %s b ON a.%q = b.%q",
		strings.Join(selectCols, ", "),
		df.tableName, howUpper, other.tableName,
		on, on,
	)
	return FromQuery(df.db, query)
}

// Union returns a new DataFrame that appends the rows of other to this DataFrame.
// Both DataFrames must have the same columns.
func (df *DataFrame) Union(other *DataFrame) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	if other.err != nil {
		return errDF(df.db, other.err), other.err
	}
	query := fmt.Sprintf("SELECT * FROM %s UNION ALL SELECT * FROM %s", df.tableName, other.tableName)
	return FromQuery(df.db, query)
}

// Head returns a new DataFrame with the first n rows.
func (df *DataFrame) Head(n int) (*DataFrame, error) {
	return df.Limit(n)
}

// Tail returns a new DataFrame with the last n rows.
// Note: order depends on the underlying table order.
func (df *DataFrame) Tail(n int) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	totalRows, _, err := df.Shape()
	if err != nil {
		return errDF(df.db, err), err
	}
	offset := totalRows - n
	if offset < 0 {
		offset = 0
	}
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", df.tableName, n, offset)
	return FromQuery(df.db, query)
}

// Dtypes returns a map of column names to their DuckDB data types.
func (df *DataFrame) Dtypes() (map[string]string, error) {
	if df.err != nil {
		return nil, df.err
	}
	query := fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", df.tableName)
	rows, err := df.db.Conn().Query(query)
	if err != nil {
		return nil, fmt.Errorf("duckframe: Dtypes query failed: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return nil, fmt.Errorf("duckframe: Dtypes scan failed: %w", err)
		}
		result[colName] = dataType
	}
	return result, nil
}

// Describe returns a new DataFrame with descriptive statistics
// (count, mean, min, max, std) for all numeric columns.
func (df *DataFrame) Describe() (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}

	// Get numeric column types
	dtypes, err := df.Dtypes()
	if err != nil {
		return errDF(df.db, err), err
	}

	var numericCols []string
	for _, col := range df.columns {
		dt := strings.ToUpper(dtypes[col])
		if strings.Contains(dt, "INT") || strings.Contains(dt, "FLOAT") ||
			strings.Contains(dt, "DOUBLE") || strings.Contains(dt, "DECIMAL") ||
			strings.Contains(dt, "NUMERIC") || strings.Contains(dt, "BIGINT") ||
			strings.Contains(dt, "SMALLINT") || strings.Contains(dt, "TINYINT") {
			numericCols = append(numericCols, col)
		}
	}

	if len(numericCols) == 0 {
		return nil, fmt.Errorf("duckframe: Describe requires at least one numeric column")
	}

	// Build UNION ALL of stats for each numeric column
	var parts []string
	for _, col := range numericCols {
		q := fmt.Sprintf(
			"SELECT '%s' AS \"column\", COUNT(%q) AS \"count\", "+
				"ROUND(AVG(%q), 4) AS \"mean\", "+
				"ROUND(STDDEV(%q), 4) AS \"std\", "+
				"MIN(%q) AS \"min\", "+
				"MAX(%q) AS \"max\" FROM %s",
			col, col, col, col, col, col, df.tableName,
		)
		parts = append(parts, q)
	}

	query := strings.Join(parts, " UNION ALL ")
	return FromQuery(df.db, query)
}

// ---------------------------------------------------------------------------
// Phase 6 — Concurrency & Streaming
// ---------------------------------------------------------------------------

// ApplyFunc is a function that transforms a DataFrame into another.
type ApplyFunc func(*DataFrame) (*DataFrame, error)

// ParallelApply applies fn to each DataFrame in dfs concurrently
// and returns the results in the same order.
// All DataFrames must share the same engine.DB.
func ParallelApply(dfs []*DataFrame, fn ApplyFunc) ([]*DataFrame, error) {
	results := make([]*DataFrame, len(dfs))
	errs := make([]error, len(dfs))

	var wg sync.WaitGroup
	wg.Add(len(dfs))

	for i, df := range dfs {
		go func(idx int, d *DataFrame) {
			defer wg.Done()
			results[idx], errs[idx] = fn(d)
		}(i, df)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			// Clean up any successfully created DataFrames
			for j, r := range results {
				if j != i && r != nil {
					r.Close()
				}
			}
			return nil, fmt.Errorf("duckframe: ParallelApply failed on DataFrame %d: %w", i, err)
		}
	}

	return results, nil
}

// ChunkResult holds a chunk produced by ReadCSVChunked.
type ChunkResult struct {
	DataFrame *DataFrame
	Index     int
	Err       error
}

// ReadCSVChunked reads a CSV file in chunks of chunkSize rows,
// sending each chunk as a DataFrame to the returned channel.
// The caller must close each DataFrame after use.
func ReadCSVChunked(ctx context.Context, db *engine.DB, path string, chunkSize int) <-chan ChunkResult {
	ch := make(chan ChunkResult)

	go func() {
		defer close(ch)

		// Count total rows first
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM read_csv_auto('%s')", path)
		var totalRows int
		if err := db.Conn().QueryRowContext(ctx, countQuery).Scan(&totalRows); err != nil {
			ch <- ChunkResult{Err: fmt.Errorf("duckframe: failed to count CSV rows: %w", err)}
			return
		}

		idx := 0
		for offset := 0; offset < totalRows; offset += chunkSize {
			select {
			case <-ctx.Done():
				ch <- ChunkResult{Err: ctx.Err(), Index: idx}
				return
			default:
			}

			query := fmt.Sprintf(
				"SELECT * FROM read_csv_auto('%s') LIMIT %d OFFSET %d",
				path, chunkSize, offset,
			)
			df, err := FromQuery(db, query)
			ch <- ChunkResult{DataFrame: df, Index: idx, Err: err}
			if err != nil {
				return
			}
			idx++
		}
	}()

	return ch
}

// FromQueryContext creates a DataFrame from a SQL query, with context support.
func FromQueryContext(ctx context.Context, db *engine.DB, query string) (*DataFrame, error) {
	tableName := nextTableName()
	createSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s AS %s", tableName, query)

	if _, err := db.Conn().ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("duckframe: FromQueryContext failed: %w", err)
	}

	columns, err := queryColumnsCtx(ctx, db, tableName)
	if err != nil {
		db.Conn().ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
		return nil, err
	}

	return &DataFrame{db: db, tableName: tableName, columns: columns, owned: true}, nil
}

// ReadCSVContext reads a CSV file into a DataFrame, with context support.
func ReadCSVContext(ctx context.Context, db *engine.DB, path string) (*DataFrame, error) {
	query := fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", path)
	return FromQueryContext(ctx, db, query)
}

// FilterContext returns a filtered DataFrame, with context support.
func (df *DataFrame) FilterContext(ctx context.Context, expr string) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", df.tableName, expr)
	return FromQueryContext(ctx, df.db, query)
}

// SortContext returns a sorted DataFrame, with context support.
func (df *DataFrame) SortContext(ctx context.Context, col string, asc bool) (*DataFrame, error) {
	if df.err != nil {
		return errDF(df.db, df.err), df.err
	}
	order := "ASC"
	if !asc {
		order = "DESC"
	}
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %q %s", df.tableName, col, order)
	return FromQueryContext(ctx, df.db, query)
}

// queryColumnsCtx returns column names with context support.
func queryColumnsCtx(ctx context.Context, db *engine.DB, tableName string) ([]string, error) {
	rows, err := db.Conn().QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", tableName))
	if err != nil {
		return nil, fmt.Errorf("duckframe: failed to query columns: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("duckframe: failed to get columns: %w", err)
	}
	return cols, nil
}
