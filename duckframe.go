// Package duckframe provides a Pandas-like DataFrame API for Go,
// powered by DuckDB as the execution engine.
package duckframe

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
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
	owned     bool // whether this DataFrame owns (and should drop) its table
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
	if df.owned && df.tableName != "" {
		_, err := df.db.Conn().Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", df.tableName))
		df.tableName = ""
		return err
	}
	return nil
}

// Shape returns the number of rows and columns in the DataFrame.
func (df *DataFrame) Shape() (rows int, cols int, err error) {
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
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", df.tableName, expr)
	return FromQuery(df.db, query)
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
	resolved := strings.ReplaceAll(query, "{df}", df.tableName)
	return FromQuery(df.db, resolved)
}
