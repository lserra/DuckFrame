// Package engine manages the DuckDB connection lifecycle.
package engine

import (
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb"
)

// DB wraps a DuckDB connection.
type DB struct {
	conn *sql.DB
}

// Open creates a new DuckDB connection.
// Pass an empty string for in-memory, or a file path for persistent storage.
func Open(path string) (*DB, error) {
	conn, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("duckframe: failed to open duckdb: %w", err)
	}

	if err := conn.Ping(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("duckframe: failed to ping duckdb: %w", err)
	}

	// Force single connection: DuckDB temporary tables are connection-scoped,
	// and DuckDB handles internal parallelism via vectorized execution.
	conn.SetMaxOpenConns(1)

	return &DB{conn: conn}, nil
}

// Conn returns the underlying *sql.DB connection.
func (db *DB) Conn() *sql.DB {
	return db.conn
}

// Close closes the DuckDB connection.
func (db *DB) Close() error {
	if db.conn != nil {
		return db.conn.Close()
	}
	return nil
}
