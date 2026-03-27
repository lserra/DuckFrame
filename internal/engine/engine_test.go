package engine_test

import (
	"testing"

	"github.com/lserra/duckframe/internal/engine"
)

func TestOpenInMemory(t *testing.T) {
	db, err := engine.Open("")
	if err != nil {
		t.Fatalf("failed to open in-memory DuckDB: %v", err)
	}
	defer db.Close()

	if db.Conn() == nil {
		t.Fatal("expected non-nil connection")
	}
}

func TestQueryVersion(t *testing.T) {
	db, err := engine.Open("")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	var version string
	err = db.Conn().QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		t.Fatalf("failed to query DuckDB version: %v", err)
	}

	if version == "" {
		t.Fatal("expected non-empty DuckDB version")
	}
	t.Logf("DuckDB version: %s", version)
}

func TestCreateAndQueryTable(t *testing.T) {
	db, err := engine.Open("")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	conn := db.Conn()

	_, err = conn.Exec("CREATE TABLE test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = conn.Exec("INSERT INTO test VALUES (1, 'alice'), (2, 'bob')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	var count int
	err = conn.QueryRow("SELECT COUNT(*) FROM test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestClose(t *testing.T) {
	db, err := engine.Open("")
	if err != nil {
		t.Fatalf("failed to open DuckDB: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("failed to close DuckDB: %v", err)
	}
}
