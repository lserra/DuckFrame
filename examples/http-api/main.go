// Example: HTTP API serving DataFrame queries
//
// This example demonstrates how to build a REST API that uses
// DuckFrame to read, query, and return data as JSON.
//
// Usage:
//
//	cd examples/http-api && go run main.go
//	curl http://localhost:8080/employees
//	curl http://localhost:8080/employees?country=Brazil
//	curl http://localhost:8080/stats
//
// The server loads data from CSV at startup and serves JSON responses.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/lserra/duckframe"
	"github.com/lserra/duckframe/internal/engine"
)

var (
	db     *engine.DB
	baseDF *duckframe.DataFrame
)

func main() {
	var err error
	db, err = engine.Open("")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Load data at startup
	baseDF, err = duckframe.ReadCSV(db, "../../testdata/employees.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer baseDF.Close()

	rows, cols, _ := baseDF.Shape()
	fmt.Printf("Loaded %d rows x %d cols\n", rows, cols)

	http.HandleFunc("/employees", handleEmployees)
	http.HandleFunc("/stats", handleStats)
	http.HandleFunc("/top", handleTop)

	addr := ":8080"
	fmt.Printf("Server running at http://localhost%s\n", addr)
	fmt.Println("Endpoints:")
	fmt.Println("  GET /employees          - all employees (JSON)")
	fmt.Println("  GET /employees?country=X - filter by country")
	fmt.Println("  GET /stats              - salary statistics by country")
	fmt.Println("  GET /top?n=3            - top N earners")
	log.Fatal(http.ListenAndServe(addr, nil))
}

// handleEmployees returns all employees or filters by country.
func handleEmployees(w http.ResponseWriter, r *http.Request) {
	country := r.URL.Query().Get("country")

	var df *duckframe.DataFrame
	var err error

	if country != "" {
		df, err = baseDF.Sql(fmt.Sprintf(
			"SELECT * FROM {df} WHERE country = '%s' ORDER BY name",
			country,
		))
	} else {
		df, err = baseDF.Sql("SELECT * FROM {df} ORDER BY name")
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer df.Close()

	writeJSON(w, df)
}

// handleStats returns salary statistics grouped by country.
func handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := baseDF.Sql(`
		SELECT
			country,
			COUNT(*) AS employees,
			ROUND(AVG(salary), 2) AS avg_salary,
			ROUND(MIN(salary), 2) AS min_salary,
			ROUND(MAX(salary), 2) AS max_salary
		FROM {df}
		GROUP BY country
		ORDER BY avg_salary DESC
	`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer stats.Close()

	writeJSON(w, stats)
}

// handleTop returns top N earners (default: 3).
func handleTop(w http.ResponseWriter, r *http.Request) {
	n := r.URL.Query().Get("n")
	if n == "" {
		n = "3"
	}

	top, err := baseDF.Sql(fmt.Sprintf(
		"SELECT name, country, salary FROM {df} ORDER BY salary DESC LIMIT %s", n,
	))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer top.Close()

	writeJSON(w, top)
}

// writeJSON collects DataFrame rows and writes them as JSON.
func writeJSON(w http.ResponseWriter, df *duckframe.DataFrame) {
	rows, err := df.Collect()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rows); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
