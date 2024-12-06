package policy

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

/*
implements serial execution of concurrent transactions on a single DuckDB instance.
Each transaction is executed in sequence within its own transaction boundary and rolled back.
After collecting all states, a random transaction is chosen as the winner and executed with a
commit. This approach simulates concurrent execution while maintaining database consistency
and avoiding lock contention
*/

type DuckDBSerialClient struct {
	currentDB    *sql.DB
	databasePath string
}

func (c *DuckDBSerialClient) GetName() string {
	return "duckdb-serial"
}

func (c *DuckDBSerialClient) GetNumTransactionsInFlight() []int {
	return []int{10, 25, 50, 100, 200, 500}
}

func (c *DuckDBSerialClient) Scaffold(schema string, inFlight int) error {
	tmpDir := os.TempDir()
	databasePath := filepath.Join(tmpDir, fmt.Sprintf("duckdb_serial_%d.db", rand.Intn(10000)))
	c.databasePath = databasePath

	db, err := sql.Open("duckdb", databasePath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	c.currentDB = db

	_, err = db.Exec(schema)
	if err != nil {
		return fmt.Errorf("error executing schema: %v", err)
	}

	return nil
}

func (c *DuckDBSerialClient) Execute(testCase TestCase, experiment *Experiment) error {
	rand.Seed(time.Now().UnixNano())

	benchmark := Benchmark{
		Experiment:       experiment,
		Policy:           c.GetName(),
		TestCase:         testCase.Name,
		TransactionCount: len(testCase.Statements),
	}
	benchmark.Start()

	var states []ExecutionResult

	// Try each statement and collect states
	for _, statement := range testCase.Statements {
		tx, err := c.currentDB.Begin()
		if err != nil {
			return fmt.Errorf("error beginning transaction: %v", err)
		}

		var values []any
		if statement.Command != "" {
			_, err = tx.Exec(statement.Command)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error executing command: %v", err)
			}
			states = append(states, ExecutionResult{Statement: statement})
		}
		if statement.Query != "" {
			rows, err := tx.Query(statement.Query)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("error executing query: %v", err)
			}

			if rows.Next() {
				scanVals := make([]any, 0)
				cols, _ := rows.Columns()
				for range cols {
					scanVals = append(scanVals, new(any))
				}
				if err := rows.Scan(scanVals...); err != nil {
					rows.Close()
					tx.Rollback()
					return fmt.Errorf("error scanning values: %v", err)
				}
				values = make([]any, len(scanVals))
				for i, v := range scanVals {
					values[i] = *(v.(*any))
				}
			}
			rows.Close()
			states = append(states, ExecutionResult{Statement: statement, Values: values})
		}

		tx.Rollback() // Roll back each transaction
	}

	// Pick random winner and execute it
	idx := rand.Intn(len(states))
	winner := states[idx]
	log.Printf("idx: %v; state: %v\n", idx, winner)

	tx, err := c.currentDB.Begin()
	if err != nil {
		return fmt.Errorf("error beginning winner transaction: %v", err)
	}

	if winner.Statement.Command != "" {
		_, err = tx.Exec(winner.Statement.Command)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error executing winning command: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing winner: %v", err)
	}

	benchmark.End()
	benchmark.Log()

	return nil
}

func (c *DuckDBSerialClient) Cleanup(cleanupSQL string) error {
	if c.currentDB != nil {
		c.currentDB.Close()
	}

	if c.databasePath != "" {
		os.Remove(c.databasePath)
	}

	c.currentDB = nil
	c.databasePath = ""

	return nil
}
