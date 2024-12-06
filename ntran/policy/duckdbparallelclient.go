package policy

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"

	// "time"

	_ "github.com/marcboeker/go-duckdb"
)

/*
 implements true concurrent execution by creating separate DuckDB database instances
 for each transaction. Each instance gets the same initial schema and data, and
 transactions execute in parallel on their respective instances. After execution completes,
 a random instance is chosen as the winner, and its state becomes the new main database state.
 This approach allows for actual parallel execution while handling potential conflicts
 through isolation.
*/

type DuckDBParallelClient struct {
	mainDB        *sql.DB
	mainDBPath    string
	instances     []*sql.DB
	instancePaths []string
}

func (c *DuckDBParallelClient) GetName() string {
	return "duckdb-parallel"
}

func (c *DuckDBParallelClient) GetNumTransactionsInFlight() []int {
	return []int{10, 25, 50, 100, 200, 500}
}

func (c *DuckDBParallelClient) Scaffold(schema string, inFlight int) error {
	// temp dir for test run
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("duckdb_test_%d", rand.Intn(10000)))
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %v", err)
	}

	// initialize main db
	c.mainDBPath = filepath.Join(tmpDir, "main.db")
	mainDB, err := sql.Open("duckdb", c.mainDBPath)
	if err != nil {
		return fmt.Errorf("failed to open main database: %v", err)
	}
	c.mainDB = mainDB

	// exec schema on main db
	_, err = mainDB.Exec(schema)
	if err != nil {
		return fmt.Errorf("error executing schema on main database: %v", err)
	}

	// instances for parallel execution
	c.instances = make([]*sql.DB, inFlight)
	c.instancePaths = make([]string, inFlight)

	for i := 0; i < inFlight; i++ {
		// new db per instance
		instancePath := filepath.Join(tmpDir, fmt.Sprintf("instance_%d.db", i))
		c.instancePaths[i] = instancePath

		instance, err := sql.Open("duckdb", instancePath)
		if err != nil {
			return fmt.Errorf("failed to open instance database %d: %v", i, err)
		}

		// apply schema
		_, err = instance.Exec(schema)
		if err != nil {
			return fmt.Errorf("error executing schema on instance %d: %v", i, err)
		}

		c.instances[i] = instance
	}

	return nil
}

func (c *DuckDBParallelClient) Execute(testCase TestCase, experiment *Experiment) error {
	if len(c.instances) == 0 {
		return fmt.Errorf("no database instances available")
	}

	// rand.Seed(time.Now().UnixNano())

	benchmark := Benchmark{
		Experiment:       experiment,
		Policy:           c.GetName(),
		TestCase:         testCase.Name,
		TransactionCount: len(testCase.Statements),
	}
	benchmark.Start()

	results := make(chan ExecutionResult, len(testCase.Statements))
	var wg sync.WaitGroup

	for i := 0; i < len(testCase.Statements); i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			db := c.instances[idx]
			stmt := testCase.Statements[idx]

			tx, err := db.Begin()
			if err != nil {
				results <- ExecutionResult{Statement: stmt, Error: err}
				return
			}
			defer tx.Rollback()

			var values []any
			if stmt.Command != "" {
				_, err = tx.Exec(stmt.Command)
				if err != nil {
					results <- ExecutionResult{Statement: stmt, Error: err}
					return
				}
			}
			if stmt.Query != "" {
				rows, err := tx.Query(stmt.Query)
				if err != nil {
					results <- ExecutionResult{Statement: stmt, Error: err}
					return
				}
				defer rows.Close()

				if rows.Next() {
					values = make([]any, 0)
					cols, err := rows.Columns()
					if err != nil {
						results <- ExecutionResult{Statement: stmt, Error: err}
						return
					}
					scanVals := make([]any, len(cols))
					for i := range cols {
						scanVals[i] = new(any)
					}
					if err := rows.Scan(scanVals...); err != nil {
						results <- ExecutionResult{Statement: stmt, Error: err}
						return
					}
					for _, v := range scanVals {
						values = append(values, *(v.(*any)))
					}
				}
			}

			if err := tx.Commit(); err != nil {
				results <- ExecutionResult{Statement: stmt, Error: err}
				return
			}

			results <- ExecutionResult{Statement: stmt, Values: values}
		}(i)
	}

	wg.Wait()
	close(results)

	var validResults []ExecutionResult
	for result := range results {
		if result.Error != nil {
			return fmt.Errorf("execution error: %v", result.Error)
		}
		validResults = append(validResults, result)
	}

	// select winner randomly (stop using checksum / majority consensus)
	winnerIdx := rand.Intn(len(validResults))

	// apply winning txn to main DB
	winnerStmt := validResults[winnerIdx].Statement
	if winnerStmt.Command != "" {
		_, err := c.mainDB.Exec(winnerStmt.Command)
		if err != nil {
			return fmt.Errorf("error applying winning command to main DB: %v", err)
		}
	}

	benchmark.End()
	benchmark.Log()

	return nil
}

func (c *DuckDBParallelClient) Cleanup(cleanupSQL string) error {
	for _, db := range c.instances {
		if db != nil {
			db.Close()
		}
	}
	if c.mainDB != nil {
		c.mainDB.Close()
	}

	if c.mainDBPath != "" {
		tmpDir := filepath.Dir(c.mainDBPath)
		os.RemoveAll(tmpDir)
	}

	// reset client state
	c.mainDB = nil
	c.mainDBPath = ""
	c.instances = nil
	c.instancePaths = nil

	return nil
}
