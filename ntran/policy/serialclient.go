package policy

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"golang.org/x/exp/rand"
)

/*
 * SerialClient - executes its statements serially, under
 * one snapshost isolation level transaction, rolling back state
 * after each command, then select and re-execute a chosen statement
 * and commit the parent transaction
 */
type SerialClient struct {
	mainConnStr string
}

func (c *SerialClient) GetName() string {
	return "serial-snapshot"
}

func (c *SerialClient) GetNumTransactionsInFlight() []int {
	return []int{10, 25, 50, 100, 200, 500}
}

func (c *SerialClient) Scaffold(sql string, inFlight int) error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}
	c.mainConnStr = os.Getenv("SERIAL_DATABASE_URL")
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), sql)
	if err != nil {
		return err
	}

	return nil
}

func (c *SerialClient) Execute(testCase TestCase, experiment *Experiment) error {
	rand.Seed(uint64(time.Now().UnixNano()))

	// Share DB connection across all TestCases
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	benchmark := Benchmark{
		Experiment:       experiment,
		Policy:           c.GetName(),
		TestCase:         testCase.Name,
		TransactionCount: len(testCase.Statements),
	}
	benchmark.Start()

	// Start parent transaction
	parentTxn, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
	if err != nil {
		log.Fatalf("Failed to begin parent transaction: %v\n", err)
	}

	var states []ExecutionResult
	for _, statement := range testCase.Statements {

		// Start nested transaction, rollback to this savepoint once state collected
		_, err := parentTxn.Exec(context.Background(), "SAVEPOINT nested_txn")
		if err != nil {
			log.Fatalf("Failed to create savepoint for nested transaction: %v\n", err)
		}

		var rows pgx.Rows
		if statement.Command != "" {

			// Command from TestCase
			_, err = parentTxn.Exec(context.Background(), statement.Command)
			if err != nil {
				return err
			}
			states = append(states, ExecutionResult{Statement: statement, Values: []any{}})

		} else {
			// Query only, no Command
			rows, err = parentTxn.Query(context.Background(), statement.Query)
			if err != nil {
				return err
			}

			// Record state from the Query
			if rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return err
				}
				states = append(states, ExecutionResult{Statement: statement, Values: v})
			}
			rows.Close() // required so connection is not considered busy during rollback
		}

		_, rollbackErr := parentTxn.Exec(context.Background(), "ROLLBACK TO SAVEPOINT nested_txn")
		if rollbackErr != nil {
			fmt.Println("failed")
			log.Fatalf("Failed to rollback to savepoint for nested transaction: %v\n", rollbackErr)
		}
	}

	// For now, choose random state as correct state
	idx := rand.Intn(len(states))
	log.Printf("idx: %v; state: %v\n", idx, states[idx])

	// Command from chosen Statement
	if states[idx].Statement.Command != "" {
		_, err = parentTxn.Exec(context.Background(), testCase.Statements[idx].Command)
		if err != nil {
			return err
		}
	}

	// Commit parent transaction with applied changes from one chosen Statement
	err = parentTxn.Commit(context.Background())
	if err != nil {
		log.Fatalf("Failed to commit parent transaction: %v\n", err)
	}

	benchmark.End()
	benchmark.Log()

	return nil
}

func (c *SerialClient) Cleanup(sql string) error {
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), sql)
	if err != nil {
		return err
	}

	return nil
}
