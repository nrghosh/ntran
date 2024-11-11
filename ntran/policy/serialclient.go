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
 * after each command (after selecting the final results).
 */
type SerialClient struct {
	mainConnStr string
	port        string
}

func (c SerialClient) GetName() string {
	return "serial-client"
}

func (c SerialClient) Scaffold() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}
	c.mainConnStr = os.Getenv("SERIAL_DATABASE_URL")
	c.port = os.Getenv("SERIAL_DATABASE_PORT")
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, balance INTEGER);")
	if err != nil {
		return err
	}
	return nil
}

func (c SerialClient) GenerateSQL() ([]TestCase, error) {
	testCases := []TestCase{
		{
			Name: "Short Insert",
			Statements: []Statement{
				{Command: "INSERT INTO users (id, balance) VALUES (1, 100);", Query: "SELECT * FROM users;"},
				{Command: "INSERT INTO users (id, balance) VALUES (2, 200);", Query: "SELECT * FROM users;"},
			},
		},
	}

	return testCases, nil
}

func (c SerialClient) Execute(testCases []TestCase) error {
	rand.Seed(uint64(time.Now().UnixNano()))

	// Share DB connection across all TestCases
	conn, err := pgx.Connect(context.Background(), c.mainConnStr)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	for i, testCase := range testCases {
		benchmark := Benchmark{Policy: c.GetName(), TestCase: testCase.Name}
		benchmark.Start()

		// Start parent transaction
		parentTxn, err := conn.BeginTx(context.Background(), pgx.TxOptions{})
		if err != nil {
			log.Fatalf("Failed to begin parent transaction: %v\n", err)
		}

		var states []interface{}
		for j, statement := range testCase.Statements {

			// Start nested transaction, rollback to this savepoint once state collected
			_, err := parentTxn.Exec(context.Background(), "SAVEPOINT nested_txn")
			if err != nil {
				log.Fatalf("Failed to create savepoint for nested transaction: %v\n", err)
			}

			var rows pgx.Rows
			if statement.Command != "" {
				fmt.Sprintf("db_%v_%v", i, j)

				// Command from TestCase
				_, err = parentTxn.Exec(context.Background(), statement.Command)
				if err != nil {
					return err
				}
				// Query from TestCase
				rows, err = parentTxn.Query(context.Background(), statement.Query)
				if err != nil {
					return err
				}
			} else {
				// Query only, no Command
				rows, err = parentTxn.Query(context.Background(), statement.Query)
				if err != nil {
					return err
				}
			}

			// Record state from the Query
			if rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return err
				}
				states = append(states, v)
			}

			// Rollback nested transaction
			_, rollbackErr := parentTxn.Exec(context.Background(), "ROLLBACK TO SAVEPOINT nested_txn")
			if rollbackErr != nil {
				log.Fatalf("Failed to rollback to savepoint for nested transaction: %v\n", rollbackErr)
			}
		}

		// For now, choose random state as correct state and log it
		idx := rand.Intn(len(states))
		log.Default().Printf("idx: %v; state: %v\n", idx, states[idx])

		// Command from chosen Statement
		_, err = parentTxn.Exec(context.Background(), testCase.Statements[idx].Command)
		if err != nil {
			return err
		}
		// Query from Chosen Statement, may do something with this later, placeholder
		_, err = parentTxn.Query(context.Background(), testCase.Statements[idx].Query)
		if err != nil {
			return err
		}

		// Commit parent transaction with applied changes from one chosen Statement
		err = parentTxn.Commit(context.Background())
		if err != nil {
			log.Fatalf("Failed to commit parent transaction: %v\n", err)
		}

		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}
