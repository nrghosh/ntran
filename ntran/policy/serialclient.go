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
			// Name for the test case for logging purposes
			Name: "Short Insert",
			Statements: []Statement{
				// Statement is single pair of COmmand and QUery. Command makes some DB change, Query then
				// grabs the resulting state for us to assess. Each Statement is essentially one resulting
				// operation to record

				// Randomly select one of these for each TestCase.
				// So from a given []Statement, after rolling all of these back and recording the state,
				// we will select one of the Statement randomly to keep, and re-execute it
				{Command: "INSERT INTO users (id, balance) VALUES (1, 100);", Query: "SELECT * FROM users;"},
				{Command: "INSERT INTO users (id, balance) VALUES (2, 200);", Query: "SELECT * FROM users;"},
			},
		},
	}

	return testCases, nil
}

func (c SerialClient) Execute(testCases []TestCase) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, testCase := range testCases {
		benchmark := Benchmark{Policy: c.GetName(), TestCase: testCase.Name}
		benchmark.Start()

		var states []interface{}

		// Required modifications:
		// For each testcase, we select one correct Statement
		// Pseudocode for each testcase
		// Begin nested transactions
		// Execute the command
		// Execute the Query
		// Append the result of Query to states
		// Rollback nested transaction, but keep the states changes
		//
		// Once all Statement handled:
		// Randomly select one of the Statement
		// Re-execute the Command and the Query
		// Commit the parent transaction
		//
		// OVERALL OUTLINE:
		// Open parent transaction
		// For each statement
		//    open nested transaction
		//    execute command
		//    execute query
		//    record result of query into states slice
		//    rollback nested transaction
		// Choose the "correct" statement randomly
		// Re-execute chosen command
		// Re-execute chosen query
		// Commit parent transactions

		for j, statement := range testCase.Statements {

			var rows pgx.Rows
			if statement.Command != "" {
				/*
				 * Execute the command to change DB
				 * Query state of database to record result
				 */
				fmt.Sprintf("db_%v_%v", i, j)

				conn, err := pgx.Connect(context.Background(), c.mainConnStr)
				if err != nil {
					return err
				}
				defer conn.Close(context.Background())

				// Command to make change to database
				_, err = conn.Exec(context.Background(), statement.Command)
				if err != nil {
					return err
				}
				// Query to grab state of database
				rows, err = conn.Query(context.Background(), statement.Query)
				if err != nil {
					return err
				}
			} else {
				/*
				 * Query DB state only, no change
				 */
				conn, err := pgx.Connect(context.Background(), c.mainConnStr)
				if err != nil {
					return err
				}
				defer conn.Close(context.Background())

				rows, err = conn.Query(context.Background(), statement.Query)
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
		}

		// States is now updated

		// dummy "consensus" step here -- take a random one.
		idx := rand.Intn(len(states))
		log.Default().Printf("idx: %v; state: %v\n", idx, states[idx])
		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}
