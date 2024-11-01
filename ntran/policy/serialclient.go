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
	// probably also load basic credentials from the environment here
	port string
}

func (c SerialClient) Scaffold() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}
	c.mainConnStr = os.Getenv("SERIAL_DATABASE_URL")
	c.port = os.Getenv("SERIAL_DATABASE_PORT") // probably can remove
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

func (c SerialClient) GenerateSQL() ([][]Statement, error) {
	statements := [][]Statement{
		{
			{Command: "INSERT INTO users (id, balance) VALUES (1, 100);", Query: "SELECT * FROM users;"},
			{Command: "INSERT INTO users (id, balance) VALUES (2, 200);", Query: "SELECT * FROM users;"},
		},
	}

	return statements, nil
}

func (c SerialClient) Execute(statementSeries [][]Statement) error {
	// Plan
	// 	Parent Transaction
	//		Begin parent transaction for consistent snapshot across all queries
	// 	Nested Transactions
	// 		For each query
	// 			Begin nested transaction, execute query
	// 			Record checksum or rating for state outcome
	// 			Rollback nested transaction
	// Re-execute chosen query
	// 	Choose “correct” query to re-execute based on recorded checksum or state outcome rating
	// 	Begin nested transaction for the query
	// 	Commit nested transaction
	// Clean-up
	// 	Commit parent transaction
	//
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, series := range statementSeries {
		benchmark := Benchmark{}
		benchmark.Start()

		var states []interface{}
		var rows pgx.Rows

		for j, statement := range series {
			if statement.Command == "" {
				continue
			}
			db := fmt.Sprintf("db_%v_%v", i, j)

			conn, err := pgx.Connect(context.Background(), c.mainConnStr)
			if err != nil {
				return err
			}
			defer conn.Close(context.Background())

			_, err = conn.Exec(context.Background(), statement.Command)
			if err != nil {
				return err
			}

			rows, err = conn.Query(context.Background(), statement.Query)
			if err != nil {
				return err
			}

			if rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return err
				}
				states = append(states, v)
			}
		}

		// Randomly choose one, re-execute the query, and commit
		idx := rand.Intn(len(states))
		log.Default().Printf("idx: %v; state: %v\n", idx, states[idx])
		benchmark.End()
		benchmark.Log(i)
	}
	return nil

	return fmt.Errorf("SerialClient unimplemented")
}
