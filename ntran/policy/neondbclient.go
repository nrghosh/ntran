package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

type NeonDBClient struct {
	mainConnStr string
}

func (c NeonDBClient) Scaffold() error {
	err := godotenv.Load()
	if err != nil {
		return fmt.Errorf("error loading .env file")
	}
	c.mainConnStr = os.Getenv("NEON_DATABASE_URL")
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

func (c NeonDBClient) GenerateSQL() ([][]Statement, error) {
	statements := [][]Statement{
		{
			{Command: "INSERT INTO users (id, balance) VALUES (1, 100);", Query: "SELECT * FROM users;"},
			{Command: "INSERT INTO users (id, balance) VALUES (2, 200);", Query: "SELECT * FROM users;"},
		},
	}

	return statements, nil
}

func (c NeonDBClient) deleteBranch(name string) {

	var stdout strings.Builder
	var stderr strings.Builder

	cmd := exec.Command(
		"neon", "branches", "delete", name,
		"--output", "json",
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Default().Println(stderr.String())
		log.Fatal(err)
	}
	log.Default().Println(stdout.String())
}

func (c NeonDBClient) createBranch(name string) string {

	var stdout strings.Builder
	var stderr strings.Builder

	cmd := exec.Command(
		"neon", "branches", "create",
		"--name", name,
		"--output", "json",
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		log.Default().Println(stderr.String())
		log.Fatal(err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout.String()), &result); err != nil {
		log.Fatal(err)
	}

	if uris, ok := result["connection_uris"].([]interface{}); ok {
		if len(uris) > 0 {
			if uri, ok := uris[0].(map[string]interface{})["connection_uri"]; ok {
				return uri.(string)
			}
		}
	}

	log.Fatal("unable to completely create a branch; might have left dangling branches")
	return ""
}

func (c NeonDBClient) Execute(statementSeries [][]Statement) error {
	for i, series := range statementSeries {
		benchmark := Benchmark{}
		benchmark.Start()

		var states []interface{}
		var rows pgx.Rows

		for j, statement := range series {
			if statement.Command != "" {
				db := fmt.Sprintf("db_%v_%v", i, j)
				connStr := c.createBranch(db)

				conn, err := pgx.Connect(context.Background(), connStr)
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

				defer c.deleteBranch(db)
			} else {
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
			if rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return err
				}
				states = append(states, v)
			}
		}
		// dummy "consensus" step here. need to figure out which to choose here.
		// should _not_ close db that wins consensus. instead, make that the new
		// mainConnStr (I think).
		log.Default().Println(states[0])
		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}
