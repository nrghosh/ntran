package policy

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"golang.org/x/exp/rand"
)

type NeonDBClient struct {
	mainConnStr string
}

func (c NeonDBClient) GetName() string {
	return "neondb"
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

func (c NeonDBClient) GenerateSQL() ([]TestCase, error) {
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

func (c NeonDBClient) Execute(testCases []TestCase) error {
	rand.Seed(uint64(time.Now().UnixNano()))
	for i, testCase := range testCases {
		benchmark := Benchmark{Policy: c.GetName(), TestCase: testCase.Name}
		benchmark.Start()

		var states []interface{}

		// cannot run the neon commands in parallel
		// https://community.neon.tech/t/project-already-has-running-operations-scheduling-of-new-ones-is-prohibited/242/3.
		// alternatively, we could create the compute nodes serially, then execute the queries in parallel, then delete the
		// compute nodes serially.
		for j, statement := range testCase.Statements {

			var rows pgx.Rows
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
		// dummy "consensus" step here -- take a random one.
		// should _not_ close db that wins consensus.
		// instead, make that the new mainConnStr (I think).
		idx := rand.Intn(len(states))
		log.Default().Printf("idx: %v; state: %v\n", idx, states[idx])
		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}
