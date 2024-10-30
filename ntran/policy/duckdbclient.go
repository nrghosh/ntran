package policy

import (
	"database/sql"

	_ "github.com/marcboeker/go-duckdb" // this doesn't work on Windows :-(
)

type DuckDBClient struct {
}

func (c DuckDBClient) Scaffold() error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, balance INTEGER);")
	if err != nil {
		return err
	}

	return nil
}

func (c DuckDBClient) GenerateSQL() ([][]Statement, error) {
	statements := [][]Statement{
		{
			{Command: "INSERT INTO users (id, balance) VALUES (1, 100);", Query: "SELECT * FROM users;"},
			{Command: "INSERT INTO users (id, balance) VALUES (1, 200);", Query: "SELECT * FROM users;"},
		},
	}

	return statements, nil
}

func (c DuckDBClient) Execute(statementSeries [][]Statement) error {
	for i, series := range statementSeries {
		benchmark := Benchmark{}
		benchmark.Start()
		for _, statement := range series {
			if statement.Command != "" {
				// we are executing a command, which will change the state of the database
				// so we should create a file copy (an improvement could be that we somehow
				// figure out the result of the command beforehand to reduce a potentially
				// redundant file copy?)
				// we will use the associated query to see the resulting state after the command
			} else {
				// we are executing a query, so we don't need to fork the database
			}
		}
		benchmark.End()
		benchmark.Log(i)
	}
	return nil
}
