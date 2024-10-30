package policy

import "fmt"

type Statement struct {
	Command string
	Query   string
}

type Policy interface {
	// Scaffold - creates the database schema
	Scaffold() error
	// GenerateSQL - creates the SQL commands and queries to be benchmarked
	GenerateSQL() ([][]Statement, error)
	// Execute - executes each SQL command and query
	Execute(statementSeries [][]Statement) error
}

func CreateClient(policy string) (Policy, error) {
	switch policy {
	case "serial-snapshot":
		return SerialClient{}, nil
	case "duckdb":
		return DuckDBClient{}, nil
	case "neondb":
		return NeonDBClient{}, nil
	default:
		return nil, fmt.Errorf("unable to create client of type %s", policy)
	}
}
