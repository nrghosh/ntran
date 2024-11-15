package policy

import "fmt"

type Statement struct {
	Command string
	Query   string
}

type TestCase struct {
	Name       string
	Statements []Statement
}

type Policy interface {
	// GetName - gets the name of the policy
	GetName() string
	// Scaffold - creates the database schema
	Scaffold() error
	// GenerateSQL - creates the SQL commands and queries to be benchmarked
	GenerateSQL(inFlight int) ([]TestCase, error)
	// Execute - executes each SQL command and query
	Execute(testCases []TestCase) error
	// Cleanup - resets the DBMS state back to pre-scaffolding state
	Cleanup() error
}

func CreateClient(policy string) (Policy, error) {
	switch policy {
	case "serial-snapshot":
		return &SerialClient{}, nil
	case "duckdb":
		return &DuckDBClient{}, nil
	case "neondb":
		return &NeonDBClient{}, nil
	default:
		return nil, fmt.Errorf("unable to create client of type %s", policy)
	}
}
