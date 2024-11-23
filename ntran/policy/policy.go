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
	Scaffold(sql string, inFlight int) error
	// GenerateSQL - creates the SQL commands and queries to be benchmarked
	GenerateSQL(inFlight int) ([]TestCase, error)
	// Execute - executes each SQL command and query
	Execute(testCases []TestCase, experiment *Experiment) error
	// Cleanup - resets the DBMS state back to pre-scaffolding state
	Cleanup(sql string) error
}

func CreateClient(policy string) (Policy, error) {
	clientRegistry := []Policy{
		&SerialClient{},
		&DuckDBClient{},
		&ColdNeonDBClient{},
		&PreWarmNeonDBClient{},
	}

	for _, client := range clientRegistry {
		if client.GetName() == policy {
			return client, nil
		}
	}

	return nil, fmt.Errorf("unable to create client of type %s", policy)
}
