package policy

import (
	"fmt"
)

type Policy interface {
	// GetName - gets the name of the policy
	GetName() string
	// GetNumTransactionsInFlight - gets the slices of numbers of concurrent transactions to test
	GetNumTransactionsInFlight() []int
	// Scaffold - creates the database schema
	Scaffold(sql string, inFlight int) error
	// Execute - executes each SQL command and query
	Execute(testCase TestCase, experiment *Experiment) error
	// Cleanup - resets the DBMS state back to pre-scaffolding state
	Cleanup(sql string) error
}

func CreateClient(policy string) (Policy, error) {
	clientRegistry := []Policy{
		&SerialClient{},
		&DuckDBParallelClient{},
		&DuckDBSerialClient{},
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
