package policy

import "fmt"

/*
 * SerialClient - executes its statements serially, under
 * one snapshost isolation level transaction, rolling back state
 * after each command (after selecting the final results).
 *
 * [PM]: We can use any database driver for this client. Perhaps let's use Postgres?
 */
type SerialClient struct {
}

func (c SerialClient) Scaffold() error {
	return fmt.Errorf("SerialClient unimplemented")
}

func (c SerialClient) GenerateSQL() ([][]Statement, error) {
	return nil, fmt.Errorf("SerialClient unimplemented")
}

func (c SerialClient) Execute(statementSeries [][]Statement) error {
	return fmt.Errorf("SerialClient unimplemented")
}
