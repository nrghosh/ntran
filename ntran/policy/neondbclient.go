package policy

import "fmt"

type NeonDBClient struct {
}

func (c NeonDBClient) Scaffold() error {
	return fmt.Errorf("NeonDBClient unimplemented")
}

func (c NeonDBClient) GenerateSQL() ([][]Statement, error) {
	return nil, fmt.Errorf("NeonDBClient unimplemented")
}

func (c NeonDBClient) Execute(commands [][]Statement) error {
	return fmt.Errorf("NeonDBClient unimplemented")
}
