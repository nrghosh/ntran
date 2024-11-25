package policy

import (
	"encoding/csv"
	"fmt"
	"os"
	"time"
)

type Experiment struct {
	Policy    string
	csvWriter *csv.Writer
	csvFile   *os.File
}

type Record struct {
	Policy           string
	TestCase         string
	TransactionCount string
	Duration         string
}

func (e *Experiment) Start(csvDirArg string) error {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	filepath := fmt.Sprintf("%s/%s_%s.csv", csvDirArg, e.Policy, timestamp)
	var err error
	e.csvFile, err = os.Create(filepath)
	if err != nil {
		return err
	}
	e.csvWriter = csv.NewWriter(e.csvFile)
	headers := []string{"Policy", "TestCase", "TransactionCount", "Duration"}
	if err := e.csvWriter.Write(headers); err != nil {
		e.csvFile.Close()
		return err
	}
	e.csvWriter.Flush()
	return nil
}

func (e *Experiment) Log(record Record) error {
	err := e.csvWriter.Write([]string{
		record.Policy,
		record.TestCase,
		record.TransactionCount,
		record.Duration,
	})
	if err != nil {
		e.csvFile.Close()
		return err
	}
	e.csvWriter.Flush()
	return nil
}

func (e *Experiment) End() {
	e.csvFile.Close()
}
