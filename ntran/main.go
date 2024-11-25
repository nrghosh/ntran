package main

import (
	"flag"
	"fmt"
	"log"
	policy "ntran/policy"
	"os"
)

func setupLog(logDir string) (*os.File, error) {
	err := os.RemoveAll(logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to remove directory: %v", err)
	}

	err = os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create directory: %v", err)
	}

	logFile, err := os.OpenFile(logDir+"/out.log", os.O_CREATE|os.O_WRONLY, 0666)

	log.SetOutput(logFile)

	return logFile, err
}

func generateSQL(inFlight int) ([]policy.TestCase, error) {

	var testCases []policy.TestCase
	for key, val := range policy.TestCaseTemplates {
		var statements []policy.Statement
		for i := 0; i < inFlight; i++ {
			statement := policy.Statement{}
			if val.Command != "" {
				statement.Command = fmt.Sprintf(val.Command, i+1)
			}
			if val.Query != "" {
				statement.Query = fmt.Sprintf(val.Query, i+1)
			}
			statements = append(statements, statement)
		}

		testCases = append(testCases, policy.TestCase{Name: key, Statements: statements})
	}

	return testCases, nil
}

func main() {
	policyArg := flag.String("policy", "serial-snapshot", "the policy to run [serial-snapshot, duckdb, cold-neondb, prewarm-neondb]")
	logDirArg := flag.String("log-dir", "./logs", "the directory to write logs to")
	csvDirArg := flag.String("csv-dir", "./results", "the directory to write csv output for analysis input")

	flag.Parse()

	logFile, err := setupLog(*logDirArg)

	if err != nil {
		fmt.Printf("error: %v", err)
	} else {
		fmt.Printf("will print logs to '%s'\n", *logDirArg)
	}

	defer logFile.Close()

	dbClient, err := policy.CreateClient(*policyArg)
	if err != nil {
		log.Fatalf("error creating the database client: %v", err)
	}

	experiment := policy.Experiment{Policy: *policyArg}
	err = experiment.Start(*csvDirArg)
	defer experiment.End()

	if err != nil {
		log.Fatalf("error: %v", err)
	}

	scaffold_schema, err := os.ReadFile("../schemas/schema.sql")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	rollback_schema, err := os.ReadFile("../schemas/rollback.sql")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	for _, inFlight := range dbClient.GetNumTransactionsInFlight() {
		testCases, err := generateSQL(inFlight)
		if err != nil {
			log.Fatalf("error generating the SQL: %v", err)
		}

		for _, testCase := range testCases {
			err = dbClient.Scaffold(string(scaffold_schema), inFlight)
			if err != nil {
				log.Fatalf("error scaffolding the database: %v", err)
			}

			err = dbClient.Execute(testCase, &experiment)
			if err != nil {
				log.Fatalf("error executing: %v", err)
			}

			err = dbClient.Cleanup(string(rollback_schema))
			if err != nil {
				log.Fatalf("error cleaning up: %v", err)
			}
		}

	}
}
