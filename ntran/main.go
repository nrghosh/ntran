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

func main() {
	policyArg := flag.String("policy", "serial-snapshot", "the policy to run [serial-snapshot, duckdb, neondb]")
	logDirArg := flag.String("log-dir", "./logs", "the directory to write logs to")

	flag.Parse()

	logFile, err := setupLog(*logDirArg)

	if err != nil {
		fmt.Printf("error: %v\n", err)
	} else {
		fmt.Printf("will print logs to '%s'\n", *logDirArg)
	}

	defer logFile.Close()

	dbClient, err := policy.CreateClient(*policyArg)
	if err != nil {
		log.Fatalf("error creating the database client: %v", err)
	}
	err = dbClient.Scaffold()
	if err != nil {
		log.Fatalf("error scaffolding the database: %v", err)
	}
	commands, err := dbClient.GenerateSQL()
	if err != nil {
		log.Fatalf("error generating the SQL: %v", err)
	}
	err = dbClient.Execute(commands)
	if err != nil {
		log.Fatalf("error executing: %v", err)
	}
}