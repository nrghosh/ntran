package main

import (
    "fmt"
    // "log"
    "time"
)

func main() {
    dbPath := "state.db" // Path to DuckDB database
    transactionType := "long" // Type of transaction to run ("short" or "long")
    n := 1 // Number of transactions to run

    fmt.Println("Starting serial execution...")
    startTime := time.Now()
    SerialExecution(dbPath, n, transactionType)
    fmt.Printf("Serial execution completed in %v\n", time.Since(startTime))

    fmt.Println("Starting parallel execution with database copying...")
    startTime = time.Now()
    ParallelExecution(dbPath, n, transactionType)
    fmt.Printf("Parallel execution completed in %v\n", time.Since(startTime))
}

