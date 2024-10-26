package main

import (
    "database/sql"
    "fmt"
    "log"

    _ "github.com/marcboeker/go-duckdb" // DuckDB driver import
)

func main() {
    dbPath := "./state.db"
    transactionType := "short" // Set to "short" or "long" for different query types

    // Connect to DuckDB database
    db, err := sql.Open("duckdb", dbPath)
    if err != nil {
        log.Fatalf("Failed to open DuckDB database: %v", err)
    }
    defer db.Close()

    fmt.Println("Starting serial execution...")
    SerialExecution(db, 10, transactionType) // Runs 10 serial transactions

    fmt.Println("Starting parallel execution...")
    ParallelExecution(db, 10, transactionType) // Runs 10 parallel transactions
}
