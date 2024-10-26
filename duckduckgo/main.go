package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"

    _ "github.com/marcboeker/go-duckdb" // DuckDB driver import
)

const dbPath = "./state.db"
const schemaPath = "./schema.sql"

// Initializes the DuckDB database
func initDatabase() {
    // Remove the existing state.db file if it exists
    if _, err := os.Stat(dbPath); err == nil {
        fmt.Println("Removing existing database file...")
        if err := os.Remove(dbPath); err != nil {
            log.Fatalf("Failed to delete existing database file: %v", err)
        }
    }

    // Connect to DuckDB (this will create a new empty database file)
    db, err := sql.Open("duckdb", dbPath)
    if err != nil {
        log.Fatalf("Failed to create new DuckDB database: %v", err)
    }
    defer db.Close()

    // Read the schema file
    schema, err := os.ReadFile(schemaPath)
    if err != nil {
        log.Fatalf("Failed to read schema file: %v", err)
    }

    // Execute the schema to set up the initial structure of the database
    _, err = db.Exec(string(schema))
    if err != nil {
        log.Fatalf("Failed to initialize database schema: %v", err)
    }

    fmt.Println("Database initialized successfully with schema.")
}

func main() {
    // Initialize db environment
    initDatabase()

    transactionType := "short" // Set to "short" or "long" for different query types

    // Connect to newly initialized DuckDB instance
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
