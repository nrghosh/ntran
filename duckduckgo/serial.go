package main

import (
    "database/sql"
    "fmt"
    "log"
    _ "github.com/mattn/go-sqlite3" // DuckDB's Go driver behaves similarly to SQLite
)

func SerialExecution(dbPath string, n int, transactionType string) {
    db, err := sql.Open("sqlite3", dbPath)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    for i := 0; i < n; i++ {
        runTransaction(db, transactionType)
    }
}

func runTransaction(db *sql.DB, transactionType string) {
    var query string
    if transactionType == "short" {
        query = "UPDATE users SET balance = balance + 10 WHERE id = 1;"
    } else {
        query = "UPDATE users SET balance = balance + 10 WHERE id > 0;" // Long-running query
    }

    tx, err := db.Begin()
    if err != nil {
        log.Fatalf("Failed to begin transaction: %v", err)
    }

    _, err = tx.Exec(query)
    if err != nil {
        tx.Rollback()
        log.Fatalf("Transaction failed: %v", err)
    } else {
        tx.Commit()
    }

    fmt.Println("Transaction completed.")
}

