package main

import (
    "database/sql"
    "fmt"
    "log"
    "sync"
)

func ParallelExecution(db *sql.DB, n int, transactionType string) {
    var wg sync.WaitGroup
    for i := 0; i < n; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            runParallelTransaction(db, transactionType)
        }()
    }
    wg.Wait()
    fmt.Println("All parallel transactions completed.")
}

func runParallelTransaction(db *sql.DB, transactionType string) {
    var query string
    if transactionType == "short" {
        query = "UPDATE users SET balance = balance + 10 WHERE id = 1;"
    } else {
        query = "UPDATE users SET balance = balance + 10 WHERE id > 0;"
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

    fmt.Println("Parallel transaction completed.")
}
