package main

import (
    "database/sql"
    // "fmt"
    "log"
    // "time"
)

func SerialExecution(db *sql.DB, n int, transactionType string) {

    for i := 0; i < n; i++ {
        runTransaction(db, transactionType)
    }
    


}

func runTransaction(db *sql.DB, transactionType string) {
    var query string
    if transactionType == "short" {
        query = ShortQuery
    } else {
        query = LongQuery
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

    // fmt.Println("Transaction completed.")
}
