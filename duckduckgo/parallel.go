package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"
    // "path/filepath"
    "io"
    "sync"
    _ "github.com/mattn/go-sqlite3"
)

func ParallelExecution(dbPath string, n int, transactionType string) {
    var wg sync.WaitGroup
    wg.Add(n)

    for i := 0; i < n; i++ {
        go func(i int) {
            defer wg.Done()

            copiedDBPath := copyDatabase(dbPath, i)
            db, err := sql.Open("sqlite3", copiedDBPath)
            if err != nil {
                log.Fatalf("Failed to open copied database: %v", err)
            }
            defer db.Close()

            runTransaction(db, transactionType)

            os.Remove(copiedDBPath) // Cleanup copied DB file after use
        }(i)
    }

    wg.Wait()
}

func copyDatabase(dbPath string, id int) string {
    copiedDBPath := fmt.Sprintf("state_copy_%d.db", id)
    input, err := os.Open(dbPath)
    if err != nil {
        log.Fatalf("Failed to open original database: %v", err)
    }
    defer input.Close()

    output, err := os.Create(copiedDBPath)
    if err != nil {
        log.Fatalf("Failed to create copied database: %v", err)
    }
    defer output.Close()

    _, err = io.Copy(output, input)
    if err != nil {
        log.Fatalf("Failed to copy database: %v", err)
    }

    return copiedDBPath
}

