package main

import (
    "fmt"
    "time"
)

type BenchmarkResult struct {
    StartTime   time.Time
    EndTime     time.Time
    Transaction string
}

func RecordBenchmark(startTime time.Time, transaction string) BenchmarkResult {
    return BenchmarkResult{
        StartTime:   startTime,
        EndTime:     time.Now(),
        Transaction: transaction,
    }
}

func PrintBenchmark(result BenchmarkResult) {
    duration := result.EndTime.Sub(result.StartTime)
    fmt.Printf("Transaction: %s | Duration: %v\n", result.Transaction, duration)
}

