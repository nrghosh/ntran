package main

import (
    "database/sql"
    "encoding/csv"
    "fmt"
    "log"
    "os"
    "time"
    _ "github.com/marcboeker/go-duckdb" // DuckDB driver import
)

const (
    dbPath     = "./state.db"
    schemaPath = "./schema.sql"
)

type ExperimentConfig struct {
    TransactionType   string
    TransactionCount  int
    ExecutionMode     string
}

type Benchmark struct {
    TransactionType   string
    ExecutionMode     string
    TransactionCount  int
    ElapsedTime       float64
}

func initDatabase() (*sql.DB, error) {
    if _, err := os.Stat(dbPath); err == nil {
        if err := os.Remove(dbPath); err != nil {
            return nil, fmt.Errorf("failed to delete existing database file: %v", err)
        }
    }

    db, err := sql.Open("duckdb", dbPath)
    if err != nil {
        return nil, fmt.Errorf("failed to create new DuckDB database: %v", err)
    }

    schema, err := os.ReadFile(schemaPath)
    if err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to read schema file: %v", err)
    }
    if _, err := db.Exec(string(schema)); err != nil {
        db.Close()
        return nil, fmt.Errorf("failed to initialize database schema: %v", err)
    }

    fmt.Println("Database initialized successfully with schema.")
    return db, nil
}

func RecordBenchmark(startTime time.Time, transactionType, executionMode string, transactionCount int) Benchmark {
    elapsedTime := time.Since(startTime).Seconds()
    return Benchmark{
        TransactionType:  transactionType,
        ExecutionMode:    executionMode,
        TransactionCount: transactionCount,
        ElapsedTime:      elapsedTime,
    }
}

func logExperiment(csvWriter *csv.Writer, benchmark Benchmark) {
    record := []string{
        benchmark.TransactionType,
        benchmark.ExecutionMode,
        fmt.Sprintf("%d", benchmark.TransactionCount),
        fmt.Sprintf("%.2f", benchmark.ElapsedTime),
    }
    if err := csvWriter.Write(record); err != nil {
        log.Fatalf("Failed to write CSV record: %v", err)
    }
    csvWriter.Flush()
}

func runExperiment(config ExperimentConfig, csvWriter *csv.Writer) error {
    fmt.Printf("Running %s execution with transaction type '%s' and count %d...\n", config.ExecutionMode, config.TransactionType, config.TransactionCount)

    startTime := time.Now()
    switch config.ExecutionMode {
    case "serial":
        db, err := initDatabase()
        if err != nil {
            return err
        }
        defer db.Close()
        SerialExecution(db, config.TransactionCount, config.TransactionType)
    case "parallel":
        db, err := initDatabase()
        if err != nil {
            return err
        }
        defer db.Close()
        if err := ParallelExecution(dbPath, config.TransactionCount, config.TransactionType); err != nil {
            return fmt.Errorf("parallel execution failed: %v", err)
        }
    default:
        return fmt.Errorf("unknown execution mode: %s", config.ExecutionMode)
    }

    benchmark := RecordBenchmark(startTime, config.TransactionType, config.ExecutionMode, config.TransactionCount)
    logExperiment(csvWriter, benchmark)

    fmt.Printf("Execution completed in %.2f seconds\n", benchmark.ElapsedTime)
    return nil
}

func main() {
    transactionTypes := []string{"short", "long"}
    executionModes := []string{"serial", "parallel"}
    transactionCounts := []int{10, 25, 50, 100, 200, 500}

    file, err := os.Create("experiment_results.csv")
    if err != nil {
        log.Fatalf("Failed to create CSV file: %v", err)
    }
    defer file.Close()
    csvWriter := csv.NewWriter(file)
    defer csvWriter.Flush()

    headers := []string{"TransactionType", "ExecutionMode", "TransactionCount", "ElapsedTime"}
    if err := csvWriter.Write(headers); err != nil {
        log.Fatalf("Failed to write CSV headers: %v", err)
    }

    for _, transactionType := range transactionTypes {
        for _, executionMode := range executionModes {
            for _, transactionCount := range transactionCounts {
                config := ExperimentConfig{
                    TransactionType:  transactionType,
                    ExecutionMode:    executionMode,
                    TransactionCount: transactionCount,
                }
                if err := runExperiment(config, csvWriter); err != nil {
                    log.Printf("Experiment failed for %v: %v", config, err)
                }
            }
        }
    }
}
