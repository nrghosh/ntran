package main

import (
    "fmt"
    "time"
    "os"
)

type BenchmarkResult struct {
    StartTime   time.Time
    EndTime     time.Time
    Transaction string
    Mode string
}

func RecordBenchmark(startTime time.Time, transaction string, mode string) BenchmarkResult {
    // filename := time.Now().Format("20060102_150405") + ".txt"
    // Open file in write mode, create if it doesn't exist
	// file, err := os.Create(fmt.Sprintf("%s/%s", "logs", filename))
	
	// defer file.Close()

	
    result :=  BenchmarkResult{
        StartTime:   startTime,
        EndTime:     time.Now(),
        Transaction: transaction,
        Mode:        mode,
    }

    // Write duration + txn type to the file
	// _, err = file.WriteString("This is a sample line of text.")
	

	// fmt.Println("Log written successfully:", filename)

    return result
}

func PrintBenchmark(result BenchmarkResult) {
    duration := result.EndTime.Sub(result.StartTime)
    fmt.Printf("Transaction: %s | Duration: %v\n", result.Transaction, duration)
}

func LogBenchmark(result BenchmarkResult) {
	dir := "logs"
	filename := time.Now().Format("20060102_150405") + ".txt"
    // filename := "log.txt"

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		return
	}

	// Open file in write mode, create if it doesn't exist
	file, err := os.OpenFile(fmt.Sprintf("%s/%s", dir, filename),os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error creating logfile:", err)
		return
	}
	defer file.Close()

	// Write info to log file
    duration := result.EndTime.Sub(result.StartTime)
	_, err = fmt.Fprintf(file, "Mode: %s | Transaction: %s | Duration: %v\n", result.Mode, result.Transaction, duration)
	if err != nil {
		fmt.Println("Error writing to logfile:", err)
		return
	}

	fmt.Println("Log written successfully:", filename)
}



