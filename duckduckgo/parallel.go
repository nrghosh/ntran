package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	// "time"
)

// DatabaseInstance represents a single DuckDB instance
type DatabaseInstance struct {
	ID       int
	Path     string
	DB       *sql.DB
	Checksum string
}

// ConsensusResult represents the final state after comparing all instances
type ConsensusResult struct {
	MajorityChecksum string
	MajorityCount    int
	TotalInstances   int
	Conflicts        []string
}

// createDBInstance creates a copy of the original database
func createDBInstance(originalPath string, id int) (*DatabaseInstance, error) {
	instancePath := filepath.Join(
		filepath.Dir(originalPath),
		fmt.Sprintf("state_%d.db", id),
	)

	// Copy the original database file
	if err := copyFile(originalPath, instancePath); err != nil {
		return nil, fmt.Errorf("failed to copy DB: %v", err)
	}

	// Open the new database instance
	db, err := sql.Open("duckdb", instancePath)
	if err != nil {
		os.Remove(instancePath)
		return nil, fmt.Errorf("failed to open DB copy: %v", err)
	}

	return &DatabaseInstance{
		ID:   id,
		Path: instancePath,
		DB:   db,
	}, nil
}

// // Add balance checking functionality
// func getBalance(db *sql.DB, id int) (int, error) {
//     var balance int
//     err := db.QueryRow("SELECT balance FROM users WHERE id = ?", id).Scan(&balance)
//     if err != nil {
//         return 0, fmt.Errorf("failed to get balance for id %d: %v", id, err)
//     }
//     return balance, nil
// }

func printDBState(db *sql.DB, label string) error {
	rows, err := db.Query("SELECT id, balance FROM users ORDER BY id")
	if err != nil {
		return fmt.Errorf("failed to query users: %v", err)
	}
	defer rows.Close()

	// fmt.Printf("\n=== Database State: %s ===\n", label)
	for rows.Next() {
		var id, balance int
		if err := rows.Scan(&id, &balance); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}
		// fmt.Printf("User %d: Balance = %d\n", id, balance)
	}
	// fmt.Println("============================")
	return nil
}

func ParallelExecution(originalDBPath string, n int, transactionType string) error {
	// Open original DB to check initial state
	originalDB, err := sql.Open("duckdb", originalDBPath)
	if err != nil {
		return fmt.Errorf("failed to open original database: %v", err)
	}

	// fmt.Println("\nChecking initial state:")
	if err := printDBState(originalDB, "Before Parallel Execution"); err != nil {
		originalDB.Close()
		return err
	}
	originalDB.Close()

	// fmt.Printf("\nCreating %d database instances...\n", n)
	var wg sync.WaitGroup
	instances := make([]*DatabaseInstance, n)

	// Create n copies of the database
	for i := 0; i < n; i++ {
		instance, err := createDBInstance(originalDBPath, i)
		if err != nil {
			return fmt.Errorf("failed to create instance %d: %v", i, err)
		}
		instances[i] = instance
		defer cleanup(instance)
		// fmt.Printf("Created instance %d at %s\n", i, instance.Path)
	}

	// fmt.Printf("\nStarting %d parallel transactions (%s type)...\n", n, transactionType)
	// Run transactions in parallel
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(instanceIdx int) {
			defer wg.Done()
			err := runTransactionOnInstance(instances[instanceIdx], transactionType)
			if err != nil {
				log.Printf("Error in instance %d: %v\n", instanceIdx, err)
			} else {
				// Print state after transaction
				if err := printDBState(instances[instanceIdx].DB,
					fmt.Sprintf("After Transaction on Instance %d", instanceIdx)); err != nil {
					log.Printf("Error printing state for instance %d: %v\n", instanceIdx, err)
				}
			}
		}(i)
	}
	wg.Wait()

	// fmt.Println("\nAll transactions completed. Reaching consensus...")
	// Reach consensus
	result, err := reachConsensus(instances)
	if err != nil {
		return fmt.Errorf("consensus failed: %v", err)
	}

	// fmt.Printf("\nConsensus Results:\n")
	// fmt.Printf("- Majority checksum: %s\n", result.MajorityChecksum[:8])
	// fmt.Printf("- Majority count: %d out of %d instances\n", result.MajorityCount, result.TotalInstances)
	if len(result.Conflicts) > 0 {
		fmt.Printf("- Conflicts found: %d\n", len(result.Conflicts))
		for _, conflict := range result.Conflicts {
			fmt.Printf("  * %s\n", conflict)
		}
	} else {
		fmt.Println("- No conflicts detected")
	}

	// Apply majority state back to original database
	// fmt.Println("\nApplying consensus state to original database...")
	if err := applyConsensusState(originalDBPath, result, instances); err != nil {
		return fmt.Errorf("failed to apply consensus state: %v", err)
	}

	// Verify final state
	finalDB, err := sql.Open("duckdb", originalDBPath)
	if err != nil {
		return fmt.Errorf("failed to open database for final verification: %v", err)
	}
	defer finalDB.Close()

	// fmt.Println("\nFinal database state:")
	if err := printDBState(finalDB, "After Consensus"); err != nil {
		return err
	}

	// fmt.Println("\nParallel execution completed successfully!")
	return nil
}

func runTransactionOnInstance(instance *DatabaseInstance, transactionType string) error {
	// Print initial state
	if err := printDBState(instance.DB, fmt.Sprintf("Before Transaction on Instance %d", instance.ID)); err != nil {
		return err
	}

	var query string
	if transactionType == "short" {
		query = ShortQuery
	} else {
		query = LongQuery
	}

	tx, err := instance.DB.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	if _, err := tx.Exec(query); err != nil {
		tx.Rollback()
		return fmt.Errorf("transaction failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %v", err)
	}

	// Calculate and store checksum after transaction
	checksum, err := calculateDBChecksum(instance.DB)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %v", err)
	}
	instance.Checksum = checksum

	return nil
}

// generate checksum of the database state
func calculateDBChecksum(db *sql.DB) (string, error) {
	// Query all data in a consistent order
	rows, err := db.Query(`
        SELECT id, balance 
        FROM users 
        ORDER BY id
    `)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	hash := sha256.New()
	for rows.Next() {
		var id int
		var balance float64
		if err := rows.Scan(&id, &balance); err != nil {
			return "", err
		}
		// Write values to hash in a consistent format
		fmt.Fprintf(hash, "%d:%.2f;", id, balance)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// find the majority database state
func reachConsensus(instances []*DatabaseInstance) (*ConsensusResult, error) {
	// Count occurrences of each checksum
	checksumCounts := make(map[string]int)
	for _, instance := range instances {
		checksumCounts[instance.Checksum]++
	}

	// Find the majority checksum
	var majorityChecksum string
	var majorityCount int

	for checksum, count := range checksumCounts {
		if count > majorityCount {
			majorityChecksum = checksum
			majorityCount = count
		}
	}

	// Collect conflicting checksums
	var conflicts []string
	for checksum, count := range checksumCounts {
		if checksum != majorityChecksum {
			conflicts = append(conflicts, fmt.Sprintf("checksum: %s, count: %d", checksum, count))
		}
	}
	sort.Strings(conflicts)

	return &ConsensusResult{
		MajorityChecksum: majorityChecksum,
		MajorityCount:    majorityCount,
		TotalInstances:   len(instances),
		Conflicts:        conflicts,
	}, nil
}

// apply majority state back to original db
func applyConsensusState(originalPath string, consensus *ConsensusResult, instances []*DatabaseInstance) error {
	// Find an instance with the majority checksum
	var majorityInstance *DatabaseInstance
	for _, instance := range instances {
		if instance.Checksum == consensus.MajorityChecksum {
			majorityInstance = instance
			break
		}
	}

	if majorityInstance == nil {
		return fmt.Errorf("no instance found with majority checksum")
	}

	// Copy majority instance back to the original path
	return copyFile(majorityInstance.Path, originalPath)
}

// remove temporary database files
func cleanup(instance *DatabaseInstance) {
	if instance.DB != nil {
		instance.DB.Close()
	}
	os.Remove(instance.Path)
}

// copy database files
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
}
