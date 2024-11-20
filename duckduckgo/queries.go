// Short and long queries
package main

const (
	ShortQuery = "UPDATE users SET balance = balance + 10 WHERE id = 1;" // point update,
	LongQuery  = "UPDATE users SET balance = balance + 10 WHERE id > 0;" // full table scan

	// Reference: https://github.com/akopytov/sysbench/blob/master/src/lua/oltp_common.lua
	// Considerations:
	// Make column and table name variable, randomly select values for ranges?
	PointSelects   = "SELECT balance FROM users WHERE id = 1"
	SimpleRanges   = "SELECT balance FROM users WHERE id BETWEEN 2 AND 4"
	SumRanges      = "SELECT SUM(balance) FROM users WHERE id BETWEEN 2 AND 4"
	OrderRanges    = "SELECT balance FROM users WHERE id BETWEEN 2 AND 4 ORDER BY balance"
	DistinctRanges = "SELECT DISTINCT balance FROM users WHERE id BETWEEN 1 AND 4 ORDER BY balance"
	Deletes        = "DELETE FROM users WHERE id = 2"
	Inserts        = "INSERT INTO users (id, balance) VALUES (1001, 19283745)"
)
