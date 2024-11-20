// Short and long queries
package main

const (
	ShortQuery = "UPDATE users SET balance = balance + 10 WHERE id = 1;" // point update,
	LongQuery  = "UPDATE users SET balance = balance + 10 WHERE id > 0;" // full table scan

	// Reference: https://github.com/akopytov/sysbench/blob/master/src/lua/oltp_common.lua
	PointSelects   = "SELECT balance FROM users WHERE id = 1"
	SimpleRanges   = "SELECT balance FROM users WHERE id BETWEEN 2 AND 4"
	SumRanges      = "SELECT SUM(balance) FROM users WHERE id BETWEEN 2 AND 4"
	OrderRanges    = "SELECT balance FROM users WHERE id BETWEEN 2 AND 4 ORDER BY balance"
	DistinctRanges = "SELECT DISTINCT balance FROM users WHERE id BETWEEN 1 AND 4 ORDER BY balance"
	//Deletes        = "DELETE FROM users WHERE id = 2"
	//Inserts        = "INSERT INTO users (id, balance) VALUES (2222, 19283745)"

	// https://github.com/nrghosh/UnitedStatesofDB/issues/2
	PointUpdateIndexed    = "UPDATE users SET balance = balance + 100 WHERE id = 23"
	PointUpdateNonIndexed = `WITH rows_to_update AS (
                                SELECT id
                                FROM users
                                WHERE status = 'inactive'
                                LIMIT 1
                            )
                            UPDATE users
                            SET balance = balance + 50
                            WHERE id IN (SELECT id FROM rows_to_update)`
	// Batch insert syntax supported by Postgres, but not DuckDB?
	BatchInsert = `INSERT INTO transactions (id, user_id, amount)
                   SELECT
                        (g + 5001) AS id,
                        (random() * 999 + 1)::INTEGER AS user_id,
                        500 AS amount
                    FROM generate_series(1, 100) AS g`
	SelectSecondaryIndexed = "SELECT * FROM transactions WHERE user_id = 23"
	SelectScan             = "SELECT * FROM users WHERE balance > 500"
	SelectJoin             = "SELECT u.id, u.balance, COUNT(t.id) as transaction_count, SUM(t.amount) as total_amount FROM users u JOIN transactions t ON u.id = t.user_id WHERE u.id = 23 GROUP BY u.id, u.balance"
)
