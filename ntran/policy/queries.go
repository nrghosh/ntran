// Short and long queries
package policy

type Statement struct {
	Command string
	Query   string
}

type TestCase struct {
	Name       string
	Statements []Statement
}

var TestCaseTemplates = map[string]Statement{
	"Short Update": {Command: "UPDATE users SET balance = balance + %d WHERE id = 1;", Query: "SELECT %d, * FROM users WHERE id = 1"},                            // point update
	"Long Update":  {Command: "UPDATE users SET balance = balance + %d WHERE status = 'inactive';", Query: "SELECT %d, * FROM users WHERE status = 'inactive';"}, // full table scan

	// Reference: https://github.com/akopytov/sysbench/blob/master/src/lua/oltp_common.lua
	"Point Select":    {Query: "SELECT %d, balance FROM users WHERE id = 1;"},
	"Simple Ranges":   {Query: "SELECT %d, balance FROM users WHERE id BETWEEN 2 AND 4;"},
	"Sum Ranges":      {Query: "SELECT %d, SUM(balance) FROM users WHERE id BETWEEN 4 AND 4;"},
	"Order Ranges":    {Query: "SELECT %d, balance FROM users WHERE id BETWEEN 2 AND 4 ORDER BY balance;"},
	"Distinct Ranges": {Query: "SELECT DISTINCT %d, balance FROM users WHERE id BETWEEN 1 AND 4 ORDER BY balance;"},
	"Short Delete":    {Command: "DELETE FROM transactions WHERE user_id = 2; -- %d", Query: "SELECT %d, * from transactions WHERE user_id = 2;"},
	"Short Insert":    {Command: "INSERT INTO users (id, balance) VALUES (200000, %d)", Query: "SELECT %d, * FROM users WHERE id = 200000;"},

	// https://github.com/nrghosh/UnitedStatesofDB/issues/2
	"Point Update Indexed": {Command: "UPDATE users SET balance = balance + %d WHERE id = 23;", Query: "SELECT %d, * FROM users WHERE id = 23;"},
	"Point Update Non-Indexed": {Command: `WITH rows_to_update AS (
		SELECT id
		FROM users
		WHERE status = 'inactive'
		LIMIT 1
	)
	UPDATE users
	SET balance = balance + %d
	WHERE id IN (SELECT id FROM rows_to_update);`, Query: "SELECT %d, * FROM users;"},

	// Batch insert syntax supported by Postgres, but not DuckDB?
	"Batched Insert": {Command: `INSERT INTO transactions (id, user_id, amount)
	SELECT
		(g + 5001) AS id,
		(random() * 999 + 1)::INTEGER AS user_id,
		(500 + %d) AS amount
	FROM generate_series(1, 100) AS g;`, Query: "SELECT %d, * FROM transactions;"},

	"Select Secondary Index": {Query: "SELECT %d, * FROM transactions WHERE user_id = 23;"},
	"Select Scan":            {Query: "SELECT %d, * FROM users WHERE balance > 500;"},
	"Select Join": {Query: `SELECT %d, u.id, u.balance, COUNT(t.id) as transaction_count, SUM(t.amount) as total_amount
							FROM users u
							JOIN transactions t ON u.id = t.user_id
							WHERE u.id = 23
							GROUP BY u.id, u.balance;`},
}

var TestCaseTemplatesLite = map[string]Statement{
	"Long Update":  TestCaseTemplates["Long Update"],
	"Short Insert": TestCaseTemplates["Short Insert"],
	"Select Scan":  TestCaseTemplates["Select Scan"],
	"Select Join":  TestCaseTemplates["Select Join"],
}
