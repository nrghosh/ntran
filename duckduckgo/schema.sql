-- -- Create users table
-- CREATE TABLE users (
--     id INTEGER PRIMARY KEY,
--     balance INTEGER
-- );

-- -- Insert (10, 100, 1k, 10k, 100k, 1M) users with a balance of 1000 each
-- INSERT INTO users (id, balance)
-- SELECT generate_series AS id, 1000 FROM generate_series(1, 1000);

-- -- File only used for initializing the database, not each individual test cause of
-- -- queries
-- New Schema
CREATE TABLE users (
    id INTEGER PRIMARY KEY,  -- Indexed by default (PRIMARY KEY)
    balance INTEGER,
    status VARCHAR(10)  -- Non-indexed field for table scans
);

CREATE TABLE transactions (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount INTEGER,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- index for join queries
CREATE INDEX idx_transactions_user_id ON transactions(user_id);

-- users - 1000 records
INSERT INTO users (id, balance, status)
SELECT
    generate_series AS id,
    1000 AS balance,
    CASE (generate_series % 2)
        WHEN 0 THEN 'active'
        ELSE 'inactive'
    END AS status
FROM generate_series(1, 1000);

-- transactions - 5000 records
INSERT INTO transactions (id, user_id, amount)
SELECT
    generate_series AS id,
    (random() * 999 + 1)::INTEGER AS user_id,
    (random() * 1000)::INTEGER AS amount
FROM generate_series(1, 5000);