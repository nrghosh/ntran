-- Create users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    balance INTEGER
);

-- Insert (10, 100, 1000) users with a balance of 1000 each
INSERT INTO users (id, balance)
SELECT generate_series AS id, 1000 FROM generate_series(1, 1000);
