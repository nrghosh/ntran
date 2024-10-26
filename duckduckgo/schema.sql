-- Create the users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    balance INTEGER
);

-- Insert 10 users with balance of 1000 each
INSERT INTO users (id, balance) VALUES
    (1, 1000),
    (2, 1000),
    (3, 1000),
    (4, 1000),
    (5, 1000),
    (6, 1000),
    (7, 1000),
    (8, 1000),
    (9, 1000),
    (10, 1000);