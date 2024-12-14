/*
 * This file is only used for cleaning up the database tables 
 * after an experiment is run
 */

DROP INDEX idx_transactions_user_id;
DROP TABLE transactions;
DROP TABLE users;
