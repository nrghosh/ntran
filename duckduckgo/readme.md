# DuckDB Naive Transaction Resolution Benchmarking

## Approach
Super simple MVP for benchmarking performance of DuckDB when attempting to reconcile state from simple/complex queries done by several agents at once 
* Serial - run transactions in order
* Parallel - fork off N copies of DuckDB database per N transactions, run each txn separately, compare states (using checksum) and select majority state to proceed with

### Experiment Setup
1. Modify ```transactionCounts``` in ```main.go``` to vary the number of txns to observe across experiment runs


### DuckDB Setup
0. Edit ```schema.sql``` to modify how many rows are created when table is spun up
1. Ensure DuckDB is installed ```brew install duckdb```
2. If state.db exists, run ```duckdb state.db```
3. Else, to start up duckdb and initialize with ```schema.sql```, run
    ```duckdb state.db``` and from duckdb console ```.read schema.sql```
4. Run ```.tables``` and ``` select * from users; ``` from duckdb console to validate what has been inserted into the duckdb instance


### TODO
- [x] add more data to duckDB on start
- [ ] more complex queries
- [x] consensus logic 
    * using checksum - but consensus not really used yet
- [ ] track granular (i.e CPU i/o, memory, etc) stats for benchmarking
- [x] visualizations using python/matplotlib
