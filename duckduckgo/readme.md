## DuckDB transaction benchmarking


Super simple MVP for benchmarking performance of DuckDB when attempting to reconcile state from simple/complex queries done by several agents at once 

### DuckDB Setup
1. Ensure DuckDB is installed ```brew install duckdb```
2. If state.db exists, run ```duckdb state.db```
3. Else, to start up duckdb and initialize with ```schema.sql```, run
    ```duckdb state.db```
    and from duckdb console ```.read schema.sql```
4. Run ```.tables``` and ``` select * from users; ``` from duckdb console to validate what has been inserted into the duckdb instance

### TODO
- add more data to duckDB on start
- more complex queries
- consensus logic 
- track granular (i.e CPU i/o, memory, etc) stats for benchmarking
- visualizationc
