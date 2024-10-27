// Short and long queries
package main

const (
    ShortQuery = "UPDATE users SET balance = balance + 10 WHERE id = 1;" // point update
    LongQuery  = "UPDATE users SET balance = balance + 10 WHERE id > 0;" // full table scan
)
