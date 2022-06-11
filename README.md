# poc-partitioning

Contains SQL script for automatic partition creation using only trigger and plpsql function.

## Setup
1. Run docker compose
2. Execute all DDL in seed-ddl.sql
3. Execute all DDL in function.sql (Tune the range of message_id per partition using constant per_partition_count in this file) 
4. Start testing using tester.sql
