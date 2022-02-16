## [0.1.0] - 2022-02-16

Initial release

pg-online-schema-change (`pg-osc`) is a tool for making schema changes (any `ALTER` statements) in Postgres tables with minimal locks, thus helping achieve zero downtime schema changes against production workloads. 

`pg-osc` uses the concept of shadow table to perform schema changes. At a high level, it copies the contents from a primary table to a shadow table, performs the schema change on the shadow table and swaps the table names in the end while preserving all changes to the primary table using triggers (via audit table).

Checkout [Readme](https://github.com/shayonj/pg-online-schema-change#readme) for more details.