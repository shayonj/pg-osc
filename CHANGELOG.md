## [0.4.0] - 2022-02-22
* Lint sourcecode, setup Rubocop proper and Lint in CI by @shayonj in https://github.com/shayonj/pg-osc/pull/46
* Uniquely identify operation_type column by @shayonj in https://github.com/shayonj/pg-osc/pull/50
* Introduce primary key on audit table for ordered reads by @shayonj in https://github.com/shayonj/pg-osc/pull/49
  - This addresses an edge case with replay.
* Uniquely identify trigger_time column by @shayonj in https://github.com/shayonj/pg-osc/pull/51
* Abstract assertions into a helper function by @shayonj in https://github.com/shayonj/pg-osc/pull/52

## [0.3.0] - 2022-02-21

- Explicitly call dependencies and bump dependencies by @shayonj https://github.com/shayonj/pg-osc/pull/44
- Introduce Dockerfile and release process https://github.com/shayonj/pg-osc/pull/45

## [0.2.0] - 2022-02-17

- Use ISOLATION LEVEL SERIALIZABLE ([#42](https://github.com/shayonj/pg-osc/pull/42)) (props to @jfrost)

## [0.1.0] - 2022-02-16

Initial release

pg-online-schema-change (`pg-osc`) is a tool for making schema changes (any `ALTER` statements) in Postgres tables with minimal locks, thus helping achieve zero downtime schema changes against production workloads. 

`pg-osc` uses the concept of shadow table to perform schema changes. At a high level, it copies the contents from a primary table to a shadow table, performs the schema change on the shadow table and swaps the table names in the end while preserving all changes to the primary table using triggers (via audit table).

Checkout [Readme](https://github.com/shayonj/pg-osc#readme) for more details.