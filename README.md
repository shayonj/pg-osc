# pg-online-schema-change / pg-osc
[![CircleCI](https://circleci.com/gh/shayonj/pg-online-schema-change/tree/main.svg?style=shield)](https://circleci.com/gh/shayonj/pg-online-schema-change/tree/main)

pg-online-schema-change (`pg-osc`) is a tool for making schema changes (any `ALTER` statements) in Postgres tables with minimal locks, thus helping achieve zero downtime schema changes against production workloads. 

`pg-osc` uses the concept of shadow table to perform schema changes. At a high level, it copies the contents from a primary table to a shadow table, performs the schema change on the shadow table and swaps the table names in the end while preserving all changes to the primary table using triggers (via audit table).

`pg-osc` is inspired by the design and workings of tools like `pg_repack` and `pt-online-schema-change` (MySQL). Read more below on [how does it work](#how-does-it-work), [prominent features](#prominent-features), the [caveats](#caveats) and [examples](#examples)

⚠️ ⚠️ THIS IS CURRENTLY WIP AND IS CONSIDERED EXPERIMENTAL ⚠️ ⚠️ 

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pg_online_schema_change'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install pg_online_schema_change

## Requirements
- PostgreSQL 9.6 and later
- Ruby 2.6 and later
- Database user should have permissions for `TRIGGER` and/or a `SUPERUSER`

## Usage

```
Usage:
  pg-online-schema-change perform -a, --alter-statement=ALTER_STATEMENT -d, --dbname=DBNAME -h, --host=HOST -p, --port=N -s, --schema=SCHEMA -u, --username=USERNAME -w, --password=PASSWORD

Options:
  -a, --alter-statement=ALTER_STATEMENT        # The ALTER statement to perform the schema change
  -s, --schema=SCHEMA                          # The schema in which the table is
                                               # Default: public
  -d, --dbname=DBNAME                          # Name of the database
  -h, --host=HOST                              # Server host where the Database is located
  -u, --username=USERNAME                      # Username for the Database
  -p, --port=N                                 # Port for the Database
                                               # Default: 5432
  -w, --password=PASSWORD                      # Password for the Database
  -v, [--verbose], [--no-verbose]              # Emit logs in debug mode
  -f, [--drop], [--no-drop]                    # Drop the original table in the end after the swap
  -k, [--kill-backends], [--no-kill-backends]  # Kill other competing queries/backends when trying to acquire lock for the shadow table creation and swap. It will wait for --wait-time-for-lock duration before killing backends and try upto 3 times.
  -w, [--wait-time-for-lock=N]                 # Time to wait before killing backends to acquire lock and/or retrying upto 3 times. It will kill backends if --kill-backends is true, otherwise try upto 3 times and exit if it cannot acquire a lock.
                                               # Default: 10
  -c, [--copy-statement=COPY_STATEMENT]        # Takes a .sql file location where you can provide a custom query to be played (ex: backfills) when pg-osc copies data from the primary to the shadow table. More examples in README.
```

```
Usage:
  pg-online-schema-change --version, -v

print the version
```
## How does it work

- **Primary table**: A table against which a potential schema change is to be run
- **Shadow table**: A copy of an existing primary table
- **Audit table**: A table to store any updates/inserts/delete on a primary table

1. Create an audit table to record changes made to the parent table.
2. Acquire a brief `ACCESS EXCLUSIVE` lock to add a trigger on the parent table (for inserts, updates, deletes) to the audit table.
3. Create a new shadow table and run ALTER/migration on the shadow table. 
4. Copy all rows from the old table.
5. Build indexes on the new table.
6. Replay all changes accumulated in the audit table against the shadow table.
   - Delete rows in the audit table as they are replayed.
7. Once the delta (remaining rows) is ~20 rows, acquire an `ACCESS EXCLUSIVE` lock against the parent table within a transaction and:
   - swap table names (shadow table <> parent table).
   - update references in other tables (FKs) by dropping and re-creating the FKs with a `NOT VALID`.
8. Runs `ANALYZE` on the new table.
9. Validates all FKs that were added with `NOT VALID`.
10. Drop parent (now old) table (OPTIONAL).

## Prominent features
- `pg-osc` supports when a column is being added, dropped or renamed with no data loss. 
- `pg-osc` acquires minimal locks throughout the process (read more below on the caveats).
- Copies over indexes and Foreign keys.
- Optionally drop or retain old tables in the end.
- Backfill old/new columns as data is copied from primary table to shadow table, and then perform the swap. [Example](#Backfill-data-and-run-alter-statements)
- **TBD**: Ability to reverse the change with no data loss. [tracking issue](https://github.com/shayonj/pg-online-schema-change/issues/14)

## Examples

### Renaming a column
```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books RENAME COLUMN email TO new_email' \
  --dbname "postgres" \
  --host "localhost" \
  --username "jamesbond" \
  --password "" \
```

### Multiple ALTER statements
```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE; ALTER TABLE books RENAME COLUMN email TO new_email;' \
  --dbname "postgres" \
  --host "localhost" \
  --username "jamesbond" \
  --password "" \
  --drop
```

### Kill other backends after 5s
If the operation is being performed on a busy table, you can use `pg-osc`'s `kill-backend` functionality to kill other backends that may be competing with the `pg-osc` operation to acquire a lock for a brief while. The `ACCESS EXCLUSIVE` lock acquired by `pg-osc` is only for a brief while and released after. You can tune how long `pg-osc` should wait before killing other backends (or if at all `pg-osc` should kill backends in the first place).

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;' \
  --dbname "postgres" \
  --host "localhost" \
  --username "jamesbond" \
  --password "" \
  --wait-time-for-lock=5 \
  --kill-backends \
  --drop
```
### Backfill data
When inserting data into the shadow table, instead of just copying all columns and rows from the primary table, you can pass in a custom sql file to perform the copy and do any additional work. For instance - backfilling certain columns. By providing the `copy-statement`, `pg-osc` will instead play the query to perform the copy operation.

**IMPORTANT NOTES:**
- It is possible to violate a constraint accidentally or not copy data, **so proceed with caution**.
- The `ALTER` statement can change the table's structure, **so proceed with caution**.
- Preserve `%{shadow_table}` as that will be replaced with the destination of the shadow table.

```sql
-- file: /src/query.sql
INSERT INTO %{shadow_table}(foo, bar, baz, rental_id, tenant_id)
SELECT a.foo,a.bar,a.baz,a.rental_id,r.tenant_id AS tenant_id
FROM ONLY examples a
LEFT OUTER JOIN rentals r
ON a.rental_id = r.id
```

```
pg-online-schema-change perform \
  --alter-statement 'ALTER TABLE books ADD COLUMN "tenant_id" VARCHAR;' \
  --dbname "postgres" \
  --host "localhost" \
  --username "jamesbond" \
  --password "" \
  --copy-statement "/src/query.sql" \
  --drop
```
### Caveats
- A primary key should exist on the table; without it, `pg-osc` will raise an exception
	- This is because - currently there is no other way to uniquely identify rows during replay.
- `pg-osc` will acquire `ACCESS EXCLUSIVE` lock on the parent table twice during the.
	- First, when setting up the triggers and the shadow table.
	- Next, when performing the swap and updating FK references
	- Note: If `kill-backends` is passed, it will attempt to terminate any competing operations during both times. 
- By design, `pg-osc` doesn't kill any other DDLs being performed. It's best to not run any DDLs against the parent table during the operation.
- Due to the nature of duplicating a table, there needs to be enough space on the disk to support the operation.
- Index, constraints and sequence names will be altered and lose their original naming.
	- Can be fixed in future releases. Feel free to open a feature req.
- Triggers are not carried over. 
- Can be fixed in future releases. Feel free to open a feature req.
- Foreign keys are dropped & re-added to referencing tables with a `NOT VALID`. A follow on `VALIDATE CONSTRAINT` is run.
 	- Ensures that integrity is maintained and re-introducing FKs doesn't acquire additional locks, hence the `NOT VALID`.
## Development

- Install ruby 3.0
```
\curl -sSL https://get.rvm.io | bash

rvm install 3.0.0

rvm use 3.0.0
```
- Spin up postgres via Docker Compose - `docker compose up`
- Then, run `bundle exec spec` to run the tests. 
- You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. 

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/shayonj/pg-online-schema-change. 

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the PgOnlineSchemaChange project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/shayonj/pg-online-schema-change/blob/main/CODE_OF_CONDUCT.md).
