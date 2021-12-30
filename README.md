# pg-online-schema-change

pg-online-schema-change is a tool for making schema changes in Postgres tables with minimal locks, thus helping achieve zero down time schema changes against production workloads. 

pg-online-schema-change is inspired from the design and workings of tools like `pg_repack` and `pt-online-schema-change` for MySQL. Read more [below](#how-does-it-work) on how it works in action.

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
- PostgreSQL 9.3 and later
- Ruby 2.6 and later

## Usage

```
Usage:
  pg-online-schema-change perform -a, --alter-statement=ALTER_STATEMENT -d, --dbname=DBNAME -h, --host=HOST -p, --port=N -s, --schema=SCHEMA -u, --username=USERNAME -w, --password=PASSWORD

Options:
  -a, --alter-statement=ALTER_STATEMENT  # The ALTER statement to perform the schema change
  -s, --schema=SCHEMA                    # The schema in which the table is
                                         # Default: public
  -d, --dbname=DBNAME                    # Name of the database
  -h, --host=HOST                        # Server host where the Database is located
  -u, --username=USERNAME                # Username for the Database
  -p, --port=N                           # Port for the Database
                                         # Default: 5432
  -w, --password=PASSWORD                # Password for the Database
  -v, [--verbose], [--no-verbose]        # Emit logs in debug mode
```

```
Usage:
  pg-online-schema-change --version, -v

print the version
```

## Caveats
- For a brief moment, towards the end of the process pgsoc will acquire a `ACCESS EXCLUSIVE lock` to perform the swap of table name and FK references
- By design it doesn't kill any other DDLs being performed. Its best to not run any DDLs against parent table during the process to avoid any issues.
- During the nature of duplicating a table, there needs to be enough space on the disk to support the operation.

## How does it work

- Primary table: A table against which a potential schema change is to be run
- Shadow table: A copy of an existing primary table
- Audit table: A table to store any updates/inserts/delete on a primary table

1. Create an audit table to record changes made to the parent table
2. Add a trigger on the parent table (for inserts, updates, deletes) to our audit table
3. Create new shadow table with all rows from old table. 
4. Run ALTER/migration on the shadow table.
5. Build indexes on the new table.
6. Replay all changes accumulated in the audit table against the shadow table
   - Delete rows in audit table as they are replayed
7. Once the delta (reamaining rows) is ~20 rows, acquire an access exclusive lock against the parent table, within a transaction and:
   - swap table names (shadow table <> parent table)
   - update references in other tables (FKs) by dropping and re-creating the FKs with a `NOT VALID`
8. Drop parent (now old) table (OPTIONAL)

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `bundle exec spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[shayonj]/pg-online-schema-change. 

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the PgOnlineSchemaChange project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/shayonj/pg-online-schema-change/blob/main/CODE_OF_CONDUCT.md).
