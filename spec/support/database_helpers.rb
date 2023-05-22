# frozen_string_literal: true

module DatabaseHelpers
  def schema
    ENV["POSTGRES_SCHEMA"] || "test_schema"
  end

  def client_options
    options = {
      alter_statement: 'ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;',
      schema: schema,
      dbname: ENV["PGDATABASE"] || "postgres",
      host: ENV["PGHOST"] || "127.0.0.1",
      username: ENV["PGUSER"] || "jamesbond",
      password: ENV["PGPASSWORD"] || "password",
      port: ENV["PGPORT"] || 5432,
      drop: false,
      kill_backends: false,
      wait_time_for_lock: 5,
      delta_count: 20,
      pull_batch_count: 1000,
      copy_statement: "",
    }
    Struct.new(*options.keys).new(*options.values)
  end

  def new_dummy_table_sql
    <<~SQL
      CREATE SCHEMA IF NOT EXISTS #{schema};

      CREATE TABLE IF NOT EXISTS #{schema}.sellers (
        id serial PRIMARY KEY,
        name VARCHAR ( 50 ) UNIQUE NOT NULL,
        "createdOn" TIMESTAMP NOT NULL,
        last_login TIMESTAMP
      );

      CREATE TABLE IF NOT EXISTS #{schema}.books (
        user_id serial PRIMARY KEY,
        username VARCHAR ( 50 ) UNIQUE NOT NULL,
        seller_id SERIAL REFERENCES #{schema}.sellers NOT NULL,
        password VARCHAR ( 50 ) NOT NULL,
        email VARCHAR ( 255 ) UNIQUE NOT NULL,
        "createdOn" TIMESTAMP NOT NULL,
        last_login TIMESTAMP
      ) WITH (autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000);

      CREATE TABLE IF NOT EXISTS #{schema}.book_audits (
        id serial PRIMARY KEY,
        book_id SERIAL REFERENCES #{schema}.books NOT NULL,
        changed_on TIMESTAMP(6) NOT NULL
      );

      CREATE OR REPLACE FUNCTION email_changes()
        RETURNS TRIGGER
        LANGUAGE PLPGSQL
        AS
      $$
      BEGIN
        IF NEW.email <> OLD.email THEN
          INSERT INTO book_audits(book_id,changed_on)
          VALUES(OLD.id,now());
        END IF;
        RETURN NEW;
      END;
      $$;

      DROP TRIGGER IF EXISTS email_changes on #{schema}.books;
      CREATE TRIGGER email_changes
      AFTER UPDATE
      ON #{schema}.books
      FOR EACH ROW
      EXECUTE PROCEDURE email_changes();

      CREATE TABLE IF NOT EXISTS #{schema}.chapters (
        id serial PRIMARY KEY,
        name VARCHAR ( 50 ) UNIQUE NOT NULL,
        book_id SERIAL REFERENCES #{schema}.books NOT NULL,
        book_name VARCHAR ( 50 ),
        "createdOn" TIMESTAMP NOT NULL,
        last_login TIMESTAMP
      );

      ALTER ROLE jamesbond SET statement_timeout = '60s';
      ALTER ROLE jamesbond SET lock_timeout = '60s';
    SQL
  end

  def setup_tables(client = nil)
    cleanup_dummy_tables(client)
    create_dummy_tables(client)
    PgOnlineSchemaChange::Query.run(client.connection, "SET search_path TO #{client.schema};")
  end

  def create_dummy_tables(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    PgOnlineSchemaChange::Query.run(client.connection, new_dummy_table_sql)
  end

  def ingest_dummy_data_into_dummy_table(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    query = <<~SQL
      INSERT INTO "#{schema}"."sellers"("name", "createdOn", "last_login")
      VALUES('local shop', clock_timestamp(), clock_timestamp());

      INSERT INTO "#{schema}"."books"("seller_id", "username", "password", "email", "createdOn", "last_login")
      VALUES
        (1, 'jamesbond2', '007', 'james1@bond.com', clock_timestamp(), clock_timestamp()),
        (1, 'jamesbond3', '008', 'james2@bond.com', clock_timestamp(), clock_timestamp()),
        (1, 'jamesbond4', '009', 'james3@bond.com', clock_timestamp(), clock_timestamp());

      CREATE OR REPLACE VIEW Books_view AS
        SELECT *
        FROM books
        WHERE seller_id = 1;
    SQL
    PgOnlineSchemaChange::Query.run(client.connection, query)
  end

  def cleanup_dummy_tables(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    PgOnlineSchemaChange::Query.run(client.connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE;")
  end

  def expect_query_result(connection:, query:, assertions:)
    rows = []
    PgOnlineSchemaChange::Query.run(connection, query) { |result| rows = result.map { |row| row } }

    assertions.each do |obj|
      expect(rows.count).to eq(obj[:count])
      expect(rows).to include(include(obj[:data][0])) if obj[:data]
    end

    rows
  end
end
