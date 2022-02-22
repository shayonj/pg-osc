# frozen_string_literal: true

module DatabaseHelpers
  def schema
    ENV["POSTGRES_SCHEMA"] || "test_schema"
  end

  def client_options
    options = {
      alter_statement: 'ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;',
      schema: schema,
      dbname: ENV["POSTGRES_DB"] || "postgres",
      host: ENV["POSTGRES_HOST"] || "127.0.0.1",
      username: ENV["POSTGRES_USER"] || "jamesbond",
      password: ENV["POSTGRES_PASSWORD"] || "password",
      port: ENV["port"] || 5432,
      drop: false,
      kill_backends: false,
      wait_time_for_lock: 5,
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

      CREATE TABLE IF NOT EXISTS #{schema}.chapters (
        id serial PRIMARY KEY,
        name VARCHAR ( 50 ) UNIQUE NOT NULL,
        book_id SERIAL REFERENCES #{schema}.books NOT NULL,
        "createdOn" TIMESTAMP NOT NULL,
        last_login TIMESTAMP
      );

      ALTER ROLE jamesbond SET statement_timeout = 60000;
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

      INSERT INTO "#{schema}"."books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
      VALUES
        (2, 1, 'jamesbond2', '007', 'james1@bond.com', clock_timestamp(), clock_timestamp()),
        (3, 1, 'jamesbond3', '008', 'james2@bond.com', clock_timestamp(), clock_timestamp()),
        (4, 1, 'jamesbond4', '009', 'james3@bond.com', clock_timestamp(), clock_timestamp());
    SQL
    PgOnlineSchemaChange::Query.run(client.connection, query)
  end

  def cleanup_dummy_tables(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    PgOnlineSchemaChange::Query.run(client.connection, "DROP SCHEMA IF EXISTS #{schema} CASCADE;")
  end

  def expect_query_result(connection:, query:, assertions:)
    rows = []
    PgOnlineSchemaChange::Query.run(connection, query) do |result|
      rows = result.map { |row| row }
    end

    assertions.each do |obj|
      expect(rows.count).to eq(obj[:count])
      expect(rows).to include(include(obj[:data][0])) if obj[:data]
    end

    rows
  end
end
