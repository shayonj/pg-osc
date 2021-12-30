module DatabaseHelpers
  def client_options
    options = {
      alter_statement: 'ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;',
      schema: ENV["POSTGRES_SCHEMA"] || "public",
      dbname: ENV["POSTGRES_DB"] || "postgres",
      host: ENV["POSTGRES_HOST"] || "127.0.0.1",
      username: ENV["POSTGRES_USER"] || "jamesbond",
      password: ENV["POSTGRES_PASSWORD"] || "password",
      port: ENV["port"] || 5432,
    }
    Struct.new(*options.keys).new(*options.values)
  end

  def new_dummy_table_sql
    <<~SQL
      CREATE TABLE IF NOT EXISTS books (
        user_id serial PRIMARY KEY,
        username VARCHAR ( 50 ) UNIQUE NOT NULL,
        password VARCHAR ( 50 ) NOT NULL,
        email VARCHAR ( 255 ) UNIQUE NOT NULL,
        created_on TIMESTAMP NOT NULL,
        last_login TIMESTAMP
      );
    SQL
  end

  def create_dummy_table(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    PgOnlineSchemaChange::Query.run(client.connection, new_dummy_table_sql)
  end

  def ingest_dummy_data_into_dummy_table(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    query = <<~SQL
      INSERT INTO "books"("user_id", "username", "password", "email", "created_on", "last_login")
      VALUES
        (2, 'jamesbond2', '007', 'james1@bond.com', 'now()', 'now()'),
        (3, 'jamesbond3', '008', 'james2@bond.com', 'now()', 'now()'),
        (4, 'jamesbond4', '009', 'james3@bond.com', 'now()', 'now()');
    SQL
    PgOnlineSchemaChange::Query.run(client.connection, query)
  end

  def cleanup_dummy_tables(client = nil)
    client ||= PgOnlineSchemaChange::Client.new(client_options)
    PgOnlineSchemaChange::Query.run(client.connection, "DROP TABLE IF EXISTS pgosc_audit_table_for_books;")
    PgOnlineSchemaChange::Query.run(client.connection, "DROP TABLE IF EXISTS pgosc_shadow_table_for_books;")
    PgOnlineSchemaChange::Query.run(client.connection, "DROP TABLE IF EXISTS books;")
  end
end
