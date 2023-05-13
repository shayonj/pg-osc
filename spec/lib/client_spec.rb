# frozen_string_literal: true

RSpec.describe(PgOnlineSchemaChange::Client) do
  it "successfully sets class variables" do
    client = described_class.new(client_options)
    expect(client.host).to eq("127.0.0.1")
    expect(client.password).to eq("password")
    expect(client.alter_statement).to eq(
      "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;",
    )
    expect(client.schema).to eq("test_schema")
    expect(client.dbname).to eq("postgres")
    expect(client.username).to eq("jamesbond")
    expect(client.port).to eq(5432)
    expect(client.connection).to be_instance_of(PG::Connection)
    expect(client.table).to eq("books")
    expect(client.table_name).to eq("books")
    expect(client.drop).to be(false)
    expect(client.copy_statement).to be_nil
    expect(client.delta_count).to eq(20)
    expect(client.pull_batch_count).to eq(1000)
  end

  it "raises error query is not ALTER" do
    options = client_options.to_h.merge(alter_statement: "CREATE DATABASE foo")
    client_options = Struct.new(*options.keys).new(*options.values)
    expect { described_class.new(client_options) }.to raise_error(
      PgOnlineSchemaChange::Error,
      "Not a valid ALTER statement: CREATE DATABASE foo",
    )
  end

  describe "handle_copy_statement" do
    it "reads file and sets statement" do
      query = <<~SQL
        INSERT INTO %{shadow_table} ("username", "seller_id", "password", "email", "createdOn", "last_login", "user_id")
        SELECT "username", "seller_id", "password", "email", "createdOn", "last_login", "user_id"
        FROM ONLY books
      SQL

      options = client_options.to_h.merge(copy_statement: "./spec/fixtures/copy.sql")
      client_options = Struct.new(*options.keys).new(*options.values)
      client = described_class.new(client_options)

      expect(client.copy_statement).to eq(query)
    end

    it "raises error if file is not valid" do
      options = client_options.to_h.merge(copy_statement: "foo")
      client_options = Struct.new(*options.keys).new(*options.values)
      expect { described_class.new(client_options) }.to raise_error(
        PgOnlineSchemaChange::Error,
        /File not found/,
      )
    end
  end
end
