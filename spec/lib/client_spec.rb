# frozen_string_literal: true

RSpec.describe PgOnlineSchemaChange::Client do
  it "succesfully sets class variables" do
    client = described_class.new(client_options)
    expect(client.host).to eq("127.0.0.1")
    expect(client.password).to eq("password")
    expect(client.alter_statement).to eq("ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;")
    expect(client.schema).to eq("public")
    expect(client.dbname).to eq("postgres")
    expect(client.username).to eq("jamesbond")
    expect(client.port).to eq(5432)
    expect(client.connection).to be_instance_of(PG::Connection)
    expect(client.table).to eq("books")
  end

  it "raises error query is not ALTER" do
    options = client_options.to_h.merge(
      alter_statement: "CREATE DATABASE foo",
    )
    client_options = Struct.new(*options.keys).new(*options.values)
    expect do
      described_class.new(client_options)
    end.to raise_error(PgOnlineSchemaChange::Error, "Not a valid ALTER statement: CREATE DATABASE foo")
  end
end
