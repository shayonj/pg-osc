# frozen_string_literal: true

require "pg_online_schema_change"
require "./spec/support/database_helpers"
require "pry"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.include DatabaseHelpers

  config.before do
    PgOnlineSchemaChange::Orchestrate.init
  end

  config.before(:suite) do
    PgOnlineSchemaChange.logger = !ENV["CI"].nil?
  end

  config.after(:all) do
    client = PgOnlineSchemaChange::Client.new(client_options)
    sql = <<~SQL
      RESET statement_timeout;
      RESET client_min_messages;
    SQL
    PgOnlineSchemaChange::Query.run(client.connection, sql)
    cleanup_dummy_tables
  end
end
