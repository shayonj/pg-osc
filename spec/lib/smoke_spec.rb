# frozen_string_literal: true

require 'English'
def log(msg)
  puts "======= #{msg} ======="
end

def setup_pgbench_tables(foreign_keys:)
  log("Setting up pgbench")
  if foreign_keys
    `pgbench --initialize -s 100 --foreign-keys --host #{client.host} -U #{client.username} -d #{client.dbname}`
  else
    `pgbench --initialize -s 100 --host #{client.host} -U #{client.username} -d #{client.dbname}`
  end

  log("Setting up pgbench validate table")

  foreign_key_statement =
    (
      if foreign_keys
        "ALTER TABLE pgbench_accounts_validate ADD FOREIGN KEY (bid) REFERENCES pgbench_branches(bid)"
      else
        ""
      end
    )

  sql = <<~SQL
    CREATE TABLE pgbench_accounts_validate AS SELECT * FROM pgbench_accounts;
    ALTER TABLE pgbench_accounts_validate ADD PRIMARY KEY (aid);
    #{foreign_key_statement};
  SQL
  PgOnlineSchemaChange::Query.run(client.connection, sql)
end

# This test spins up a pgbench tables and a pgbench_accounts_validate
# table. During the pgbench run, it updates the same rows/data in both
# table. We then run pg-osc and perform an ALTER/schema change. Once that
# is done, we expect to see no discrepancies in the data between two tables.
# We do this after pg-osc has run successfully, as it would have performed
# a swap with not issues.
RSpec.describe("SmokeSpec") do
  before do
    log("Cleaning up")

    sql = <<~SQL
      DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history, pgbench_accounts_validate;
    SQL

    PgOnlineSchemaChange::Query.run(client.connection, sql)
    setup_pgbench_tables(foreign_keys: false)
  end

  after do
    log("Cleaning up")

    sql = <<~SQL
      DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history, pgbench_accounts_validate;
    SQL

    PgOnlineSchemaChange::Query.run(client.connection, sql)
  end

  describe "dataset" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    it "is setup succesfully" do
      expect_query_result(
        connection: client.connection,
        query: "select count(*) from pgbench_accounts",
        assertions: [{ count: 1, data: [{ "count" => "10000000" }] }],
      )

      expect_query_result(
        connection: client.connection,
        query: "select count(*) from pgbench_accounts_validate",
        assertions: [{ count: 1, data: [{ "count" => "10000000" }] }],
      )
    end

    it "matches after pg-osc run" do
      pid =
        fork do
          log("Running pgbench")
          exec(
            "pgbench --file spec/fixtures/bench.sql -T 600000 -c 15 --host #{client.host} -U #{client.username} -d #{client.dbname}",
          )
        end
      Process.detach(pid)
      sleep(10)

      log("Running pg-osc")
      statement = <<~SCRIPT
        PGPASSWORD="#{client.password}" DEBUG=true bundle exec bin/pg-online-schema-change perform \
        -a 'ALTER TABLE pgbench_accounts ALTER COLUMN aid TYPE BIGINT' \
        -d #{client.dbname} \
        -h #{client.host} \
        -u #{client.username} \
        --drop
      SCRIPT
      IO.popen(statement) do |io|
        io.each do |line|
          puts line
          output << line
        end
      end

      expect(output.join(",")).to match(/All tasks successfully completed/)
      Process.kill("KILL", pid)

      log("Comparing data between two tables")

      sql = <<~SQL
        (TABLE pgbench_accounts EXCEPT TABLE pgbench_accounts_validate)
        UNION ALL
        (TABLE pgbench_accounts_validate EXCEPT TABLE pgbench_accounts);
      SQL

      expect_query_result(connection: client.connection, query: sql, assertions: [{ count: 0 }])
    ensure
      begin
        Process.kill("KILL", pid)
      rescue Errno::ESRCH
        log("pgbench closed")
      end
    end
  end

  describe "with foreign keys" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      log("Cleaning up")

      sql = <<~SQL
        DROP TABLE IF EXISTS pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history, pgbench_accounts_validate;
      SQL

      PgOnlineSchemaChange::Query.run(client.connection, sql)
      setup_pgbench_tables(foreign_keys: true)
    end

    it "matches after pg-osc run" do
      pid =
        fork do
          log("Running pgbench")
          exec(
            "pgbench --file spec/fixtures/bench.sql -T 600000 -c 15 --host #{client.host} -U #{client.username} -d #{client.dbname} >/dev/null 2>&1",
          )
        end
      Process.detach(pid)

      sleep(10)

      log("Running pg-osc")
      statement = <<~SCRIPT
        PGPASSWORD="#{client.password}" DEBUG=true bundle exec bin/pg-online-schema-change perform \
        -a 'ALTER TABLE pgbench_accounts ALTER COLUMN aid TYPE BIGINT' \
        -d #{client.dbname} \
        -h #{client.host} \
        -u #{client.username} \
        --drop
      SCRIPT
      output = []
      IO.popen(statement) do |io|
        io.each do |line|
          puts line
          output << line
        end
      end

      expect(output.join(",")).to match(/All tasks successfully completed/)
      Process.kill("KILL", pid)

      log("Comparing data between two tables")

      sql = <<~SQL
        (TABLE pgbench_accounts EXCEPT TABLE pgbench_accounts_validate)
        UNION ALL
        (TABLE pgbench_accounts_validate EXCEPT TABLE pgbench_accounts);
      SQL

      expect_query_result(connection: client.connection, query: sql, assertions: [{ count: 0 }])
    ensure
      begin
        Process.kill("KILL", pid)
      rescue Errno::ESRCH
        log("pgbench closed")
      end
    end
  end
end
