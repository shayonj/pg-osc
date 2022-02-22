# frozen_string_literal: true

RSpec.describe PgOnlineSchemaChange::Replay do
  describe ".play!" do
    describe "when alter adds a column" do
      let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        setup_tables(client)
        PgOnlineSchemaChange::Orchestrate.setup!(client_options)

        ingest_dummy_data_into_dummy_table(client)

        PgOnlineSchemaChange::Orchestrate.setup_audit_table!
        PgOnlineSchemaChange::Orchestrate.setup_trigger!
        PgOnlineSchemaChange::Orchestrate.setup_shadow_table!
        PgOnlineSchemaChange::Orchestrate.disable_vacuum!
        PgOnlineSchemaChange::Orchestrate.run_alter_statement!
        PgOnlineSchemaChange::Orchestrate.copy_data!
      end

      it "replays INSERT data and cleanups the rows in audit table after" do
        user_id = 10
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect new row not present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          { count: 0 },
        ])

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10 "''i am bond 🚀''"', '0010', 'james10@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";

          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(11, 1, 'jamesbond11 "''i am bond 🚀''"', '0011', 'james11@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "email" => "james10@bond.com",
              "last_login" => be_instance_of(String),
              "password" => "0010",
              "purchased" => "f",
              "seller_id" => "1",
              "user_id" => "10",
              "username" => "jamesbond10 \"'i am bond 🚀'\"",
            }],
          },
        ])

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end

      it "replays UPDATE data" do
        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "username" => "jamesbond2",
            }],
          },
        ])

        # Update an entry for the trigger
        query = <<~SQL
          UPDATE books SET username = 'bondjames'
          WHERE user_id=\'#{user_id}\';
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "username" => "bondjames",
              "createdOn" => be_instance_of(String),
              "email" => "james1@bond.com",
              "last_login" => be_instance_of(String),
              "password" => "007",
              "purchased" => "f",
              "seller_id" => "1",
              "user_id" => "2",
            }],
          },
        ])

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end

      it "replays DELETE data and cleanups the rows in audit table after" do
        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
          },
        ])

        # Delete an entry for the trigger
        query = <<~SQL
          DELETE FROM books WHERE user_id=\'#{user_id}\';
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Expect row being added to the audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          {
            count: 1,
          },
        ])

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row not being present in shadow table anymore
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          { count: 0 },
        ])

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end
    end

    describe "when alter removes a column" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books DROP \"email\";",
        )
        client_options = Struct.new(*options.keys).new(*options.values)

        PgOnlineSchemaChange::Client.new(client_options)
      end

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        setup_tables(client)
        PgOnlineSchemaChange::Orchestrate.setup!(client_options)

        ingest_dummy_data_into_dummy_table(client)

        PgOnlineSchemaChange::Orchestrate.setup_audit_table!
        PgOnlineSchemaChange::Orchestrate.setup_trigger!
        PgOnlineSchemaChange::Orchestrate.setup_shadow_table!
        PgOnlineSchemaChange::Orchestrate.disable_vacuum!
        PgOnlineSchemaChange::Orchestrate.run_alter_statement!
        PgOnlineSchemaChange::Orchestrate.copy_data!
      end

      it "replays INSERT data" do
        expect(described_class.dropped_columns_list).to eq(["email"])

        user_id = 10
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect new row not present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          { count: 0 },
        ])

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "0010",
              "seller_id" => "1",
              "user_id" => "10",
              "username" => "jamesbond10",
            }],
          },
        ])

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end

      it "replays UPDATE data" do
        expect(described_class.dropped_columns_list).to eq(["email"])

        user_id = 2
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL
        # Expect existing row being present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "007",
              "seller_id" => "1",
              "user_id" => user_id.to_s,
              "username" => "jamesbond2",
            }],
          },
        ])

        # Update an entry for the trigger
        query = <<~SQL
          UPDATE books SET username = 'bondjames'
          WHERE user_id=\'#{user_id}\';
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "007",
              "seller_id" => "1",
              "user_id" => user_id.to_s,
              "username" => "bondjames",
            }],
          },
        ])

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end
    end

    describe "when alter renames a column" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books RENAME email to new_email ;",
        )
        client_options = Struct.new(*options.keys).new(*options.values)

        PgOnlineSchemaChange::Client.new(client_options)
      end

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        setup_tables(client)
        PgOnlineSchemaChange::Orchestrate.setup!(client_options)

        ingest_dummy_data_into_dummy_table(client)

        PgOnlineSchemaChange::Orchestrate.setup_audit_table!
        PgOnlineSchemaChange::Orchestrate.setup_trigger!
        PgOnlineSchemaChange::Orchestrate.setup_shadow_table!
        PgOnlineSchemaChange::Orchestrate.disable_vacuum!
        PgOnlineSchemaChange::Orchestrate.run_alter_statement!
        PgOnlineSchemaChange::Orchestrate.copy_data!
      end

      it "replays INSERT data" do
        expect(described_class.dropped_columns_list).to eq([])
        expect(described_class.renamed_columns_list).to eq([{ old_name: "email",
                                                              new_name: "new_email" }])

        user_id = 10
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect new row not present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          { count: 0 },
        ])

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        shadow_rows = expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "0010",
              "seller_id" => "1",
              "user_id" => "10",
              "username" => "jamesbond10",
              "new_email" => "james10@bond.com",
            }],
          },
        ])
        expect(shadow_rows.first["email"]).to eq(nil)

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end

      it "replays UPDATE data" do
        expect(described_class.dropped_columns_list).to eq([])
        expect(described_class.renamed_columns_list).to eq([{ old_name: "email",
                                                              new_name: "new_email" }])

        user_id = 2
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "007",
              "seller_id" => "1",
              "user_id" => user_id.to_s,
              "username" => "jamesbond2",
              "new_email" => "james1@bond.com",
            }],
          },
        ])

        # Update an entry for the trigger
        query = <<~SQL
          UPDATE books SET username = 'bondjames'
          WHERE user_id=\'#{user_id}\';
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Fetch rows
        select_query = <<~SQL
          SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
          rows = result.map { |row| row }
        end

        described_class.play!(rows)

        # Expect row being added into shadow table
        shadow_rows = expect_query_result(connection: client.connection, query: shadow_table_query, assertions: [
          {
            count: 1,
            data: [{
              "createdOn" => be_instance_of(String),
              "last_login" => be_instance_of(String),
              "password" => "007",
              "seller_id" => "1",
              "user_id" => user_id.to_s,
              "username" => "bondjames",
              "new_email" => "james1@bond.com",
            }],
          },
        ])
        expect(shadow_rows.first["email"]).to eq(nil)

        # Expect row being removed from audit table
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        expect_query_result(connection: client.connection, query: audit_table_query, assertions: [
          { count: 0 },
        ])
      end
    end
  end
end
