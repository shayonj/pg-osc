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
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end
        expect(rows.count).to eq(0)

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10 "''i am bond 🚀''"', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";

          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(11, 1, 'jamesbond11 "''i am bond 🚀''"', '0011', 'james11@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["user_id"]).to eq("10")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("0010")
        expect(shadow_rows.first["email"]).to eq("james10@bond.com")
        expect(shadow_rows.first["username"]).to eq("jamesbond10 \"'i am bond 🚀'\"")
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
      end

      it "replays UPDATE data" do
        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(1)
        expect(rows.first["username"]).to eq("jamesbond2")

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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["username"]).to eq("bondjames")
        expect(shadow_rows.first["user_id"]).to eq("2")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("007")
        expect(shadow_rows.first["email"]).to eq("james1@bond.com")
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
      end

      it "replays DELETE data and cleanups the rows in audit table after" do
        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end
        expect(rows.count).to eq(1)

        # Delete an entry for the trigger
        query = <<~SQL
          DELETE FROM books WHERE user_id=\'#{user_id}\';
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, query)

        # Expect row being added to the audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(1)

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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(0)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
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
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end
        expect(rows.count).to eq(0)

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["user_id"]).to eq("10")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("0010")
        expect(shadow_rows.first["email"]).to eq(nil)
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
      end

      it "replays UPDATE data" do
        expect(described_class.dropped_columns_list).to eq(["email"])

        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(1)
        expect(rows.first["username"]).to eq("jamesbond2")

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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["username"]).to eq("bondjames")
        expect(shadow_rows.first["user_id"]).to eq("2")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("007")
        expect(shadow_rows.first["email"]).to eq(nil)
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
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
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect new row not present in into shadow table
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end
        expect(rows.count).to eq(0)

        # Add an entry for the trigger
        query = <<~SQL
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["user_id"]).to eq("10")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("0010")
        expect(shadow_rows.first["email"]).to eq(nil)
        expect(shadow_rows.first["new_email"]).to eq("james10@bond.com")
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
      end

      it "replays UPDATE data" do
        expect(described_class.dropped_columns_list).to eq([])
        expect(described_class.renamed_columns_list).to eq([{ old_name: "email",
                                                              new_name: "new_email" }])

        user_id = 2
        rows = []
        shadow_table_query = <<~SQL
          SELECT * from #{described_class.shadow_table} WHERE #{described_class.primary_key}=#{user_id};
        SQL

        # Expect existing row being present in into shadow table
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(1)
        expect(rows.first["username"]).to eq("jamesbond2")

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
        shadow_rows = []
        PgOnlineSchemaChange::Query.run(client.connection, shadow_table_query) do |result|
          shadow_rows = result.map { |row| row }
        end
        expect(shadow_rows.count).to eq(1)
        expect(shadow_rows.first["username"]).to eq("bondjames")
        expect(shadow_rows.first["user_id"]).to eq("2")
        expect(shadow_rows.first["seller_id"]).to eq("1")
        expect(shadow_rows.first["password"]).to eq("007")
        expect(shadow_rows.first["email"]).to eq(nil)
        expect(shadow_rows.first["new_email"]).to eq("james1@bond.com")
        expect(shadow_rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(shadow_rows.all? { |r| !r["last_login"].nil? }).to eq(true)

        # Expect row being removed from audit table
        audit_rows = []
        audit_table_query = <<~SQL
          SELECT * from \"#{described_class.audit_table}\" WHERE #{described_class.primary_key}=#{user_id};
        SQL
        PgOnlineSchemaChange::Query.run(client.connection, audit_table_query) do |result|
          audit_rows = result.map { |row| row }
        end
        expect(audit_rows.count).to eq(0)
      end
    end
  end
end
