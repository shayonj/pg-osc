# frozen_string_literal: true

RSpec.describe PgOnlineSchemaChange::Orchestrate do
  describe ".setup!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
    end

    it "sets the defaults & functions" do
      client = PgOnlineSchemaChange::Client.new(client_options)
      expect(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)

      expect(client.connection).to receive(:async_exec).with("BEGIN;").exactly(5).times.and_call_original
      expect(client.connection).to receive(:async_exec).with("SET statement_timeout = 0;\nSET client_min_messages = warning;\nSET search_path TO #{client.schema};\n").and_call_original
      expect(client.connection).to receive(:async_exec).with(FUNC_FIX_SERIAL_SEQUENCE).and_call_original
      expect(client.connection).to receive(:async_exec).with(FUNC_CREATE_TABLE_ALL).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").exactly(5).times.and_call_original
      expect(client.connection).to receive(:async_exec).with("SHOW statement_timeout;").and_call_original
      expect(client.connection).to receive(:async_exec).with("SHOW client_min_messages;").and_call_original

      described_class.setup!(client_options)

      PgOnlineSchemaChange::Query.run(client.connection, "SHOW statement_timeout;") do |result|
        expect(result.count).to eq(1)
        result.each do |row|
          expect(row).to eq({ "statement_timeout" => "0" })
        end
      end

      PgOnlineSchemaChange::Query.run(client.connection, "SHOW client_min_messages;") do |result|
        expect(result.count).to eq(1)
        result.each do |row|
          expect(row).to eq({ "client_min_messages" => "warning" })
        end
      end
      RSpec::Mocks.space.reset_all

      functions = <<~SQL
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_type='FUNCTION'
          AND specific_schema=\'#{client.schema}\'
          AND routine_name='fix_serial_sequence';
      SQL
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, functions) do |result|
        rows = result.map { |row| row }
      end

      expect(rows.count).to eq(1)
    end
  end

  describe ".setup_audit_table!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      described_class.setup!(client_options)
      setup_tables(client)
    end

    after do
      client = PgOnlineSchemaChange::Client.new(client_options)
      cleanup_dummy_tables(client)
    end

    it "creates the audit table with columns from parent table and additional identifiers" do
      described_class.setup_audit_table!

      RSpec::Mocks.space.reset_all
      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.audit_table.to_s)

      expect(columns).to eq([
                              { "column_name" => "\"operation_type\"", "type" => "text", "column_position" => 1,
                                "column_name_regular" => "operation_type" },
                              { "column_name" => "\"trigger_time\"", "type" => "timestamp without time zone",
                                "column_position" => 2, "column_name_regular" => "trigger_time" },
                              { "column_name" => "\"user_id\"", "type" => "integer", "column_position" => 3,
                                "column_name_regular" => "user_id" },
                              { "column_name" => "\"username\"", "type" => "character varying(50)",
                                "column_position" => 4, "column_name_regular" => "username" },
                              { "column_name" => "\"seller_id\"", "type" => "integer", "column_position" => 5,
                                "column_name_regular" => "seller_id" },
                              { "column_name" => "\"password\"", "type" => "character varying(50)",
                                "column_position" => 6, "column_name_regular" => "password" },
                              { "column_name" => "\"email\"", "type" => "character varying(255)",
                                "column_position" => 7, "column_name_regular" => "email" },
                              { "column_name" => "\"createdOn\"", "type" => "timestamp without time zone",
                                "column_position" => 8, "column_name_regular" => "createdOn" },
                              { "column_name" => "\"last_login\"", "type" => "timestamp without time zone",
                                "column_position" => 9, "column_name_regular" => "last_login" },
                            ])
    end
  end

  describe ".setup_trigger!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
    end

    it "creates the function and sets up trigger" do
      result = <<~SQL
        DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table};

        CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
        RETURNS TRIGGER AS
        $$
        BEGIN
          IF ( TG_OP = 'INSERT') THEN
            INSERT INTO "#{described_class.audit_table}" select 'INSERT', now(), NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'UPDATE') THEN
            INSERT INTO "#{described_class.audit_table}" select 'UPDATE', now(),  NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'DELETE') THEN
            INSERT INTO "#{described_class.audit_table}" select 'DELETE', now(), OLD.* ;
            RETURN NEW;
          END IF;
        END;
        $$ LANGUAGE PLPGSQL SECURITY DEFINER;

        CREATE TRIGGER primary_to_audit_table_trigger
        AFTER INSERT OR UPDATE OR DELETE ON books
        FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
      SQL

      expect(client.connection).to receive(:async_exec).with("BEGIN;").exactly(3).times.and_call_original
      expect(client.connection).to receive(:async_exec).with("SET lock_timeout = '5s';\nLOCK TABLE books IN ACCESS EXCLUSIVE MODE;\n").and_call_original
      expect(client.connection).to receive(:async_exec).with(result).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").twice.and_call_original

      described_class.setup_trigger!
      expect(described_class.audit_table).to eq(described_class.audit_table.to_s)
    end

    it "closes transaction when it couldn't acquire lock" do
      expect(PgOnlineSchemaChange::Query).to receive(:run).with(client.connection, "COMMIT;").once.and_call_original
      expect(PgOnlineSchemaChange::Query).to receive(:open_lock_exclusive).and_raise(PgOnlineSchemaChange::AccessExclusiveLockNotAcquired)

      expect do
        described_class.setup_trigger!
      end.to raise_error(PgOnlineSchemaChange::AccessExclusiveLockNotAcquired)
    end

    it "verifies function and trigger are setup" do
      described_class.setup_trigger!

      query = <<~SQL
        select p.oid::regprocedure from pg_proc p
        join pg_namespace n
        on p.pronamespace = n.oid
        where n.nspname not in ('pg_catalog', 'information_schema');
      SQL

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end
      row = rows.detect { |row| row["oid"] == "primary_to_audit_table_trigger()" }
      expect(row).to eq({ "oid" => "primary_to_audit_table_trigger()" })

      query = <<~SQL
        select tgname
        from pg_trigger
        where not tgisinternal
        and tgrelid = \'#{client.table}\'::regclass::oid;
      SQL

      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        expect(result.count).to eq(1)
        result.each do |row|
          expect(row).to eq("tgname" => "primary_to_audit_table_trigger")
        end
      end
    end

    it "adds entries to the audit table for INSERT/UPDATE/DELETE" do
      described_class.setup_trigger!

      query = <<~SQL
        INSERT INTO "sellers"("name", "createdOn", "last_login")
        VALUES('local shop', 'now()', 'now()');

        INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
        VALUES(1, 1, 'jamesbond', '007', 'james@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";

        UPDATE books SET username = 'bondjames'
        WHERE user_id='1';

        DELETE FROM books WHERE user_id='1';
      SQL

      PgOnlineSchemaChange::Query.run(client.connection, query)
      query = <<~SQL
        select * from #{described_class.audit_table}
        where operation_type IN ('INSERT', 'UPDATE', 'DELETE')
      SQL

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end
      expect(rows.count).to eq(3)

      insert = rows.detect { |r| r["operation_type"] == "INSERT" }
      expect(insert).to include(
        "operation_type" => "INSERT",
        "trigger_time" => be_instance_of(String),
        "seller_id" => "1",
        "username" => "jamesbond",
        "password" => "007",
        "email" => "james@bond.com",
        "createdOn" => be_instance_of(String),
        "last_login" => be_instance_of(String),
      )

      update = rows.detect { |r| r["operation_type"] == "UPDATE" }
      expect(update).to include(
        "operation_type" => "UPDATE",
        "trigger_time" => be_instance_of(String),
        "seller_id" => "1",
        "username" => "bondjames",
        "password" => "007",
        "email" => "james@bond.com",
        "createdOn" => be_instance_of(String),
        "last_login" => be_instance_of(String),
      )

      delete = rows.detect { |r| r["operation_type"] == "DELETE" }
      expect(delete).to include(
        "operation_type" => "DELETE",
        "trigger_time" => be_instance_of(String),
        "seller_id" => "1",
        "username" => "bondjames",
        "password" => "007",
        "email" => "james@bond.com",
        "createdOn" => be_instance_of(String),
        "last_login" => be_instance_of(String),
      )
    end
  end

  describe ".setup_shadow_table!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)
    end

    after do
      client = PgOnlineSchemaChange::Client.new(client_options)
      cleanup_dummy_tables(client)
    end

    it "creates the shadow table matching parent table" do
      described_class.setup_shadow_table!

      RSpec::Mocks.space.reset_all
      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

      expect(columns).to eq([
                              { "column_name" => "\"user_id\"", "type" => "integer", "column_position" => 1,
                                "column_name_regular" => "user_id" },
                              { "column_name" => "\"username\"", "type" => "character varying(50)",
                                "column_position" => 2, "column_name_regular" => "username" },
                              { "column_name" => "\"seller_id\"", "type" => "integer", "column_position" => 3,
                                "column_name_regular" => "seller_id" },
                              { "column_name" => "\"password\"", "type" => "character varying(50)",
                                "column_position" => 4, "column_name_regular" => "password" },
                              { "column_name" => "\"email\"", "type" => "character varying(255)",
                                "column_position" => 5, "column_name_regular" => "email" },
                              { "column_name" => "\"createdOn\"", "type" => "timestamp without time zone",
                                "column_position" => 6, "column_name_regular" => "createdOn" },
                              { "column_name" => "\"last_login\"", "type" => "timestamp without time zone",
                                "column_position" => 7, "column_name_regular" => "last_login" },
                            ])

      columns = PgOnlineSchemaChange::Query.get_indexes_for(client, described_class.shadow_table.to_s)
      expect(columns).to eq(["CREATE UNIQUE INDEX #{described_class.shadow_table}_pkey ON #{described_class.shadow_table} USING btree (user_id)",
                             "CREATE UNIQUE INDEX #{described_class.shadow_table}_username_key ON #{described_class.shadow_table} USING btree (username)",
                             "CREATE UNIQUE INDEX #{described_class.shadow_table}_email_key ON #{described_class.shadow_table} USING btree (email)"])

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client,
                                                                      described_class.shadow_table.to_s)
      expect(foreign_keys).to eq([
                                   { "table_on" => described_class.shadow_table.to_s, "table_from" => "sellers",
                                     "constraint_type" => "f", "constraint_name" => "#{client.table}_seller_id_fkey", "constraint_validated" => "t", "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id)" },
                                 ])
      primary_keys = PgOnlineSchemaChange::Query.get_primary_keys_for(client,
                                                                      described_class.shadow_table.to_s)
      expect(primary_keys).to eq([
                                   { "constraint_name" => "#{described_class.shadow_table}_pkey", "constraint_type" => "p",
                                     "constraint_validated" => "t", "definition" => "PRIMARY KEY (user_id)", "table_from" => "-", "table_on" => described_class.shadow_table.to_s },
                                 ])
    end

    it "creates the shadow table matching parent table with no data" do
      described_class.setup_shadow_table!

      query = <<~SQL
        select count(*) from #{described_class.shadow_table}
      SQL

      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        result.each do |row|
          expect(row).to eq({ "count" => "0" })
        end
      end
    end
  end

  describe ".disable_vacuum!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_shadow_table!
    end

    it "succesfully" do
      query = <<~SQL
        ALTER TABLE #{described_class.shadow_table} SET (
          autovacuum_enabled = false, toast.autovacuum_enabled = false
        );

        ALTER TABLE #{described_class.audit_table} SET (
          autovacuum_enabled = false, toast.autovacuum_enabled = false
        );
      SQL
      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with("SELECT array_to_string(reloptions, ',') as params FROM pg_class WHERE relname='books';\n").and_call_original
      expect(client.connection).to receive(:async_exec).with(query).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.disable_vacuum!

      expect(described_class.primary_table_storage_parameters).to eq("autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000")
      RSpec::Mocks.space.reset_all

      query = <<~SQL
        select reloptions from pg_class where relname = \'#{described_class.shadow_table}\';
      SQL
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        result.each do |row|
          expect(row).to eq({ "reloptions" => "{autovacuum_enabled=false}" })
        end
      end

      query = <<~SQL
        select reloptions from pg_class where relname = \'#{described_class.audit_table}\';
      SQL
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        result.each do |row|
          expect(row).to eq({ "reloptions" => "{autovacuum_enabled=false}" })
        end
      end
    end
  end

  describe ".copy_data!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      ingest_dummy_data_into_dummy_table(client)

      described_class.setup_shadow_table!
      described_class.run_alter_statement!
    end

    it "succesfully" do
      described_class.copy_data!

      RSpec::Mocks.space.reset_all

      query = <<~SQL
        select * from #{described_class.shadow_table};
      SQL
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end

      expect(rows.count).to eq(3)
      expect(rows.map { |r| r["user_id"] }).to eq(%w[2 3 4])
      expect(rows.map { |r| r["seller_id"] }).to eq(%w[1 1 1])
      expect(rows.map { |r| r["password"] }).to eq(%w[007 008 009])
      expect(rows.map do |r|
        r["email"]
      end).to eq(["james1@bond.com", "james2@bond.com", "james3@bond.com"])
      expect(rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
      expect(rows.all? { |r| !r["last_login"].nil? }).to eq(true)
    end

    describe "from copy_statement" do
      let(:client) do
        options = client_options.to_h.merge(
          copy_statement: "./spec/fixtures/copy.sql",
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "succesfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(3)
        expect(rows.map { |r| r["user_id"] }).to eq(%w[2 3 4])
        expect(rows.map { |r| r["seller_id"] }).to eq(%w[1 1 1])
        expect(rows.map { |r| r["password"] }).to eq(%w[007 008 009])
        expect(rows.map do |r|
          r["email"]
        end).to eq(["james1@bond.com", "james2@bond.com", "james3@bond.com"])
        expect(rows.all? { |r| !r["createdOn"].nil? }).to eq(true)
        expect(rows.all? { |r| !r["last_login"].nil? }).to eq(true)
      end
    end

    describe "when column is dropped" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books DROP COLUMN \"user_id\";",
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "succesfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(3)
        expect(rows.map { |r| r["user_id"] }.compact).to eq([])
      end
    end

    describe "when column is renamed" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books RENAME COLUMN \"user_id\" to \"new_user_id\"; ",
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "succesfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows.count).to eq(3)
        expect(rows.map { |r| r["user_id"] }.compact).to eq([])
        expect(rows.map { |r| r["new_user_id"] }).to eq(%w[2 3 4])
      end
    end
  end

  describe ".run_alter_statement!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_shadow_table!
    end

    it "succesfully" do
      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with("ALTER TABLE #{described_class.shadow_table} ADD COLUMN purchased boolean DEFAULT false").and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.run_alter_statement!
      RSpec::Mocks.space.reset_all

      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

      expect(columns).to eq([
                              { "column_name" => "\"user_id\"", "type" => "integer", "column_position" => 1,
                                "column_name_regular" => "user_id" },
                              { "column_name" => "\"username\"", "type" => "character varying(50)",
                                "column_position" => 2, "column_name_regular" => "username" },
                              { "column_name" => "\"seller_id\"", "type" => "integer", "column_position" => 3,
                                "column_name_regular" => "seller_id" },
                              { "column_name" => "\"password\"", "type" => "character varying(50)",
                                "column_position" => 4, "column_name_regular" => "password" },
                              { "column_name" => "\"email\"", "type" => "character varying(255)",
                                "column_position" => 5, "column_name_regular" => "email" },
                              { "column_name" => "\"createdOn\"", "type" => "timestamp without time zone",
                                "column_position" => 6, "column_name_regular" => "createdOn" },
                              { "column_name" => "\"last_login\"", "type" => "timestamp without time zone",
                                "column_position" => 7, "column_name_regular" => "last_login" },
                              { "column_name" => "\"purchased\"", "type" => "boolean", "column_position" => 8,
                                "column_name_regular" => "purchased" },
                            ])
      expect(described_class.dropped_columns_list).to eq([])
      expect(described_class.renamed_columns_list).to eq([])
    end

    describe "dropped column" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books DROP \"email\";",
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        cleanup_dummy_tables(client)
        create_dummy_tables(client)

        described_class.setup!(client_options)

        described_class.setup_audit_table!
        described_class.setup_shadow_table!
      end

      it "succesfully" do
        expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
        expect(client.connection).to receive(:async_exec).with("ALTER TABLE #{described_class.shadow_table} DROP email").and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

        expect(columns).to eq([
                                { "column_name" => "\"user_id\"", "type" => "integer", "column_position" => 1,
                                  "column_name_regular" => "user_id" },
                                { "column_name" => "\"username\"", "type" => "character varying(50)",
                                  "column_position" => 2, "column_name_regular" => "username" },
                                { "column_name" => "\"seller_id\"", "type" => "integer", "column_position" => 3,
                                  "column_name_regular" => "seller_id" },
                                { "column_name" => "\"password\"", "type" => "character varying(50)",
                                  "column_position" => 4, "column_name_regular" => "password" },
                                { "column_name" => "\"createdOn\"", "type" => "timestamp without time zone",
                                  "column_position" => 6, "column_name_regular" => "createdOn" },
                                { "column_name" => "\"last_login\"", "type" => "timestamp without time zone",
                                  "column_position" => 7, "column_name_regular" => "last_login" },
                              ])
        expect(described_class.dropped_columns_list).to eq(["email"])
        expect(described_class.renamed_columns_list).to eq([])
      end
    end

    describe "renamed column" do
      let(:client) do
        options = client_options.to_h.merge(
          alter_statement: "ALTER TABLE books RENAME COLUMN email TO new_email;",
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        setup_tables(client)
        described_class.setup!(client_options)

        described_class.setup_audit_table!
        described_class.setup_shadow_table!
      end

      it "succesfully" do
        expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
        expect(client.connection).to receive(:async_exec).with("ALTER TABLE #{described_class.shadow_table} RENAME COLUMN email TO new_email").and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

        expect(columns).to eq([
                                { "column_name" => "\"user_id\"", "type" => "integer", "column_position" => 1,
                                  "column_name_regular" => "user_id" },
                                { "column_name" => "\"username\"", "type" => "character varying(50)",
                                  "column_position" => 2, "column_name_regular" => "username" },
                                { "column_name" => "\"seller_id\"", "type" => "integer", "column_position" => 3,
                                  "column_name_regular" => "seller_id" },
                                { "column_name" => "\"password\"", "type" => "character varying(50)",
                                  "column_position" => 4, "column_name_regular" => "password" },
                                { "column_name" => "\"new_email\"", "type" => "character varying(255)",
                                  "column_position" => 5, "column_name_regular" => "new_email" },
                                { "column_name" => "\"createdOn\"", "type" => "timestamp without time zone",
                                  "column_position" => 6, "column_name_regular" => "createdOn" },
                                { "column_name" => "\"last_login\"", "type" => "timestamp without time zone",
                                  "column_position" => 7, "column_name_regular" => "last_login" },
                              ])

        expect(described_class.dropped_columns_list).to eq([])
        expect(described_class.renamed_columns_list).to eq([
                                                             {
                                                               old_name: "email",
                                                               new_name: "new_email",
                                                             },
                                                           ])
      end
    end
  end

  describe ".swap!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      ingest_dummy_data_into_dummy_table(client)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.disable_vacuum!
      described_class.run_alter_statement!
      described_class.copy_data!

      rows = PgOnlineSchemaChange::Replay.rows_to_play
      PgOnlineSchemaChange::Replay.play!(rows)
    end

    it "ensures helper table names are of proper length" do
      expect(described_class.shadow_table.length).to eq(21)
      expect(described_class.audit_table.length).to eq(21)
      expect(described_class.old_primary_table.length).to eq(20)
    end

    it "sucessfully renames the tables" do
      described_class.swap!

      # Fetch rows from the original primary table
      select_query = <<~SQL
        SELECT * FROM #{described_class.old_primary_table};
      SQL
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
        rows = result.map { |row| row }
      end
      expect(rows.count).to eq(3)

      # Fetch rows from the renamed table
      select_query = <<~SQL
        SELECT * FROM books;
      SQL
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
        rows = result.map { |row| row }
      end
      expect(rows.count).to eq(3)

      # confirm indexes on newly renamed table
      columns = PgOnlineSchemaChange::Query.get_indexes_for(client, "books")
      expect(columns).to eq(["CREATE UNIQUE INDEX #{described_class.shadow_table}_pkey ON books USING btree (user_id)",
                             "CREATE UNIQUE INDEX #{described_class.shadow_table}_username_key ON books USING btree (username)",
                             "CREATE UNIQUE INDEX #{described_class.shadow_table}_email_key ON books USING btree (email)"])
    end

    it "sucessfully drops the trigger" do
      result = []

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection,
                                      "SELECT trigger_name FROM information_schema.triggers WHERE event_object_table ='#{client.table}';") do |result|
        rows = result.map { |row| row }
      end

      expect(rows.count).to eq(3)
      expect(rows.map { |n| n["trigger_name"] }.uniq).to eq(["primary_to_audit_table_trigger"])

      described_class.swap!

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection,
                                      "SELECT trigger_name FROM information_schema.triggers WHERE event_object_table ='#{client.table}';") do |result|
        rows = result.map { |row| row }
      end
      expect(rows.count).to eq(0)
    end

    it "closes transaction when it couldn't acquire lock" do
      expect(PgOnlineSchemaChange::Query).to receive(:get_foreign_keys_to_refresh).with(client, client.table)
      expect(PgOnlineSchemaChange::Query).to receive(:run).with(client.connection, "COMMIT;").once.and_call_original
      expect(PgOnlineSchemaChange::Query).to receive(:open_lock_exclusive).and_raise(PgOnlineSchemaChange::AccessExclusiveLockNotAcquired)

      expect do
        described_class.swap!
      end.to raise_error(PgOnlineSchemaChange::AccessExclusiveLockNotAcquired)
    end
  end

  describe ".run_analyze!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      ingest_dummy_data_into_dummy_table(client)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.disable_vacuum!
      described_class.run_alter_statement!
      described_class.copy_data!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
    end

    it "sucessfully renames the tables" do
      query = "SELECT last_analyze FROM pg_stat_all_tables WHERE relname = 'books';"
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end
      expect(rows[0]["last_analyze"]).to eq(nil)

      described_class.run_analyze!
      sleep(1)

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["last_analyze"]).not_to eq(nil)
    end
  end

  describe ".validate_constraints!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      ingest_dummy_data_into_dummy_table(client)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.disable_vacuum!
      described_class.run_alter_statement!
      described_class.copy_data!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
    end

    it "sucessfully validates the constraints the tables" do
      result = [
        { "table_on" => "chapters", "table_from" => "books",
          "constraint_type" => "f", "constraint_name" => "chapters_book_id_fkey", "constraint_validated" => "f", "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID" },
      ]

      # swap has happened w/ not valid
      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)

      described_class.validate_constraints!

      # w/o not valid
      result[0]["constraint_validated"] = "t"
      result[0]["definition"] = "FOREIGN KEY (book_id) REFERENCES books(user_id)"

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)
    end
  end

  describe ".drop_and_cleanup!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      described_class.setup!(client_options)

      ingest_dummy_data_into_dummy_table(client)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.disable_vacuum!
      described_class.copy_data!
      described_class.run_alter_statement!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
    end

    it "sucessfully drops audit table" do
      described_class.drop_and_cleanup!

      query = <<~SQL
        SELECT to_regclass(\'#{described_class.audit_table}\') as exists;
      SQL

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["exists"]).to eq(nil)
    end

    it "sucessfully resets session vars" do
      described_class.drop_and_cleanup!

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, "SHOW statement_timeout;") do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["statement_timeout"]).to eq("1min")

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, "SHOW client_min_messages;") do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["client_min_messages"]).to eq("notice")
    end

    describe "primary table" do
      let(:client) do
        options = client_options.to_h.merge(
          drop: true,
        )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "sucessfully drops primary table" do
        described_class.drop_and_cleanup!

        query = <<~SQL
          SELECT to_regclass(\'#{described_class.old_primary_table}\') as exists;
        SQL

        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows[0]["exists"]).to eq(nil)
      end
    end
  end
end
