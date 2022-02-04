# frozen_string_literal: true

require "pry"
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
      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with("CREATE TABLE pgosc_audit_table_for_books (operation_type text, trigger_time timestamp, LIKE books);\n").and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.setup_audit_table!

      RSpec::Mocks.space.reset_all
      columns = PgOnlineSchemaChange::Query.table_columns(client, "pgosc_audit_table_for_books")
      expect(columns).to eq([
                              { "column_name" => "operation_type", "type" => "text",
                                "column_position" => 1 },
                              { "column_name" => "trigger_time", "type" => "timestamp without time zone",
                                "column_position" => 2 },
                              { "column_name" => "user_id", "type" => "integer",
                                "column_position" => 3 },
                              { "column_name" => "username", "type" => "character varying(50)",
                                "column_position" => 4 },
                              { "column_name" => "seller_id", "type" => "integer",
                                "column_position" => 5 },
                              { "column_name" => "password", "type" => "character varying(50)",
                                "column_position" => 6 },
                              { "column_name" => "email", "type" => "character varying(255)",
                                "column_position" => 7 },
                              { "column_name" => "created_on", "type" => "timestamp without time zone",
                                "column_position" => 8 },
                              { "column_name" => "last_login", "type" => "timestamp without time zone",
                                "column_position" => 9 },
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
        CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
        RETURNS TRIGGER AS
        $$
        BEGIN
          IF ( TG_OP = 'INSERT') THEN
            INSERT INTO "pgosc_audit_table_for_books" select 'INSERT', now(), NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'UPDATE') THEN
            INSERT INTO "pgosc_audit_table_for_books" select 'UPDATE', now(),  NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'DELETE') THEN
            INSERT INTO "pgosc_audit_table_for_books" select 'DELETE', now(), OLD.* ;
            RETURN NEW;
          END IF;
        END;
        $$ LANGUAGE PLPGSQL SECURITY DEFINER;

        CREATE TRIGGER primary_to_audit_table_trigger
        AFTER INSERT OR UPDATE OR DELETE ON books
        FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
      SQL

      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(result).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.setup_trigger!
      expect(described_class.audit_table).to eq("pgosc_audit_table_for_books")
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
        INSERT INTO "sellers"("name", "created_on", "last_login")
        VALUES('local shop', 'now()', 'now()');

        INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "created_on", "last_login")
        VALUES(1, 1, 'jamesbond', '007', 'james@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "created_on", "last_login";

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
        "created_on" => be_instance_of(String),
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
        "created_on" => be_instance_of(String),
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
        "created_on" => be_instance_of(String),
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
      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with("SELECT fix_serial_sequence('books', 'pgosc_shadow_table_for_books');").and_call_original
      expect(client.connection).to receive(:async_exec).with("SELECT create_table_all('books', 'pgosc_shadow_table_for_books');").and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.setup_shadow_table!

      RSpec::Mocks.space.reset_all
      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)
      expect(columns).to eq([
                              { "column_name" => "user_id", "type" => "integer",
                                "column_position" => 1 },
                              { "column_name" => "username", "type" => "character varying(50)",
                                "column_position" => 2 },
                              { "column_name" => "seller_id", "type" => "integer",
                                "column_position" => 3 },
                              { "column_name" => "password", "type" => "character varying(50)",
                                "column_position" => 4 },
                              { "column_name" => "email", "type" => "character varying(255)",
                                "column_position" => 5 },
                              { "column_name" => "created_on", "type" => "timestamp without time zone",
                                "column_position" => 6 },
                              { "column_name" => "last_login",
                                "type" => "timestamp without time zone", "column_position" => 7 },
                            ])

      columns = PgOnlineSchemaChange::Query.get_indexes_for(client, "pgosc_shadow_table_for_books")
      expect(columns).to eq(["CREATE UNIQUE INDEX pgosc_shadow_table_for_books_pkey ON pgosc_shadow_table_for_books USING btree (user_id)",
                             "CREATE UNIQUE INDEX pgosc_shadow_table_for_books_username_key ON pgosc_shadow_table_for_books USING btree (username)",
                             "CREATE UNIQUE INDEX pgosc_shadow_table_for_books_email_key ON pgosc_shadow_table_for_books USING btree (email)"])

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client,
                                                                      "pgosc_shadow_table_for_books")
      expect(foreign_keys).to eq([
                                   { "table_on" => "pgosc_shadow_table_for_books", "table_from" => "sellers",
                                     "constraint_type" => "f", "constraint_name" => "pgosc_shadow_table_for_books_seller_id_fkey", "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id)" },
                                 ])
      primary_keys = PgOnlineSchemaChange::Query.get_primary_keys_for(client,
                                                                      "pgosc_shadow_table_for_books")
      expect(primary_keys).to eq([
                                   { "constraint_name" => "pgosc_shadow_table_for_books_pkey", "constraint_type" => "p",
                                     "definition" => "PRIMARY KEY (user_id)", "table_from" => "-", "table_on" => "pgosc_shadow_table_for_books" },
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
        ALTER TABLE pgosc_shadow_table_for_books SET (
          autovacuum_enabled = false, toast.autovacuum_enabled = false
        );

        ALTER TABLE pgosc_audit_table_for_books SET (
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
    end

    it "succesfully" do
      column_query = <<~SQL
        SELECT attname as column_name, format_type(atttypid, atttypmod) as type, attnum as column_position FROM   pg_attribute
        WHERE  attrelid = 'books'::regclass AND attnum > 0 AND NOT attisdropped
        ORDER  BY attnum;
      SQL
      insert_query = <<~SQL
        INSERT INTO pgosc_shadow_table_for_books
        SELECT user_id, username, seller_id, password, email, created_on, last_login
        FROM ONLY books
      SQL
      expect(client.connection).to receive(:async_exec).with("BEGIN;").twice.and_call_original
      expect(client.connection).to receive(:async_exec).with(column_query).and_call_original
      expect(client.connection).to receive(:async_exec).with(insert_query).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").twice.and_call_original

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
      expect(rows.all? { |r| !r["created_on"].nil? }).to eq(true)
      expect(rows.all? { |r| !r["last_login"].nil? }).to eq(true)
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
      expect(client.connection).to receive(:async_exec).with("ALTER TABLE pgosc_shadow_table_for_books ADD COLUMN purchased boolean DEFAULT false").and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.run_alter_statement!
      RSpec::Mocks.space.reset_all

      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)
      expect(columns).to eq([
                              { "column_name" => "user_id", "type" => "integer",
                                "column_position" => 1 },
                              { "column_name" => "username", "type" => "character varying(50)",
                                "column_position" => 2 },
                              { "column_name" => "seller_id", "type" => "integer",
                                "column_position" => 3 },
                              { "column_name" => "password", "type" => "character varying(50)",
                                "column_position" => 4 },
                              { "column_name" => "email", "type" => "character varying(255)",
                                "column_position" => 5 },
                              { "column_name" => "created_on", "type" => "timestamp without time zone",
                                "column_position" => 6 },
                              { "column_name" => "last_login", "type" => "timestamp without time zone",
                                "column_position" => 7 },
                              { "column_name" => "purchased", "type" => "boolean",
                                "column_position" => 8 },
                            ])
      expect(described_class.dropped_columns).to eq([])
      expect(described_class.renamed_columns).to eq([])
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
        expect(client.connection).to receive(:async_exec).with("ALTER TABLE pgosc_shadow_table_for_books DROP email").and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)
        expect(columns).to eq([
                                { "column_name" => "user_id", "type" => "integer",
                                  "column_position" => 1 },
                                { "column_name" => "username", "type" => "character varying(50)",
                                  "column_position" => 2 },
                                { "column_name" => "seller_id", "type" => "integer",
                                  "column_position" => 3 },
                                { "column_name" => "password", "type" => "character varying(50)",
                                  "column_position" => 4 },
                                { "column_name" => "created_on", "type" => "timestamp without time zone",
                                  "column_position" => 6 },
                                { "column_name" => "last_login", "type" => "timestamp without time zone",
                                  "column_position" => 7 },
                              ])
        expect(described_class.dropped_columns).to eq(["email"])
        expect(described_class.renamed_columns).to eq([])
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
        expect(client.connection).to receive(:async_exec).with("ALTER TABLE pgosc_shadow_table_for_books RENAME COLUMN email TO new_email").and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)
        expect(columns).to eq([
                                { "column_name" => "user_id", "type" => "integer",
                                  "column_position" => 1 },
                                { "column_name" => "username", "type" => "character varying(50)",
                                  "column_position" => 2 },
                                { "column_name" => "seller_id", "type" => "integer",
                                  "column_position" => 3 },
                                { "column_name" => "password", "type" => "character varying(50)",
                                  "column_position" => 4 },
                                { "column_name" => "new_email", "type" => "character varying(255)",
                                  "column_position" => 5 },
                                { "column_name" => "created_on", "type" => "timestamp without time zone",
                                  "column_position" => 6 },
                                { "column_name" => "last_login", "type" => "timestamp without time zone",
                                  "column_position" => 7 },
                              ])
        expect(described_class.dropped_columns).to eq([])
        expect(described_class.renamed_columns).to eq([
                                                        {
                                                          old_name: "email",
                                                          new_name: "new_email",
                                                        },
                                                      ])
      end
    end
  end

  describe ".replay_data!" do
    describe "when alter adds a column" do
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
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "created_on", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "created_on", "last_login";
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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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

        described_class.replay_data!(rows)

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
        described_class.setup!(client_options)

        ingest_dummy_data_into_dummy_table(client)

        described_class.setup_audit_table!
        described_class.setup_trigger!
        described_class.setup_shadow_table!
        described_class.disable_vacuum!
        described_class.copy_data!
        described_class.run_alter_statement!
      end

      it "replays INSERT data" do
        expect(described_class.dropped_columns).to eq(["email"])

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
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "created_on", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "created_on", "last_login";
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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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
        expect(described_class.dropped_columns).to eq(["email"])

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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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
        described_class.setup!(client_options)

        ingest_dummy_data_into_dummy_table(client)

        described_class.setup_audit_table!
        described_class.setup_trigger!
        described_class.setup_shadow_table!
        described_class.disable_vacuum!
        described_class.copy_data!
        described_class.run_alter_statement!
      end

      it "replays INSERT data" do
        expect(described_class.dropped_columns).to eq([])
        expect(described_class.renamed_columns).to eq([{ old_name: "email",
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
          INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "created_on", "last_login")
          VALUES(10, 1, 'jamesbond10', '0010', 'james10@bond.com', 'now()', 'now()') RETURNING "user_id", "username", "password", "email", "created_on", "last_login";
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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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
        expect(described_class.dropped_columns).to eq([])
        expect(described_class.renamed_columns).to eq([{ old_name: "email",
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

        described_class.replay_data!(rows)

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
        expect(shadow_rows.all? { |r| !r["created_on"].nil? }).to eq(true)
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
      described_class.copy_data!
      described_class.run_alter_statement!

      # Fetch rows to replay data
      select_query = <<~SQL
        SELECT * FROM #{described_class.audit_table} ORDER BY #{described_class.primary_key} LIMIT 1000;
      SQL
      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, select_query) do |result|
        rows = result.map { |row| row }
      end

      described_class.replay_data!(rows)
    end

    it "sucessfully renames the tables" do
      described_class.swap!

      # Fetch rows from the original primary table
      select_query = <<~SQL
        SELECT * FROM pgosc_old_primary_table_books;
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
      expect(columns).to eq(["CREATE UNIQUE INDEX pgosc_shadow_table_for_books_pkey ON books USING btree (user_id)",
                             "CREATE UNIQUE INDEX pgosc_shadow_table_for_books_username_key ON books USING btree (username)",
                             "CREATE UNIQUE INDEX pgosc_shadow_table_for_books_email_key ON books USING btree (email)"])
    end

    it "sucessfully renames the tables and transfers foreign keys" do
      result = [
        { "table_on" => "chapters", "table_from" => "books",
          "constraint_type" => "f", "constraint_name" => "chapters_book_id_fkey", "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id)" },
      ]

      # before (w/o not valid)
      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)

      described_class.swap!

      result = [
        { "table_on" => "chapters", "table_from" => "books",
          "constraint_type" => "f", "constraint_name" => "chapters_book_id_fkey", "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID" },
      ]

      # before (w/ not valid)
      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)
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
      described_class.copy_data!
      described_class.run_alter_statement!
      described_class.replay_data!([])
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
end
