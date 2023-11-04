# frozen_string_literal: true

RSpec.describe(PgOnlineSchemaChange::Orchestrate) do
  describe ".setup!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
    end

    it "sets the defaults & functions" do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      query = <<~SQL
        SELECT pg_get_triggerdef(oid) as tdef FROM pg_trigger
        WHERE  tgrelid = '#{client.schema}.books'::regclass AND tgisinternal = FALSE;
      SQL

      expect(client.connection).to receive(:async_exec)
        .with("BEGIN;")
        .exactly(9)
        .times
        .and_call_original
      expect(client.connection).to receive(:async_exec).with(/array_to_string/).and_call_original
      expect(client.connection).to receive(:async_exec).with(
        /convalidated AS constraint_validated/,
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "SET statement_timeout = 0;\nSET client_min_messages = warning;\nSET search_path TO #{client.schema};\n",
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with(
        FUNC_FIX_SERIAL_SEQUENCE,
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with(
        FUNC_CREATE_TABLE_ALL,
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with(query).and_call_original
      expect(client.connection).to receive(:async_exec)
        .with("COMMIT;")
        .exactly(9)
        .times
        .and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "SHOW statement_timeout;",
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "SHOW client_min_messages;",
      ).and_call_original

      described_class.setup!(client_options)

      expect_query_result(
        connection: client.connection,
        query: "SHOW statement_timeout;",
        assertions: [{ count: 1, data: [{ "statement_timeout" => "0" }] }],
      )

      expect_query_result(
        connection: client.connection,
        query: "SHOW client_min_messages;",
        assertions: [{ count: 1, data: [{ "client_min_messages" => "warning" }] }],
      )

      RSpec::Mocks.space.reset_all

      functions = <<~SQL
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_type='FUNCTION'
          AND specific_schema='#{client.schema}'
          AND routine_name='fix_serial_sequence';
      SQL
      expect_query_result(
        connection: client.connection,
        query: functions,
        assertions: [{ count: 1 }],
      )
    end
  end

  describe ".setup_audit_table!" do
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

    it "creates the audit table with columns from parent table and additional identifiers" do
      described_class.setup_audit_table!

      RSpec::Mocks.space.reset_all
      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.audit_table.to_s)

      expect(columns).to eq(
        [
          {
            "column_name" => "\"#{described_class.audit_table_pk}\"",
            "type" => "integer",
            "column_position" => 1,
            "column_name_regular" => described_class.audit_table_pk.to_s,
          },
          {
            "column_name" => "\"#{described_class.operation_type_column}\"",
            "type" => "text",
            "column_position" => 2,
            "column_name_regular" => described_class.operation_type_column.to_s,
          },
          {
            "column_name" => "\"#{described_class.trigger_time_column}\"",
            "type" => "timestamp without time zone",
            "column_position" => 3,
            "column_name_regular" => described_class.trigger_time_column.to_s,
          },
          {
            "column_name" => "\"user_id\"",
            "type" => "integer",
            "column_position" => 4,
            "column_name_regular" => "user_id",
          },
          {
            "column_name" => "\"username\"",
            "type" => "character varying(50)",
            "column_position" => 5,
            "column_name_regular" => "username",
          },
          {
            "column_name" => "\"seller_id\"",
            "type" => "integer",
            "column_position" => 6,
            "column_name_regular" => "seller_id",
          },
          {
            "column_name" => "\"password\"",
            "type" => "character varying(50)",
            "column_position" => 7,
            "column_name_regular" => "password",
          },
          {
            "column_name" => "\"email\"",
            "type" => "character varying(255)",
            "column_position" => 8,
            "column_name_regular" => "email",
          },
          {
            "column_name" => "\"createdOn\"",
            "type" => "timestamp without time zone",
            "column_position" => 9,
            "column_name_regular" => "createdOn",
          },
          {
            "column_name" => "\"last_login\"",
            "type" => "timestamp without time zone",
            "column_position" => 10,
            "column_name_regular" => "last_login",
          },
        ],
      )
    end

    it "creates the shadow table with autovacuum disabled" do
      described_class.setup_audit_table!

      query = <<~SQL
        select reloptions from pg_class where relname = '#{described_class.audit_table}';
       SQL

      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "reloptions" => "{autovacuum_enabled=false}" }] }],
      )
    end

    describe "when table has a long name" do
      let(:client) do
        options =
          client_options.to_h.merge(
            alter_statement:
              "ALTER TABLE this_is_a_table_with_a_very_long_name ADD COLUMN \"user_id\" int;",
          )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      before do
        allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
        described_class.setup!(client_options)
        setup_tables(client)
      end

      it "successfully" do
        described_class.setup_audit_table!

        sequence_name =
          PgOnlineSchemaChange::Query.get_sequence_name(
            client,
            described_class.audit_table,
            described_class.audit_table_pk,
          )
        expect(described_class.audit_table_pk_sequence).to eq(sequence_name)
      end
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
            INSERT INTO "#{described_class.audit_table}" select nextval('#{described_class.audit_table_pk_sequence}'), 'INSERT', clock_timestamp(), NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'UPDATE') THEN
            INSERT INTO "#{described_class.audit_table}" select nextval('#{described_class.audit_table_pk_sequence}'), 'UPDATE', clock_timestamp(),  NEW.* ;
            RETURN NEW;
          ELSIF ( TG_OP = 'DELETE') THEN
            INSERT INTO "#{described_class.audit_table}" select nextval('#{described_class.audit_table_pk_sequence}'), 'DELETE', clock_timestamp(), OLD.* ;
            RETURN NEW;
          END IF;
        END;
        $$ LANGUAGE PLPGSQL SECURITY DEFINER;

        CREATE TRIGGER primary_to_audit_table_trigger
        AFTER INSERT OR UPDATE OR DELETE ON books
        FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
      SQL

      expect(client.connection).to receive(:async_exec).with(result).and_call_original
      expect(client.connection).to receive(:async_exec)
        .with("BEGIN;")
        .exactly(3)
        .times
        .and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "SET lock_timeout = '5s';\nLOCK TABLE books IN ACCESS EXCLUSIVE MODE;\n",
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").twice.and_call_original

      described_class.setup_trigger!
      expect(described_class.audit_table).to eq(described_class.audit_table.to_s)
    end

    it "closes transaction when it couldn't acquire lock" do
      expect(PgOnlineSchemaChange::Query).to receive(:run)
        .with(client.connection, "COMMIT;")
        .once
        .and_call_original
      allow(PgOnlineSchemaChange::Query).to receive(:open_lock_exclusive).and_raise(
        PgOnlineSchemaChange::AccessExclusiveLockNotAcquired,
      )

      expect { described_class.setup_trigger! }.to raise_error(
        PgOnlineSchemaChange::AccessExclusiveLockNotAcquired,
      )
    end

    it "verifies function and trigger are setup" do
      described_class.setup_trigger!

      query = <<~SQL
        select p.oid::regprocedure from pg_proc p
        join pg_namespace n
        on p.pronamespace = n.oid
        where n.nspname = 'test_schema';
      SQL

      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [
          {
            count: 3,
            data: [
              { "oid" => "fix_serial_sequence(regclass,text)" },
              { "oid" => "create_table_all(text,text)" },
              { "oid" => "primary_to_audit_table_trigger()" },
            ],
          },
        ],
      )

      query = <<~SQL
        select tgname
        from pg_trigger
        where not tgisinternal
        and tgrelid = '#{client.table}'::regclass::oid;
      SQL
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 2, data: ["tgname" => "primary_to_audit_table_trigger"] }],
      )
    end

    it "adds entries to the audit table for INSERT/UPDATE/DELETE" do
      described_class.setup_trigger!
      query = <<~SQL
        INSERT INTO "sellers"("name", "createdOn", "last_login")
        VALUES('local shop', clock_timestamp(), clock_timestamp());

        INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
        VALUES(1, 1, 'jamesbond', '007', 'james@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";

        UPDATE books SET username = 'bondjames'
        WHERE user_id='1';

        DELETE FROM books WHERE user_id='1';
      SQL

      PgOnlineSchemaChange::Query.run(client.connection, query)
      query =
        "select * from #{described_class.audit_table} where #{described_class.operation_type_column} IN ('INSERT', 'UPDATE', 'DELETE')"
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [
          {
            count: 3,
            data: [
              {
                described_class.audit_table_pk => "1",
                "createdOn" => be_instance_of(String),
                "email" => "james@bond.com",
                "last_login" => be_instance_of(String),
                described_class.operation_type_column => "INSERT",
                "password" => "007",
                "seller_id" => "1",
                described_class.trigger_time_column => be_instance_of(String),
                "user_id" => "1",
                "username" => "jamesbond",
              },
              {
                described_class.audit_table_pk => "2",
                "createdOn" => be_instance_of(String),
                "email" => "james@bond.com",
                "last_login" => be_instance_of(String),
                described_class.operation_type_column => "UPDATE",
                "password" => "007",
                "seller_id" => "1",
                described_class.trigger_time_column => be_instance_of(String),
                "user_id" => "1",
                "username" => "bondjames",
              },
              {
                described_class.audit_table_pk => "3",
                "createdOn" => be_instance_of(String),
                "email" => "james@bond.com",
                "last_login" => be_instance_of(String),
                described_class.operation_type_column => "DELETE",
                "password" => "007",
                "seller_id" => "1",
                described_class.trigger_time_column => be_instance_of(String),
                "user_id" => "1",
                "username" => "bondjames",
              },
            ],
          },
        ],
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

      expect(columns).to eq(
        [
          {
            "column_name" => "\"user_id\"",
            "type" => "integer",
            "column_position" => 1,
            "column_name_regular" => "user_id",
          },
          {
            "column_name" => "\"username\"",
            "type" => "character varying(50)",
            "column_position" => 2,
            "column_name_regular" => "username",
          },
          {
            "column_name" => "\"seller_id\"",
            "type" => "integer",
            "column_position" => 3,
            "column_name_regular" => "seller_id",
          },
          {
            "column_name" => "\"password\"",
            "type" => "character varying(50)",
            "column_position" => 4,
            "column_name_regular" => "password",
          },
          {
            "column_name" => "\"email\"",
            "type" => "character varying(255)",
            "column_position" => 5,
            "column_name_regular" => "email",
          },
          {
            "column_name" => "\"createdOn\"",
            "type" => "timestamp without time zone",
            "column_position" => 6,
            "column_name_regular" => "createdOn",
          },
          {
            "column_name" => "\"last_login\"",
            "type" => "timestamp without time zone",
            "column_position" => 7,
            "column_name_regular" => "last_login",
          },
        ],
      )

      columns =
        PgOnlineSchemaChange::Query.get_indexes_for(client, described_class.shadow_table.to_s)
      expect(columns).to eq(
        [
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_pkey ON #{described_class.shadow_table} USING btree (user_id)",
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_username_key ON #{described_class.shadow_table} USING btree (username)",
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_email_key ON #{described_class.shadow_table} USING btree (email)",
        ],
      )

      foreign_keys =
        PgOnlineSchemaChange::Query.get_foreign_keys_for(client, described_class.shadow_table.to_s)
      expect(foreign_keys).to eq([])
      primary_keys =
        PgOnlineSchemaChange::Query.get_primary_keys_for(client, described_class.shadow_table.to_s)
      expect(primary_keys).to eq(
        [
          {
            "constraint_name" => "#{described_class.shadow_table}_pkey",
            "constraint_type" => "p",
            "constraint_validated" => "t",
            "definition" => "PRIMARY KEY (user_id)",
            "table_from" => "-",
            "table_on" => described_class.shadow_table.to_s,
          },
        ],
      )
    end

    it "creates the shadow table matching parent table with no data" do
      described_class.setup_shadow_table!

      query = <<~SQL
        select count(*) from #{described_class.shadow_table}
      SQL
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "count" => "0" }] }],
      )
    end

    it "creates the shadow table with autovacuum disabled" do
      described_class.setup_shadow_table!

      query = <<~SQL
        select reloptions from pg_class where relname = '#{described_class.shadow_table}';
       SQL

      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "reloptions" => "{autovacuum_enabled=false}" }] }],
      )
    end
  end

  describe ".copy_data!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.run_alter_statement!
    end

    it "succesfully truncates audit table" do
      # add an entry to insert a row (via trigger)
      # to audit table
      query = <<~SQL
        INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
        VALUES(4, 1, 'jamesbond', '007', 'james@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
      SQL
      PgOnlineSchemaChange::Query.run(client.connection, query)

      query = <<~SQL
        select * from #{described_class.audit_table};
      SQL
      expect_query_result(connection: client.connection, query: query, assertions: [{ count: 1 }])

      described_class.copy_data!

      expect_query_result(connection: client.connection, query: query, assertions: [{ count: 0 }])
    end

    it "successfully" do
      described_class.copy_data!

      RSpec::Mocks.space.reset_all

      query = <<~SQL
        select * from #{described_class.shadow_table};
      SQL
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [
          {
            count: 3,
            data: [
              {
                "createdOn" => be_instance_of(String),
                "email" => "james1@bond.com",
                "last_login" => be_instance_of(String),
                "password" => "007",
                "purchased" => "f",
                "seller_id" => "1",
                "user_id" => "1",
                "username" => "jamesbond2",
              },
              {
                "createdOn" => be_instance_of(String),
                "email" => "james2@bond.com",
                "last_login" => be_instance_of(String),
                "password" => "008",
                "purchased" => "f",
                "seller_id" => "1",
                "user_id" => "2",
                "username" => "jamesbond3",
              },
              {
                "createdOn" => be_instance_of(String),
                "email" => "james3@bond.com",
                "last_login" => be_instance_of(String),
                "password" => "009",
                "purchased" => "f",
                "seller_id" => "1",
                "user_id" => "3",
                "username" => "jamesbond4",
              },
            ],
          },
        ],
      )
    end

    describe "from copy_statement" do
      let(:client) do
        options = client_options.to_h.merge(copy_statement: "./spec/fixtures/copy.sql")
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "successfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        expect_query_result(
          connection: client.connection,
          query: query,
          assertions: [
            {
              count: 3,
              data: [
                {
                  "createdOn" => be_instance_of(String),
                  "email" => "james1@bond.com",
                  "last_login" => be_instance_of(String),
                  "password" => "007",
                  "purchased" => "f",
                  "seller_id" => "1",
                  "user_id" => "1",
                  "username" => "jamesbond2",
                },
                {
                  "createdOn" => be_instance_of(String),
                  "email" => "james2@bond.com",
                  "last_login" => be_instance_of(String),
                  "password" => "008",
                  "purchased" => "f",
                  "seller_id" => "1",
                  "user_id" => "2",
                  "username" => "jamesbond3",
                },
                {
                  "createdOn" => be_instance_of(String),
                  "email" => "james3@bond.com",
                  "last_login" => be_instance_of(String),
                  "password" => "009",
                  "purchased" => "f",
                  "seller_id" => "1",
                  "user_id" => "3",
                  "username" => "jamesbond4",
                },
              ],
            },
          ],
        )
      end
    end

    describe "when column is dropped" do
      let(:client) do
        options =
          client_options.to_h.merge(alter_statement: "ALTER TABLE books DROP COLUMN \"user_id\";")
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "successfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        rows =
          expect_query_result(
            connection: client.connection,
            query: query,
            assertions: [{ count: 3 }],
          )

        expect(rows.filter_map { |r| r["user_id"] }).to eq([])
      end
    end

    describe "when column is renamed" do
      let(:client) do
        options =
          client_options.to_h.merge(
            alter_statement: "ALTER TABLE books RENAME COLUMN \"user_id\" to \"new_user_id\"; ",
          )
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "successfully" do
        described_class.copy_data!

        RSpec::Mocks.space.reset_all

        query = <<~SQL
          select * from #{described_class.shadow_table};
        SQL
        rows =
          expect_query_result(
            connection: client.connection,
            query: query,
            assertions: [
              {
                count: 3,
                data: [
                  {
                    "createdOn" => be_instance_of(String),
                    "email" => "james1@bond.com",
                    "last_login" => be_instance_of(String),
                    "new_user_id" => "1",
                    "password" => "007",
                    "seller_id" => "1",
                    "username" => "jamesbond2",
                  },
                  {
                    "createdOn" => be_instance_of(String),
                    "email" => "james2@bond.com",
                    "last_login" => be_instance_of(String),
                    "new_user_id" => "2",
                    "password" => "008",
                    "seller_id" => "1",
                    "username" => "jamesbond3",
                  },
                  {
                    "createdOn" => be_instance_of(String),
                    "email" => "james3@bond.com",
                    "last_login" => be_instance_of(String),
                    "new_user_id" => "3",
                    "password" => "009",
                    "seller_id" => "1",
                    "username" => "jamesbond4",
                  },
                ],
              },
            ],
          )
        expect(rows.filter_map { |r| r["user_id"] }).to eq([])
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

    it "successfully" do
      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "ALTER TABLE #{described_class.shadow_table} ADD COLUMN purchased boolean DEFAULT false",
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      described_class.run_alter_statement!
      RSpec::Mocks.space.reset_all

      columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

      expect(columns).to eq(
        [
          {
            "column_name" => "\"user_id\"",
            "type" => "integer",
            "column_position" => 1,
            "column_name_regular" => "user_id",
          },
          {
            "column_name" => "\"username\"",
            "type" => "character varying(50)",
            "column_position" => 2,
            "column_name_regular" => "username",
          },
          {
            "column_name" => "\"seller_id\"",
            "type" => "integer",
            "column_position" => 3,
            "column_name_regular" => "seller_id",
          },
          {
            "column_name" => "\"password\"",
            "type" => "character varying(50)",
            "column_position" => 4,
            "column_name_regular" => "password",
          },
          {
            "column_name" => "\"email\"",
            "type" => "character varying(255)",
            "column_position" => 5,
            "column_name_regular" => "email",
          },
          {
            "column_name" => "\"createdOn\"",
            "type" => "timestamp without time zone",
            "column_position" => 6,
            "column_name_regular" => "createdOn",
          },
          {
            "column_name" => "\"last_login\"",
            "type" => "timestamp without time zone",
            "column_position" => 7,
            "column_name_regular" => "last_login",
          },
          {
            "column_name" => "\"purchased\"",
            "type" => "boolean",
            "column_position" => 8,
            "column_name_regular" => "purchased",
          },
        ],
      )
      expect(described_class.dropped_columns_list).to eq([])
      expect(described_class.renamed_columns_list).to eq([])
    end

    describe "dropped column" do
      let(:client) do
        options = client_options.to_h.merge(alter_statement: "ALTER TABLE books DROP \"email\";")
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

      it "successfully" do
        expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
        expect(client.connection).to receive(:async_exec).with(
          "ALTER TABLE #{described_class.shadow_table} DROP email",
        ).and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

        expect(columns).to eq(
          [
            {
              "column_name" => "\"user_id\"",
              "type" => "integer",
              "column_position" => 1,
              "column_name_regular" => "user_id",
            },
            {
              "column_name" => "\"username\"",
              "type" => "character varying(50)",
              "column_position" => 2,
              "column_name_regular" => "username",
            },
            {
              "column_name" => "\"seller_id\"",
              "type" => "integer",
              "column_position" => 3,
              "column_name_regular" => "seller_id",
            },
            {
              "column_name" => "\"password\"",
              "type" => "character varying(50)",
              "column_position" => 4,
              "column_name_regular" => "password",
            },
            {
              "column_name" => "\"createdOn\"",
              "type" => "timestamp without time zone",
              "column_position" => 6,
              "column_name_regular" => "createdOn",
            },
            {
              "column_name" => "\"last_login\"",
              "type" => "timestamp without time zone",
              "column_position" => 7,
              "column_name_regular" => "last_login",
            },
          ],
        )
        expect(described_class.dropped_columns_list).to eq(["email"])
        expect(described_class.renamed_columns_list).to eq([])
      end
    end

    describe "renamed column" do
      let(:client) do
        options =
          client_options.to_h.merge(
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

      it "successfully" do
        expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
        expect(client.connection).to receive(:async_exec).with(
          "ALTER TABLE #{described_class.shadow_table} RENAME COLUMN email TO new_email",
        ).and_call_original
        expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

        described_class.run_alter_statement!
        RSpec::Mocks.space.reset_all

        columns = PgOnlineSchemaChange::Query.table_columns(client, described_class.shadow_table)

        expect(columns).to eq(
          [
            {
              "column_name" => "\"user_id\"",
              "type" => "integer",
              "column_position" => 1,
              "column_name_regular" => "user_id",
            },
            {
              "column_name" => "\"username\"",
              "type" => "character varying(50)",
              "column_position" => 2,
              "column_name_regular" => "username",
            },
            {
              "column_name" => "\"seller_id\"",
              "type" => "integer",
              "column_position" => 3,
              "column_name_regular" => "seller_id",
            },
            {
              "column_name" => "\"password\"",
              "type" => "character varying(50)",
              "column_position" => 4,
              "column_name_regular" => "password",
            },
            {
              "column_name" => "\"new_email\"",
              "type" => "character varying(255)",
              "column_position" => 5,
              "column_name_regular" => "new_email",
            },
            {
              "column_name" => "\"createdOn\"",
              "type" => "timestamp without time zone",
              "column_position" => 6,
              "column_name_regular" => "createdOn",
            },
            {
              "column_name" => "\"last_login\"",
              "type" => "timestamp without time zone",
              "column_position" => 7,
              "column_name_regular" => "last_login",
            },
          ],
        )

        expect(described_class.dropped_columns_list).to eq([])
        expect(described_class.renamed_columns_list).to eq(
          [{ old_name: "email", new_name: "new_email" }],
        )
      end
    end
  end

  describe ".swap!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
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
      expect_query_result(
        connection: client.connection,
        query: select_query,
        assertions: [{ count: 3 }],
      )

      # Fetch rows from the renamed table
      select_query = <<~SQL
        SELECT * FROM books;
      SQL
      expect_query_result(
        connection: client.connection,
        query: select_query,
        assertions: [{ count: 3 }],
      )

      # confirm indexes on newly renamed table
      columns = PgOnlineSchemaChange::Query.get_indexes_for(client, "books")
      expect(columns).to eq(
        [
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_pkey ON books USING btree (user_id)",
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_username_key ON books USING btree (username)",
          "CREATE UNIQUE INDEX #{described_class.shadow_table}_email_key ON books USING btree (email)",
        ],
      )
    end

    it "sucessfully updates the PK sequence" do
      select_query = <<~SQL
        SELECT * FROM books;
      SQL
      expect_query_result(
        connection: client.connection,
        query: select_query,
        assertions: [{ count: 3 }],
      )

      described_class.swap!

      query = <<~SQL
        INSERT INTO "books"("seller_id", "username", "password", "email", "createdOn", "last_login")
        VALUES(1, 'jamesbond', '007', 'james@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
      SQL
      PgOnlineSchemaChange::Query.run(client.connection, query)

      select_query = <<~SQL
        SELECT * FROM books;
      SQL
      expect_query_result(
        connection: client.connection,
        query: select_query,
        assertions: [{ count: 4 }],
      )

      # Fetch rows from the original primary table
      select_query = <<~SQL
        SELECT user_id FROM books ORDER BY user_id DESC LIMIT 1;
      SQL

      expect_query_result(
        connection: client.connection,
        query: select_query,
        assertions: [{ count: 1, data: [{ "user_id" => "4" }] }],
      )
    end

    it "sucessfully drops the trigger" do
      query =
        "SELECT trigger_name FROM information_schema.triggers WHERE event_object_table ='#{client.table}';"
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [
          {
            count: 4,
            data: [
              { "trigger_name" => "email_changes" },
              { "trigger_name" => "primary_to_audit_table_trigger" },
              { "trigger_name" => "primary_to_audit_table_trigger" },
              { "trigger_name" => "primary_to_audit_table_trigger" },
            ],
          },
        ],
      )
      described_class.swap!

      query =
        "SELECT trigger_name FROM information_schema.triggers WHERE event_object_table ='#{client.table}';"
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "trigger_name" => "email_changes" }] }],
      )
    end

    it "sucessfully resets the autovacuum" do
      described_class.swap!

      expect(
        PgOnlineSchemaChange::Query.storage_parameters_for(client, client.table_name, true),
      ).to eq(
        "autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000",
      )
    end

    it "transfers the foreign keys from parent table" do
      described_class.swap!

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, client.table)
      expect(foreign_keys).to eq(
        [
          {
            "table_on" => client.table,
            "table_from" => "sellers",
            "constraint_type" => "f",
            "constraint_name" => "#{client.table}_seller_id_fkey",
            "constraint_validated" => "f",
            "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id) NOT VALID",
          },
        ],
      )

      query = <<~SQL
        select reloptions from pg_class where relname = '#{described_class.audit_table}';
      SQL
      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "reloptions" => "{autovacuum_enabled=false}" }] }],
      )
    end

    it "closes transaction when it couldn't acquire lock" do
      expect(PgOnlineSchemaChange::Query).to receive(:run)
        .with(client.connection, "SET statement_timeout = 0;")
        .once
        .and_call_original
      expect(PgOnlineSchemaChange::Query).to receive(:run)
        .with(client.connection, "COMMIT;")
        .once
        .and_call_original
      allow(PgOnlineSchemaChange::Query).to receive(:open_lock_exclusive).and_raise(
        PgOnlineSchemaChange::AccessExclusiveLockNotAcquired,
      )

      expect { described_class.swap! }.to raise_error(
        PgOnlineSchemaChange::AccessExclusiveLockNotAcquired,
      )
    end
  end

  describe ".run_analyze!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
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
      expect(rows[0]["last_analyze"]).to be_nil

      described_class.run_analyze!
      sleep(1)

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["last_analyze"]).not_to be_nil
    end
  end

  describe ".validate_constraints! with skip" do
    let(:client) do
      options = client_options.to_h.merge(skip_foreign_key_validation: true)
      client_options = Struct.new(*options.keys).new(*options.values)
      PgOnlineSchemaChange::Client.new(client_options)
    end

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.run_alter_statement!
      described_class.copy_data!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
    end

    it "skips" do
      result = [
        {
          "table_on" => "chapters",
          "table_from" => "books",
          "constraint_type" => "f",
          "constraint_name" => "chapters_book_id_fkey",
          "constraint_validated" => "f",
          "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID",
        },
      ]

      # swap has happened w/ not valid
      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)

      described_class.validate_constraints!

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, "chapters")
      expect(foreign_keys).to eq(result)
    end
  end

  describe ".validate_constraints!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.run_alter_statement!
      described_class.copy_data!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
    end

    it "sucessfully validates the constraints on the referential tables" do
      result = [
        {
          "table_on" => "chapters",
          "table_from" => "books",
          "constraint_type" => "f",
          "constraint_name" => "chapters_book_id_fkey",
          "constraint_validated" => "f",
          "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID",
        },
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

    it "succesfully validations contraints on the primary table" do
      # swap has happened w/ not valid
      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, client.table)
      expect(foreign_keys).to eq(
        [
          {
            "table_on" => client.table,
            "table_from" => "sellers",
            "constraint_type" => "f",
            "constraint_name" => "#{client.table}_seller_id_fkey",
            "constraint_validated" => "f",
            "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id) NOT VALID",
          },
        ],
      )

      described_class.validate_constraints!

      foreign_keys = PgOnlineSchemaChange::Query.get_foreign_keys_for(client, client.table)
      expect(foreign_keys).to eq(
        [
          {
            "table_on" => client.table,
            "table_from" => "sellers",
            "constraint_type" => "f",
            "constraint_name" => "#{client.table}_seller_id_fkey",
            "constraint_validated" => "t",
            "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id)",
          },
        ],
      )
    end
  end

  describe ".replace_views!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)
      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.run_alter_statement!
      described_class.copy_data!
      described_class.dropped_columns_list
      PgOnlineSchemaChange::Replay.play!([])
    end

    it "succesfully recreates the view" do
      expected_views_result = [
        {
          "temp_views.books_temp_view" =>
            "SELECT books.user_id,\n    books.username,\n    books.seller_id,\n    books.password,\n    books.email,\n    books.\"createdOn\",\n    books.last_login\n   FROM books\n  WHERE (books.seller_id = 1);",
        },
        {
          "test_schema.books_view" =>
            "SELECT books.user_id,\n    books.username,\n    books.seller_id,\n    books.password,\n    books.email,\n    books.\"createdOn\",\n    books.last_login\n   FROM books\n  WHERE (books.seller_id = 1);",
        },
      ]
      expected_views_result_op_table = [
        {
          "temp_views.books_temp_view" =>
            "SELECT pgosc_op_table_books.user_id,\n    pgosc_op_table_books.username,\n    pgosc_op_table_books.seller_id,\n    pgosc_op_table_books.password,\n    pgosc_op_table_books.email,\n    pgosc_op_table_books.\"createdOn\",\n    pgosc_op_table_books.last_login\n   FROM pgosc_op_table_books\n  WHERE (pgosc_op_table_books.seller_id = 1);",
        },
        {
          "test_schema.books_view" =>
            "SELECT pgosc_op_table_books.user_id,\n    pgosc_op_table_books.username,\n    pgosc_op_table_books.seller_id,\n    pgosc_op_table_books.password,\n    pgosc_op_table_books.email,\n    pgosc_op_table_books.\"createdOn\",\n    pgosc_op_table_books.last_login\n   FROM pgosc_op_table_books\n  WHERE (pgosc_op_table_books.seller_id = 1);",
        },
      ]

      rows = []
      PgOnlineSchemaChange::Query.run(
        client.connection,
        "select count(*) from  books_view;",
      ) { |result| rows = result.map { |row| row } }
      expect(rows.first["count"]).to eq("3")

      expect(
        PgOnlineSchemaChange::Query.view_definitions_for(client, described_class.old_primary_table),
      ).to eq([])
      expect(PgOnlineSchemaChange::Query.view_definitions_for(client, client.table)).to eq(
        expected_views_result,
      )

      described_class.swap!

      expect(
        PgOnlineSchemaChange::Query.view_definitions_for(client, described_class.old_primary_table),
      ).to eq(expected_views_result_op_table)
      expect(PgOnlineSchemaChange::Query.view_definitions_for(client, client.table)).to eq([])

      # Add an entry to check re-creation of view was successful with new data
      query = <<~SQL
        INSERT INTO "books"("user_id", "seller_id", "username", "password", "email", "createdOn", "last_login")
        VALUES(4, 1, 'jamesbond', '007', 'james@bond.com', clock_timestamp(), clock_timestamp()) RETURNING "user_id", "username", "password", "email", "createdOn", "last_login";
       SQL
      PgOnlineSchemaChange::Query.run(client.connection, query)

      described_class.replace_views!

      expect(
        PgOnlineSchemaChange::Query.view_definitions_for(client, described_class.old_primary_table),
      ).to eq([])
      expect(PgOnlineSchemaChange::Query.view_definitions_for(client, client.table)).to eq(
        expected_views_result,
      )

      rows = []
      PgOnlineSchemaChange::Query.run(
        client.connection,
        "select count(*) from books_view;",
      ) { |result| rows = result.map { |row| row } }
      expect(rows.first["count"]).to eq("4")
    end
  end

  describe ".drop_and_cleanup!" do
    let(:client) { PgOnlineSchemaChange::Client.new(client_options) }

    before do
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      setup_tables(client)
      ingest_dummy_data_into_dummy_table(client)
      described_class.setup!(client_options)

      described_class.setup_audit_table!
      described_class.setup_trigger!
      described_class.setup_shadow_table!
      described_class.run_alter_statement!
      described_class.copy_data!
      PgOnlineSchemaChange::Replay.play!([])
      described_class.swap!
      described_class.replace_views!
    end

    it "sucessfully drops audit table" do
      described_class.drop_and_cleanup!

      query = <<~SQL
        SELECT to_regclass('#{described_class.audit_table}') as exists;
      SQL

      rows = []
      PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
        rows = result.map { |row| row }
      end

      expect(rows[0]["exists"]).to be_nil
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
        options = client_options.to_h.merge(drop: true)
        client_options = Struct.new(*options.keys).new(*options.values)
        PgOnlineSchemaChange::Client.new(client_options)
      end

      it "sucessfully drops primary table" do
        described_class.drop_and_cleanup!

        query = <<~SQL
          SELECT to_regclass('#{described_class.old_primary_table}') as exists;
        SQL

        rows = []
        PgOnlineSchemaChange::Query.run(client.connection, query) do |result|
          rows = result.map { |row| row }
        end

        expect(rows[0]["exists"]).to be_nil
      end
    end
  end
end
