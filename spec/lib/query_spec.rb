# frozen_string_literal: true

RSpec.describe(PgOnlineSchemaChange::Query) do
  describe ".alter_statement?" do
    it "returns true" do
      query = "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;"
      expect(described_class.alter_statement?(query)).to be(true)
    end

    it "returns false" do
      query = "CREATE DATABASE FOO"
      expect(described_class.alter_statement?(query)).to be(false)
    end

    it "returns false when thrown Error" do
      query = "ALTER"
      expect(described_class.alter_statement?(query)).to be(false)
    end

    it "returns true with multiple alter statements" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
      SQL

      expect(described_class.alter_statement?(query)).to be(true)
    end

    it "returns false with multiple statements and only one alter statement" do
      query =
        "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE; CREATE DATABASE FOO;"
      expect(described_class.alter_statement?(query)).to be(false)
    end
  end

  describe ".same_table?" do
    it "returns true with multiple statements" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books RENAME COLUMN "email" to "new_email";
      SQL

      expect(described_class.same_table?(query)).to be(true)
    end

    it "returns false with multiple statements" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE cards RENAME COLUMN "email" to "new_email";
      SQL

      expect(described_class.same_table?(query)).to be(false)
    end

    it "returns true with multiple statements and a rename" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books RENAME COLUMN "email" to "new_email";
      SQL

      expect(described_class.same_table?(query)).to be(true)
    end

    it "returns false" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books RENAME COLUMN "email" to "new_email";
        ALTER TABLE cards RENAME COLUMN "email" to "new_email";
      SQL

      expect(described_class.same_table?(query)).to be(false)
    end
  end

  describe ".table" do
    it "returns the table name" do
      query = "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;"
      expect(described_class.table(query)).to eq("books")
    end

    it "returns the table name for uppercase tables" do
      query = "ALTER TABLE \"Books\" ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;"
      expect(described_class.table(query)).to eq("Books")
    end

    it "returns the table name for rename statements" do
      query = "ALTER TABLE books RENAME COLUMN \"email\" to \"new_email\";"
      expect(described_class.table(query)).to eq("books")
    end
  end

  describe ".table_name" do
    it "returns the table name for lowercase tables" do
      query = "ALTER TABLE books ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;"
      expect(described_class.table(query)).to eq("books")
    end

    it "returns quoted table name for uppercase tables" do
      query = "ALTER TABLE \"Books\" ADD COLUMN \"purchased\" BOOLEAN DEFAULT FALSE;"
      expect(described_class.table_name(query, "Books")).to eq("\"Books\"")
    end
  end

  describe ".run" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    let(:alter_query) { "ALTER TABLE chapters DROP CONSTRAINT chapters_book_id_fkey;" }
    let(:result) do
      {
        "table_on" => "chapters",
        "table_from" => "books",
        "constraint_type" => "f",
        "constraint_name" => "chapters_book_id_fkey",
        "constraint_validated" => "t",
        "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id)",
      }
    end

    before { setup_tables(client) }

    it "runs the query and yields the result" do
      query = "SELECT 'FooBar' as result"

      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(
        "SELECT 'FooBar' as result",
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      expect_query_result(
        connection: client.connection,
        query: query,
        assertions: [{ count: 1, data: [{ "result" => "FooBar" }] }],
      )
    end

    it "returns the alter query successfully" do
      query =
        described_class
          .get_all_constraints_for(client)
          .find { |row| row["constraint_name"] == "chapters_book_id_fkey" }

      expect(query).to eq(result)

      expect(client.connection).to receive(:async_exec).with("BEGIN;").twice.and_call_original
      expect(client.connection).to receive(:async_exec).with(
        /FROM   	pg_constraint/,
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").twice.and_call_original
      allow(client.connection).to receive(:async_exec).with(alter_query).and_call_original

      described_class.run(client.connection, alter_query)

      query =
        described_class
          .get_all_constraints_for(client)
          .find { |row| row["constraint_name"] == "chapters_book_id_fkey" }
      expect(query).to be_nil
    end

    it "runs the alter query and rollsback successfully" do
      query =
        described_class
          .get_all_constraints_for(client)
          .find { |row| row["constraint_name"] == "chapters_book_id_fkey" }
      expect(query).to eq(result)

      expect(client.connection).to receive(:async_exec).with("BEGIN;").twice.and_call_original
      expect(client.connection).to receive(:async_exec).with("ROLLBACK;").and_call_original
      expect(client.connection).to receive(:async_exec).with(
        /FROM   	pg_constraint/,
      ).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").twice.and_call_original

      allow(client.connection).to receive(:async_exec).with(alter_query).and_raise(
        PG::DependentObjectsStillExist,
      )
      expect { described_class.run(client.connection, alter_query) }.to raise_error(
        PG::DependentObjectsStillExist,
      )

      query =
        described_class
          .get_all_constraints_for(client)
          .find { |row| row["constraint_name"] == "chapters_book_id_fkey" }
      expect(query).to eq(result)
    end
  end

  describe ".table_columns" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns column names" do
      described_class.run(client.connection, new_dummy_table_sql)

      result = [
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
      ]

      expect(described_class.table_columns(client)).to eq(result)
    end
  end

  describe ".get_all_constraints_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns all constraints" do
      result = [
        {
          "table_on" => "sellers",
          "table_from" => "-",
          "constraint_type" => "p",
          "constraint_name" => "sellers_pkey",
          "constraint_validated" => "t",
          "definition" => "PRIMARY KEY (id)",
        },
        {
          "table_on" => "books",
          "table_from" => "-",
          "constraint_type" => "p",
          "constraint_name" => "books_pkey",
          "constraint_validated" => "t",
          "definition" => "PRIMARY KEY (user_id)",
        },
        {
          "table_on" => "books",
          "table_from" => "sellers",
          "constraint_type" => "f",
          "constraint_name" => "books_seller_id_fkey",
          "constraint_validated" => "t",
          "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id)",
        },
        {
          "table_on" => "book_audits",
          "table_from" => "-",
          "constraint_type" => "p",
          "constraint_name" => "book_audits_pkey",
          "constraint_validated" => "t",
          "definition" => "PRIMARY KEY (id)",
        },
        {
          "table_on" => "book_audits",
          "table_from" => "books",
          "constraint_type" => "f",
          "constraint_name" => "book_audits_book_id_fkey",
          "constraint_validated" => "t",
          "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id)",
        },
        {
          "table_on" => "chapters",
          "table_from" => "-",
          "constraint_type" => "p",
          "constraint_name" => "chapters_pkey",
          "constraint_validated" => "t",
          "definition" => "PRIMARY KEY (id)",
        },
        {
          "table_on" => "chapters",
          "table_from" => "books",
          "constraint_type" => "f",
          "constraint_name" => "chapters_book_id_fkey",
          "constraint_validated" => "t",
          "definition" => "FOREIGN KEY (book_id) REFERENCES books(user_id)",
        },
      ]

      expect(described_class.get_all_constraints_for(client)).to eq(result)
    end
  end

  describe ".get_foreign_keys_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns all constraints" do
      result = [
        {
          "table_on" => "books",
          "table_from" => "sellers",
          "constraint_type" => "f",
          "constraint_name" => "books_seller_id_fkey",
          "constraint_validated" => "t",
          "definition" => "FOREIGN KEY (seller_id) REFERENCES sellers(id)",
        },
      ]
      expect(described_class.get_foreign_keys_for(client, "books")).to eq(result)
    end
  end

  describe ".get_primary_keys_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns all constraints" do
      result = [
        {
          "constraint_name" => "books_pkey",
          "constraint_type" => "p",
          "constraint_validated" => "t",
          "definition" => "PRIMARY KEY (user_id)",
          "table_from" => "-",
          "table_on" => "books",
        },
      ]
      expect(described_class.get_primary_keys_for(client, "books")).to eq(result)
    end
  end

  describe ".referential_foreign_keys_to_refresh" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    let(:result) do
      "ALTER TABLE book_audits DROP CONSTRAINT book_audits_book_id_fkey; ALTER TABLE book_audits ADD CONSTRAINT book_audits_book_id_fkey FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID;ALTER TABLE chapters DROP CONSTRAINT chapters_book_id_fkey; ALTER TABLE chapters ADD CONSTRAINT chapters_book_id_fkey FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID;"
    end

    before { setup_tables(client) }

    it "returns drop and add statements" do
      expect(described_class.referential_foreign_keys_to_refresh(client, "books")).to eq(result)
    end

    it "returns drop and add statements accordingly when NOT VALID is present" do
      client = PgOnlineSchemaChange::Client.new(client_options)
      described_class.run(
        client.connection,
        "ALTER TABLE chapters DROP CONSTRAINT chapters_book_id_fkey;",
      )
      described_class.run(
        client.connection,
        "ALTER TABLE chapters ADD CONSTRAINT chapters_book_id_fkey FOREIGN KEY (book_id) REFERENCES books(user_id) NOT VALID;",
      )

      expect(described_class.referential_foreign_keys_to_refresh(client, "books")).to eq(result)
    end
  end

  describe ".self_foreign_keys_to_refresh" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    let(:result) do
      "ALTER TABLE books ADD CONSTRAINT books_seller_id_fkey FOREIGN KEY (seller_id) REFERENCES sellers(id) NOT VALID;"
    end

    before { setup_tables(client) }

    it "returns add statements" do
      expect(described_class.self_foreign_keys_to_refresh(client, "books")).to eq(result)
    end

    it "returns add statements accordingly when NOT VALID is present" do
      client = PgOnlineSchemaChange::Client.new(client_options)
      described_class.run(
        client.connection,
        "ALTER TABLE books DROP CONSTRAINT books_seller_id_fkey;",
      )
      described_class.run(
        client.connection,
        "ALTER TABLE books ADD CONSTRAINT books_seller_id_fkey FOREIGN KEY (seller_id) REFERENCES sellers(id) NOT VALID;",
      )

      expect(described_class.self_foreign_keys_to_refresh(client, "books")).to eq(result)
    end
  end

  describe ".get_foreign_keys_to_validate" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns drop and add statements" do
      result =
        "ALTER TABLE book_audits VALIDATE CONSTRAINT book_audits_book_id_fkey;ALTER TABLE chapters VALIDATE CONSTRAINT chapters_book_id_fkey;ALTER TABLE books VALIDATE CONSTRAINT books_seller_id_fkey;"
      expect(described_class.get_foreign_keys_to_validate(client, "books")).to eq(result)
    end
  end

  describe ".alter_statement_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    it "returns alter statement for shadow table" do
      result = described_class.alter_statement_for(client, "new_books")
      expect(result).to eq("ALTER TABLE new_books ADD COLUMN purchased boolean DEFAULT false")
    end

    it "returns alter statement for shadow table when muliple queries are present" do
      query = <<~SQL
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
        ALTER TABLE books ADD COLUMN "purchased" BOOLEAN DEFAULT FALSE;
      SQL

      options = client_options.to_h.merge(alter_statement: query)
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.alter_statement_for(client, "new_books")
      expect(result).to eq(
        "ALTER TABLE new_books ADD COLUMN purchased boolean DEFAULT false; ALTER TABLE new_books ADD COLUMN purchased boolean DEFAULT false",
      )
    end

    it "returns alter statement for shadow table with RENAME" do
      options =
        client_options.to_h.merge(
          alter_statement: "ALTER TABLE books RENAME COLUMN \"email\" to \"new_email\";",
        )
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.alter_statement_for(client, "new_books")
      expect(result).to eq("ALTER TABLE new_books RENAME COLUMN email TO new_email")
    end

    it "returns alter statement for shadow table with DROP" do
      options = client_options.to_h.merge(alter_statement: "ALTER TABLE books DROP \"email\"")
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.alter_statement_for(client, "new_books")
      expect(result).to eq("ALTER TABLE new_books DROP email")
    end
  end

  describe ".get_indexes_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    it "returns index statements for the given table on client" do
      query = <<~SQL
        SELECT indexdef, schemaname
        FROM pg_indexes
        WHERE schemaname = '#{client.schema}' AND tablename = 'books'
      SQL

      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(query).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      result = described_class.get_indexes_for(client, "books")
      expect(result).to eq(
        [
          "CREATE UNIQUE INDEX books_pkey ON #{client.schema}.books USING btree (user_id)",
          "CREATE UNIQUE INDEX books_username_key ON #{client.schema}.books USING btree (username)",
          "CREATE UNIQUE INDEX books_email_key ON #{client.schema}.books USING btree (email)",
        ],
      )
    end
  end

  describe ".get_triggers_for" do
    before { setup_tables(client) }

    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    it "returns trigger statements for the given table on client" do
      query = <<~SQL
        SELECT pg_get_triggerdef(oid) as tdef FROM pg_trigger
        WHERE  tgrelid = '#{client.schema}.books'::regclass AND tgisinternal = FALSE;
      SQL

      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(query).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      result = described_class.get_triggers_for(client, "books")
      expect(result).to eq(
        "CREATE TRIGGER email_changes AFTER UPDATE ON books FOR EACH ROW EXECUTE PROCEDURE public.email_changes();",
      )
    end
  end

  describe ".primary_key_for" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "returns index statements for shadow table with altered index name" do
      query = <<~SQL
        SELECT
          pg_attribute.attname as column_name
        FROM pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          pg_class.oid = '#{client.table_name}'::regclass AND
          indrelid = pg_class.oid AND
          nspname = '#{client.schema}' AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey)
        AND indisprimary
      SQL

      expect(client.connection).to receive(:async_exec).with("BEGIN;").and_call_original
      expect(client.connection).to receive(:async_exec).with(query).and_call_original
      expect(client.connection).to receive(:async_exec).with("COMMIT;").and_call_original

      result = described_class.primary_key_for(client, client.table_name)
      expect(result).to eq("user_id")
    end
  end

  describe ".dropped_columns" do
    it "returns column being dropped" do
      options = client_options.to_h.merge(alter_statement: "ALTER TABLE books DROP \"email\";")
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.dropped_columns(client)
      expect(result).to eq(["email"])
    end

    it "returns all columns being dropped" do
      options =
        client_options.to_h.merge(
          alter_statement: "ALTER TABLE books DROP \"email\";ALTER TABLE books DROP \"foobar\";",
        )
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)
      result = described_class.dropped_columns(client)
      expect(result).to eq(['email', 'foobar'])
    end

    it "returns no being dropped" do
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.dropped_columns(client)
      expect(result).to eq([])
    end
  end

  describe ".renamed_columns" do
    it "returns column being renamed" do
      options =
        client_options.to_h.merge(
          alter_statement: "ALTER TABLE books RENAME COLUMN \"email\" to \"new_email\";",
        )
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.renamed_columns(client)
      expect(result).to eq([{ old_name: "email", new_name: "new_email" }])
    end

    it "returns all columns being renamed" do
      options =
        client_options.to_h.merge(
          alter_statement:
            "ALTER TABLE books RENAME COLUMN \"email\" to \"new_email\";ALTER TABLE books RENAME COLUMN \"foobar\" to \"new_foobar\";",
        )
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)
      result = described_class.renamed_columns(client)
      expect(result).to eq(
        [
          { old_name: "email", new_name: "new_email" },
          { old_name: "foobar", new_name: "new_foobar" },
        ],
      )
    end

    it "returns no being renamed" do
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.renamed_columns(client)
      expect(result).to eq([])
    end
  end

  describe ".storage_parameters_for" do
    it "returns all the parameters successfully" do
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.storage_parameters_for(client, "books")
      expect(result).to eq(
        "autovacuum_enabled=true,autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=20000",
      )
    end

    it "returns empty string when no sotrage params exist" do
      client = PgOnlineSchemaChange::Client.new(client_options)

      result = described_class.storage_parameters_for(client, "sellers")
      expect(result).to be_nil
    end
  end

  describe ".copy_data_statement" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)

      PgOnlineSchemaChange::Store.set(
        :dropped_columns_list,
        described_class.dropped_columns(client),
      )
      PgOnlineSchemaChange::Store.set(
        :renamed_columns_list,
        described_class.renamed_columns(client),
      )

      client
    end

    before { setup_tables(client) }

    it "returns the copy statement from shadow table" do
      statement = described_class.copy_data_statement(client, "pgosc_shadow_table_for_books")
      result = <<~SQL
        INSERT INTO pgosc_shadow_table_for_books("user_id", "username", "seller_id", "password", "email", "createdOn", "last_login")
        SELECT "user_id", "username", "seller_id", "password", "email", "createdOn", "last_login"
        FROM ONLY books
      SQL
      expect(statement).to eq(result)
    end

    describe "on dropped column" do
      let(:client) do
        options =
          client_options.to_h.merge(alter_statement: "ALTER TABLE books DROP COLUMN \"user_id\";")
        client_options = Struct.new(*options.keys).new(*options.values)
        client = PgOnlineSchemaChange::Client.new(client_options)

        PgOnlineSchemaChange::Store.set(
          :dropped_columns_list,
          described_class.dropped_columns(client),
        )
        PgOnlineSchemaChange::Store.set(
          :renamed_columns_list,
          described_class.renamed_columns(client),
        )

        client
      end

      it "returns the copy statement from shadow table" do
        statement = described_class.copy_data_statement(client, "pgosc_shadow_table_for_books")
        result = <<~SQL
          INSERT INTO pgosc_shadow_table_for_books("username", "seller_id", "password", "email", "createdOn", "last_login")
          SELECT "username", "seller_id", "password", "email", "createdOn", "last_login"
          FROM ONLY books
        SQL
        expect(statement).to eq(result)
      end
    end

    describe "on renamed column" do
      let(:client) do
        options =
          client_options.to_h.merge(
            alter_statement: "ALTER TABLE books RENAME COLUMN \"user_id\" to \"new_user_id\"; ",
          )
        client_options = Struct.new(*options.keys).new(*options.values)
        client = PgOnlineSchemaChange::Client.new(client_options)

        PgOnlineSchemaChange::Store.set(
          :dropped_columns_list,
          described_class.dropped_columns(client),
        )
        PgOnlineSchemaChange::Store.set(
          :renamed_columns_list,
          described_class.renamed_columns(client),
        )

        client
      end

      it "returns the copy statement from shadow table" do
        statement = described_class.copy_data_statement(client, "pgosc_shadow_table_for_books")
        result = <<~SQL
          INSERT INTO pgosc_shadow_table_for_books("new_user_id", "username", "seller_id", "password", "email", "createdOn", "last_login")
          SELECT "user_id", "username", "seller_id", "password", "email", "createdOn", "last_login"
          FROM ONLY books
        SQL
        expect(statement).to eq(result)
      end
    end
  end

  describe ".open_lock_exclusive" do
    let(:client) do
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client
    end

    before { setup_tables(client) }

    it "successfully acquires lock on first attempt and returns true" do
      query =
        "SELECT * FROM pg_locks WHERE locktype = 'relation' AND relation='#{client.table}'::regclass::oid"
      lock = []

      # Ensure lock is not present on books table (OID)
      described_class.run(client.connection, query) { |result| lock = result.map { |row| row } }
      expect(lock).to eq([])

      acquired = described_class.open_lock_exclusive(client, client.table)
      expect(acquired).to be(true)

      # Ensure lock is present on books table (OID)
      described_class.run(client.connection, query) { |result| lock = result.map { |row| row } }

      expect(lock.size).to eq(1)
      expect(lock.first["mode"]).to eq("AccessExclusiveLock")
      expect(lock.first["granted"]).to eq("t")
      expect(lock.first["pid"].to_i).to eq(client.connection.backend_pid)

      client.connection.async_exec("COMMIT;")
    end
  end

  describe ".open_lock_exclusive with forked process and kills backend" do
    it "cannot acquire lock at first, kills backend (forked process), sucesfully acquires lock and returns true" do
      pid =
        fork do
          new_client = PgOnlineSchemaChange::Client.new(client_options)
          setup_tables(new_client)
          new_client.connection.async_exec(
            "SET search_path to #{new_client.schema}; BEGIN; LOCK TABLE #{new_client.table_name} IN ACCESS EXCLUSIVE MODE;",
          )

          sleep(50)
        rescue StandardError => e
          puts e.inspect
          # do nothing. there err messages from backend being terminated and/or
          # query being cancelled
        end
      Process.detach(pid)

      options = client_options.to_h.merge(kill_backends: true)
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client.connection.async_exec("SET search_path to #{client.schema};")

      sleep 0.5

      acquired = described_class.open_lock_exclusive(client, client.table)
      expect(acquired).to be(true)

      client.connection.async_exec("COMMIT;")
    end
  end

  describe ".open_lock_exclusive with forked process" do
    it "cannot acquire lock and returns false" do
      # acquire a lock from another process to test
      pid =
        fork do
          new_client = PgOnlineSchemaChange::Client.new(client_options)
          setup_tables(new_client)
          new_client.connection.async_exec(
            "SET search_path to #{new_client.schema}; BEGIN; LOCK TABLE #{new_client.table_name} IN ACCESS EXCLUSIVE MODE;",
          )

          sleep(50)
        rescue StandardError
          # do nothing. there err messages from backend being terminated and/or
          # query being cancelled
        end
      Process.detach(pid)

      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client.connection.async_exec("SET search_path to #{client.schema};")

      sleep(0.5)

      acquired = described_class.open_lock_exclusive(client, client.table)
      expect(acquired).to be(false)
    ensure
      Process.kill("KILL", pid)
    end

    # This test is ensuring on last try it can acquire lock.
    # It does so by ensuring the forked process dies after 10s
    # thus losing the lock and since the wait_time_for_lock is for 5
    # it will/should pass on 3rd try (>10s).
    it "acquires lock on 3rd try and returns true" do
      pid =
        fork do
          new_client = PgOnlineSchemaChange::Client.new(client_options)
          setup_tables(new_client)
          new_client.connection.async_exec(
            "SET search_path to #{new_client.schema}; BEGIN; LOCK TABLE #{new_client.table_name} IN ACCESS EXCLUSIVE MODE;",
          )

          sleep(10)
        rescue StandardError
          # do nothing. there err messages from backend being terminated and/or
          # query being cancelled
        end
      Process.detach(pid)

      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)
      client.connection.async_exec("SET search_path to #{client.schema};")

      sleep 0.5

      acquired = described_class.open_lock_exclusive(client, client.table)
      expect(acquired).to be(true)

      query =
        "SELECT * FROM pg_locks WHERE locktype = 'relation' AND relation='#{client.table}'::regclass::oid"
      lock = []

      described_class.run(client.connection, query) { |result| lock = result.map { |row| row } }
      expect(lock.size).to eq(1)
      expect(lock.first["mode"]).to eq("AccessExclusiveLock")
      expect(lock.first["granted"]).to eq("t")
      expect(lock.first["pid"].to_i).to eq(client.connection.backend_pid)

      client.connection.async_exec("COMMIT;")
    end
  end

  describe ".kill_backends" do
    it "returns empty response when no queries are present to kill" do
      options = client_options.to_h.merge(kill_backends: true)
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)

      client.connection.async_exec("SET search_path to #{client.schema};")
      result = described_class.kill_backends(client, client.table).map { |n| n }
      expect(result.first).to be_nil
    end

    it "successfully kills open transaction/backends" do
      # acquire a lock from another process to test

      pid =
        fork do
          new_client = PgOnlineSchemaChange::Client.new(client_options)
          setup_tables(new_client)
          new_client.connection.async_exec(
            "SET search_path to #{new_client.schema}; BEGIN; LOCK TABLE #{new_client.table_name} IN ACCESS EXCLUSIVE MODE;",
          )

          sleep(5)
        rescue StandardError
          # do nothing. there err messages from backend being terminated and/or
          # query being cancelled
        end
      Process.detach(pid)
      sleep(1)

      options = client_options.to_h.merge(kill_backends: true)
      client_options = Struct.new(*options.keys).new(*options.values)
      client = PgOnlineSchemaChange::Client.new(client_options)
      allow(PgOnlineSchemaChange::Client).to receive(:new).and_return(client)

      client.connection.async_exec("SET search_path to #{client.schema};")

      result = described_class.kill_backends(client, client.table).map { |n| n }

      expect(result.first).to eq({ "pg_terminate_backend" => "t" })
    ensure
      Process.kill("KILL", pid)
    end
  end
end
