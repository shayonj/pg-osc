# frozen_string_literal: true

require "pg_query"
require "pg"

module PgOnlineSchemaChange
  class Query
    extend Helper

    INDEX_SUFFIX = "_pgosc"
    DROPPED_COLUMN_TYPE = :AT_DropColumn
    RENAMED_COLUMN_TYPE = :AT_RenameColumn
    LOCK_ATTEMPT = 4

    class << self
      def alter_statement?(query)
        PgQuery
          .parse(query)
          .tree
          .stmts
          .all? do |statement|
            statement.stmt.alter_table_stmt.instance_of?(PgQuery::AlterTableStmt) ||
              statement.stmt.rename_stmt.instance_of?(PgQuery::RenameStmt)
          end
      rescue PgQuery::ParseError
        false
      end

      def same_table?(query)
        tables =
          PgQuery
            .parse(query)
            .tree
            .stmts
            .filter_map do |statement|
              if statement.stmt.alter_table_stmt.instance_of?(PgQuery::AlterTableStmt)
                statement.stmt.alter_table_stmt.relation.relname
              elsif statement.stmt.rename_stmt.instance_of?(PgQuery::RenameStmt)
                statement.stmt.rename_stmt.relation.relname
              end
            end

        tables.uniq.count == 1
      rescue PgQuery::ParseError
        false
      end

      def table(query)
        from_rename_statement =
          PgQuery
            .parse(query)
            .tree
            .stmts
            .filter_map { |statement| statement.stmt.rename_stmt&.relation&.relname }[
            0
          ]
        PgQuery.parse(query).tables[0] || from_rename_statement
      end

      def table_name(query, table)
        table_name = "\"#{table}\""
        if table =~ /[A-Z]/ && query.include?(table_name) && table[0] != '"'
          table_name
        else
          table
        end
      end

      def run(connection, query, reuse_trasaction = false, &block)
        if [PG::PQTRANS_INERROR, PG::PQTRANS_UNKNOWN].include?(connection.transaction_status)
          connection.cancel
        end

        logger.debug("Running query", { query: query })

        connection.async_exec("BEGIN;")

        result = connection.async_exec(query, &block)
      rescue Exception # rubocop:disable Lint/RescueException
        connection.cancel if connection.transaction_status != PG::PQTRANS_IDLE
        connection.block
        logger.info("Exception raised, rolling back query", { rollback: true, query: query })
        connection.async_exec("ROLLBACK;")
        connection.async_exec("COMMIT;")
        raise
      else
        connection.async_exec("COMMIT;") unless reuse_trasaction
        result
      end

      def table_columns(client, table = nil, reuse_trasaction = false)
        sql = <<~SQL
          SELECT attname as column_name, format_type(atttypid, atttypmod) as type, attnum as column_position FROM   pg_attribute
          WHERE  attrelid = '#{table || client.table_name}'::regclass AND attnum > 0 AND NOT attisdropped
          ORDER  BY attnum;
        SQL
        mapped_columns = []

        run(client.connection, sql, reuse_trasaction) do |result|
          mapped_columns =
            result.map do |row|
              row["column_name_regular"] = row["column_name"]
              row["column_name"] = client.connection.quote_ident(row["column_name"])
              row["column_position"] = row["column_position"].to_i
              row
            end
        end

        mapped_columns
      end

      def alter_statement_for(client, shadow_table)
        parsed_query = PgQuery.parse(client.alter_statement)

        parsed_query.tree.stmts.each do |statement|
          if statement.stmt.alter_table_stmt
            statement.stmt.alter_table_stmt.relation.relname = shadow_table
          end

          statement.stmt.rename_stmt.relation.relname = shadow_table if statement.stmt.rename_stmt
        end
        parsed_query.deparse
      end

      def get_indexes_for(client, table)
        query = <<~SQL
          SELECT indexdef, schemaname
          FROM pg_indexes
          WHERE schemaname = '#{client.schema}' AND tablename = '#{table}'
        SQL

        indexes = []
        run(client.connection, query) { |result| indexes = result.map { |row| row["indexdef"] } }

        indexes
      end

      def get_triggers_for(client, table)
        query = <<~SQL
          SELECT pg_get_triggerdef(oid) as tdef FROM pg_trigger
          WHERE  tgrelid = '#{client.schema}.#{table}'::regclass AND tgisinternal = FALSE;
        SQL

        triggers = []
        run(client.connection, query) { |result| triggers = result.map { |row| "#{row["tdef"]};" } }

        triggers.join(";")
      end

      def get_all_constraints_for(client)
        query = <<~SQL
          SELECT  conrelid::regclass AS table_on,
                  confrelid::regclass AS table_from,
                  contype as constraint_type,
                  conname AS constraint_name,
                  convalidated AS constraint_validated,
                  pg_get_constraintdef(oid) AS definition
          FROM   	pg_constraint
          WHERE  	contype IN ('f', 'p')
        SQL

        constraints = []
        run(client.connection, query) { |result| constraints = result.map { |row| row } }

        constraints
      end

      def get_primary_keys_for(client, table)
        get_all_constraints_for(client).select do |row|
          row["table_on"] == table && row["constraint_type"] == "p"
        end
      end

      def get_foreign_keys_for(client, table)
        get_all_constraints_for(client).select do |row|
          row["table_on"] == table && row["constraint_type"] == "f"
        end
      end

      def referential_foreign_keys_to_refresh(client, table)
        references =
          get_all_constraints_for(client).select do |row|
            row["table_from"] == table && row["constraint_type"] == "f"
          end

        references
          .map do |row|
            add_statement =
              if row["definition"].end_with?("NOT VALID")
                "ALTER TABLE #{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]};"
              else
                "ALTER TABLE #{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]} NOT VALID;"
              end

            drop_statement =
              "ALTER TABLE #{row["table_on"]} DROP CONSTRAINT #{row["constraint_name"]};"

            "#{drop_statement} #{add_statement}"
          end
          .join
      end

      def self_foreign_keys_to_refresh(client, table)
        references =
          get_all_constraints_for(client).select do |row|
            row["table_on"] == table && row["constraint_type"] == "f"
          end

        references
          .map do |row|
            add_statement =
              if row["definition"].end_with?("NOT VALID")
                "ALTER TABLE #{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]};"
              else
                "ALTER TABLE #{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]} NOT VALID;"
              end
            add_statement
          end
          .join
      end

      def get_foreign_keys_to_validate(client, table)
        constraints = get_all_constraints_for(client)
        referential_foreign_keys =
          constraints.select { |row| row["table_from"] == table && row["constraint_type"] == "f" }

        self_foreign_keys =
          constraints.select { |row| row["table_on"] == table && row["constraint_type"] == "f" }

        [referential_foreign_keys, self_foreign_keys].flatten
          .map do |row|
            "ALTER TABLE #{row["table_on"]} VALIDATE CONSTRAINT #{row["constraint_name"]};"
          end
          .join
      end

      def dropped_columns(client)
        PgQuery
          .parse(client.alter_statement)
          .tree
          .stmts
          .map do |statement|
            next if statement.stmt.alter_table_stmt.nil?

            statement.stmt.alter_table_stmt.cmds.map do |cmd|
              cmd.alter_table_cmd.name if cmd.alter_table_cmd.subtype == DROPPED_COLUMN_TYPE
            end
          end
          .flatten
          .compact
      end

      def renamed_columns(client)
        PgQuery
          .parse(client.alter_statement)
          .tree
          .stmts
          .map do |statement|
            next if statement.stmt.rename_stmt.nil?

            {
              old_name: statement.stmt.rename_stmt.subname,
              new_name: statement.stmt.rename_stmt.newname,
            }
          end
          .flatten
          .compact
      end

      def primary_key_for(client, table)
        query = <<~SQL
          SELECT
            pg_attribute.attname as column_name
          FROM pg_index, pg_class, pg_attribute, pg_namespace
          WHERE
            pg_class.oid = '#{table}'::regclass AND
            indrelid = pg_class.oid AND
            nspname = '#{client.schema}' AND
            pg_class.relnamespace = pg_namespace.oid AND
            pg_attribute.attrelid = pg_class.oid AND
            pg_attribute.attnum = any(pg_index.indkey)
          AND indisprimary
        SQL

        columns = []
        run(client.connection, query) { |result| columns = result.map { |row| row["column_name"] } }

        columns.first
      end

      def storage_parameters_for(client, table, reuse_trasaction = false)
        query = <<~SQL
          SELECT array_to_string(reloptions, ',') as params FROM pg_class WHERE relname='#{table}';
        SQL

        columns = []
        run(client.connection, query, reuse_trasaction) do |result|
          columns = result.map { |row| row["params"] }
        end

        columns.first
      end

      def view_definitions_for(client, table)
        query = <<~SQL
          select *
          from INFORMATION_SCHEMA.VIEWS
          where VIEW_DEFINITION like '%#{table}%'
        SQL

        definitions = []
        run(client.connection, query) do |result|
          definitions = result.map { |row| {row["table_name"] => row["view_definition"].strip} }
        end

        definitions
      end

      # This function acquires the lock and keeps the transaction
      # open. If a lock is acquired, its upon the caller
      # to call COMMIT to end the transaction. If a lock
      # is not acquired, transaction is closed and a new transaction
      # is started to acquire lock again
      def open_lock_exclusive(client, table)
        attempts ||= 1

        query = <<~SQL
          SET lock_timeout = '#{client.wait_time_for_lock}s';
          LOCK TABLE #{client.table_name} IN ACCESS EXCLUSIVE MODE;
        SQL
        run(client.connection, query, true)

        true
      rescue PG::LockNotAvailable, PG::InFailedSqlTransaction
        if (attempts += 1) < LOCK_ATTEMPT
          logger.info("Couldn't acquire lock, attempt: #{attempts}")

          run(client.connection, "RESET lock_timeout;")
          kill_backends(client, table)

          retry
        end

        logger.info("Lock acquire failed")
        run(client.connection, "RESET lock_timeout;")

        false
      end

      def kill_backends(client, table)
        return unless client.kill_backends

        logger.info("Terminating other backends")

        query = <<~SQL
          SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'relation' AND relation = '#{table}'::regclass::oid AND pid <> pg_backend_pid()
        SQL

        run(client.connection, query, true)
      end

      def copy_data_statement(client, shadow_table, reuse_trasaction = false)
        select_columns =
          table_columns(client, client.table_name, reuse_trasaction).map do |entry|
            entry["column_name_regular"]
          end

        select_columns -= dropped_columns_list if dropped_columns_list.any?

        insert_into_columns = select_columns.dup

        if renamed_columns_list.any?
          renamed_columns_list.each do |obj|
            insert_into_columns.each_with_index do |insert_into_column, index|
              insert_into_columns[index] = obj[:new_name] if insert_into_column == obj[:old_name]
            end
          end
        end

        insert_into_columns.map! do |insert_into_column|
          client.connection.quote_ident(insert_into_column)
        end

        select_columns.map! { |select_column| client.connection.quote_ident(select_column) }

        <<~SQL
          INSERT INTO #{shadow_table}(#{insert_into_columns.join(", ")})
          SELECT #{select_columns.join(", ")}
          FROM ONLY #{client.table_name}
        SQL
      end

      def primary_key_sequence(shadow_table, primary_key, opened)
        query = <<~SQL
          SELECT pg_get_serial_sequence('#{shadow_table}', '#{primary_key}') as sequence_name
        SQL

        result = run(client.connection, query, opened)

        result.map { |row| row["sequence_name"] }&.first
      end

      def query_for_primary_key_refresh(shadow_table, primary_key, table, opened)
        sequence_name = primary_key_sequence(shadow_table, primary_key, opened)

        return "" if sequence_name.nil?

        <<~SQL
          SELECT setval((select pg_get_serial_sequence('#{shadow_table}', '#{primary_key}')), (SELECT max(#{primary_key}) FROM #{table}));
        SQL
      end
    end
  end
end
