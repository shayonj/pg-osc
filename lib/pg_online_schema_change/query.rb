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

      def run(connection, query, reuse_trasaction = false, &block) # rubocop:disable Style/ArgumentsForwarding
        if [PG::PQTRANS_INERROR, PG::PQTRANS_UNKNOWN].include?(connection.transaction_status)
          connection.cancel
        end

        logger.debug("Running query", { query: query })

        connection.async_exec("BEGIN;")

        result = connection.async_exec(query, &block) # rubocop:disable Style/ArgumentsForwarding
      rescue Exception # rubocop:disable Lint/RescueException
        connection.cancel if connection.transaction_status != PG::PQTRANS_IDLE
        connection.block
        logger.info("Exception raised, rolling back query", { rollback: true, query: query })
        connection.async_exec("ROLLBACK;")
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

      def get_index_names_for(client, table)
        get_indexes_with_definitions_for(client, table).map { |index| index[:name] }
      end

      def get_indexes_with_definitions_for(client, table)
        query = <<~SQL
          SELECT indexname, indexdef
          FROM pg_indexes
          WHERE schemaname = '#{client.schema}' AND tablename = '#{table}'
          ORDER BY indexname
        SQL

        indexes = []
        run(client.connection, query) do |result|
          indexes = result.map { |row| { name: row["indexname"], definition: row["indexdef"] } }
        end

        indexes
      end

      def get_constraint_names_for(client, table)
        get_constraints_with_definitions_for(client, table).map { |constraint| constraint[:name] }
      end

      def get_constraints_with_definitions_for(client, table)
        query = <<~SQL
          SELECT conname, pg_get_constraintdef(oid) AS condef
          FROM pg_constraint
          WHERE conrelid = '#{client.schema}.#{table}'::regclass
          ORDER BY conname
        SQL

        constraints = []
        run(client.connection, query) do |result|
          constraints = result.map { |row| { name: row["conname"], definition: row["condef"] } }
        end

        constraints
      end

      # An index/constraint definition with its own name and its table's name blanked
      # out, so the same object on the primary and shadow tables compares equal. The
      # name is substituted before the table so that a name containing the table name
      # (index_widgets_on_x, pgosc_st_widgets_ab12cd_x_idx) doesn't leave a fragment
      # behind for the table pass to hit.
      def definition_signature(definition, name, table)
        [[name, "__NAME__"], [table, "__TABLE__"]].reduce(definition) do |signature, (value, placeholder)|
          signature.gsub(/"#{Regexp.escape(value)}"|\b#{Regexp.escape(value)}\b/, placeholder)
        end
      end

      # Pairs each object on the shadow table with the primary table object it was
      # copied from, matching on definition rather than name — LIKE ... INCLUDING ALL
      # discards the original names, so they can't be recovered from the shadow's.
      #
      # Objects whose definitions collide (two indexes over the same columns) have no
      # meaningful pairing, so they're matched in name order on both sides. Which of
      # the interchangeable names each one ends up with is arbitrary, but it's the same
      # arbitrary result on every run.
      def pair_by_definition(primary_objects, shadow_objects, primary_table, shadow_table)
        primary_by_signature =
          primary_objects
            .sort_by { |object| object[:name] }
            .group_by { |object| definition_signature(object[:definition], object[:name], primary_table) }

        shadow_objects
          .sort_by { |object| object[:name] }
          .group_by { |object| definition_signature(object[:definition], object[:name], shadow_table) }
          .flat_map do |signature, shadow_group|
            primary_group = primary_by_signature[signature] || []
            shadow_group.zip(primary_group).filter_map do |shadow, primary|
              next unless primary

              { shadow_name: shadow[:name], original_name: primary[:name] }
            end
          end
      end

      # Indexes and constraints created via "LIKE source_table INCLUDING ALL" on the
      # shadow table are renamed by Postgres, so a swap would otherwise leave the live
      # table with names like pgosc_st_widgets_ab12cd_pkey. This builds statements to
      # put the primary table's names back, pairing objects by definition — the
      # original names aren't recoverable from the shadow's, which are generated from
      # the shadow table and the indexed columns.
      #
      # The primary table (renamed to old_primary_table earlier in the same swap
      # transaction) still holds those names, since renaming a table doesn't rename its
      # indexes/constraints, so they have to be moved aside first or the restores below
      # would collide. Names are read now, before the swap SQL runs and while the table
      # is still client.table; the generated statements target old_primary_table, which
      # is what it's called by the time they execute.
      def restore_names_statement_for(client, shadow_table, old_primary_table)
        index_pairs =
          pair_by_definition(
            get_indexes_with_definitions_for(client, client.table),
            get_indexes_with_definitions_for(client, shadow_table),
            client.table,
            shadow_table
          ).reject { |pair| pair[:shadow_name] == pair[:original_name] }

        # Constraints backed by an index (primary key, unique) share its name, so the
        # index rename above already covers them; renaming again would collide.
        shadow_index_names = get_index_names_for(client, shadow_table)
        constraint_pairs =
          pair_by_definition(
            get_constraints_with_definitions_for(client, client.table),
            get_constraints_with_definitions_for(client, shadow_table).reject do |constraint|
              shadow_index_names.include?(constraint[:name])
            end,
            client.table,
            shadow_table
          ).reject { |pair| pair[:shadow_name] == pair[:original_name] }

        statements =
          index_pairs.map { |pair| "ALTER INDEX #{pair[:original_name]} RENAME TO pgosc_op_#{pair[:original_name]};" } +
          constraint_pairs.map do |pair|
            "ALTER TABLE #{old_primary_table} RENAME CONSTRAINT #{pair[:original_name]} TO pgosc_op_#{pair[:original_name]};"
          end +
          index_pairs.map { |pair| "ALTER INDEX #{pair[:shadow_name]} RENAME TO #{pair[:original_name]};" } +
          constraint_pairs.map do |pair|
            "ALTER TABLE #{client.table_name} RENAME CONSTRAINT #{pair[:shadow_name]} TO #{pair[:original_name]};"
          end

        statements.join
      end

      # fetches the sequence name of a table and column combination
      def get_sequence_name(client, table, column)
        query = <<~SQL
          SELECT pg_get_serial_sequence('#{table}', '#{column}');
        SQL

        run(client.connection, query) do |result|
          result.map { |row| row["pg_get_serial_sequence"] }
        end.first
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

        [referential_foreign_keys, self_foreign_keys].flatten.map do |row|
          "ALTER TABLE #{row["table_on"]} VALIDATE CONSTRAINT #{row["constraint_name"]};"
        end
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
          SELECT DISTINCT
            dependent_view.relname AS view_name,
            pg_get_viewdef(dependent_view.oid) AS view_definition,
            view_ns.nspname AS schema_name
          FROM pg_class AS source_table
          JOIN pg_depend ON pg_depend.refobjid = source_table.oid
          JOIN pg_rewrite ON pg_rewrite.oid = pg_depend.objid
          JOIN pg_class AS dependent_view ON dependent_view.oid = pg_rewrite.ev_class
          JOIN pg_namespace AS view_ns ON dependent_view.relnamespace = view_ns.oid
          AND dependent_view.relkind = 'v'
          AND source_table.relname = '#{table}';
        SQL

        definitions = []
        run(client.connection, query) do |result|
          definitions =
            result.map do |row|
              { "\"#{row["schema_name"]}\".#{row["view_name"]}" => row["view_definition"].strip }
            end
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

      def get_table_size(connection, schema, table_name)
        size_query = "SELECT pg_table_size('#{schema}.#{table_name}');"
        result = run(connection, size_query).first
        result["pg_table_size"].to_i
      rescue StandardError => e
        logger.error("Error getting table size: #{e.message}")
        0
      end
    end
  end
end
