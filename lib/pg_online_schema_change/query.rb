require "pg_query"
require "pg"

module PgOnlineSchemaChange
  class Query
    INDEX_SUFFIX = "_pgosc".freeze
    DROPPED_COLUMN_TYPE = :AT_DropColumn
    RENAMED_COLUMN_TYPE = :AT_RenameColumn

    class << self
      def alter_statement?(query)
        PgQuery.parse(query).tree.stmts.all? do |statement|
          statement.stmt.alter_table_stmt.instance_of?(PgQuery::AlterTableStmt) || statement.stmt.rename_stmt.instance_of?(PgQuery::RenameStmt)
        end
      rescue PgQuery::ParseError => e
        false
      end

      def table(query)
        from_rename_statement = PgQuery.parse(query).tree.stmts.map do |statement|
          statement.stmt.rename_stmt&.relation&.relname
        end.compact[0]
        PgQuery.parse(query).tables[0] || from_rename_statement
      end

      def run(connection, query, &block)
        PgOnlineSchemaChange.logger.debug("Running query", { query: query })

        connection.exec("BEGIN;")
        connection.exec(query, &block)
        connection.exec("COMMIT;")
      end

      def table_columns(client, table = nil)
        sql = <<~SQL
          SELECT attname as column_name, format_type(atttypid, atttypmod) as type, attnum as column_position FROM   pg_attribute
          WHERE  attrelid = \'#{table || client.table}\'::regclass AND attnum > 0 AND NOT attisdropped
          ORDER  BY attnum;
        SQL
        mapped_columns = []

        run(client.connection, sql) do |result|
          mapped_columns = result.map do |row|
            row["column_position"] = row["column_position"].to_i
            row
          end
        end

        mapped_columns
      end

      def alter_statement_for(client, shadow_table)
        parsed_query = PgQuery.parse(client.alter_statement)

        parsed_query.tree.stmts.each do |statement|
          statement.stmt.alter_table_stmt.relation.relname = shadow_table if statement.stmt.alter_table_stmt

          statement.stmt.rename_stmt.relation.relname = shadow_table if statement.stmt.rename_stmt
        end
        parsed_query.deparse
      end

      def get_indexes_for(client, table)
        query = <<~SQL
          SELECT indexdef, schemaname
          FROM pg_indexes
          WHERE schemaname = \'#{client.schema}\' AND tablename = \'#{table}\'
        SQL

        indexes = []
        run(client.connection, query) do |result|
          indexes = result.map { |row| row["indexdef"] }
        end

        indexes
      end

      def get_all_constraints_for(client)
        query = <<~SQL
          SELECT  conrelid::regclass AS table_on,
                  confrelid::regclass AS table_from,
                  contype as constraint_type,
                  conname AS constraint_name,
                  pg_get_constraintdef(oid) AS definition
          FROM   	pg_constraint
          WHERE  	contype IN ('f', 'p')
        SQL

        constraints = []
        run(client.connection, query) do |result|
          constraints = result.map { |row| row }
        end

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

      def get_foreign_keys_to_add(client, table)
        references = get_all_constraints_for(client).select do |row|
          row["table_from"] == table && row["constraint_type"] == "f"
        end

        references.map do |row|
          if row["definition"].end_with?("NOT VALID")
            add_statement = "ALTER TABLE #{client.schema}.#{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]};"
          else
            add_statement = "ALTER TABLE #{client.schema}.#{row["table_on"]} ADD CONSTRAINT #{row["constraint_name"]} #{row["definition"]} NOT VALID;"
          end

          drop_statement = "ALTER TABLE #{client.schema}.#{row["table_on"]} DROP CONSTRAINT #{row["constraint_name"]};"

          "#{drop_statement} #{add_statement}"
        end.join
      end

      def dropped_columns(client)
        PgQuery.parse(client.alter_statement).tree.stmts.map do |statement|
          next if statement.stmt.alter_table_stmt.nil?

          statement.stmt.alter_table_stmt.cmds.map do |cmd|
            cmd.alter_table_cmd.name if cmd.alter_table_cmd.subtype == DROPPED_COLUMN_TYPE
          end
        end.flatten.compact
      end

      def renamed_columns(client)
        PgQuery.parse(client.alter_statement).tree.stmts.map do |statement|
          next if statement.stmt.rename_stmt.nil?

          {
            old_name: statement.stmt.rename_stmt.subname,
            new_name: statement.stmt.rename_stmt.newname,
          }
        end.flatten.compact
      end

      def primary_key_for(client, table)
        query = <<~SQL
          SELECT
            pg_attribute.attname as column_name
          FROM pg_index, pg_class, pg_attribute, pg_namespace
          WHERE
            pg_class.oid = \'#{table}\'::regclass AND
            indrelid = pg_class.oid AND
            nspname = \'#{client.schema}\' AND
            pg_class.relnamespace = pg_namespace.oid AND
            pg_attribute.attrelid = pg_class.oid AND
            pg_attribute.attnum = any(pg_index.indkey)
          AND indisprimary
        SQL

        columns = []
        run(client.connection, query) do |result|
          columns = result.map { |row| row["column_name"] }
        end

        columns.first
      end
    end
  end
end
