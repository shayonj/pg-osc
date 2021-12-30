require "pg_query"
require "pg"

module PgOnlineSchemaChange
  class Query
    class << self
      def alter_statement?(query)
        PgQuery.parse(query).tree.stmts.all? do |statement|
          statement.stmt.alter_table_stmt.instance_of?(PgQuery::AlterTableStmt)
        end
      end

      def table(query)
        PgQuery.parse(query).tables[0]
      end

      def run(connection, query, _timeout = nil, &block)
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
          statement.stmt.alter_table_stmt.relation.relname = shadow_table
        end

        parsed_query.deparse
      end

      def get_indexes_for(client, shadow_table)
        query = <<~SQL
          SELECT indexdef
          FROM pg_indexes
          WHERE schemaname = \'#{client.schema}\' AND tablename = \'#{client.table}\'
        SQL

        indexes = []
        run(client.connection, query) do |result|
          indexes = result.map { |row| row["indexdef"] }
        end

        # Ensure index statements are run against the shadow table
        indexes.map! do |index|
          parsed_query = PgQuery.parse(index)
          parsed_query.tree.stmts.each do |statement|
            statement.stmt.index_stmt.relation.relname = shadow_table
            statement.stmt.index_stmt.relation.schemaname = client.schema
          end

          parsed_query.deparse
        end

        indexes
      end
    end
  end
end
