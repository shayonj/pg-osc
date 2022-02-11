module PgOnlineSchemaChange
  class Replay
    extend Helper

    class << self
      PULL_BATCH_COUNT = 1000
      DELTA_COUNT = 20
      RESERVED_COLUMNS = %w[operation_type trigger_time].freeze

      # This, picks PULL_BATCH_COUNT rows by primary key from audit_table,
      # replays it on the shadow_table. Once the batch is done,
      # it them deletes those PULL_BATCH_COUNT rows from audit_table. Then, pull another batch,
      # check if the row count matches PULL_BATCH_COUNT, if so swap, otherwise
      # continue. Swap because, the row count is minimal to replay them altogether
      # and perform the rename while holding an access exclusive lock for minimal time.
      def begin!
        loop do
          rows = rows_to_play

          raise CountBelowDelta if rows.count <= DELTA_COUNT

          play!(rows)
        end
      end

      def rows_to_play(reuse_trasaction = false)
        select_query = <<~SQL
          SELECT * FROM #{audit_table} ORDER BY #{primary_key} LIMIT #{PULL_BATCH_COUNT};
        SQL

        rows = []
        Query.run(client.connection, select_query, reuse_trasaction) { |result| rows = result.map { |row| row } }

        rows
      end

      def play!(rows, reuse_trasaction = false)
        PgOnlineSchemaChange.logger.info("Replaying rows, count: #{rows.size}")

        to_be_deleted_rows = []
        to_be_replayed = []
        rows.each do |row|
          new_row = row.dup

          # Remove audit table cols, since we will be
          # re-mapping them for inserts and updates
          RESERVED_COLUMNS.each do |col|
            new_row.delete(col)
          end

          if dropped_columns.any?
            dropped_columns.each do |dropped_column|
              new_row.delete(dropped_column)
            end
          end

          if renamed_columns.any?
            renamed_columns.each do |object|
              value = new_row.delete(object[:old_name])
              new_row[object[:new_name]] = value
            end
          end

          new_row = new_row.compact

          # quote indent column to preserve case insensitivity
          # ensure rows are escaped
          new_row = new_row.transform_keys do |column|
            client.connection.quote_ident(column)
          end

          new_row = new_row.transform_values do |value|
            client.connection.escape_string(value)
          end

          case row["operation_type"]
          when "INSERT"
            values = new_row.map { |_, val| "'#{val}'" }.join(",")

            sql = <<~SQL
              INSERT INTO #{shadow_table} (#{new_row.keys.join(",")})
                VALUES (#{values});
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[primary_key]}'"
          when "UPDATE"
            set_values = new_row.map do |column, value|
              "#{column} = '#{value}'"
            end.join(",")

            sql = <<~SQL
              UPDATE #{shadow_table}
              SET #{set_values}
              WHERE #{primary_key}=\'#{row[primary_key]}\';
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[primary_key]}'"
          when "DELETE"
            sql = <<~SQL
              DELETE FROM #{shadow_table} WHERE #{primary_key}=\'#{row[primary_key]}\';
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[primary_key]}'"
          end
        end

        Query.run(client.connection, to_be_replayed.join, reuse_trasaction)

        # Delete items from the audit now that are replayed
        if to_be_deleted_rows.count >= 1
          delete_query = <<~SQL
            DELETE FROM #{audit_table} WHERE #{primary_key} IN (#{to_be_deleted_rows.join(",")})
          SQL
          Query.run(client.connection, delete_query, reuse_trasaction)
        end
      end
    end
  end
end
