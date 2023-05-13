# frozen_string_literal: true

module PgOnlineSchemaChange
  class Replay
    extend Helper

    class << self
      # This, picks PULL_BATCH_COUNT rows by primary key from audit_table,
      # replays it on the shadow_table. Once the batch is done,
      # it them deletes those PULL_BATCH_COUNT rows from audit_table. Then, pull another batch,
      # check if the row count matches PULL_BATCH_COUNT, if so swap, otherwise
      # continue. Swap because, the row count is minimal to replay them altogether
      # and perform the rename while holding an access exclusive lock for minimal time.
      def begin!
        loop do
          rows = rows_to_play

          raise CountBelowDelta if rows.count <= client.delta_count

          play!(rows)
        end
      end

      def rows_to_play(reuse_trasaction = false)
        select_query = <<~SQL
          SELECT * FROM #{audit_table} ORDER BY #{audit_table_pk} LIMIT #{client.pull_batch_count};
        SQL

        rows = []
        Query.run(client.connection, select_query, reuse_trasaction) do |result|
          rows = result.map { |row| row }
        end

        rows
      end

      def reserved_columns
        @reserved_columns ||= [trigger_time_column, operation_type_column, audit_table_pk]
      end

      def play!(rows, reuse_trasaction = false)
        logger.info("Replaying rows, count: #{rows.size}")

        to_be_deleted_rows = []
        to_be_replayed = []
        rows.each do |row|
          new_row = row.dup

          # Remove audit table cols, since we will be
          # re-mapping them for inserts and updates
          reserved_columns.each { |col| new_row.delete(col) }

          if dropped_columns_list.any?
            dropped_columns_list.each { |dropped_column| new_row.delete(dropped_column) }
          end

          if renamed_columns_list.any?
            renamed_columns_list.each do |object|
              value = new_row.delete(object[:old_name])
              new_row[object[:new_name]] = value
            end
          end

          new_row = new_row.compact

          # quote indent column to preserve case insensitivity
          # ensure rows are escaped
          new_row = new_row.transform_keys { |column| client.connection.quote_ident(column) }

          new_row = new_row.transform_values { |value| client.connection.escape_string(value) }

          case row[operation_type_column]
          when "INSERT"
            values = new_row.map { |_, val| "'#{val}'" }.join(",")

            sql = <<~SQL
              INSERT INTO #{shadow_table} (#{new_row.keys.join(",")})
                VALUES (#{values});
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[audit_table_pk]}'"
          when "UPDATE"
            set_values = new_row.map { |column, value| "#{column} = '#{value}'" }.join(",")

            sql = <<~SQL
              UPDATE #{shadow_table}
              SET #{set_values}
              WHERE #{primary_key}='#{row[primary_key]}';
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[audit_table_pk]}'"
          when "DELETE"
            sql = <<~SQL
              DELETE FROM #{shadow_table} WHERE #{primary_key}='#{row[primary_key]}';
            SQL
            to_be_replayed << sql

            to_be_deleted_rows << "'#{row[audit_table_pk]}'"
          end
        end

        Query.run(client.connection, to_be_replayed.join, reuse_trasaction)

        # Delete items from the audit now that are replayed
        return unless to_be_deleted_rows.count >= 1

        delete_query = <<~SQL
          DELETE FROM #{audit_table} WHERE #{audit_table_pk} IN (#{to_be_deleted_rows.join(",")})
        SQL
        Query.run(client.connection, delete_query, reuse_trasaction)
      end
    end
  end
end

# rubocop:enable Metrics/AbcSize, Metrics/MethodLength
