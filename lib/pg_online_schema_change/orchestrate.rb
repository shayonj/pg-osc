module PgOnlineSchemaChange
  class Orchestrate
    class << self
      PULL_BATCH_COUNT = 1000
      DELTA_COUNT = 20
      RESERVED_COLUMNS = %w[operation_type trigger_time].freeze

      attr_accessor :client, :audit_table, :shadow_table, :primary_key, :parent_table_columns, :dropped_columns,
                    :renamed_columns, :old_primary_table

      def init
        @dropped_columns = []
        @renamed_columns = []
      end

      def setup!(options)
        @client = Client.new(options)

        sql = <<~SQL
          SET statement_timeout = 0;
          SET client_min_messages = warning;
        SQL

        Query.run(client.connection, sql)
        # install functions
        Query.run(client.connection, FIX_SERIAL_SEQUENCE)
      end

      def run!(options)
        setup!(options)

        raise Error, "Parent table has no primary key, exiting..." if primary_key.nil?

        setup_audit_table!
        setup_trigger!
        setup_shadow_table!
        disable_vacuum!
        copy_data!
        run_alter_statement!
        replay_and_swap!
        # run_analyze!
        # drop_and_cleanup!
      rescue StandardError => e
        PgOnlineSchemaChange.logger.fatal("Something went wrong: #{e.message}", { e: e })

        # drop_and_cleanup!

        raise e
      end

      def setup_audit_table!
        @audit_table = "pgosc_audit_table_for_#{client.table}"
        PgOnlineSchemaChange.logger.info("Setting up audit table", { audit_table: @audit_table })

        sql = <<~SQL
          CREATE TABLE #{@audit_table} (operation_type text, trigger_time timestamp, LIKE #{client.table});
        SQL

        Query.run(client.connection, sql)
      end

      def setup_trigger!
        PgOnlineSchemaChange.logger.info("Setting up triggers")

        sql = <<~SQL
          CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
          RETURNS TRIGGER AS
          $$
          BEGIN
            IF ( TG_OP = 'INSERT') THEN
              INSERT INTO \"#{audit_table}\" select 'INSERT', now(), NEW.* ;
              RETURN NEW;
            ELSIF ( TG_OP = 'UPDATE') THEN
              INSERT INTO \"#{audit_table}\" select 'UPDATE', now(),  NEW.* ;
              RETURN NEW;
            ELSIF ( TG_OP = 'DELETE') THEN
              INSERT INTO \"#{audit_table}\" select 'DELETE', now(), OLD.* ;
              RETURN NEW;
            END IF;
          END;
          $$ LANGUAGE PLPGSQL SECURITY DEFINER;

          CREATE TRIGGER primary_to_audit_table_trigger
          AFTER INSERT OR UPDATE OR DELETE ON #{client.table}
          FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
        SQL

        Query.run(client.connection, sql)
      end

      def setup_shadow_table!
        @shadow_table = "pgosc_shadow_table_for_#{client.table}"
        PgOnlineSchemaChange.logger.info("Setting up shadow table", { shadow_table: shadow_table })

        sql = <<~SQL
          CREATE TABLE #{shadow_table} (LIKE #{client.table} INCLUDING ALL);
        SQL
        Query.run(client.connection, sql)

        # update serials
        Query.run(client.connection, "SELECT fix_serial_sequence('#{client.table}', '#{shadow_table}');")
      end

      # Disabling vacuum to avoid any issues during the process
      def disable_vacuum!
        PgOnlineSchemaChange.logger.debug("Disabling vacuum on shadow and audit table",
                                          { shadow_table: shadow_table, audit_table: audit_table })
        sql = <<~SQL
          ALTER TABLE #{shadow_table} SET (
            autovacuum_enabled = false, toast.autovacuum_enabled = false
          );

          ALTER TABLE #{audit_table} SET (
            autovacuum_enabled = false, toast.autovacuum_enabled = false
          );
        SQL
        Query.run(client.connection, sql)
      end

      # Begin the process to copy data into copy table
      # depending on the size of the table, this can be a time
      # taking operation.
      def copy_data!
        PgOnlineSchemaChange.logger.info("Copying contents onto on shadow table from parent table...",
                                         { shadow_table: shadow_table, parent_table: client.table })

        @parent_table_columns = Query.table_columns(client).map { |entry| entry["column_name"] }
        columns = parent_table_columns.join(", ")

        sql = <<~SQL
          INSERT INTO #{shadow_table}
          SELECT #{columns}
          FROM ONLY #{client.table}
        SQL
        Query.run(client.connection, sql)
      end

      def run_alter_statement!
        statement = Query.alter_statement_for(client, shadow_table)
        PgOnlineSchemaChange.logger.info("Running alter statement on shadow table",
                                         { shadow_table: shadow_table, parent_table: client.table })
        Query.run(client.connection, statement)

        @dropped_columns = Query.dropped_columns(client)
        @renamed_columns = Query.renamed_columns(client)
      end

      # This, picks PULL_BATCH_COUNT rows by primary key from audit_table,
      # replays it on the shadow_table. Once the batch is done,
      # it them deletes those PULL_BATCH_COUNT rows from audit_table. Then, pull another batch,
      # check if the row count matches PULL_BATCH_COUNT, if so swap, otherwise
      # continue. Swap because, the row count is minimal to replay them altogether
      # and perform the rename while holding an access exclusive lock for minimal time.
      def replay_and_swap!
        loop do
          sleep 0.5

          select_query = <<~SQL
            SELECT * FROM #{audit_table} ORDER BY #{primary_key} LIMIT #{PULL_BATCH_COUNT};
          SQL

          rows = []
          Query.run(client.connection, select_query) { |result| rows = result.map { |row| row } }

          raise CountBelowDelta if rows.count <= DELTA_COUNT

          replay_data!(rows)
        end
      rescue CountBelowDelta
        PgOnlineSchemaChange.logger.info("Remaining rows below delta count, proceeding towards swap")

        swap!
      end

      def replay_data!(rows)
        to_be_deleted_rows = []
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

          case row["operation_type"]
          when "INSERT"
            values = new_row.map { |_, val| "'#{val}'" }.join(",")

            sql = <<~SQL
              INSERT INTO #{shadow_table} (#{new_row.keys.join(",")})
              VALUES (#{values});
            SQL
            Query.run(client.connection, sql)

            to_be_deleted_rows << new_row[primary_key]
          when "UPDATE"
            set_values = new_row.map do |column, value|
              "#{column} = '#{value}'"
            end.join(",")

            sql = <<~SQL
              UPDATE #{shadow_table}
              SET #{set_values}
              WHERE #{primary_key}=\'#{row[primary_key]}\';
            SQL
            Query.run(client.connection, sql)

            to_be_deleted_rows << row[primary_key]
          when "DELETE"
            sql = <<~SQL
              DELETE FROM #{shadow_table} WHERE #{primary_key}=\'#{row[primary_key]}\';
            SQL
            Query.run(client.connection, sql)
            to_be_deleted_rows << row[primary_key]
          end
        end

        # Delete items from the audit now that are replayed
        if to_be_deleted_rows.count >= 1
          delete_query = <<~SQL
            DELETE FROM #{audit_table} WHERE #{primary_key} IN (#{to_be_deleted_rows.join(",")})
          SQL
          Query.run(client.connection, delete_query)
        end
      end

      def swap!
        @old_primary_table = "pgosc_old_primary_table_#{client.table}"

        sql = <<~SQL
          LOCK TABLE #{client.table} IN ACCESS EXCLUSIVE MODE;
          ALTER TABLE #{client.table} RENAME to #{old_primary_table};
          ALTER TABLE #{shadow_table} RENAME to #{client.table};
        SQL

        Query.run(client.connection, sql)
      end

      def run_analyze!
      end

      def drop_and_cleanup!
      end

      def primary_key
        @primary_key ||= Query.primary_key_for(client, client.table)
      end
    end
  end
end
