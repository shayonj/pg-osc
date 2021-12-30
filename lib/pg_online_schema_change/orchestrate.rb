module PgOnlineSchemaChange
  class Orchestrate
    class << self
      PULL_BATCH_COUNT = 1000
      DELTA_COUNT = 20

      attr_accessor :client, :audit_table, :shadow_table, :primary_key, :parent_table_columns

      def setup!(options)
        @client = Client.new(options)

        sql = <<~SQL
          SET statement_timeout = 0;
          SET client_min_messages = warning;
        SQL

        Query.run(client.connection, sql)
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
        add_indexes_to_shadow_table!
        # replay_and_swap!
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
          CREATE TABLE #{@audit_table} (operation_type text, trigger_time timestamp, like #{client.table});
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
          CREATE TABLE #{shadow_table} (LIKE #{client.table});
        SQL
        Query.run(client.connection, sql)
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
        columns = Query.table_columns(client).map { |entry| entry["column_name"] }.join(", ")
        @parent_table_columns = columns

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
      end

      def add_indexes_to_shadow_table!
        PgOnlineSchemaChange.logger.info("Adding indexes to the shadow table")
        indexes = Query.get_updated_indexes_for(client, shadow_table)

        indexes.each do |index|
          Query.run(client.connection, index)
        end
      end

      # TODO: Hold access share lock
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
          Query.run(client.connection, statement) { |result| rows = result }

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
          case row["operation_type"]
          when "INSERT"
            values = @parent_table_columns.map { |column| row[column] }

            sql = <<~SQL
              INSERT INTO \"#{shadow_table}\" (#{@parent_table_columns})
              VALUES (#{values});
            SQL

            Query.run(client.connection, sql)

            to_be_deleted_rows << row[primary_key]
          when "UPDATE"
            to_be_deleted_rows << row[primary_key]
          when "DELETE"
            to_be_deleted_rows << row[primary_key]
          end
        end

        # Delete items from the audit now that are replayed
        delete_query = <<~SQL
          DELETE FROM #{audit_table} WHERE id IN (#{to_be_deleted_rows.join(",")})
        SQL
        Query.run(client.connection, delete_query)
      end

      def swap!
      end

      def drop_and_cleanup!
      end

      private def primary_key
        @primary_key ||= Query.primary_key_for(client, client.table)
      end
    end
  end
end
