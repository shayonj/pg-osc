module PgOnlineSchemaChange
  class Orchestrate
    class << self
      attr_accessor :client, :audit_table, :shadow_table

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

        setup_audit_table!
        setup_trigger!
        setup_shadow_table!
        disable_vacuum!
        copy_data!
        # run_alter_statement!
        # add_indexes_to_shadow_table!
        # replay_and_swap!
        # drop_and_cleanup!
      rescue StandardError => e
        PgOnlineSchemaChange.logger.fatal("Soemthing went wrong: #{e.message}", { e: e })
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

      def replay_and_swap!
      end

      def drop_and_cleanup!
      end
    end
  end
end
