module PgOnlineSchemaChange
  class Orchestrate
    extend Helper

    class << self
      def setup!(options)
        client = Store.set(:client, Client.new(options))

        sql = <<~SQL
          SET statement_timeout = 0;
          SET client_min_messages = warning;
          SET search_path TO #{client.schema};
        SQL

        Query.run(client.connection, sql)
        # install functions
        Query.run(client.connection, FUNC_FIX_SERIAL_SEQUENCE)
        Query.run(client.connection, FUNC_CREATE_TABLE_ALL)
      end

      def run!(options)
        setup!(options)
        Thread.new { handle_signals! }

        raise Error, "Parent table has no primary key, exiting..." if primary_key.nil?

        setup_audit_table!
        setup_trigger!
        setup_shadow_table!
        disable_vacuum!
        run_alter_statement!
        copy_data!
        run_analyze!
        replay_and_swap!
        run_analyze!
        validate_constraints!
        drop_and_cleanup!
      rescue StandardError => e
        logger.fatal("Something went wrong: #{e.message}", { e: e })

        drop_and_cleanup!

        raise e
      end

      def setup_signals!
        reader, writer = IO.pipe

        %w[TERM QUIT INT].each do |sig|
          trap(sig) { writer.puts sig }
        end

        reader
      end

      def handle_signals!
        reader = setup_signals!
        signal = reader.gets.chomp

        while !reader.closed? && IO.select([reader])
          logger.info "Signal #{signal} received, cleaning up"

          client.connection.cancel
          drop_and_cleanup!
          reader.close

          exit Signal.list[signal]
        end
      end

      def setup_audit_table!
        audit_table = Store.set(:audit_table, "pgosc_audit_table_for_#{client.table}")
        logger.info("Setting up audit table", { audit_table: audit_table })

        sql = <<~SQL
          CREATE TABLE #{audit_table} (operation_type text, trigger_time timestamp, LIKE #{client.table});
        SQL

        Query.run(client.connection, sql)
      end

      def setup_trigger!
        # acquire access exclusive lock to ensure audit triggers
        # are setup fine. This also calls kill_backends (if opted in via flag)
        # so any competing backends will be killed to setup the trigger
        opened = Query.open_lock_exclusive(client, client.table)

        raise AccessExclusiveLockNotAcquired unless opened

        logger.info("Setting up triggers")

        sql = <<~SQL
          DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table};

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

        Query.run(client.connection, sql, opened)
      ensure
        Query.run(client.connection, "COMMIT;")
      end

      def setup_shadow_table!
        shadow_table = Store.set(:shadow_table, "pgosc_shadow_table_for_#{client.table}")

        logger.info("Setting up shadow table", { shadow_table: shadow_table })

        Query.run(client.connection, "SELECT create_table_all('#{client.table}', '#{shadow_table}');")

        # update serials
        Query.run(client.connection, "SELECT fix_serial_sequence('#{client.table}', '#{shadow_table}');")
      end

      # Disabling vacuum to avoid any issues during the process
      def disable_vacuum!
        result = Query.storage_parameters_for(client, client.table) || ""
        primary_table_storage_parameters = Store.set(:primary_table_storage_parameters, result)

        logger.debug("Disabling vacuum on shadow and audit table",
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

      def run_alter_statement!
        statement = Query.alter_statement_for(client, shadow_table)
        logger.info("Running alter statement on shadow table",
                    { shadow_table: shadow_table, parent_table: client.table })
        Query.run(client.connection, statement)

        Store.set(:dropped_columns_list, Query.dropped_columns(client))
        Store.set(:renamed_columns_list, Query.renamed_columns(client))
      end

      # Begin the process to copy data into copy table
      # depending on the size of the table, this can be a time
      # taking operation.
      def copy_data!
        logger.info("Copying contents..", { shadow_table: shadow_table, parent_table: client.table })

        if client.copy_statement
          query = format(client.copy_statement, shadow_table: shadow_table)
          return Query.run(client.connection, query)
        end

        sql = Query.copy_data_statement(client, shadow_table)
        Query.run(client.connection, sql)
      end

      def replay_and_swap!
        Replay.begin!
      rescue CountBelowDelta
        logger.info("Remaining rows below delta count, proceeding towards swap")

        swap!
      end

      def swap!
        logger.info("Performing swap!")

        old_primary_table = Store.set(:old_primary_table, "pgosc_old_primary_table_#{client.table}")

        foreign_key_statements = Query.get_foreign_keys_to_refresh(client, client.table)
        storage_params_reset = primary_table_storage_parameters.empty? ? "" : "ALTER TABLE #{client.table} SET (#{primary_table_storage_parameters});"

        # From here on, all statements are carried out in a single
        # transaction with access exclusive lock

        opened = Query.open_lock_exclusive(client, client.table)

        raise AccessExclusiveLockNotAcquired unless opened

        rows = Replay.rows_to_play(opened)
        Replay.play!(rows, opened)

        sql = <<~SQL
          ALTER TABLE #{client.table} RENAME to #{old_primary_table};
          ALTER TABLE #{shadow_table} RENAME to #{client.table};
          #{foreign_key_statements}
          #{storage_params_reset}
          DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table};
        SQL

        Query.run(client.connection, sql)
      ensure
        Query.run(client.connection, "COMMIT;")
      end

      def run_analyze!
        logger.info("Performing ANALYZE!")

        Query.run(client.connection, "ANALYZE VERBOSE #{client.table};")
      end

      def validate_constraints!
        logger.info("Validating constraints!")

        validate_statements = Query.get_foreign_keys_to_validate(client, client.table)

        Query.run(client.connection, validate_statements)
      end

      def drop_and_cleanup!
        primary_drop = client.drop ? "DROP TABLE IF EXISTS #{old_primary_table};" : ""
        audit_table_drop = audit_table ? "DROP TABLE IF EXISTS #{audit_table}" : ""
        shadow_table_drop = shadow_table ? "DROP TABLE IF EXISTS #{shadow_table}" : ""

        sql = <<~SQL
          #{audit_table_drop};
          #{shadow_table_drop};
          #{primary_drop}
          RESET statement_timeout;
          RESET client_min_messages;
          RESET lock_timeout;
        SQL

        Query.run(client.connection, sql)
      end
    end
  end
end
