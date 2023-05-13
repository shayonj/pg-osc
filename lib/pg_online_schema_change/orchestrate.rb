# frozen_string_literal: true

require "securerandom"

module PgOnlineSchemaChange
  class Orchestrate
    SWAP_STATEMENT_TIMEOUT = "5s"

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

        setup_store
      end

      def setup_store
        # Set this early on to ensure their creation and cleanup (unexpected)
        # happens at all times. IOW, the calls from Store.get always return
        # the same value.
        Store.set(:old_primary_table, "pgosc_op_table_#{client.table.downcase}")
        Store.set(:audit_table, "pgosc_at_#{client.table.downcase}_#{pgosc_identifier}")
        Store.set(:operation_type_column, "operation_type_#{pgosc_identifier}")
        Store.set(:trigger_time_column, "trigger_time_#{pgosc_identifier}")
        Store.set(:audit_table_pk, "at_#{pgosc_identifier}_id")
        Store.set(:audit_table_pk_sequence, "#{audit_table}_#{audit_table_pk}_seq")
        Store.set(:shadow_table, "pgosc_st_#{client.table.downcase}_#{pgosc_identifier}")

        Store.set(
          :referential_foreign_key_statements,
          Query.referential_foreign_keys_to_refresh(client, client.table_name),
        )
        Store.set(
          :self_foreign_key_statements,
          Query.self_foreign_keys_to_refresh(client, client.table_name),
        )
        Store.set(:trigger_statements, Query.get_triggers_for(client, client.table_name))
      end

      def run!(options)
        setup!(options)
        Thread.new { handle_signals! }

        raise Error, "Parent table has no primary key, exiting..." if primary_key.nil?

        setup_audit_table!

        setup_trigger!
        setup_shadow_table! # re-uses transaction with serializable
        disable_vacuum! # re-uses transaction with serializable
        run_alter_statement! # re-uses transaction with serializable
        copy_data! # re-uses transaction with serializable
        run_analyze!
        replay_and_swap!
        run_analyze!
        validate_constraints!
        drop_and_cleanup!

        logger.info("All tasks successfully completed")
      rescue StandardError => e
        logger.fatal("Something went wrong: #{e.message}", { e: e })

        drop_and_cleanup!

        raise e
      end

      def setup_signals!
        reader, writer = IO.pipe

        ['TERM', 'QUIT', 'INT'].each { |sig| trap(sig) { writer.puts sig } }

        reader
      end

      def handle_signals!
        reader = setup_signals!
        signal = reader.gets.chomp

        while !reader.closed? && reader.wait_readable # rubocop:disable Lint/UnreachableLoop
          logger.info("Signal #{signal} received, cleaning up")

          client.connection.cancel
          drop_and_cleanup!
          reader.close

          exit(Signal.list[signal])
        end
      end

      def setup_audit_table!
        logger.info("Setting up audit table", { audit_table: audit_table })

        sql = <<~SQL
          CREATE TABLE #{audit_table} (#{audit_table_pk} SERIAL PRIMARY KEY, #{operation_type_column} text, #{trigger_time_column} timestamp, LIKE #{client.table_name});
        SQL

        Query.run(client.connection, sql)
      end

      def setup_trigger!
        # acquire access exclusive lock to ensure audit triggers
        # are setup fine. This also calls kill_backends (if opted in via flag)
        # so any competing backends will be killed to setup the trigger
        opened = Query.open_lock_exclusive(client, client.table_name)

        raise AccessExclusiveLockNotAcquired unless opened

        logger.info("Setting up triggers")

        sql = <<~SQL
          DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table_name};

          CREATE OR REPLACE FUNCTION primary_to_audit_table_trigger()
          RETURNS TRIGGER AS
          $$
          BEGIN
            IF ( TG_OP = 'INSERT') THEN
              INSERT INTO "#{audit_table}" select nextval('#{audit_table_pk_sequence}'), 'INSERT', clock_timestamp(), NEW.* ;
              RETURN NEW;
            ELSIF ( TG_OP = 'UPDATE') THEN
              INSERT INTO "#{audit_table}" select nextval('#{audit_table_pk_sequence}'), 'UPDATE', clock_timestamp(),  NEW.* ;
              RETURN NEW;
            ELSIF ( TG_OP = 'DELETE') THEN
              INSERT INTO "#{audit_table}" select nextval('#{audit_table_pk_sequence}'), 'DELETE', clock_timestamp(), OLD.* ;
              RETURN NEW;
            END IF;
          END;
          $$ LANGUAGE PLPGSQL SECURITY DEFINER;

          CREATE TRIGGER primary_to_audit_table_trigger
          AFTER INSERT OR UPDATE OR DELETE ON #{client.table_name}
          FOR EACH ROW EXECUTE PROCEDURE primary_to_audit_table_trigger();
        SQL

        Query.run(client.connection, sql, opened)
      ensure
        Query.run(client.connection, "COMMIT;")
      end

      def setup_shadow_table!
        # re-uses transaction with serializable
        # This ensures that all queries from here till copy_data run with serializable.
        # This is to to ensure that once the trigger is added to the primay table
        # and contents being copied into the shadow, after a delete all on audit table,
        # any replaying of rows that happen next from audit table do not contain
        # any duplicates. We are ensuring there are no race conditions between
        # adding the trigger, till the copy ends, since they all happen in the
        # same serializable transaction.
        Query.run(client.connection, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", true)
        logger.info("Setting up shadow table", { shadow_table: shadow_table })

        Query.run(
          client.connection,
          "SELECT create_table_all('#{client.table_name}', '#{shadow_table}');",
          true,
        )

        # update serials
        Query.run(
          client.connection,
          "SELECT fix_serial_sequence('#{client.table_name}', '#{shadow_table}');",
          true,
        )
      end

      def disable_vacuum!
        # re-uses transaction with serializable
        # Disabling vacuum to avoid any issues during the process
        result = Query.storage_parameters_for(client, client.table_name, true) || ""
        Store.set(:primary_table_storage_parameters, result)

        logger.debug(
          "Disabling vacuum on shadow and audit table",
          { shadow_table: shadow_table, audit_table: audit_table },
        )
        sql = <<~SQL
          ALTER TABLE #{shadow_table} SET (
            autovacuum_enabled = false, toast.autovacuum_enabled = false
          );

          ALTER TABLE #{audit_table} SET (
            autovacuum_enabled = false, toast.autovacuum_enabled = false
          );
        SQL
        Query.run(client.connection, sql, true)
      end

      def run_alter_statement!
        # re-uses transaction with serializable
        statement = Query.alter_statement_for(client, shadow_table)
        logger.info(
          "Running alter statement on shadow table",
          { shadow_table: shadow_table, parent_table: client.table_name },
        )
        Query.run(client.connection, statement, true)

        Store.set(:dropped_columns_list, Query.dropped_columns(client))
        Store.set(:renamed_columns_list, Query.renamed_columns(client))
      end

      def copy_data!
        # re-uses transaction with serializable
        # Begin the process to copy data into copy table
        # depending on the size of the table, this can be a time
        # taking operation.
        logger.info(
          "Clearing contents of audit table before copy..",
          { shadow_table: shadow_table, parent_table: client.table_name },
        )
        Query.run(client.connection, "DELETE FROM #{audit_table}", true)

        logger.info(
          "Copying contents..",
          { shadow_table: shadow_table, parent_table: client.table_name },
        )
        if client.copy_statement
          query = format(client.copy_statement, shadow_table: shadow_table)
          return Query.run(client.connection, query, true)
        end

        sql = Query.copy_data_statement(client, shadow_table, true)
        Query.run(client.connection, sql, true)
      ensure
        Query.run(client.connection, "COMMIT;") # commit the serializable transaction
      end

      def replay_and_swap!
        Replay.begin!
      rescue CountBelowDelta
        logger.info("Remaining rows below delta count, proceeding towards swap")

        swap!
      end

      def swap!
        logger.info("Performing swap!")
        puts primary_table_storage_parameters
        storage_params_reset =
          (
            if primary_table_storage_parameters.empty?
              "ALTER TABLE #{shadow_table} RESET (autovacuum_enabled, toast.autovacuum_enabled);"
            else
              "ALTER TABLE #{shadow_table} SET (#{primary_table_storage_parameters});"
            end
          )

        # From here on, all statements are carried out in a single
        # transaction with access exclusive lock

        opened = Query.open_lock_exclusive(client, client.table_name)

        raise AccessExclusiveLockNotAcquired unless opened

        Query.run(
          client.connection,
          "SET statement_timeout to '#{SWAP_STATEMENT_TIMEOUT}';",
          opened,
        )

        rows = Replay.rows_to_play(opened)
        Replay.play!(rows, opened)

        query_for_primary_key_refresh =
          Query.query_for_primary_key_refresh(shadow_table, primary_key, client.table_name, opened)

        sql = <<~SQL
          #{query_for_primary_key_refresh};
          ALTER TABLE #{client.table_name} RENAME to #{old_primary_table};
          ALTER TABLE #{shadow_table} RENAME to #{client.table_name};
          #{referential_foreign_key_statements}
          #{self_foreign_key_statements}
          #{trigger_statements}
          #{storage_params_reset}
          DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table_name};
        SQL

        Query.run(client.connection, sql, opened)
      ensure
        Query.run(client.connection, "COMMIT;")
        Query.run(client.connection, "SET statement_timeout = 0;")
      end

      def run_analyze!
        logger.info("Performing ANALYZE!")

        Query.run(client.connection, "ANALYZE VERBOSE #{client.table_name};")
      end

      def validate_constraints!
        logger.info("Validating constraints!")

        validate_statements = Query.get_foreign_keys_to_validate(client, client.table_name)

        Query.run(client.connection, validate_statements)
      end

      def drop_and_cleanup!
        primary_drop = client.drop ? "DROP TABLE IF EXISTS #{old_primary_table};" : ""
        audit_table_drop = audit_table ? "DROP TABLE IF EXISTS #{audit_table}" : ""
        shadow_table_drop = shadow_table ? "DROP TABLE IF EXISTS #{shadow_table}" : ""

        sql = <<~SQL
          DROP TRIGGER IF EXISTS primary_to_audit_table_trigger ON #{client.table_name};
          #{audit_table_drop};
          #{shadow_table_drop};
          #{primary_drop}
          RESET statement_timeout;
          RESET client_min_messages;
          RESET lock_timeout;
        SQL

        Query.run(client.connection, sql)
      end

      private

      def pgosc_identifier
        @pgosc_identifier ||= SecureRandom.hex(3)
      end
    end
  end
end
