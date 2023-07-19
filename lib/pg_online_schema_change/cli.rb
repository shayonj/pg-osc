# frozen_string_literal: true

require "thor"

module PgOnlineSchemaChange
  PULL_BATCH_COUNT = 1000
  DELTA_COUNT = 20
  class CLI < Thor
    desc "perform", "Safely apply schema changes with minimal locks"
    method_option :alter_statement,
                  aliases: "-a",
                  type: :string,
                  required: true,
                  desc: "The ALTER statement to perform the schema change"
    method_option :schema,
                  aliases: "-s",
                  type: :string,
                  required: true,
                  default: "public",
                  desc: "The schema in which the table is"
    method_option :dbname,
                  aliases: "-d",
                  type: :string,
                  required: true,
                  desc: "Name of the database"
    method_option :host,
                  aliases: "-h",
                  type: :string,
                  required: true,
                  desc: "Server host where the Database is located"
    method_option :username,
                  aliases: "-u",
                  type: :string,
                  required: true,
                  desc: "Username for the Database"
    method_option :port,
                  aliases: "-p",
                  type: :numeric,
                  required: true,
                  default: 5432,
                  desc: "Port for the Database"
    method_option :password,
                  aliases: "-w",
                  type: :string,
                  required: false,
                  default: "",
                  desc:
                    "DEPRECATED: Password for the Database. Please pass PGPASSWORD environment variable instead."
    method_option :verbose,
                  aliases: "-v",
                  type: :boolean,
                  default: false,
                  desc: "Emit logs in debug mode"
    method_option :drop,
                  aliases: "-f",
                  type: :boolean,
                  default: false,
                  desc: "Drop the original table in the end after the swap"
    method_option :kill_backends,
                  aliases: "-k",
                  type: :boolean,
                  default: false,
                  desc:
                    "Kill other competing queries/backends when trying to acquire lock for the shadow table creation and swap. It will wait for --wait-time-for-lock duration before killing backends and try upto 3 times."
    method_option :wait_time_for_lock,
                  aliases: "-w",
                  type: :numeric,
                  default: 10,
                  desc:
                    "Time to wait before killing backends to acquire lock and/or retrying upto 3 times. It will kill backends if --kill-backends is true, otherwise try upto 3 times and exit if it cannot acquire a lock."
    method_option :copy_statement,
                  aliases: "-c",
                  type: :string,
                  required: false,
                  default: "",
                  desc:
                    "Takes a .sql file location where you can provide a custom query to be played (ex: backfills) when pgosc copies data from the primary to the shadow table. More examples in README."
    method_option :pull_batch_count,
                  aliases: "-b",
                  type: :numeric,
                  required: false,
                  default: PULL_BATCH_COUNT,
                  desc:
                    "Number of rows to be replayed on each iteration after copy. This can be tuned for faster catch up and swap. Best used with delta-count."
    method_option :delta_count,
                  aliases: "-e",
                  type: :numeric,
                  required: false,
                  default: DELTA_COUNT,
                  desc:
                    "Indicates how many rows should be remaining before a swap should be performed. This can be tuned for faster catch up and swap, especially on highly volume tables. Best used with pull-batch-count."

    def perform
      client_options = Struct.new(*options.keys.map(&:to_sym)).new(*options.values)
      PgOnlineSchemaChange.logger(verbose: client_options.verbose)

      if client_options.password
        PgOnlineSchemaChange.logger.warn(
          "DEPRECATED: -w is deprecated. Please pass PGPASSWORD environment variable instead.",
        )
      end

      client_options.password = ENV["PGPASSWORD"] || client_options.password

      PgOnlineSchemaChange::Orchestrate.run!(client_options)
    end

    map ['--version', '-v'] => :version
    desc "--version, -v", "print the version"

    def version
      puts PgOnlineSchemaChange::VERSION
    end

    def self.exit_on_failure?
      true
    end

    default_task :perform
  end
end
