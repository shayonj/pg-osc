# frozen_string_literal: true

require "pg"

module PgOnlineSchemaChange
  class Client
    attr_accessor :alter_statement,
                  :schema,
                  :dbname,
                  :host,
                  :username,
                  :port,
                  :password,
                  :connection,
                  :table,
                  :table_name,
                  :drop,
                  :kill_backends,
                  :wait_time_for_lock,
                  :copy_statement,
                  :pull_batch_count,
                  :delta_count

    def initialize(options)
      @alter_statement = options.alter_statement
      @schema = options.schema
      @dbname = options.dbname
      @host = options.host
      @username = options.username
      @port = options.port
      @password = options.password
      @drop = options.drop
      @kill_backends = options.kill_backends
      @wait_time_for_lock = options.wait_time_for_lock
      @pull_batch_count = options.pull_batch_count
      @delta_count = options.delta_count

      handle_copy_statement(options.copy_statement)
      handle_validations

      @connection =
        PG.connect(dbname: @dbname, host: @host, user: @username, password: @password, port: @port)

      @table = Query.table(@alter_statement)
      @table_name = Query.table_name(@alter_statement, @table)

      PgOnlineSchemaChange.logger.debug("Connection established")
    end

    def handle_validations
      unless Query.alter_statement?(@alter_statement)
        raise Error, "Not a valid ALTER statement: #{@alter_statement}"
      end

      return if Query.same_table?(@alter_statement)

      raise Error("All statements should belong to the same table: #{@alter_statement}")
    end

    def handle_copy_statement(statement)
      return if statement.nil? || statement == ""

      file_path = File.expand_path(statement)
      raise Error, "File not found: #{file_path}" unless File.file?(file_path)

      @copy_statement = File.binread(file_path)
    end
  end
end
