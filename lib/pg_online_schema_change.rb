# frozen_string_literal: true

require "json"
require "ougai"

require "pg_online_schema_change/version"
require "pg_online_schema_change/cli"
require "pg_online_schema_change/client"
require "pg_online_schema_change/query"
require "pg_online_schema_change/orchestrate"

module PgOnlineSchemaChange
  class Error < StandardError; end

  def self.logger=(verbose)
    @@logger ||= begin
      logger = Ougai::Logger.new($stdout)
      logger.level = verbose ? Ougai::Logger::TRACE : Ougai::Logger::INFO
      logger.with_fields = { version: PgOnlineSchemaChange::VERSION }
      logger
    end
  end

  def self.logger
    @@logger
  end
end
