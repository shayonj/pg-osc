# frozen_string_literal: true

require "json"
require "ougai"

require "pg_online_schema_change/version"
require "pg_online_schema_change/helper"
require "pg_online_schema_change/functions"
require "pg_online_schema_change/client"
require "pg_online_schema_change/query"
require "pg_online_schema_change/store"
require "pg_online_schema_change/replay"
require "pg_online_schema_change/orchestrate"
require "pg_online_schema_change/cli"

module PgOnlineSchemaChange
  class Error < StandardError; end
  class CountBelowDelta < StandardError; end
  class AccessExclusiveLockNotAcquired < StandardError; end

  def self.logger(verbose: false)
    @logger ||= begin
      logger = Ougai::Logger.new($stdout)
      logger.level = verbose ? Ougai::Logger::TRACE : Ougai::Logger::INFO
      logger.with_fields = { version: PgOnlineSchemaChange::VERSION }
      logger
    end
  end
end
