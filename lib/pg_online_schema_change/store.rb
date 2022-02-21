# frozen_string_literal: true

require "pg_query"
require "pg"

module PgOnlineSchemaChange
  class Store
    class << self
      @object = {}

      def get(key)
        @object ||= {}
        @object[key.to_s] || @object[key.to_sym]
      end

      def set(key, value)
        @object ||= {}
        @object[key.to_sym] = value
      end
    end
  end
end
