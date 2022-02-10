require "pg_query"
require "pg"

module PgOnlineSchemaChange
  class Store
    class << self
      @@object = {}

      def get(key)
        @@object[key.to_s] || @@object[key.to_sym]
      end

      def set(key, value)
        @@object[key.to_sym] = value
      end
    end
  end
end
