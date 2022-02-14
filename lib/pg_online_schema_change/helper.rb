module PgOnlineSchemaChange
  module Helper
    def primary_key
      result = Store.get(:primary_key)
      return result if result

      Store.set(:primary_key, Query.primary_key_for(client, client.table))
    end

    def logger
      PgOnlineSchemaChange.logger
    end

    def method_missing(method, *_args)
      result = Store.send(:get, method)
      return result if result

      raise ArgumentError, "Method `#{method}` doesn't exist."
    end
  end
end
