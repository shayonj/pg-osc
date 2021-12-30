require "thor"

module PgOnlineSchemaChange
  class CLI < Thor
    desc "perform", "Perform the set of operations to safely apply the schema change with minimal locks"
    method_option :alter_statement, aliases: "-a", type: :string, required: true,
                                    desc: "The ALTER statement to perform the schema change"
    method_option :schema, aliases: "-s", type: :string, required: true, default: "public",
                           desc: "The schema in which the table is"
    method_option :dbname, aliases: "-d", type: :string, required: true, desc: "Name of the database"
    method_option :host, aliases: "-h", type: :string, required: true, desc: "Server host where the Database is located"
    method_option :username, aliases: "-u", type: :string, required: true, desc: "Username for the Database"
    method_option :port, aliases: "-p", type: :numeric, required: true, default: 5432, desc: "Port for the Database"
    method_option :password, aliases: "-w", type: :string, required: true, desc: "Password for the Database"
    method_option :verbose, aliases: "-v", type: :boolean, default: false, desc: "Emit logs in debug mode"

    def perform
      client_options = Struct.new(*options.keys.map(&:to_sym)).new(*options.values)

      PgOnlineSchemaChange.logger = client_options.verbose
      PgOnlineSchemaChange::Orchestrate.run!(client_options)
    end

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
