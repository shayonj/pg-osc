# frozen_string_literal: true

require_relative "lib/pg_online_schema_change/version"

Gem::Specification.new do |spec|
  spec.name = "pg_online_schema_change"
  spec.version = PgOnlineSchemaChange::VERSION
  spec.authors = ["Shayon Mukherjee"]
  spec.email = ["shayog@gmail.com"]

  spec.summary = "pg-online-schema-change is a tool for schema changes for Postgres tables with minimal locks"
  spec.description = "pg-online-schema-change is a tool for schema changes for Postgres tables with minimal locks"
  spec.homepage = "https://github.com/shayonj/pg-online-schema-change"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 2.6.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "https://github.com/shayonj/pg_online_schema_change/blob/main/CODE_OF_CONDUCT.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:test|spec|features)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end
  spec.bindir = "bin"
  spec.executables = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.metadata = {
    "rubygems_mfa_required" => "true"
  }
end
