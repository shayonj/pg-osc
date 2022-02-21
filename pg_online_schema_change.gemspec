# frozen_string_literal: true

require_relative "lib/pg_online_schema_change/version"

Gem::Specification.new do |spec|
  spec.name = "pg_online_schema_change"
  spec.version = PgOnlineSchemaChange::VERSION
  spec.authors = ["Shayon Mukherjee"]
  spec.email = ["shayonj@gmail.com"]

  spec.description = "Easy CLI tool for making non-blocking zero downtime schema changes in PostgreSQL"
  spec.summary = spec.description
  spec.homepage = "https://github.com/shayonj/pg-osc"
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
    "rubygems_mfa_required" => "true",
  }

  spec.add_runtime_dependency "ougai", "~> 2.0.0"
  spec.add_runtime_dependency "pg", "~> 1.3.2"
  spec.add_runtime_dependency "pg_query", "~> 2.1.3"
  spec.add_runtime_dependency "thor", "~> 1.2.1"

  spec.add_development_dependency "pry", "~> 0.14.1"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "rubocop", "~> 1.23.0"
  spec.add_development_dependency "rubocop-packaging", "~> 0.5.1"
  spec.add_development_dependency "rubocop-performance", "~> 1.12.0"
  spec.add_development_dependency "rubocop-rake", "~> 0.6.0"
  spec.add_development_dependency "rubocop-rspec", "~> 2.7.0"
end
