# frozen_string_literal: true

RSpec.describe PgOnlineSchemaChange do
  it "has a version number" do
    expect(PgOnlineSchemaChange::VERSION).not_to be nil
  end

  it "sets and gets a logger instance" do
    described_class.logger = true
    expect(described_class.logger).to be_instance_of(Ougai::Logger)
  end
end
