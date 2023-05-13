# frozen_string_literal: true

RSpec.describe(PgOnlineSchemaChange::Store) do
  describe ".set & .get" do
    it "returns the value as a string" do
      described_class.set("foo", 1)
      expect(described_class.get("foo")).to eq(1)
    end

    it "returns the value as a symbol" do
      described_class.set("foo", 1)
      expect(described_class.get(:foo)).to eq(1)
    end
  end
end
