#!/bin/bash

set -euo pipefail

export VERSION=$1
echo "VERSION: ${VERSION}"

echo "=== Building Gem ===="
gem build pg_online_schema_change.gemspec

echo "=== Pushing gem ===="
gem push pg_online_schema_change-"$VERSION".gem

echo "=== Sleeping for 15s ===="
sleep 15

echo "=== Pushing tags to github ===="
git tag v"$VERSION"
git push origin --tags

echo "=== Cleaning up ===="
rm pg_online_schema_change-"$VERSION".gem
