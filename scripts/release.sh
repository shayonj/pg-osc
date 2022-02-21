export VERSION=$1
echo "VERSION: ${VERSION}"

echo "=== Pushing tags to github ===="
git tag v$VERSION
git push origin --tags

echo "=== Building Gem ===="
gem build pg_online_schema_change.gemspec

echo "=== Pushing gem ===="
gem push pg_online_schema_change-$VERSION.gem

echo "=== Sleeping for 5s ===="
sleep 5 

echo "=== Building Image ===="
docker build . --build-arg VERSION=$VERSION -t pg-osc

echo "=== Tagging Image ===="
docker image tag shayonj/pg-osc:$VERSION shayonj/pg-osc:latest

echo "=== Pushing Image ===="
docker push shayonj/pg-osc:$VERSION
docker push shayonj/pg-osc:latest

echo "=== Cleaning up ===="
rm pg_online_schema_change-$VERSION.gem