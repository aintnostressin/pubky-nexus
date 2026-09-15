#!/bin/bash
# Loads the generated synthetic graph into the dockerised Neo4j (replaces whatever is there).
# Run from the host: docker exec neo4j bash /test-graph/synthetic/load.sh
set -e
cd "$(dirname "$0")"
echo "Dropping existing graph..."
cypher-shell -u neo4j -p 12345678 "MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"
for f in 0*.cypher; do
  echo "Loading $f..."
  time cypher-shell -u neo4j -p 12345678 -f "$f" > /dev/null
done
cypher-shell -u neo4j -p 12345678 "MATCH (n) RETURN labels(n)[0] AS label, count(*) AS c ORDER BY c DESC"
cypher-shell -u neo4j -p 12345678 "MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS c ORDER BY c DESC"
