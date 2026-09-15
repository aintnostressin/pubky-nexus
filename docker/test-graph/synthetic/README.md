# Synthetic test graph

Generates a realistic-looking Nexus graph at a chosen scale so queries and
benchmarks can be measured against more than the ~600 posts of the fixtures.
The default scale is 1,000 users, 31,500 posts (20,000 roots, 10,000 replies,
1,500 reposts), ~23k follows, ~43k tags and ~3k bookmarks, with 85% of the
users carrying a trust score. Popularity is Zipf-distributed, so a few accounts
dominate authorship, follows and tags, as on a live network.

Ids are real: user ids are z-base-32 Ed25519 public keys (what `PubkyId`
validates), post ids are 13-character Crockford base32 microsecond timestamps.
Node and relationship properties mirror `docker/test-graph/skunk.cypher`, so
`nexusd db mock --mock-type redis` can build the Redis cache from this graph
and the `stream` benchmarks that use `source=all` run against it unchanged.

## Usage

```bash
# 1. Generate the Cypher files (needs python3 and the `cryptography` package)
python3 docker/test-graph/synthetic/generate.py            # default scale
python3 docker/test-graph/synthetic/generate.py --users 5000 --posts 200000 --replies 80000

# 2. Load them into the dockerised Neo4j. This REPLACES the current graph.
docker exec neo4j bash /test-graph/synthetic/load.sh

# 3. Build the Redis cache from the graph
cargo run -p nexusd -- db mock --mock-type redis

# 4. Benchmark the global streams (the other stream benches use fixture ids)
cargo bench -p nexus-webapi --bench streams -- 'stream_posts_all|stream_post_keys_all|stream_posts_tag|stream_posts_kind'

# Back to the fixtures
cargo run -p nexusd -- db mock
```

`generate.py --help` lists every knob (`--seed` makes a run reproducible). The
tag vocabulary includes `free`, the label the tag stream benchmark filters on.

The load is plain `UNWIND ... CREATE` batches of 500 rows; the default scale
loads in about 90 seconds and the Redis reindex takes another ~40 seconds.
