#!/usr/bin/env python3
"""Generate a realistic-looking Pubky Nexus graph as Cypher files.

Usage: generate.py [--users 1000] [--posts 20000] [--replies 10000] [--reposts 1500]
                   [--post-tags 40000] [--user-tags 3000] [--follows-per-user 30]
                   [--bookmarks 3000] [--ranked 0.85] [--seed 42] [--out DIR]

Ids are real: user ids are z-base-32 Ed25519 public keys (52 chars, what
PubkyId validates), post ids are 13-char Crockford base32 microsecond
timestamps, tag ids are 26-char Crockford strings. Node/relationship
properties mirror docker/test-graph/skunk.cypher so the nexusd reindex can
build the Redis cache from this graph.
"""
import argparse, json, os, random, time, math
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import serialization

Z32 = "ybndrfg8ejkmcpqxot1uwisza345h769"
CROCK = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def b32(data: bytes, alphabet: str) -> str:
    bits = "".join(f"{b:08b}" for b in data)
    bits += "0" * ((5 - len(bits) % 5) % 5)
    return "".join(alphabet[int(bits[i:i + 5], 2)] for i in range(0, len(bits), 5))

def pubky_id() -> str:
    pk = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return b32(pk, Z32)  # 52 chars

def post_id(micros: int) -> str:
    return b32(micros.to_bytes(8, "big"), CROCK)  # 13 chars

def rand_id(n=26) -> str:
    return "".join(random.choice(CROCK) for _ in range(n))

def zipf_weights(n, s=0.9):
    return [1.0 / (i + 1) ** s for i in range(n)]

def cy(v):
    if v is None: return "null"
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float)): return repr(v)
    return json.dumps(v, ensure_ascii=False)

def row(d): return "{" + ", ".join(f"{k}: {cy(v)}" for k, v in d.items()) + "}"

def write_batches(path, header, rows, body, batch=500):
    with open(path, "w") as f:
        f.write(header)
        for i in range(0, len(rows), batch):
            f.write("UNWIND [\n" + ",\n".join(row(r) for r in rows[i:i + batch]) + "\n] AS r\n" + body + ";\n")

WORDS = ("bitcoin lightning pubky nostr privacy free freedom opensource rust decentralized web3 selfhosted keys identity "
         "dns pkarr homeserver censorship encryption zk wallet node relay sync feed stream tag wot trust rank graph "
         "coffee music art travel photo cycling running hiking climbing chess gaming books film science space ai "
         "ml llm agents crypto defi nft dao markets macro energy climate cooking recipe garden dogs cats birds "
         "berlin lisbon austin tokyo london zurich prague madrid rome paris kyiv warsaw amsterdam oslo helsinki "
         "meme lol news politics history philosophy stoic health fitness sleep fasting yoga surf ski sail fish "
         "design ux typography fonts color css html js ts python go c cpp zig nix docker kube neo4j redis sql "
         "startup founder vc bootstrapping remote nomad jobs hiring learning teaching podcast video live audio "
         "🔥 ❤️ 👀 🚀 💯 🎉 🙏 ✨").split()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=1000)
    ap.add_argument("--posts", type=int, default=20000, help="root posts")
    ap.add_argument("--replies", type=int, default=10000)
    ap.add_argument("--reposts", type=int, default=1500)
    ap.add_argument("--post-tags", type=int, default=40000)
    ap.add_argument("--user-tags", type=int, default=3000)
    ap.add_argument("--follows-per-user", type=int, default=30)
    ap.add_argument("--bookmarks", type=int, default=3000)
    ap.add_argument("--ranked", type=float, default=0.85, help="share of users with a trust score")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    a = ap.parse_args()
    random.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)

    now_ms = int(time.time() * 1000) - 60_000
    start_ms = now_ms - 540 * 86_400_000  # ~18 months of history
    def ts(): return random.randint(start_ms, now_ms)

    # ---- users: popularity is Zipf so a few accounts dominate authorship/follows
    users = []
    for i in range(a.users):
        users.append({"id": pubky_id(), "name": f"user_{i:05d}", "bio": f"bio of user {i}",
                      "links": "[]", "status": "noStatus", "indexed_at": ts()})
    pop = zipf_weights(a.users, 0.9)
    ranked_n = int(a.users * a.ranked)
    ranked_idx = set(random.sample(range(a.users), ranked_n))
    for i, u in enumerate(users):
        if i in ranked_idx:
            u["trust"] = round(pop[i] * 0.9 + random.random() * 0.01, 6)  # popular accounts rank higher
    write_batches(os.path.join(a.out, "01_users.cypher"), "// users\n", users,
                  "CREATE (u:User {id: r.id, name: r.name, bio: r.bio, links: r.links, status: r.status, indexed_at: r.indexed_at})\n"
                  "SET u.trust = r.trust")

    # ---- follows: preferential attachment
    follows, seen = [], set()
    for i in range(a.users):
        k = max(1, int(random.expovariate(1 / a.follows_per_user)))
        for t in random.choices(range(a.users), weights=pop, k=k):
            if t != i and (i, t) not in seen:
                seen.add((i, t)); follows.append({"a": users[i]["id"], "b": users[t]["id"], "id": rand_id(13), "indexed_at": ts()})
    write_batches(os.path.join(a.out, "02_follows.cypher"), "// follows\n", follows,
                  "MATCH (a:User {id: r.a}), (b:User {id: r.b}) CREATE (a)-[:FOLLOWS {id: r.id, indexed_at: r.indexed_at}]->(b)")

    # ---- posts (roots, replies, reposts) with unique timestamp ids
    kinds = ["short"] * 70 + ["long"] * 10 + ["image"] * 10 + ["link"] * 7 + ["video"] * 2 + ["file"] * 1
    total = a.posts + a.replies + a.reposts
    micros = sorted(random.sample(range(start_ms * 1000, now_ms * 1000), total))
    authors = random.choices(range(a.users), weights=pop, k=total)
    posts = []
    for n, (m, au) in enumerate(zip(micros, authors)):
        posts.append({"id": post_id(m), "author": users[au]["id"], "kind": random.choice(kinds),
                      "content": f"post {n} by {users[au]['name']}: " + " ".join(random.choices(WORDS, k=random.randint(3, 25))),
                      "indexed_at": m // 1000})
    random.shuffle(posts)
    roots, replies, reposts = posts[:a.posts], posts[a.posts:a.posts + a.replies], posts[a.posts + a.replies:]
    write_batches(os.path.join(a.out, "03_posts.cypher"), "// all posts (roots, replies, reposts) + AUTHORED\n", posts,
                  "MATCH (u:User {id: r.author}) CREATE (u)-[:AUTHORED]->(:Post {id: r.id, kind: r.kind, content: r.content, indexed_at: r.indexed_at})")
    # replies target roots, skewed to popular authors' posts; reposts likewise
    author_pop = {u["id"]: pop[i] for i, u in enumerate(users)}
    root_w = [author_pop[p["author"]] for p in roots]
    rels = [{"child": p["id"], "parent": t["id"]} for p, t in zip(replies, random.choices(roots, weights=root_w, k=len(replies)))]
    write_batches(os.path.join(a.out, "04_replies.cypher"), "// REPLIED\n", rels,
                  "MATCH (c:Post {id: r.child}), (p:Post {id: r.parent}) CREATE (c)-[:REPLIED]->(p)")
    rels = [{"child": p["id"], "parent": t["id"]} for p, t in zip(reposts, random.choices(roots, weights=root_w, k=len(reposts)))]
    write_batches(os.path.join(a.out, "05_reposts.cypher"), "// REPOSTED\n", rels,
                  "MATCH (c:Post {id: r.child}), (p:Post {id: r.parent}) CREATE (c)-[:REPOSTED]->(p)")

    # ---- tags: Zipf vocabulary, skewed to popular posts; no duplicate (tagger,label,post)
    label_w = zipf_weights(len(WORDS), 1.0)
    tags, seen = [], set()
    targets = random.choices(posts, weights=[author_pop[p["author"]] for p in posts], k=a.post_tags)
    taggers = random.choices(users, weights=pop, k=a.post_tags)
    labels = random.choices(WORDS, weights=label_w, k=a.post_tags)
    for p, u, l in zip(targets, taggers, labels):
        if (u["id"], l, p["id"]) in seen: continue
        seen.add((u["id"], l, p["id"]))
        tags.append({"tagger": u["id"], "post": p["id"], "label": l, "id": rand_id(), "indexed_at": max(p["indexed_at"], ts())})
    write_batches(os.path.join(a.out, "06_post_tags.cypher"), "// TAGGED user->post\n", tags,
                  "MATCH (u:User {id: r.tagger}), (p:Post {id: r.post}) CREATE (u)-[:TAGGED {id: r.id, label: r.label, indexed_at: r.indexed_at}]->(p)")
    utags, seen = [], set()
    for u, t, l in zip(random.choices(users, weights=pop, k=a.user_tags), random.choices(users, weights=pop, k=a.user_tags),
                       random.choices(WORDS, weights=label_w, k=a.user_tags)):
        if u is t or (u["id"], l, t["id"]) in seen: continue
        seen.add((u["id"], l, t["id"]))
        utags.append({"tagger": u["id"], "target": t["id"], "label": l, "id": rand_id(), "indexed_at": ts()})
    write_batches(os.path.join(a.out, "07_user_tags.cypher"), "// TAGGED user->user\n", utags,
                  "MATCH (u:User {id: r.tagger}), (t:User {id: r.target}) CREATE (u)-[:TAGGED {id: r.id, label: r.label, indexed_at: r.indexed_at}]->(t)")

    # ---- bookmarks
    bms, seen = [], set()
    for u, p in zip(random.choices(users, weights=pop, k=a.bookmarks), random.choices(posts, k=a.bookmarks)):
        if (u["id"], p["id"]) in seen: continue
        seen.add((u["id"], p["id"]))
        bms.append({"user": u["id"], "post": p["id"], "id": rand_id(13), "indexed_at": max(p["indexed_at"], ts())})
    write_batches(os.path.join(a.out, "08_bookmarks.cypher"), "// BOOKMARKED\n", bms,
                  "MATCH (u:User {id: r.user}), (p:Post {id: r.post}) CREATE (u)-[:BOOKMARKED {id: r.id, indexed_at: r.indexed_at}]->(p)")

    print(f"users={len(users)} (ranked {ranked_n}) follows={len(follows)} posts={len(posts)} "
          f"(roots {len(roots)}, replies {len(replies)}, reposts {len(reposts)}) post_tags={len(tags)} user_tags={len(utags)} bookmarks={len(bms)}")

if __name__ == "__main__":
    main()
