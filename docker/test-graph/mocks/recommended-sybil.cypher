// Sybil attack on recommended users (stream/users with source `recommended`).
// Kept inert against the global suites the same way recommended.cypher is: post
// indexed_at far below every window start, no tags, user ids that sort high, and
// no account with more than two followers.
//
// A farm needs one honest follow-back, nothing else:
//   FOLLOWER     -> FOLLOWBACKER          an honest user and the account they follow
//   FOLLOWBACKER -> HONEST_A, HONEST_B
//   HONEST_A     -> HONEST_C
//   ENTRY        -> FOLLOWBACKER          the farm's entry account follows first...
//   FOLLOWBACKER -> ENTRY                 ...and gets followed back
//   ENTRY        -> FARM (20 accounts)
// HONEST_A/B/C, ENTRY and every farm account have 5 posts, the activity threshold.
// The farm follows no one and nobody honest follows it.
// Trust: the follow-back came after the last recompute, so ENTRY and the farm are
// unranked (0.0). HONEST_C (0.35) outranks every other candidate (0.3).

:param follower => 'y43z4cq94w8d58cjuuuw36146nk6ommekj348oe1b4yh9z8j9hwo';
:param followbacker => 'y5ef6cm9izid7kmzs6b1xpg54pk6t5pei587eb7jaa67deaeht4o';
:param honest_a => 'ya589gd689xq9bmw7cgqwdtj3mtz83oir9ysiextb4d5pbi35tjy';
:param honest_b => 'yaz6jxtasz5tce9ued88sf6z7sc6o3jm1xgdt79r5wwbgofsfjfy';
:param honest_c => 'ybfb1ruk9druwtjocjn9ao5iyzk1f8tbefwkqcqnatmsyz5tiuio';
:param entry => 'ydhwtunxg54hxkb1sm6zrbsxb5skpawxx5sj5pwdjto5kw5ja1so';
:param farm => ['yedx9ojnqf56byn69taoq4ta74azy7tuq8wxhb1gzrw7453eqzzo', 'ygtaybbtzdqgq7kcsd5x66n4wd5r7isj35cpbn17qi9kjt36ctfy', 'ytid6ficaeyi7cctun1bauufzwqbeqbobnojx5z5c4zw914cqqoy', 'yw9wzcqezh9bijhwac9az5exb3kewy3de7knfbbyak4yy7juk6do', 'yxb133nz5xybooh8aiwekqpaomrwa8p7yg38zcjdybsepwwchisy', 'yxcro45r8tz1tkqp36e91848r8p8a44gzadjqrssow3ty47podco', 'yxj44hsw7wqp11kpy4a7yjjeqhair66qqrp3ghedbompp1cdus5o', 'z8mtg13esnztw9xjdfxte81xutfki8f5dmqdcdma8sin4s9ke9qy', 'z9zrsjtrwk4wnjxjef6sh7xsk9j3pcitk8i5nsjtq984t9mxbn5y', 'zbtf1364pkb4fkohphqxx67dckmjw7gwq9mzuyycoyktbji6hf9o', 'zc8yh8tmhjinxkup5xfotzfqsh8cexc6ehya5coudpa9sw78foxo', 'zd7pj9d8gwtsdoaez8bbnmfaja37pba3rqjp6hgi81ow557ry9oo', 'zdakxtcsb6gbri4q8psdj1dj44hjgnrwawk5yx4sj4tcja9bsrio', 'zigtrrbqk1j89z5siuunkbpxfpspo1kynwj6azskpzbwmsc1qufo', 'zj1hx8zk5987x9ppko4rrdh7ykw9rpcoyngwu61z3sybb8d9h1xy', 'zj33e91o3giqwwptfm1miwxx1iht4djb9ta1r1i9jj66y4fjyg8y', 'zmyigmkdy1yai6nytem7watfcahrqcykj4efpbg9fdytp88ripay', 'znyk6xk66ptys6eau3nfyf483en9cijdsjric55zan6j86kxahao', 'zoxohaxkcxzrtdq8yq4ajdhtd4iewbd95r1tfjo7c4mh46sg6xao', 'zsgi98en7wg8e8oe7bfjgeyeppxd9bh1gk1pt6o971dc447qgu3y'];

// ##############################
// ##### Create users ###########
// ##############################
MERGE (u:User {id: $follower}) SET u.name = "sybil_attack_follower", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
MERGE (u:User {id: $followbacker}) SET u.name = "sybil_attack_followbacker", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
MERGE (u:User {id: $honest_a}) SET u.name = "sybil_attack_honest_a", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
MERGE (u:User {id: $honest_b}) SET u.name = "sybil_attack_honest_b", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
MERGE (u:User {id: $honest_c}) SET u.name = "sybil_attack_honest_c", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
MERGE (u:User {id: $entry}) SET u.name = "sybil_attack_entry", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";
UNWIND range(0, size($farm) - 1) AS i
MERGE (u:User {id: $farm[i]}) SET u.name = "sybil_attack_farm_" + toString(i + 1), u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]";

// ##############################
// ##### Trust ##################
// ##############################
// Set on every account, so trust.cypher, which gives 0.3 to users without a score,
// leaves them alone. The next recompute would rank the farm through FOLLOWBACKER,
// low enough that the trust order keeps it out of full recommendation pools only.
MATCH (u:User) WHERE u.id IN [$follower, $followbacker, $honest_a, $honest_b] SET u.trust = 0.3;
MATCH (u:User {id: $honest_c}) SET u.trust = 0.35;
MATCH (u:User) WHERE u.id = $entry OR u.id IN $farm SET u.trust = 0.0;

// ##############################
// ##### Create follows #########
// ##############################
MATCH (u1:User {id: $follower}), (u2:User {id: $followbacker}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003001, id: "SYBFOLLOW0001"}]->(u2);
MATCH (u1:User {id: $followbacker}), (u2:User {id: $honest_a}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003002, id: "SYBFOLLOW0002"}]->(u2);
MATCH (u1:User {id: $followbacker}), (u2:User {id: $honest_b}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003003, id: "SYBFOLLOW0003"}]->(u2);
MATCH (u1:User {id: $honest_a}), (u2:User {id: $honest_c}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003004, id: "SYBFOLLOW0004"}]->(u2);
MATCH (u1:User {id: $entry}), (u2:User {id: $followbacker}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003005, id: "SYBFOLLOW0005"}]->(u2);
MATCH (u1:User {id: $followbacker}), (u2:User {id: $entry}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003006, id: "SYBFOLLOW0006"}]->(u2);
UNWIND range(0, size($farm) - 1) AS i
MATCH (u1:User {id: $entry}), (u2:User {id: $farm[i]})
MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003101 + i, id: "SYBFOLLOW" + right("0000" + toString(101 + i), 4)}]->(u2);

// ##############################
// ##### Create posts ###########
// ##############################
// Five each for HONEST_A/B/C, ENTRY and the farm, ids SYBPOST<author><n>.
WITH [$honest_a, $honest_b, $honest_c, $entry] + $farm AS authors
UNWIND range(0, size(authors) - 1) AS a
UNWIND range(1, 5) AS n
MATCH (u:User {id: authors[a]})
MERGE (p:Post {id: "SYBPOST" + right("000" + toString(a + 1), 3) + right("000" + toString(n), 3)})
SET p.content = "sybil fixture entry", p.kind = "short", p.indexed_at = 1600000002000 + a * 10 + n
MERGE (u)-[:AUTHORED]->(p);
