// Follow lists fixture (a user's followers, following, friends and relationship).
// Kept inert against the global suites the same way wot.cypher is: no posts, no
// tags, follow indexed_at far below every window start, and user ids that sort high.
//
// Topology, with the follow's indexed_at in brackets:
//   C -> HUB [3003]     HUB -> E [3013]   one way out
//   B -> HUB [3003]     HUB -> D [3013]
//   A -> HUB [3002]     HUB -> A [3012]
//   D -> HUB [3001]     HUB -> C [3011]
//   LONER               no follows either way
// B and C follow HUB in the same millisecond, and HUB follows D and E in the same
// one. Neither list, read oldest first or newest first, is in id order, and the
// follows are created in an order that is none of them either, forwards or backwards
// (B, D, A, C into HUB, then HUB to A, D, C, E), with one tie written in id order and
// the other against it. So neither the ids nor the order the graph happens to return
// the edges in can stand in for "by the time of the follow, then the id":
//                     oldest first    newest first
//   followers of HUB: D, A, B, C      B, C, A, D
//   HUB follows:      C, A, D, E      D, E, A, C
//   friends of HUB:   C, A, D         D, A, C      (B only follows HUB, E is only followed)

:param hub => 'xhgarsdgh9jm8wm7taaj4w9ip53mds7rcyjenjyea9hupqpuo6zo';
:param a => 'xjeupd9ajn1s9fdwtgrj31zoaf7is5jab8wr6xhq1nigokhueoso';
:param b => 'yirm8hcpdw5ahabkbzx9exuzfhewurzinzbjuma34o7x5s6gbe1o';
:param c => 'ytafkhk8qkjciy6ti1ewyftizeiztge66hg6wfbqui7i5hxb1gmy';
:param d => 'yyr8c3kt8zbci6o16qhdskde66sxgrrs7ccm1wpg5zzzuns1fouy';
:param e => 'z3uwwyh11iooisfgiu1ikc7wxc3oqcmpbbphc8mixrzbpb8656po';
:param loner => 'z4zbqwfs1yw1f4kp4rhwerf3s9q98m463jh81rd54zkbn6h97rmy';

// ##############################
// ##### Create users ###########
// ##############################
MERGE (u:User {id: $hub}) SET u.name = "follows_hub", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://xhgarsdgh9jm8wm7taaj4w9ip53mds7rcyjenjyea9hupqpuo6zo/pub/pubky.app/profile.json";
MERGE (u:User {id: $a}) SET u.name = "follows_a", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://xjeupd9ajn1s9fdwtgrj31zoaf7is5jab8wr6xhq1nigokhueoso/pub/pubky.app/profile.json";
MERGE (u:User {id: $b}) SET u.name = "follows_b", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://yirm8hcpdw5ahabkbzx9exuzfhewurzinzbjuma34o7x5s6gbe1o/pub/pubky.app/profile.json";
MERGE (u:User {id: $c}) SET u.name = "follows_c", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://ytafkhk8qkjciy6ti1ewyftizeiztge66hg6wfbqui7i5hxb1gmy/pub/pubky.app/profile.json";
MERGE (u:User {id: $d}) SET u.name = "follows_d", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://yyr8c3kt8zbci6o16qhdskde66sxgrrs7ccm1wpg5zzzuns1fouy/pub/pubky.app/profile.json";
MERGE (u:User {id: $e}) SET u.name = "follows_e", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://z3uwwyh11iooisfgiu1ikc7wxc3oqcmpbbphc8mixrzbpb8656po/pub/pubky.app/profile.json";
MERGE (u:User {id: $loner}) SET u.name = "follows_loner", u.bio = "", u.status = "undefined", u.indexed_at = 1650000000000, u.links = "[]", u.uri = "pubky://z4zbqwfs1yw1f4kp4rhwerf3s9q98m463jh81rd54zkbn6h97rmy/pub/pubky.app/profile.json";

// ##############################
// ##### Create follows #########
// ##############################
MATCH (u1:User {id: $b}), (u2:User {id: $hub}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003003, id: "FOLFOLLOW0003"}]->(u2);
MATCH (u1:User {id: $d}), (u2:User {id: $hub}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003001, id: "FOLFOLLOW0001"}]->(u2);
MATCH (u1:User {id: $a}), (u2:User {id: $hub}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003002, id: "FOLFOLLOW0002"}]->(u2);
MATCH (u1:User {id: $c}), (u2:User {id: $hub}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003003, id: "FOLFOLLOW0004"}]->(u2);
MATCH (u1:User {id: $hub}), (u2:User {id: $a}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003012, id: "FOLFOLLOW0006"}]->(u2);
MATCH (u1:User {id: $hub}), (u2:User {id: $d}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003013, id: "FOLFOLLOW0007"}]->(u2);
MATCH (u1:User {id: $hub}), (u2:User {id: $c}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003011, id: "FOLFOLLOW0005"}]->(u2);
MATCH (u1:User {id: $hub}), (u2:User {id: $e}) MERGE (u1)-[:FOLLOWS {indexed_at: 1230000003013, id: "FOLFOLLOW0008"}]->(u2);
