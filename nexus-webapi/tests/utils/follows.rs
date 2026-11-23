//! Ids from docker/test-graph/mocks/follows.cypher, the fixture behind the
//! follow list and relationship tests. See its header for the follow topology.

pub const HUB: &str = "xhgarsdgh9jm8wm7taaj4w9ip53mds7rcyjenjyea9hupqpuo6zo";
/// Follows HUB and is followed back: the oldest follow both ways.
pub const A: &str = "xjeupd9ajn1s9fdwtgrj31zoaf7is5jab8wr6xhq1nigokhueoso";
/// Follows HUB only, in the same millisecond as C.
pub const B: &str = "yirm8hcpdw5ahabkbzx9exuzfhewurzinzbjuma34o7x5s6gbe1o";
/// Follows HUB and is followed back, in the same millisecond as D.
pub const C: &str = "ytafkhk8qkjciy6ti1ewyftizeiztge66hg6wfbqui7i5hxb1gmy";
/// Follows HUB and is followed back: HUB's newest follower.
pub const D: &str = "yyr8c3kt8zbci6o16qhdskde66sxgrrs7ccm1wpg5zzzuns1fouy";
/// Followed by HUB only: HUB's newest follow.
pub const E: &str = "z3uwwyh11iooisfgiu1ikc7wxc3oqcmpbbphc8mixrzbpb8656po";
/// Exists, with no follows either way.
pub const LONER: &str = "z4zbqwfs1yw1f4kp4rhwerf3s9q98m463jh81rd54zkbn6h97rmy";

pub const HUB_FOLLOWERS: [&str; 4] = [D, B, C, A];
pub const HUB_FOLLOWING: [&str; 4] = [E, C, D, A];
pub const HUB_FRIENDS: [&str; 3] = [C, D, A];
