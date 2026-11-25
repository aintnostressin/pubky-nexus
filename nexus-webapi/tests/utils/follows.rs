//! Ids from docker/test-graph/mocks/follows.cypher, the fixture behind the
//! follow list and relationship tests. See its header for the follow topology.

pub const HUB: &str = "xhgarsdgh9jm8wm7taaj4w9ip53mds7rcyjenjyea9hupqpuo6zo";
/// Follows HUB and is followed back.
pub const A: &str = "xjeupd9ajn1s9fdwtgrj31zoaf7is5jab8wr6xhq1nigokhueoso";
/// Follows HUB only, in the same millisecond as C.
pub const B: &str = "yirm8hcpdw5ahabkbzx9exuzfhewurzinzbjuma34o7x5s6gbe1o";
/// Follows HUB and is followed back: HUB's oldest follow.
pub const C: &str = "ytafkhk8qkjciy6ti1ewyftizeiztge66hg6wfbqui7i5hxb1gmy";
/// HUB's oldest follower, followed back in the same millisecond as E.
pub const D: &str = "yyr8c3kt8zbci6o16qhdskde66sxgrrs7ccm1wpg5zzzuns1fouy";
/// Followed by HUB only.
pub const E: &str = "z3uwwyh11iooisfgiu1ikc7wxc3oqcmpbbphc8mixrzbpb8656po";
/// Exists, with no follows either way.
pub const LONER: &str = "z4zbqwfs1yw1f4kp4rhwerf3s9q98m463jh81rd54zkbn6h97rmy";

/// The lists as the API pages them: the oldest follow first, then the id.
pub const HUB_FOLLOWERS: [&str; 4] = [D, A, B, C];
pub const HUB_FOLLOWING: [&str; 4] = [C, A, D, E];
pub const HUB_FRIENDS: [&str; 3] = [C, A, D];

/// The same lists with the newest follow first, then the id.
pub const HUB_FOLLOWERS_NEWEST: [&str; 4] = [B, C, A, D];
pub const HUB_FOLLOWING_NEWEST: [&str; 4] = [D, E, A, C];
pub const HUB_FRIENDS_NEWEST: [&str; 3] = [D, A, C];
