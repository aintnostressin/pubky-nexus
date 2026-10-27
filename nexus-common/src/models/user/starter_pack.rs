use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::db::kv::RedisResult;
use crate::db::{fetch_all_rows_from_graph, queries, RedisOps};
use crate::models::error::ModelResult;

/// Share of a participant's standing that flows along their tags each round; the rest goes back
/// out in proportion to global trust.
const STARTER_PACK_DAMPING: f64 = 0.85;
/// Total (L1) change in a round below which the standings count as converged.
const STARTER_PACK_TOLERANCE: f64 = 1e-9;
/// Rounds after which propagation stops even short of the tolerance.
const STARTER_PACK_MAX_ITERATIONS: usize = 100;
/// Distinct endorsers a candidate needs: a single tag, often a bot's or the candidate's own, is
/// not a reputation.
const STARTER_PACK_MIN_ENDORSERS: usize = 2;
/// How long a label's ranking stays cached, the period of the this-month hot tags it is
/// pre-warmed from. A tag with the label drops it sooner.
const STARTER_PACK_RANK_TTL_SECS: i64 = 86_400;

/// One row of [`queries::get::starter_pack_label_edges`].
#[derive(Debug, Clone, Deserialize)]
struct LabelEdge {
    tagger_id: String,
    tagger_trust: f64,
    candidate_id: String,
    candidate_trust: f64,
    candidate_deleted: bool,
    /// The tagger's tags with the label on the candidate's profile and posts.
    votes: u64,
}

/// A candidate who qualifies for a label.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedCandidate {
    pub id: String,
    /// The label standing that flowed in from the candidate's endorsers.
    pub score: f64,
    pub endorsers: usize,
    /// Newest post when the ranking was built, `None` if the candidate never posted.
    pub last_post_at: Option<i64>,
}

/// One label's ranked candidates, best first. Cached whole, so a request only filters it: the
/// ranking depends on the label alone.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StarterPackRank {
    pub label: String,
    /// Milliseconds since the epoch.
    pub computed_at: i64,
    pub candidates: Vec<RankedCandidate>,
}

/// Keyed by the bare label, which cannot contain `:`.
#[async_trait]
impl RedisOps for StarterPackRank {
    async fn prefix() -> String {
        String::from("Cache:StarterPack")
    }
}

impl StarterPackRank {
    /// The label's cached ranking, built and cached on a miss.
    pub async fn get_or_compute(label: &str) -> ModelResult<Self> {
        match Self::try_from_index_json(&[label], None).await? {
            Some(cached) => Ok(cached),
            None => Self::compute_and_cache(label).await,
        }
    }

    /// Builds the label's ranking from the graph and caches it. An empty ranking is cached too,
    /// so a label nobody uses costs one graph read a day, not one per request.
    pub async fn compute_and_cache(label: &str) -> ModelResult<Self> {
        let rank = Self::compute(label).await?;
        rank.put_index_json(&[label], None, Some(STARTER_PACK_RANK_TTL_SECS))
            .await?;
        Ok(rank)
    }

    /// Drops the label's cached ranking, so the next read rebuilds it.
    pub async fn invalidate(label: &str) -> RedisResult<()> {
        Self::remove_from_index_multiple_json(&[&[label]]).await
    }

    async fn compute(label: &str) -> ModelResult<Self> {
        let query = queries::get::starter_pack_label_edges(label);
        let edges = fetch_all_rows_from_graph(query)
            .await?
            .iter()
            .map(|row| row.to::<LabelEdge>())
            .collect::<Result<Vec<_>, _>>()?;

        let mut candidates = rank_label(&edges);
        let last_posts = Self::last_post_at(&candidates).await?;
        for candidate in &mut candidates {
            candidate.last_post_at = last_posts.get(&candidate.id).copied().flatten();
        }

        Ok(Self {
            label: label.to_string(),
            computed_at: Utc::now().timestamp_millis(),
            candidates,
        })
    }

    async fn last_post_at(
        candidates: &[RankedCandidate],
    ) -> ModelResult<HashMap<String, Option<i64>>> {
        if candidates.is_empty() {
            return Ok(HashMap::new());
        }
        let ids: Vec<&str> = candidates.iter().map(|c| c.id.as_str()).collect();
        let rows = fetch_all_rows_from_graph(queries::get::users_last_post_at(&ids)).await?;

        let mut last_posts = HashMap::with_capacity(rows.len());
        for row in rows {
            last_posts.insert(row.get("id")?, row.get("last_post_at")?);
        }
        Ok(last_posts)
    }
}

/// Ranks a label's candidates by the standing for the label that their endorsers pass on.
///
/// Standing propagates over who tagged whom with the label, seeded by global trust, so a tag
/// weighs what its author's own standing for the label is: that standing comes from being tagged
/// with the label by people who have some. A tagger's flow is split over their tags with the
/// label, so nobody hands out more than they hold, and a clique nobody outside trusts passes on
/// nothing. Candidates rank by what flowed in rather than by their standing, so their own global
/// trust buys them no place. The same edges rank the same way in any order.
fn rank_label(edges: &[LabelEdge]) -> Vec<RankedCandidate> {
    let graph = LabelGraph::new(edges);
    graph.rank(&graph.propagate())
}

/// A label's tag graph over its participants, taggers and candidates alike.
///
/// Participants are indexed in id order and votes in (tagger, candidate) order, so every float
/// sum runs in one order whatever order the rows came in.
struct LabelGraph<'a> {
    ids: Vec<&'a str>,
    trust: Vec<f64>,
    deleted: Vec<bool>,
    /// `(tagger, candidate, votes)`, one entry per pair.
    votes: Vec<(usize, usize, f64)>,
    /// Each participant's votes in all; zero for one who tagged nobody.
    out: Vec<f64>,
}

impl<'a> LabelGraph<'a> {
    fn new(edges: &'a [LabelEdge]) -> Self {
        // The query already drops self-tags; this keeps the ranking honest without it.
        let edges: Vec<&LabelEdge> = edges
            .iter()
            .filter(|e| e.tagger_id != e.candidate_id && e.votes > 0)
            .collect();

        // `max` from 0 keeps a missing or invalid trust at zero, whichever row a node comes from.
        let mut nodes: BTreeMap<&str, (f64, bool)> = BTreeMap::new();
        for &edge in &edges {
            let tagger = nodes.entry(&edge.tagger_id).or_default();
            tagger.0 = tagger.0.max(edge.tagger_trust);
            let candidate = nodes.entry(&edge.candidate_id).or_default();
            candidate.0 = candidate.0.max(edge.candidate_trust);
            candidate.1 |= edge.candidate_deleted;
        }
        let index: HashMap<&str, usize> =
            nodes.keys().enumerate().map(|(i, id)| (*id, i)).collect();

        let mut pairs: BTreeMap<(usize, usize), f64> = BTreeMap::new();
        for &edge in &edges {
            let pair = (
                index[edge.tagger_id.as_str()],
                index[edge.candidate_id.as_str()],
            );
            *pairs.entry(pair).or_default() += edge.votes as f64;
        }
        let mut out = vec![0.0; nodes.len()];
        for (&(tagger, _), votes) in &pairs {
            out[tagger] += votes;
        }

        Self {
            ids: nodes.keys().copied().collect(),
            trust: nodes.values().map(|(trust, _)| *trust).collect(),
            deleted: nodes.values().map(|(_, deleted)| *deleted).collect(),
            votes: pairs.into_iter().map(|((t, c), v)| (t, c, v)).collect(),
            out,
        }
    }

    /// Each participant's converged standing for the label. It sums to one.
    fn propagate(&self) -> Vec<f64> {
        let teleport = self.teleport();
        let mut standing = teleport.clone();
        for _ in 0..STARTER_PACK_MAX_ITERATIONS {
            // Whoever tagged nobody has nowhere to send theirs, so it goes back out the way the
            // teleport does instead of leaking.
            let dangling: f64 = standing
                .iter()
                .zip(&self.out)
                .filter(|(_, out)| **out == 0.0)
                .map(|(share, _)| share)
                .sum();
            let mut next: Vec<f64> = teleport
                .iter()
                .map(|t| (1.0 - STARTER_PACK_DAMPING) * t + STARTER_PACK_DAMPING * dangling * t)
                .collect();
            for &(tagger, candidate, votes) in &self.votes {
                next[candidate] += self.flow(&standing, tagger, votes);
            }

            let change: f64 = next.iter().zip(&standing).map(|(a, b)| (a - b).abs()).sum();
            standing = next;
            if change < STARTER_PACK_TOLERANCE {
                break;
            }
        }
        standing
    }

    /// Global trust as a share of the participants' total, or an even share while nobody has
    /// any, as before the trust job first runs.
    fn teleport(&self) -> Vec<f64> {
        let total: f64 = self.trust.iter().sum();
        if total > 0.0 {
            self.trust.iter().map(|trust| trust / total).collect()
        } else {
            vec![1.0 / self.ids.len() as f64; self.ids.len()]
        }
    }

    /// What `tagger` passes on through `votes` of their tags with the label.
    fn flow(&self, standing: &[f64], tagger: usize, votes: f64) -> f64 {
        STARTER_PACK_DAMPING * standing[tagger] * votes / self.out[tagger]
    }

    /// Eligible candidates by inflow, then endorsers, then id.
    fn rank(&self, standing: &[f64]) -> Vec<RankedCandidate> {
        let mut inflow = vec![0.0; self.ids.len()];
        let mut endorsers = vec![0; self.ids.len()];
        for &(tagger, candidate, votes) in &self.votes {
            inflow[candidate] += self.flow(standing, tagger, votes);
            endorsers[candidate] += 1;
        }

        let mut ranked: Vec<RankedCandidate> = (0..self.ids.len())
            .filter(|&i| endorsers[i] >= STARTER_PACK_MIN_ENDORSERS && !self.deleted[i])
            .map(|i| RankedCandidate {
                id: self.ids[i].to_string(),
                score: inflow[i],
                endorsers: endorsers[i],
                last_post_at: None,
            })
            .collect();
        ranked.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then(b.endorsers.cmp(&a.endorsers))
                .then_with(|| a.id.cmp(&b.id))
        });
        ranked
    }
}

#[cfg(test)]
mod tests {
    use super::{rank_label, LabelEdge, LabelGraph, RankedCandidate};

    /// Rows as the query returns them, from `(tagger, candidate, votes)`; every id's trust comes
    /// from `trust`, zero when absent.
    fn edges(trust: &[(&str, f64)], tags: &[(&str, &str, u64)]) -> Vec<LabelEdge> {
        let trust_of = |id: &str| {
            trust
                .iter()
                .find(|(node, _)| *node == id)
                .map_or(0.0, |(_, trust)| *trust)
        };
        tags.iter()
            .map(|&(tagger, candidate, votes)| LabelEdge {
                tagger_id: tagger.to_string(),
                tagger_trust: trust_of(tagger),
                candidate_id: candidate.to_string(),
                candidate_trust: trust_of(candidate),
                candidate_deleted: false,
                votes,
            })
            .collect()
    }

    fn ids(ranked: &[RankedCandidate]) -> Vec<&str> {
        ranked.iter().map(|c| c.id.as_str()).collect()
    }

    fn score(ranked: &[RankedCandidate], id: &str) -> f64 {
        ranked
            .iter()
            .find(|c| c.id == id)
            .unwrap_or_else(|| panic!("{id} should be ranked, got {:?}", ids(ranked)))
            .score
    }

    #[test]
    fn a_tagger_splits_their_flow_across_their_tags() {
        let trust = [("t", 0.5), ("u", 0.5)];
        let alone = rank_label(&edges(&trust, &[("t", "a", 1), ("u", "a", 1)]));
        let shared = rank_label(&edges(
            &trust,
            &[("t", "a", 1), ("t", "b", 1), ("u", "a", 1)],
        ));

        assert!(
            score(&shared, "a") < score(&alone, "a"),
            "a second tag should take a share of what the first one carried"
        );
    }

    #[test]
    fn a_clique_nobody_trusts_passes_on_nothing() {
        // c1 and c2 vouch for each other and for `clique`; `vouched` has a trusted endorser and
        // an untrusted one.
        let ranked = rank_label(&edges(
            &[("trusted", 1.0)],
            &[
                ("trusted", "vouched", 1),
                ("c1", "vouched", 1),
                ("c1", "c2", 1),
                ("c2", "c1", 1),
                ("c1", "clique", 1),
                ("c2", "clique", 1),
            ],
        ));

        assert_eq!(ids(&ranked), ["vouched", "clique"]);
        assert_eq!(score(&ranked, "clique"), 0.0);
    }

    #[test]
    fn a_tag_weighs_its_authors_standing_for_the_label() {
        // Neither `expert` nor `novice` has global trust, but trusted people tagged `expert`
        // with the label, so only `expert`'s own tag carries weight.
        let ranked = rank_label(&edges(
            &[("seed1", 0.5), ("seed2", 0.5)],
            &[
                ("seed1", "expert", 1),
                ("seed2", "expert", 1),
                ("expert", "x", 1),
                ("other", "x", 1),
                ("novice", "y", 1),
                ("other", "y", 1),
            ],
        ));

        assert!(score(&ranked, "x") > 0.0);
        assert_eq!(score(&ranked, "y"), 0.0);
    }

    #[test]
    fn a_candidates_own_trust_buys_no_place() {
        // `famous` holds most of the trust but weaker endorsements than `known`, who holds none.
        let ranked = rank_label(&edges(
            &[("famous", 0.9), ("t1", 0.05), ("t2", 0.05)],
            &[
                ("t1", "known", 1),
                ("t2", "known", 1),
                ("t1", "famous", 1),
                ("x", "famous", 1),
            ],
        ));

        assert_eq!(ids(&ranked), ["known", "famous"]);
    }

    #[test]
    fn without_any_trust_the_teleport_is_even_and_ties_fall_back_to_id() {
        // Every tagger tagged once and was never tagged, so each passes on the same amount.
        let ranked = rank_label(&edges(
            &[],
            &[
                ("t1", "b", 1),
                ("t2", "b", 1),
                ("t3", "a", 1),
                ("t4", "a", 1),
                ("t5", "c", 1),
                ("t6", "c", 1),
                ("t7", "c", 1),
            ],
        ));

        assert_eq!(ids(&ranked), ["c", "a", "b"]);
        assert!(score(&ranked, "a") > 0.0, "an even teleport still flows");
        assert_eq!(score(&ranked, "a"), score(&ranked, "b"));
    }

    #[test]
    fn a_lone_endorsement_or_a_deleted_account_is_not_ranked() {
        let mut rows = edges(
            &[("t1", 0.5), ("t2", 0.5)],
            &[
                ("t1", "lone", 1),
                ("t1", "pair", 1),
                ("t2", "pair", 1),
                ("t1", "gone", 1),
                ("t2", "gone", 1),
            ],
        );
        for row in rows.iter_mut().filter(|row| row.candidate_id == "gone") {
            row.candidate_deleted = true;
        }

        assert_eq!(ids(&rank_label(&rows)), ["pair"]);
    }

    #[test]
    fn a_self_tag_is_ignored() {
        // Counted, it would give `me` a second endorser and split `me`'s flow to `a`.
        let trust = [("t", 0.5), ("me", 0.5)];
        let tags = [("t", "a", 1), ("me", "a", 1), ("t", "me", 1)];
        let with_self_tag = [&tags[..], &[("me", "me", 3)]].concat();

        assert_eq!(
            rank_label(&edges(&trust, &with_self_tag)),
            rank_label(&edges(&trust, &tags))
        );
    }

    #[test]
    fn standing_sums_to_one_when_participants_tag_nobody() {
        // t1 and t2 are never tagged; b and c tag nobody, so theirs must go back out.
        let rows = edges(
            &[("t1", 0.3), ("t2", 0.1)],
            &[
                ("t1", "a", 2),
                ("t2", "a", 1),
                ("t2", "b", 1),
                ("a", "c", 1),
            ],
        );
        let total: f64 = LabelGraph::new(&rows).propagate().iter().sum();

        assert!(
            (total - 1.0).abs() < 1e-9,
            "standing should sum to one, got {total}"
        );
    }

    #[test]
    fn the_same_edges_rank_identically_in_any_order() {
        let rows = edges(
            &[("t1", 0.2), ("t2", 0.7), ("t3", 0.1)],
            &[
                ("t1", "a", 1),
                ("t2", "a", 3),
                ("t3", "b", 1),
                ("t1", "b", 2),
                ("t2", "c", 1),
                ("t3", "c", 1),
                ("a", "c", 1),
            ],
        );
        let mut reversed = rows.clone();
        reversed.reverse();

        let ranked = rank_label(&rows);
        assert_eq!(ranked, rank_label(&rows));
        assert_eq!(ranked, rank_label(&reversed));
    }
}
