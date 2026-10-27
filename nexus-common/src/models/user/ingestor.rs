use std::future::Future;
use std::sync::Arc;

use pubky_app_specs::{ParsedUri, PubkyId};

use super::{PkdnsHomeserverResolver, PkdnsRecord, StoredRecord};
use crate::db::PubkyClientResult;
use crate::models::error::{ModelError, ModelResult};
use crate::models::homeserver::HsBlacklist;
use crate::models::traits::Collection;
use crate::models::user::{
    get_user_homeserver, set_user_homeserver_from_record, HomeserverRecheck, MappingUpdate,
    RecheckOutcome, StaleMapping, UserDetails, UserHsCursor,
};
use crate::StackConfig;

/// Ingests previously-unknown users unless their HS is blacklisted.
#[derive(Clone)]
pub struct UserIngestor {
    hs_blacklist: HsBlacklist,
    /// Looks up the HS a user publishes.
    resolver: Arc<dyn PkdnsHomeserverResolver>,
}

/// Names the blacklist only: the resolver is a trait object.
impl std::fmt::Debug for UserIngestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserIngestor")
            .field("hs_blacklist", &self.hs_blacklist)
            .finish_non_exhaustive()
    }
}

impl UserIngestor {
    /// Builds an ingestor enforcing the given HS blacklist, looking users' homeservers up
    /// through `resolver`.
    pub fn new(
        external_hs_pk_blacklist: impl IntoIterator<Item = PubkyId>,
        resolver: Arc<dyn PkdnsHomeserverResolver>,
    ) -> Self {
        Self {
            hs_blacklist: HsBlacklist::new(external_hs_pk_blacklist),
            resolver,
        }
    }

    /// Builds an ingestor enforcing `config`'s HS blacklist, looking users' homeservers up
    /// through `resolver`.
    pub fn from_config(config: &StackConfig, resolver: Arc<dyn PkdnsHomeserverResolver>) -> Self {
        Self::new(
            config.net.external_hs_pk_blacklist.iter().cloned(),
            resolver,
        )
    }

    /// Ingests the author of a referenced post, if unknown.
    pub async fn maybe_ingest_author_of_post(
        &self,
        referenced_post_uri: &ParsedUri,
    ) -> ModelResult<()> {
        self.maybe_ingest_user(&referenced_post_uri.user_id).await
    }

    /// Resolves the HS hosting `user_id` and refuses it if blacklisted.
    ///
    /// # Returns
    /// - `Ok(Some(hs_id))` if the user's HS resolved and is not blacklisted
    /// - `Ok(None)` if the user has no published HS or is an HS PK itself
    /// - [`ModelError::HsBlacklisted`] if the resolved HS is blacklisted
    pub async fn ensure_hs_not_blacklisted(
        &self,
        user_id: &PubkyId,
    ) -> ModelResult<Option<String>> {
        let user_pk = user_id.to_public_key();
        let record = self
            .resolve_unless_blacklisted(user_id, self.resolver.resolve_record(&user_pk))
            .await?;
        Ok(record.homeserver().map(|hs_id| hs_id.to_string()))
    }

    /// `lookup`'s record for `user_id`, refused if the user's own key or the
    /// HS the record names is blacklisted.
    async fn resolve_unless_blacklisted(
        &self,
        user_id: &PubkyId,
        lookup: impl Future<Output = PubkyClientResult<PkdnsRecord>>,
    ) -> ModelResult<PkdnsRecord> {
        // `user_id` may itself be an HS PK (e.g. a file `src` of the form
        // `pubky://<hs_pk>/...` that addresses the HS directly). The resolver
        // returns no HS for an HS PK, so without this self-check a blacklisted HS
        // used as a direct source would slip through and we'd reach out to it.
        if self.hs_blacklist.is_blacklisted(user_id.as_ref()) {
            return Err(ModelError::HsBlacklisted {
                hs_id: user_id.to_string(),
            });
        }

        let record = lookup.await.map_err(ModelError::from_generic)?;
        if let Some(hs_id) = record.homeserver() {
            if self.hs_blacklist.is_blacklisted(hs_id.as_ref()) {
                return Err(ModelError::HsBlacklisted {
                    hs_id: hs_id.to_string(),
                });
            }
        }

        Ok(record)
    }

    /// Resolves and persists a previously-unknown user.
    ///
    /// Returns [`ModelError::HsBlacklisted`] if the user's resolved HS is blacklisted.
    #[tracing::instrument(name = "user.ingest", skip_all)]
    pub async fn maybe_ingest_user(&self, user_id: &PubkyId) -> ModelResult<()> {
        let user_id_str = user_id.to_string();
        if UserDetails::get_by_id(&user_id_str).await?.is_some() {
            tracing::debug!("Skipping ingestion: {user_id_str} already known");
            return Ok(());
        }

        let user_pk = user_id.to_public_key();
        let record = self
            .resolve_unless_blacklisted(user_id, self.resolver.resolve_record(&user_pk))
            .await
            .inspect_err(|e| tracing::warn!("Aborting ingestion of {user_id}: {e}"))?;

        let (PkdnsRecord::Published(packet), Some(hs_pk)) = (&record, record.homeserver()) else {
            tracing::warn!("Skipping ingestion: {user_id} has no published HS or is an HS PK");
            return Ok(());
        };
        let hs_id = hs_pk.to_string();
        let record = StoredRecord::from(packet);

        let user_details = UserDetails::from_pubky(user_id.clone());

        // Do not add to index, as this would affect the timeline of events for this user.
        // Only create stub graph node for HS-resolver to store user-HS mapping.
        user_details
            .put_to_graph()
            .await
            .inspect(|_| tracing::info!("Ingested user {user_id} from HS {hs_id}"))
            .inspect_err(|e| tracing::error!("Failed to ingest user {user_id}: {e}"))?;

        // Bind the user to their HS (HOSTED_BY + resolved_at) from the record just resolved
        set_user_homeserver_from_record(&user_id_str, &hs_id, &record).await?;

        // Seed the user's HS cursor floor (no-op if we're already tracking them).
        UserHsCursor::init(user_id, &hs_id).await?;

        Ok(())
    }

    /// Re-checks a known user's homeserver against the network, asking for the
    /// newest record, and follows it: a record naming another homeserver moves
    /// the mapping there, as the sweep does with stale mapping disabled, and a
    /// record older than the one the mapping was applied from is ignored.
    ///
    /// Returns `None` for a user nexus does not know; ingest them instead.
    ///
    /// Returns [`ModelError::HsBlacklisted`] if the published HS is blacklisted.
    #[tracing::instrument(name = "user.recheck", skip_all)]
    pub async fn recheck_user(&self, user_id: &PubkyId) -> ModelResult<Option<HomeserverRecheck>> {
        let user_id_str = user_id.to_string();
        if UserDetails::get_by_id(&user_id_str).await?.is_none() {
            tracing::debug!("Skipping recheck: {user_id_str} unknown");
            return Ok(None);
        }

        let user_pk = user_id.to_public_key();
        let record = self
            .resolve_unless_blacklisted(user_id, self.resolver.resolve_record_fresh(&user_pk))
            .await
            .inspect_err(|e| tracing::warn!("Aborting recheck of {user_id}: {e}"))?;

        let stored = get_user_homeserver(&user_id_str).await?;
        let update = MappingUpdate::decide(stored.as_ref(), &record, StaleMapping::Disabled);
        update.apply(&user_id_str, record.stored().as_ref()).await?;
        if let Some(hs_id) = update.bound_to() {
            UserHsCursor::init(user_id, hs_id).await?;
        }

        Ok(Some(HomeserverRecheck {
            homeserver: update.homeserver_after(stored.as_ref()),
            outcome: RecheckOutcome::from(&update),
        }))
    }
}

#[cfg(test)]
mod tests {
    use pubky::{Keypair, PublicKey};

    use super::*;
    use crate::db::{
        exec_single_row, fetch_row_from_graph, graph::Query, queries, PubkyClientResult,
    };
    use crate::utils::test_utils::{random_pubky_id, signed_packet_naming};
    use crate::{types::DynError, StackConfig, StackManager};

    /// Resolves every user to one homeserver, without the network.
    struct FixedResolver(PubkyId);

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for FixedResolver {
        async fn resolve_record(&self, _: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            Ok(PkdnsRecord::Published(signed_packet_naming(
                &Keypair::random(),
                Some(&self.0),
                1,
            )))
        }
    }

    /// Answers every lookup with `keypair`'s own record naming one homeserver,
    /// authored at `timestamp`.
    struct SigningResolver {
        keypair: Keypair,
        homeserver: PubkyId,
        timestamp: u64,
    }

    #[async_trait::async_trait]
    impl PkdnsHomeserverResolver for SigningResolver {
        async fn resolve_record(&self, _: &PublicKey) -> PubkyClientResult<PkdnsRecord> {
            Ok(PkdnsRecord::Published(signed_packet_naming(
                &self.keypair,
                Some(&self.homeserver),
                self.timestamp,
            )))
        }
    }

    /// A recheck follows the newest record: a record naming another homeserver
    /// moves a known user there, with a cursor on the new homeserver; a record
    /// older than the one applied is ignored; a blacklisted homeserver is
    /// refused; an unknown user is not rechecked.
    #[tokio_shared_rt::test(shared)]
    async fn recheck_follows_the_newest_record() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let user_id = random_pubky_id();
        let (hs_a, hs_b) = (random_pubky_id(), random_pubky_id());
        let ingestor = |blacklist: Vec<PubkyId>, homeserver: &PubkyId, timestamp| {
            UserIngestor::new(
                blacklist,
                Arc::new(SigningResolver {
                    keypair: Keypair::random(),
                    homeserver: homeserver.clone(),
                    timestamp,
                }),
            )
        };

        assert_eq!(
            ingestor(vec![], &hs_a, 10).recheck_user(&user_id).await?,
            None
        );
        ingestor(vec![], &hs_a, 10)
            .maybe_ingest_user(&user_id)
            .await?;

        let recheck = ingestor(vec![], &hs_b, 20).recheck_user(&user_id).await?;
        assert_eq!(
            recheck,
            Some(HomeserverRecheck {
                homeserver: Some(hs_b.to_string()),
                outcome: RecheckOutcome::Switched,
            })
        );
        let mapping = get_user_homeserver(user_id.as_ref()).await?.expect("bound");
        assert_eq!(
            (mapping.hs_id.as_str(), mapping.stale, mapping.record_ts),
            (hs_b.as_ref(), false, Some(20))
        );
        assert_eq!(
            UserHsCursor::read(&[user_id.as_ref()], hs_b.as_ref()).await?,
            vec![0],
            "a cursor on the new homeserver"
        );

        let recheck = ingestor(vec![], &hs_a, 5).recheck_user(&user_id).await?;
        assert_eq!(
            recheck,
            Some(HomeserverRecheck {
                homeserver: Some(hs_b.to_string()),
                outcome: RecheckOutcome::Ignored,
            })
        );

        let refused = ingestor(vec![hs_a.clone()], &hs_a, 30)
            .recheck_user(&user_id)
            .await
            .unwrap_err();
        assert!(
            matches!(refused, ModelError::HsBlacklisted { hs_id } if hs_id == hs_a.to_string())
        );
        assert_eq!(
            get_user_homeserver(user_id.as_ref())
                .await?
                .map(|m| m.hs_id),
            Some(hs_b.to_string()),
            "a refused recheck changes nothing"
        );

        exec_single_row(queries::del::delete_user(user_id.as_ref())).await?;
        Ok(())
    }

    /// A newly seen user is bound from the record just resolved, which stays on
    /// the edge as the user's own signed packet.
    #[tokio_shared_rt::test(shared)]
    async fn ingested_user_is_bound_from_the_resolved_record() -> Result<(), DynError> {
        StackManager::setup(&StackConfig::default()).await?;
        let user = Keypair::random();
        let user_id = PubkyId::from(user.public_key());
        let homeserver = random_pubky_id();
        let ingestor = UserIngestor::new(
            [],
            Arc::new(SigningResolver {
                keypair: user,
                homeserver: homeserver.clone(),
                timestamp: 7,
            }),
        );

        ingestor.maybe_ingest_user(&user_id).await?;

        let query = Query::new(
            "ingested_user_record",
            "MATCH (u:User {id: $user_id})-[r:HOSTED_BY]->(hs:Homeserver)
             RETURN hs.id AS hs_id, r.record AS packet, r.record_ts AS timestamp",
        )
        .param("user_id", user_id.to_string());
        let row = fetch_row_from_graph(query)
            .await?
            .expect("the user is bound");
        assert_eq!(row.get::<String>("hs_id")?, homeserver.to_string());
        let stored = StoredRecord {
            packet: row.get("packet")?,
            timestamp: row.get("timestamp")?,
        };
        assert_eq!(stored.timestamp, 7);
        let packet = stored.verify()?;
        assert_eq!(packet.public_key().to_z32(), user_id.to_string());
        assert_eq!(
            PkdnsRecord::Published(packet).homeserver(),
            Some(homeserver)
        );

        exec_single_row(queries::del::delete_user(user_id.as_ref())).await?;
        Ok(())
    }

    /// The blacklist check looks the user's homeserver up through the injected resolver.
    #[tokio::test]
    async fn blacklist_check_uses_the_injected_resolver() {
        let blacklisted = random_pubky_id();
        let allowed = random_pubky_id();
        let user = random_pubky_id();

        let refusing = UserIngestor::new(
            [blacklisted.clone()],
            Arc::new(FixedResolver(blacklisted.clone())),
        );
        let err = refusing.ensure_hs_not_blacklisted(&user).await.unwrap_err();
        assert!(
            matches!(err, ModelError::HsBlacklisted { hs_id } if hs_id == blacklisted.to_string())
        );

        let accepting = UserIngestor::new([blacklisted], Arc::new(FixedResolver(allowed.clone())));
        let hs_id = accepting.ensure_hs_not_blacklisted(&user).await.unwrap();
        assert_eq!(hs_id, Some(allowed.to_string()));
    }
}
