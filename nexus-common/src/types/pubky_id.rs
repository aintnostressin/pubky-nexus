use pubky::pkarr::errors::PublicKeyError;
use pubky::PublicKey;
use pubky_app_specs::PubkyId;

/// A [`PubkyId`] whose 52 z32 characters decode cleanly but are not an Ed25519 curve
/// point, so the id names no key.
#[derive(Debug, thiserror::Error)]
#[error("NotAPublicKey: {id}: {source}")]
pub struct NotAPublicKey {
    pub id: String,
    #[source]
    source: PublicKeyError,
}

/// The [`PubkyId`] ↔ [`PublicKey`] conversions.
///
/// `pubky-app-specs` is built without its `sdk` feature, so the SDK release that crate
/// pins cannot collide with the one this workspace picks, and in exchange the crate no
/// longer offers these conversions itself. It also no longer rejects an id that is
/// well-formed z32 but not a curve point: such an id now builds, and fails here, at the
/// one point that needs it to name a real key.
pub trait PubkyIdExt: Sized {
    /// The id naming `public_key`. Infallible: a public key's z32 is always a valid id.
    fn from_public_key(public_key: &PublicKey) -> Self;

    /// The public key this id names.
    fn to_public_key(&self) -> Result<PublicKey, NotAPublicKey>;
}

impl PubkyIdExt for PubkyId {
    fn from_public_key(public_key: &PublicKey) -> Self {
        PubkyId::try_from(&public_key.to_z32()).expect("a public key's z32 is a valid Pubky id")
    }

    fn to_public_key(&self) -> Result<PublicKey, NotAPublicKey> {
        PublicKey::try_from(self.as_ref()).map_err(|source| NotAPublicKey {
            id: self.to_string(),
            source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pubky::Keypair;

    /// 52 z32 characters that decode cleanly but are not an Ed25519 curve point.
    const NON_CURVE_POINT: &str = "byyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";

    #[test]
    fn public_key_round_trips_through_the_id() {
        let public_key = Keypair::random().public_key();
        let id = PubkyId::from_public_key(&public_key);

        assert_eq!(id.as_ref(), public_key.to_z32());
        assert_eq!(id.to_public_key().unwrap(), public_key);
    }

    /// Without the `sdk` feature the specs crate checks the z32 format alone, so the
    /// curve-point verdict moves here.
    #[test]
    fn an_id_that_is_not_a_curve_point_builds_but_names_no_key() {
        let id = PubkyId::try_from(NON_CURVE_POINT).expect("well-formed z32");

        assert!(id.to_public_key().is_err());
    }
}
