#[allow(unused_imports)]
use manager::Migration;

pub mod builder;
pub mod manager;
mod migrations_list;
mod utils;

pub use builder::MigrationBuilder;
pub use manager::MigrationManager;

use crate::migrations::migrations_list::post_content_index_author_setup_1780531200::PostContentIndexAuthorSetup1780531200;
use crate::migrations::migrations_list::post_content_index_setup_1780444800::PostContentIndexSetup1780444800;
use crate::migrations::migrations_list::remove_muted_1771718400::RemoveMuted1771718400;
use crate::migrations::migrations_list::resource_node_setup_1774000000::ResourceNodeSetup1774000000;
use crate::migrations::migrations_list::users_by_pk_reindex_1751635096::UsersByPkReindex1751635096;
use crate::migrations::migrations_list::users_by_tags_index_backfill_1786924800::UsersByTagsIndexBackfill1786924800;
/// Registers migrations with the `MigrationManager`
///
/// # Description
/// This function populates the migration manager with a list of migration tasks.
/// Each migration should be manually added to the `migrations` vector after it is
/// created in the `/db/migrations/migration_list` folder. The migration ID must be copied and
/// referenced in this function to ensure it is included in the execution process.
///
/// # Steps to Add a New Migration in pub fn import_migrations:
/// 1. Create a migration using the CLI: `cargo run -- db migration new DumpNotifications`
/// 2. Copy the migration struct name (e.g., `DumpNotifications1739459200`).
/// 3. Add it to the `migrations` vector as `Box::new(DumpNotifications1739459200)`.
/// 4. Ensure the migration is registered by calling `migration_manager.register(migration)`
///
/// # Example:
/// ```rust
/// let migrations: Vec<Box<dyn Migration>> = vec![
///     Box::new(DumpNotifications1739459200),
///     Box::new(AnotherMigration1739459201), // Add new migrations here
/// ];
/// ```
///
/// # Parameters
/// - `migration_manager`: A mutable reference to `MigrationManager` where migrations will be registered.
///
fn build_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        // Note: Add your migrations here to be picked up by the manager
        Box::new(UsersByPkReindex1751635096),
        Box::new(RemoveMuted1771718400),
        Box::new(ResourceNodeSetup1774000000),
        Box::new(PostContentIndexSetup1780444800),
        Box::new(PostContentIndexAuthorSetup1780531200),
        Box::new(UsersByTagsIndexBackfill1786924800),
    ]
}

pub fn import_migrations(migration_manager: &mut MigrationManager) {
    for migration in build_migrations() {
        migration_manager.register(migration);
    }
}

#[cfg(test)]
mod tests {
    use super::build_migrations;
    use crate::migrations::migrations_list::post_content_index_author_setup_1780531200::POST_CONTENT_INDEX_SCHEMA_ARGS_V2;
    use nexus_common::db::kv::POST_CONTENT_INDEX_SCHEMA_ARGS;

    /// The live `setup_cache` declaration and the frozen v2 migration must agree
    /// on the `postContentIdx` schema while v2 is the newest migration touching
    /// this index. If a later migration changes the schema, this assertion should
    /// be retargeted to that migration's frozen arg list.
    #[test]
    fn setup_cache_schema_matches_frozen_v2_migration_schema() {
        assert_eq!(
            POST_CONTENT_INDEX_SCHEMA_ARGS, POST_CONTENT_INDEX_SCHEMA_ARGS_V2,
            "setup_cache postContentIdx schema must match the frozen v2 migration schema"
        );
    }

    /// Extracts the trailing unix timestamp from a migration id such as
    /// `PostContentIndexAuthorSetup1780531200`.
    fn trailing_timestamp(id: &str) -> u64 {
        let digits: String = id
            .chars()
            .rev()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        digits
            .chars()
            .rev()
            .collect::<String>()
            .parse()
            .expect("migration id must end with a unix timestamp")
    }

    /// The migration registry is a hand-maintained `Vec`. If a newer index
    /// migration were inserted above `PostContentIndexAuthorSetup1780531200`, the
    /// frozen v2 drop+create would silently overwrite it on a fresh environment.
    /// This test guards ordering by asserting the vec is sorted ascending by the
    /// trailing timestamp in each `id()`.
    #[test]
    fn migrations_are_registered_in_chronological_order() {
        let migrations = build_migrations();

        let timestamps: Vec<u64> = migrations
            .iter()
            .map(|m| trailing_timestamp(m.id()))
            .collect();

        let mut sorted = timestamps.clone();
        sorted.sort_unstable();

        assert_eq!(
            timestamps, sorted,
            "migrations must be registered in ascending chronological order"
        );
    }
}
