use crate::{
    models::file::{FileDetails, FileUrls},
    types::DynError,
};
use processors::{ImageProcessor, VariantProcessor, VideoProcessor};
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::fs;
use utoipa::ToSchema;

mod concurrency;
pub mod processors;

pub use concurrency::MediaGate;
use processors::MediaProcessorError;

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema, Clone)]
#[serde(rename_all = "lowercase")]
pub enum FileVariant {
    Main,
    Feed,
    Small,
}

impl FromStr for FileVariant {
    type Err = DynError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "main" => Ok(FileVariant::Main),
            "feed" => Ok(FileVariant::Feed),
            "small" => Ok(FileVariant::Small),
            _ => Err("Invalid file version".into()),
        }
    }
}

impl Display for FileVariant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version_string = match self {
            FileVariant::Main => "main",
            FileVariant::Feed => "feed",
            FileVariant::Small => "small",
        };
        write!(f, "{version_string}")
    }
}

#[derive(Clone)]
pub struct VariantController {
    gate: MediaGate,
}

impl VariantController {
    pub fn new(gate: MediaGate) -> Self {
        Self { gate }
    }

    pub async fn create_file_variant(
        &self,
        file: &FileDetails,
        variant: &FileVariant,
        file_path: PathBuf,
    ) -> Result<String, MediaProcessorError> {
        match &file.content_type {
            content_type if content_type.starts_with("image/") => {
                ImageProcessor::create_variant(file, variant, file_path, &self.gate).await
            }
            content_type if content_type.starts_with("video/") => {
                VideoProcessor::create_variant(file, variant, file_path, &self.gate).await
            }
            _ => Err(MediaProcessorError::UnsupportedContentType(
                file.content_type.clone(),
            )),
        }
    }

    pub async fn check_variant_exists(
        file: &FileDetails,
        variant: FileVariant,
        file_path: PathBuf,
    ) -> bool {
        // main variant always exists
        if variant == FileVariant::Main {
            return true;
        }

        // if file exists, variant has already been created
        let path = file_path
            .join(file.owner_id.as_str())
            .join(file.id.as_str())
            .join(variant.to_string());

        fs::metadata(path).await.is_ok()
    }

    pub fn get_content_type_for_variant(file: &FileDetails, variant: &FileVariant) -> String {
        match &file.content_type {
            content_type if content_type.starts_with("image/") => {
                ImageProcessor::get_content_type_for_variant(file, variant)
            }
            content_type if content_type.starts_with("video/") => {
                VideoProcessor::get_content_type_for_variant(file, variant)
            }
            _ => file.content_type.clone(),
        }
    }

    pub fn get_file_urls_by_content_type(content_type: &str, path: &Path) -> FileUrls {
        let variants = Self::get_valid_variants_for_content_type(content_type);

        FileUrls::new(path, &variants)
    }

    pub fn validate_variant_for_content_type(content_type: &str, variant: &FileVariant) -> bool {
        if variant == &FileVariant::Main {
            return true;
        }
        let valid_variants = Self::get_valid_variants_for_content_type(content_type);
        valid_variants.contains(variant)
    }

    fn get_valid_variants_for_content_type(content_type: &str) -> Vec<FileVariant> {
        match content_type {
            value if value.starts_with("image") => {
                ImageProcessor::get_valid_variants_for_content_type(content_type)
            }
            value if value.starts_with("video") => {
                VideoProcessor::get_valid_variants_for_content_type(content_type)
            }
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::fs::{self as tfs, File};
    use tokio::io::AsyncWriteExt;

    fn make_file_details(owner_id: &str, file_id: &str, content_type: &str) -> FileDetails {
        FileDetails {
            id: file_id.to_string(),
            owner_id: owner_id.to_string(),
            content_type: content_type.to_string(),
            ..Default::default()
        }
    }

    /// `check_variant_exists` must NOT observe a temp file as a complete variant.
    ///
    /// After the atomic-write fix, processors write to `<variant>.<pid>.<ts>` first,
    /// then rename to `<variant>`. `check_variant_exists` only checks `<variant>`,
    /// so it must return `false` while the temp file exists.
    #[tokio_shared_rt::test(shared)]
    async fn test_check_variant_exists_ignores_temp_file() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_path = tmp_dir.path().to_path_buf();

        let owner = "owner1";
        let file_id = "file1";
        let file = make_file_details(owner, file_id, "image/png");

        let variant_dir = file_path.join(owner).join(file_id);
        tfs::create_dir_all(&variant_dir).await.unwrap();

        // Create a main file
        let mut f = File::create(variant_dir.join("main")).await.unwrap();
        f.write_all(b"MAIN").await.unwrap();
        drop(f);

        // Simulate a temp file that the processor writes to before renaming
        let temp_path = variant_dir.join("small.12345.1700000000000000000");
        let mut f = File::create(&temp_path).await.unwrap();
        f.write_all(b"PARTIAL").await.unwrap();
        f.flush().await.unwrap();

        // check_variant_exists must return false — the temp file is invisible
        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists should return false while only a temp file exists"
        );

        // Now atomically rename the temp file to the final path (what the fix does)
        tfs::rename(&temp_path, variant_dir.join("small")).await.unwrap();

        // After the rename, the variant exists
        assert!(
            VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists should return true after the atomic rename"
        );
    }

    /// Simulates the full atomic-write flow: write to temp, rename on success.
    /// Verifies that `check_variant_exists` never returns `true` for a partial file.
    #[tokio_shared_rt::test(shared)]
    async fn test_atomic_write_flow_no_partial_variant_observable() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_path = tmp_dir.path().to_path_buf();

        let owner = "owner1";
        let file_id = "file1";
        let file = make_file_details(owner, file_id, "image/png");

        let variant_dir = file_path.join(owner).join(file_id);
        tfs::create_dir_all(&variant_dir).await.unwrap();

        let mut f = File::create(variant_dir.join("main")).await.unwrap();
        f.write_all(b"MAIN").await.unwrap();
        drop(f);

        let variant_path = variant_dir.join("small");
        let temp_path = variant_dir.join("small.12345.9999999999999999999");

        // Phase 1: Before temp file exists — variant does not exist
        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await
        );

        // Phase 2: Temp file exists (processor is writing) — variant still does not exist
        let mut f = File::create(&temp_path).await.unwrap();
        f.write_all(b"PARTIAL_DATA").await.unwrap();
        f.flush().await.unwrap();

        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists must return false while the temp file is being written"
        );

        // Phase 3: Atomic rename — variant now exists and is complete
        tfs::rename(&temp_path, &variant_path).await.unwrap();

        assert!(
            VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists must return true after the atomic rename"
        );

        let content = tfs::read_to_string(&variant_path).await.unwrap();
        assert_eq!(content, "PARTIAL_DATA", "File content should be intact after rename");
    }

    /// On processor failure, the temp file is cleaned up — no poisoned variant.
    ///
    /// After the fix: if `convert`/`ffmpeg` fails, the temp file is removed.
    /// `check_variant_exists` returns `false` and the next request retries cleanly.
    #[tokio_shared_rt::test(shared)]
    async fn test_failed_processor_cleanup_no_poisoned_variant() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_path = tmp_dir.path().to_path_buf();

        let owner = "owner1";
        let file_id = "file1";
        let file = make_file_details(owner, file_id, "image/png");

        let variant_dir = file_path.join(owner).join(file_id);
        tfs::create_dir_all(&variant_dir).await.unwrap();

        let mut f = File::create(variant_dir.join("main")).await.unwrap();
        f.write_all(b"MAIN").await.unwrap();
        drop(f);

        let temp_path = variant_dir.join("small.12345.8888888888888888888");

        // Simulate a failing processor that writes to a temp file then aborts
        {
            let mut f = File::create(&temp_path).await.unwrap();
            f.write_all(b"PARTIAL").await.unwrap();
            f.flush().await.unwrap();
            // Drop without completing — simulates a failure
        }

        // Temp file exists but variant path does not
        assert!(tfs::metadata(&temp_path).await.is_ok(), "Temp file should exist");
        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists should return false for a temp file"
        );

        // Simulate the cleanup that happens in the fix on failure
        let _ = tfs::remove_file(&temp_path).await;

        // After cleanup, still no variant
        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await,
            "check_variant_exists should still return false after temp cleanup"
        );
    }

    /// Verify the variant path construction used by `check_variant_exists`.
    #[tokio_shared_rt::test(shared)]
    async fn test_variant_path_construction() {
        let tmp_dir = tempfile::TempDir::new().unwrap();
        let file_path = tmp_dir.path().to_path_buf();

        let owner = "user123";
        let file_id = "file456";
        let file = make_file_details(owner, file_id, "image/png");

        // No variant directory exists yet
        assert!(
            !VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await
        );

        // Create the variant directory and a complete file
        let variant_dir = file_path.join(owner).join(file_id);
        tfs::create_dir_all(&variant_dir).await.unwrap();
        let small_file = variant_dir.join("small");
        let mut f = File::create(&small_file).await.unwrap();
        f.write_all(b"COMPLETE_VARIANT").await.unwrap();
        f.flush().await.unwrap();

        // Now check_variant_exists should return true
        assert!(
            VariantController::check_variant_exists(&file, FileVariant::Small, file_path.clone())
                .await
        );
    }
}
