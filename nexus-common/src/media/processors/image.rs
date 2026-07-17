use async_trait::async_trait;
use tokio::process::Command;

use crate::{
    media::{processors::MediaProcessorError, FileVariant},
    models::file::FileDetails,
};

use super::{BaseProcessingOptions, VariantProcessor};

const SMALL_IMAGE_WIDTH: &str = "320";
const FEED_IMAGE_WIDTH: &str = "720";
const IMAGE_FORMAT: &str = "webp";

// ImageMagick resource ceilings; these can later move to runtime config.
// -limit flags are global and must appear before the input path.
const LIMIT_MEMORY: &str = "256MiB"; // in-RAM pixel cache before spilling to map/disk
const LIMIT_MAP: &str = "512MiB"; // mmap cache before spilling to disk
const LIMIT_DISK: &str = "1GiB"; // hard ceiling on disk spill (exceed => abort)
const LIMIT_AREA: &str = "128MB"; // per-image area over which IM caches to disk
const LIMIT_WIDTH: &str = "8192"; // reject oversized source frames upfront
const LIMIT_HEIGHT: &str = "8192";
const LIMIT_TIME: &str = "20"; // seconds; abort runaway coalesce/resize

pub struct ImageOptions {
    width: String,
    format: String,
    content_type: String,
}

impl BaseProcessingOptions for ImageOptions {
    fn content_type(&self) -> String {
        self.content_type.clone()
    }
}

pub struct ImageProcessor;

#[async_trait]
impl VariantProcessor for ImageProcessor {
    type ProcessingOptions = ImageOptions;

    fn get_valid_variants_for_content_type(_content_type: &str) -> Vec<FileVariant> {
        vec![FileVariant::Main, FileVariant::Small, FileVariant::Feed]
    }

    fn get_content_type_for_variant(file: &FileDetails, variant: &FileVariant) -> String {
        if variant.eq(&FileVariant::Main) {
            return file.content_type.clone();
        }
        String::from("image/webp")
    }

    fn get_options_for_variant(
        file: &FileDetails,
        variant: &FileVariant,
    ) -> Result<ImageOptions, MediaProcessorError> {
        let width = match variant {
            FileVariant::Small => String::from(SMALL_IMAGE_WIDTH),
            FileVariant::Feed => String::from(FEED_IMAGE_WIDTH),
            _ => return Err(MediaProcessorError::UnsupportedFileVariant),
        };
        let content_type = Self::get_content_type_for_variant(file, variant);
        Ok(ImageOptions {
            format: IMAGE_FORMAT.to_string(),
            width,
            content_type,
        })
    }

    async fn process(
        origin_file_path: &str,
        output_file_path: &str,
        options: &ImageOptions,
    ) -> Result<String, MediaProcessorError> {
        let origin_file_format = ImageProcessor::get_format(origin_file_path)
            .await?
            .to_lowercase();

        let output = match origin_file_format == options.format {
            true => output_file_path.to_string(),
            false => format!("{}:{}", options.format, output_file_path),
        };

        let child_output = Command::new("convert")
            .arg("-limit")
            .arg("memory")
            .arg(LIMIT_MEMORY)
            .arg("-limit")
            .arg("map")
            .arg(LIMIT_MAP)
            .arg("-limit")
            .arg("disk")
            .arg(LIMIT_DISK)
            .arg("-limit")
            .arg("area")
            .arg(LIMIT_AREA)
            .arg("-limit")
            .arg("width")
            .arg(LIMIT_WIDTH)
            .arg("-limit")
            .arg("height")
            .arg(LIMIT_HEIGHT)
            .arg("-limit")
            .arg("time")
            .arg(LIMIT_TIME)
            .arg(origin_file_path)
            .arg("-coalesce")
            .arg("-resize")
            .arg(format!("{}x", options.width))
            .arg("-auto-orient") // https://github.com/ImageMagick/ImageMagick/issues/6396
            .arg(output)
            .output() // Automatically pipes stdout and stderr
            .await
            .map_err(MediaProcessorError::command_failed)?;

        if child_output.status.success() {
            Ok(String::from_utf8_lossy(&child_output.stdout).to_string())
        } else {
            Err(MediaProcessorError::command_failed(format!(
                "ImageMagick command failed: {}",
                String::from_utf8_lossy(&child_output.stdout)
            )))
        }
    }
}

impl ImageProcessor {
    // function to get image format
    async fn get_format(file_path: &str) -> Result<String, MediaProcessorError> {
        let child_output = Command::new("identify")
            .arg("-format")
            .arg("%m")
            .arg(format!("{file_path}[0]"))
            .output() // Automatically pipes stdout and stderr
            .await
            .map_err(MediaProcessorError::command_failed)?;

        if child_output.status.success() {
            Ok(String::from_utf8_lossy(&child_output.stdout).to_string())
        } else {
            Err(MediaProcessorError::command_failed(format!(
                "ImageMagick format extraction failed: {}",
                String::from_utf8_lossy(&child_output.stderr)
            )))
        }
    }
}
