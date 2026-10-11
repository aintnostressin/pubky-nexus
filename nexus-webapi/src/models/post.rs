use std::collections::{HashMap, HashSet};

use crate::Result;
use nexus_common::models::{file::FileDetails, post::PostView};
use serde::{Deserialize, Serialize};
use tracing::warn;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
pub struct PostViewDetailed {
    #[serde(flatten)]
    pub view: PostView,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments_metadata: Vec<FileDetails>,
}

impl PostViewDetailed {
    pub fn new(view: PostView, attachments_metadata: Vec<FileDetails>) -> Self {
        Self {
            view,
            attachments_metadata,
        }
    }

    pub async fn get_by_id(
        author_id: &str,
        post_id: &str,
        viewer_id: Option<&str>,
        limit_tags: Option<usize>,
        limit_taggers: Option<usize>,
        include_attachment_metadata: bool,
    ) -> Result<Option<Self>> {
        let Some(view) =
            PostView::get_by_id(author_id, post_id, viewer_id, limit_tags, limit_taggers).await?
        else {
            return Ok(None);
        };

        let attachments_metadata = if include_attachment_metadata {
            let attachment_uris = view.details.attachments.as_deref().unwrap_or(&[]);
            fetch_attachment_metadata(attachment_uris)
                .await?
                .into_iter()
                .map(|(_, details)| details)
                .collect()
        } else {
            vec![]
        };

        Ok(Some(Self {
            view,
            attachments_metadata,
        }))
    }
}

#[derive(Deserialize, Serialize, ToSchema, Default)]
pub struct PostStreamDetailed(pub Vec<PostViewDetailed>);

impl PostStreamDetailed {
    pub async fn from_post_views(
        views: Vec<PostView>,
        include_attachment_metadata: bool,
    ) -> Result<Self> {
        if !include_attachment_metadata {
            let views_detailed = views
                .into_iter()
                .map(|view| PostViewDetailed::new(view, vec![]))
                .collect();
            return Ok(Self(views_detailed));
        }

        // Collect unique attachment URIs across all posts for a single batched fetch
        let all_uris: Vec<String> = views
            .iter()
            .flat_map(|v| v.details.attachments.as_deref().unwrap_or(&[]))
            .collect::<HashSet<_>>()
            .into_iter()
            .cloned()
            .collect();

        // Single batched fetch, then a lookup by attachment URI
        let metadata_by_uri: HashMap<String, FileDetails> = fetch_attachment_metadata(&all_uris)
            .await?
            .into_iter()
            .collect();

        // Distribute results back to each post
        let detailed = views
            .into_iter()
            .map(|view| {
                let attachments_metadata = view
                    .details
                    .attachments
                    .as_deref()
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(|uri| metadata_by_uri.get(uri).cloned())
                    .collect();
                PostViewDetailed::new(view, attachments_metadata)
            })
            .collect();

        Ok(Self(detailed))
    }
}

/// Fetches the metadata of the files stored under attachment URIs in one batched lookup, in
/// order, each next to its URI. A URI no indexed file has is logged and skipped.
async fn fetch_attachment_metadata(attachments: &[String]) -> Result<Vec<(String, FileDetails)>> {
    let uris: Vec<&str> = attachments.iter().map(String::as_str).collect();
    let results = FileDetails::get_by_uris(&uris).await?;

    Ok(attachments
        .iter()
        .zip(results)
        .filter_map(|(uri, details)| {
            if details.is_none() {
                warn!("Attachment metadata not found for URI: {}", uri);
            }
            Some((uri.clone(), details?))
        })
        .collect())
}
