use anyhow::Error as AnyhowError;
use thiserror::Error;

use crate::snapshot::types::SnapshotId;

pub type RepositoryResult<T> = Result<T, RepositoryError>;

pub(crate) fn validate_attached_drive_virtual_size(
    drive_id: &str,
    virtual_size: u64,
) -> RepositoryResult<()> {
    if virtual_size != 0 {
        return Ok(());
    }
    Err(RepositoryError::InvalidRequest {
        reason: format!("attached drive '{drive_id}' virtual_size must be non-zero"),
    })
}

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("invalid repository request: {reason}")]
    InvalidRequest { reason: String },

    #[error("snapshot not found: {lookup}")]
    SnapshotNotFound { lookup: String },

    #[error("snapshot alias not found: {alias}")]
    AliasNotFound { alias: String },

    #[error("alias '{alias}' already points to '{existing}', cannot rebind to '{new_id}'")]
    AliasConflict {
        alias: String,
        existing: SnapshotId,
        new_id: SnapshotId,
    },

    #[error("artifact not found: {artifact}")]
    ArtifactNotFound { artifact: String },

    #[error("managed layer not found: {digest}")]
    ManagedLayerNotFound { digest: String },

    #[error("integrity mismatch for {artifact}: expected {expected}, got {actual}")]
    IntegrityMismatch {
        artifact: String,
        expected: String,
        actual: String,
    },

    #[error("unsupported operation: {feature}")]
    Unsupported { feature: String },

    #[error("snapshot unavailable: {reason}")]
    Unavailable { reason: String },

    #[error("backend error: {message}")]
    Backend {
        message: String,
        #[source]
        source: Option<AnyhowError>,
    },
}

impl RepositoryError {
    pub fn backend(message: impl Into<String>, source: impl Into<AnyhowError>) -> Self {
        Self::Backend {
            message: message.into(),
            source: Some(source.into()),
        }
    }
}
