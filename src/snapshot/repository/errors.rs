use anyhow::Error as AnyhowError;
use thiserror::Error;

use crate::snapshot::types::SnapshotId;

pub type RepositoryResult<T> = Result<T, RepositoryError>;

pub(crate) fn validate_legacy_artifact_namespace(namespace: Option<&str>) -> RepositoryResult<()> {
    let Some(namespace) = namespace else {
        return Ok(());
    };
    if matches!(namespace, "." | "..")
        || namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(RepositoryError::InvalidRequest {
            reason: "legacy snapshot artifact namespace is invalid".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_legacy_artifact_namespace;

    #[test]
    fn legacy_artifact_namespace_is_one_safe_path_segment() {
        for valid in [None, Some("attempt-old"), Some("attempt-019abc.def_1")] {
            validate_legacy_artifact_namespace(valid).expect("legacy namespace should be valid");
        }
        for invalid in [Some(""), Some("."), Some(".."), Some("attempt/escape")] {
            assert!(validate_legacy_artifact_namespace(invalid).is_err());
        }
    }
}

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

    #[error("concurrent snapshot catalog modification: {resource}")]
    ConcurrentModification { resource: String },

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
