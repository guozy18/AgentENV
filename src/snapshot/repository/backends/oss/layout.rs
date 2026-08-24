use crate::snapshot::SnapshotId;

/// Committed object layout for the OSS snapshot backend.
pub(crate) struct OssSnapshotArtifactLayout<'a> {
    snapshot_id: &'a SnapshotId,
    namespace: Option<&'a str>,
}

impl<'a> OssSnapshotArtifactLayout<'a> {
    pub(super) fn new(snapshot_id: &'a SnapshotId) -> Self {
        Self {
            snapshot_id,
            namespace: None,
        }
    }

    pub(super) fn with_namespace(mut self, namespace: &'a str) -> Self {
        self.namespace = Some(namespace);
        self
    }

    pub(super) fn alias_key(alias: &str) -> String {
        format!("catalog/aliases/{alias}.json")
    }

    pub(super) fn record_key(id: &SnapshotId) -> String {
        format!("catalog/records/{id}.json")
    }

    pub(crate) fn managed_layer_key(digest: &str) -> String {
        format!("managed-layers/{digest}")
    }

    pub(super) fn artifact_prefix(&self) -> String {
        match self.namespace {
            Some(namespace) => format!("artifacts/{}/{namespace}/", self.snapshot_id),
            None => format!("artifacts/{}/", self.snapshot_id),
        }
    }

    pub(super) fn artifact_key(&self, relative_path: &str) -> String {
        format!("{}{}", self.artifact_prefix(), relative_path)
    }
}
