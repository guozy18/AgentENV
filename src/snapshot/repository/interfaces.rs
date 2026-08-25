use std::sync::Arc;

use async_trait::async_trait;

use super::errors::{RepositoryError, RepositoryResult};
use crate::sandbox::FirecrackerSnapshotManifest;
use crate::snapshot::types::{
    RunnableSnapshot, SnapshotId, SnapshotPublishMetadata, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildStatus,
};

/// Snapshot record list filter.
///
/// When multiple fields are present they combine with AND semantics.
/// When all fields are `None`, the filter matches all publicly visible
/// snapshot records, including pending template builds and committed
/// snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotListFilter {
    /// Match aliases that start with this prefix.
    pub alias_prefix: Option<String>,
    /// Restrict results to this exact set of snapshot ids.
    pub snapshot_ids: Option<Vec<SnapshotId>>,
    /// Restrict results to a single snapshot id or exact alias.
    pub snapshot_id_or_alias: Option<String>,
    /// Restrict results to snapshots captured from this source sandbox id.
    ///
    /// This only matches records whose source is [`SnapshotSourceKind::Sandbox`].
    pub source_sandbox_id: Option<String>,
    /// Restrict results to records with these source kinds.
    pub sources: Option<Vec<SnapshotSourceKind>>,
    /// Restrict results to template records whose build status is in this set.
    ///
    /// Sandbox records never match this field.
    pub template_statuses: Option<Vec<TemplateBuildStatus>>,
}

impl SnapshotListFilter {
    pub fn matches_all() -> Self {
        Self::default()
    }

    pub fn by_ids<I>(snapshot_ids: I) -> Self
    where
        I: IntoIterator<Item = SnapshotId>,
    {
        Self {
            snapshot_ids: Some(snapshot_ids.into_iter().collect()),
            ..Self::default()
        }
    }

    pub fn templates() -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Template]),
            ..Self::default()
        }
    }

    pub fn sandbox_snapshots(
        source_sandbox_id: Option<String>,
        snapshot_id_or_alias: Option<String>,
    ) -> Self {
        Self {
            sources: Some(vec![SnapshotSourceKind::Sandbox]),
            source_sandbox_id,
            snapshot_id_or_alias: snapshot_id_or_alias.map(|value| {
                let unqualified = value
                    .rsplit_once('/')
                    .map_or(value.as_str(), |(_, name)| name);
                unqualified
                    .split_once(':')
                    .map_or(unqualified, |(name, _)| name)
                    .to_string()
            }),
            ..Self::default()
        }
    }

    pub(crate) fn matches(&self, record: &SnapshotRecord) -> bool {
        if let Some(alias_prefix) = self.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = self.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = self.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = self.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = self.sources.as_ref() {
            if !sources.contains(&record.source.kind()) {
                return false;
            }
        }

        if let Some(statuses) = self.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
/// Durable snapshot repository.
///
/// This trait owns the repository truth for [`SnapshotRecord`] values and committed snapshot
/// artifacts. A record is the catalog identity and lifecycle state for a snapshot:
///
/// - template records may exist before build artifacts are committed
/// - sandbox records are created by publishing an already captured runtime snapshot
/// - committed records carry a [`crate::snapshot::CommittedSnapshot`] payload with artifact
///   references
///
/// Implementations are responsible for durable state that may be shared across processes or nodes:
///
/// - snapshot records and template build state
/// - alias-to-snapshot bindings
/// - committed artifacts such as Firecracker snapshots and managed layers
/// - publish / delete visibility rules
///
/// Callers may assume returned records describe durable repository state rather than process-local
/// working directories or node-local runtime cache files.
///
/// The repository boundary intentionally excludes node-local derived state. In particular:
///
/// - local build-artifact allocation is owned by the manager rather than the repository
/// - runtime-ready `image.json` files should be materialized by [`SnapshotRuntimeResolver`]
/// - node-local cache directories should not leak back into snapshot records
/// - repository implementations should not require sandbox launch code to understand backend-specific
///   local build layouts
///
/// Backend guidance for shared POSIX or distributed filesystems:
///
/// - alias claim / release should be concurrency-safe
/// - publish should only make aliases visible after the snapshot record and committed artifact
///   payload are durable enough for subsequent readers to resolve
/// - delete should avoid exposing partially removed records or committed artifacts
/// - shared artifact imports should use atomic protocols so concurrent writers do not expose
///   half-written managed layers
pub trait SnapshotRepository: Send + Sync {
    /// Creates a durable template snapshot record before build artifacts exist.
    ///
    /// Backends should reject records that already contain a committed artifact payload and should
    /// only accept records whose source kind is template.
    async fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;

    /// Publishes manager-owned local artifacts into committed repository state.
    ///
    /// Implementations are responsible for:
    ///
    /// - reading build artifacts from the provided local artifact description
    /// - validating publish metadata
    /// - importing any repository-owned managed layers
    /// - committing a durable snapshot record with a committed artifact payload
    /// - binding aliases only after the record is durable
    /// - cleaning up or rolling back partially committed state on failure
    ///
    /// On success, the returned [`SnapshotRecord`] must contain committed artifact state.
    /// On failure, callers may assume the backend attempted best-effort cleanup of partially published
    /// state, but durable shared artifacts may still be retained when doing so is safe and intentional.
    async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> RepositoryResult<SnapshotRecord>;

    /// Materializes backend-owned remote lowers that a node-local capture
    /// cannot represent by reference. Implementations may replace transient
    /// config paths with local copies before the local repository imports them.
    ///
    /// The default is sufficient for repositories whose capture inputs are
    /// already local. A backend must fail closed when it cannot make a
    /// captured memory closure local; returning a record with a dangling
    /// remote-only memory layer would make Local recovery non-runnable.
    async fn prepare_local_capture(
        &self,
        _manifest: &mut FirecrackerSnapshotManifest,
    ) -> RepositoryResult<()> {
        Ok(())
    }

    /// Commits an already materialized logical record without importing its bytes.
    ///
    /// This is the canonical metadata commit used for Local snapshots: the
    /// node-local POSIX store has already committed the immutable artifact
    /// closure, while this repository owns public identity, alias, lifecycle,
    /// and placement. Implementations must keep Preparing records hidden and
    /// make the alias and Ready record visible as one recoverable operation.
    async fn commit_record(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord>;

    /// Loads an exact record, including a hidden Preparing record.
    ///
    /// Public callers use [`Self::get`]. This exact read exists for metadata
    /// commit recovery and must never interpret UUID text as an alias.
    async fn get_record(&self, id: &SnapshotId) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Loads an exact snapshot record only when its committed artifact closure
    /// is publicly runnable.
    ///
    /// Unlike [`Self::get_record`], this read excludes hidden lifecycle states,
    /// records without committed artifacts, and backend-specific records whose
    /// visibility/commit marker is missing.
    async fn get_committed_record(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        Ok(self
            .get_record(id)
            .await?
            .filter(|record| record.is_ready() && record.committed.is_some()))
    }

    /// Loads one snapshot record by repository id or alias.
    async fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>>;

    /// Loads the exact identity selected for a delete, including hidden
    /// Preparing/Deleting records. The returned ID must be used for the
    /// subsequent delete so an alias cannot be rebound to a different
    /// identity between lookup and cleanup.
    async fn get_for_delete(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.get(id_or_alias).await
    }

    /// Lists snapshot records matching the provided filter.
    async fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>>;

    /// Deletes one snapshot record by repository id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    ///
    /// For committed records, implementations should also remove per-snapshot committed artifacts and
    /// any alias binding that still points at the deleted id.
    async fn delete(&self, id_or_alias: &str) -> RepositoryResult<()>;

    /// Deletes exactly one snapshot id without interpreting its UUID text as an alias.
    ///
    /// This is required when a manager has already resolved a cross-repository identity: aliases
    /// are allowed to look like UUIDs, so routing the id back through [`Self::delete`] could delete
    /// an unrelated alias target in a repository where that id is absent.
    /// Returns whether an exact identity existed and its delete lifecycle was
    /// completed. Backends may retain a hidden terminal tombstone after
    /// physical artifacts are removed to fence stale writers.
    async fn delete_by_id(&self, _id: &SnapshotId) -> RepositoryResult<bool> {
        Err(RepositoryError::Unsupported {
            feature: "deleting a snapshot by exact id".to_string(),
        })
    }

    /// Resolves a human-readable alias to the current snapshot id.
    async fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>>;

    /// Atomically transitions one template build from waiting to building.
    ///
    /// Backends should reject non-template records and template records that are no longer waiting.
    async fn try_start_build(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord>;

    /// Marks one template build as failed.
    ///
    /// Backends should preserve the existing record identity, alias, resources, and source while
    /// recording the failure state and reason.
    async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()>;
}

#[async_trait]
/// Resolves committed snapshot records into node-local runnable paths.
///
/// This trait sits at the boundary between repository truth and launch-time derived state.
/// Implementations consume a committed [`SnapshotRecord`] and may materialize node-local helper files
/// such as runnable overlaybd `image.json` configs for the current node.
///
/// Contract:
///
/// - consumes a [`SnapshotRecord`] whose committed payload is present
/// - returns paths that are directly usable by sandbox / firecracker launch code on the current node
/// - may materialize node-local derived files such as runnable `image.json`
/// - must not mutate committed repository truth when generating node-local cache files
///
/// Backend guidance:
///
/// - runtime cache directories should be treated as node-local derived state
/// - shared repository storage and node-local runtime cache should be separated whenever possible
/// - concurrent resolves on one node should prefer atomic cache writes so readers never observe
///   partially written derived configs
pub trait SnapshotRuntimeResolver: Send + Sync {
    /// Resolves one committed snapshot record into a runtime-ready view for the current node.
    async fn resolve(&self, snapshot: Arc<SnapshotRecord>) -> RepositoryResult<RunnableSnapshot>;
}
