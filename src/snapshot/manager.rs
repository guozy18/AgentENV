use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::{info, warn};

use super::p2p::{fixed_artifact_key, SnapshotP2pArtifact};
use super::types::{now_unix_ms, SNAPSHOT_ARTIFACT_LAYOUT};
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::{build_local_snapshot_backend, build_snapshot_backend};
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
use crate::snapshot::{
    OverlaybdLayerRef, RunnableSnapshot, SnapshotAlias, SnapshotId, SnapshotLifecycle,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, SnapshotType,
};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;
/// Local closures without canonical metadata are retained long enough for an
/// operator or a subsequent startup to recover a partial metadata commit.
const LOCAL_ORPHAN_GRACE_PERIOD: Duration = Duration::from_secs(24 * 60 * 60);

fn managed_layer_uuids(layers: &[OverlaybdLayerRef]) -> HashSet<String> {
    layers
        .iter()
        .filter_map(|layer| match layer {
            OverlaybdLayerRef::Managed(managed) => managed.uuid.clone(),
            OverlaybdLayerRef::External(_) => None,
        })
        .collect()
}

#[derive(Clone)]
struct SnapshotStore {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
}

impl SnapshotStore {
    fn new(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    ) -> Self {
        Self {
            repository,
            runtime_resolver,
        }
    }

    async fn get_template_alias(
        &self,
        alias: &str,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let Some(id) = self.repository.resolve_alias(alias).await? else {
            return Ok(None);
        };
        let record = self.repository.get_record(&id).await?;
        Ok(record.filter(|record| {
            record.is_ready() && record.source.kind() == SnapshotSourceKind::Template
        }))
    }
}

#[derive(Clone)]
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Durable reachability of committed snapshots is owned entirely by the
/// [`SnapshotRepository`] (PosixFS `managed-layers/`, OSS object storage, or the
/// source registry). The node-local overlaybd layer cache (`image-cache/commits/`)
/// is reclaimable - committed snapshots never pin it - so this manager records no
/// local image ref pins.
pub struct SnapshotManager {
    primary: SnapshotStore,
    local: Option<SnapshotStore>,
    node_id: String,
    p2p_transport: Option<Arc<dyn P2pTransport>>,
}

impl SnapshotManager {
    /// Builds a manager using the configured repository backend.
    pub fn new(
        node_id: String,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> anyhow::Result<Self> {
        let (repository, runtime_resolver) = build_snapshot_backend(p2p_transport.clone())?;
        let (local_repository, local_runtime_resolver) = build_local_snapshot_backend()?;
        Ok(Self {
            primary: SnapshotStore::new(repository, runtime_resolver),
            local: Some(SnapshotStore::new(local_repository, local_runtime_resolver)),
            node_id,
            p2p_transport,
        })
    }

    /// Builds a manager from the given components.
    pub fn from_parts(
        repository: Arc<dyn SnapshotRepository>,
        runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
        p2p_transport: Option<Arc<dyn P2pTransport>>,
    ) -> Self {
        Self {
            primary: SnapshotStore::new(repository, runtime_resolver),
            local: None,
            node_id: String::new(),
            p2p_transport,
        }
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.primary.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let record = self
            .primary
            .repository
            .publish(metadata, manifest.clone())
            .await?;
        self.publish_p2p_artifacts(&record, &manifest).await;
        Ok(record)
    }

    #[tracing::instrument(skip(self, metadata), fields(snapshot_id = %metadata.id))]
    pub async fn publish_captured(
        &self,
        metadata: SnapshotPublishMetadata,
        captured_snapshot: CapturedSandboxSnapshot,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let manifest = captured_snapshot
            .downcast_ref::<FirecrackerCapturedSnapshot>()
            .map(|snapshot| snapshot.manifest().clone())
            .ok_or_else(|| RepositoryError::Unsupported {
                feature: "publishing captured snapshots for this sandbox backend".to_string(),
            })?;

        self.publish_captured_manifest(metadata, manifest).await
    }

    async fn publish_captured_manifest(
        &self,
        mut metadata: SnapshotPublishMetadata,
        mut manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let requested_type = metadata.snapshot_type;
        if requested_type == SnapshotType::Local && metadata.alias.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "Local snapshots do not support aliases".to_string(),
            });
        }
        let Some(local) = self.local.as_ref() else {
            return match requested_type {
                SnapshotType::Local => Err(RepositoryError::Unsupported {
                    feature: "durable local snapshot repository is not configured".to_string(),
                }),
                SnapshotType::Distributed => self.publish(metadata, manifest).await,
            };
        };

        // Every sandbox snapshot first becomes one immutable node-local
        // closure. Distributed publication promotes that same closure and ID;
        // it never captures the live sandbox a second time.
        metadata.snapshot_type = SnapshotType::Local;
        metadata.owner_node_id = Some(self.node_id.clone());
        let requested_alias = metadata.alias.take();
        self.primary
            .repository
            .prepare_local_capture(&mut manifest)
            .await?;
        let snapshot_id = metadata.id.clone();
        let local_record = local.repository.publish(metadata, manifest).await?;
        match requested_type {
            // The configured primary repository is the only public metadata
            // authority, even when the immutable bytes remain node-local.
            SnapshotType::Local => self.primary.repository.commit_record(local_record).await,
            SnapshotType::Distributed => {
                let runnable = local
                    .runtime_resolver
                    .resolve(Arc::new(local_record.clone()))
                    .await
                    .map_err(|error| RepositoryError::Unavailable {
                        reason: format!(
                            "resolve private local closure for snapshot '{snapshot_id}': {error}"
                        ),
                    })?;
                let distributed = self
                    .publish_distributed_closure(
                        &local_record,
                        requested_alias,
                        runnable.manifest(),
                    )
                    .await?;

                self.publish_p2p_artifacts(&distributed, runnable.manifest())
                    .await;
                drop(runnable);
                self.cleanup_local_recovery_copy(&snapshot_id).await;
                Ok(distributed)
            }
        }
    }

    /// Promotes one reusable Local snapshot to Distributed without recapture.
    ///
    /// The lookup is always resolved by the canonical repository. A missing or
    /// foreign-owner Local snapshot is never substituted from node-local state.
    pub async fn promote(
        &self,
        id_or_alias: &str,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let Some(record) = self.primary.repository.get_for_delete(id_or_alias).await? else {
            return Ok(None);
        };
        if record.lifecycle == SnapshotLifecycle::Deleting && record.committed.is_none() {
            return Ok(None);
        }
        if !matches!(record.source, SnapshotSource::Sandbox { .. }) {
            return Ok(None);
        }
        let alias = record.alias.clone();
        self.promote_record(record, alias).await.map(Some)
    }

    async fn promote_record(
        &self,
        record: SnapshotRecord,
        alias: Option<SnapshotAlias>,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let snapshot_id = record.id.clone();

        // Re-read so a retry observes a promotion completed by an earlier
        // request instead of replaying its rollback. Repository CAS and fixed
        // artifact keys provide the cross-process safety boundary.
        let record = self
            .primary
            .repository
            .get_committed_record(&snapshot_id)
            .await?
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!("canonical snapshot '{snapshot_id}' disappeared during promotion"),
            })?;
        if record.snapshot_type == SnapshotType::Distributed {
            self.cleanup_local_recovery_copy(&snapshot_id).await;
            return Ok(record);
        }
        let runnable = self.resolve_local_runtime(record.clone()).await?;
        let promoted = self
            .publish_distributed_closure(&record, alias, runnable.manifest())
            .await?;

        // Keep publication ordered with delete: otherwise delete can
        // unpublish the keys before this best-effort advertisement recreates
        // stale entries for an already deleted snapshot.
        self.publish_p2p_artifacts(&promoted, runnable.manifest())
            .await;
        drop(runnable);
        self.cleanup_local_recovery_copy(&snapshot_id).await;
        Ok(promoted)
    }

    async fn cleanup_local_recovery_copy(&self, snapshot_id: &SnapshotId) {
        let Some(local) = self.local.as_ref() else {
            return;
        };
        match local.repository.purge_by_id(snapshot_id).await {
            Ok(true) => {
                info!(%snapshot_id, "removed local recovery copy for Distributed snapshot");
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    %snapshot_id,
                    %error,
                    "failed to remove local recovery copy for Distributed snapshot"
                );
                return;
            }
        }
        match local.repository.gc_unreferenced_artifacts().await {
            Ok(removed) if removed > 0 => {
                info!(
                    %snapshot_id,
                    removed,
                    "removed unreferenced node-local snapshot managed layers after promotion"
                );
            }
            Ok(_) => {}
            Err(error) => {
                warn!(
                    %snapshot_id,
                    %error,
                    "failed to garbage-collect node-local snapshot managed layers after promotion"
                );
            }
        }
    }

    fn require_local_owner(
        &self,
        record: &SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<()> {
        match record.owner_node_id.as_deref() {
            Some(owner) if owner == self.node_id => Ok(()),
            Some(owner) => Err(RepositoryError::Unavailable {
                reason: format!(
                    "Local snapshot '{}' belongs to node '{owner}', not '{}'",
                    record.id, self.node_id
                ),
            }),
            None => Err(RepositoryError::Unavailable {
                reason: format!("Local snapshot '{}' has no owner metadata", record.id),
            }),
        }
    }

    async fn resolve_local_runtime(
        &self,
        canonical: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<RunnableSnapshot> {
        let snapshot_id = canonical.id.clone();
        self.require_local_owner(&canonical)?;
        let local = self
            .local
            .as_ref()
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: "node-local snapshot store is not configured".to_string(),
            })?;
        // The canonical record owns identity, lifecycle, and logical artifact
        // references.  The local resolver owns the physical commit marker,
        // fixed artifacts, managed-layer files, and runtime materialization
        // checks.  Reading a second full SnapshotRecord here only duplicated
        // metadata authority without validating any artifact bytes.
        local
            .runtime_resolver
            .resolve(Arc::new(canonical))
            .await
            .map_err(|error| RepositoryError::Unavailable {
                reason: format!(
                    "resolve node-local artifacts for snapshot '{snapshot_id}': {error}"
                ),
            })
    }

    async fn resolve_distributed_runtime(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        const CONTEXT: &str = "resolve committed snapshot into runnable runtime paths";
        let snapshot_id = snapshot.id.clone();
        self.primary
            .runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .map_err(|error| {
                anyhow::Error::new(RepositoryError::Unavailable {
                    reason: format!(
                        "resolve Distributed snapshot '{snapshot_id}' artifacts: {error}"
                    ),
                })
            })
            .context(CONTEXT)
    }

    fn distributed_metadata(
        record: &SnapshotRecord,
        alias: Option<SnapshotAlias>,
    ) -> crate::snapshot::RepositoryResult<SnapshotPublishMetadata> {
        let SnapshotSource::Sandbox { source_sandbox_id } = &record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: "only sandbox snapshots can be promoted".to_string(),
            });
        };
        let committed =
            record
                .committed
                .as_ref()
                .ok_or_else(|| RepositoryError::InvalidRequest {
                    reason: format!("snapshot '{}' has no committed artifacts", record.id),
                })?;
        Ok(SnapshotPublishMetadata {
            id: record.id.clone(),
            snapshot_type: SnapshotType::Distributed,
            owner_node_id: None,
            alias,
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: source_sandbox_id.clone(),
            },
            context: committed.context.clone(),
            startup: committed.startup.clone(),
            resources: record.resources,
            runtime_versions: committed.runtime_versions.clone(),
            virtualization_mode: committed.virtualization_mode,
            image_configs: committed.image_configs.clone(),
            custom_extension_params: committed.custom_extension_params.clone(),
        })
    }

    async fn publish_distributed_closure(
        &self,
        source: &SnapshotRecord,
        alias: Option<SnapshotAlias>,
        manifest: &FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let snapshot_id = &source.id;
        let metadata = Self::distributed_metadata(source, alias)?;
        self.primary
            .repository
            .publish(metadata, manifest.clone())
            .await
            .map_err(|error| match error {
                error @ (RepositoryError::ArtifactNotFound { .. }
                | RepositoryError::ManagedLayerNotFound { .. }
                | RepositoryError::IntegrityMismatch { .. }) => RepositoryError::Unavailable {
                    reason: format!(
                        "publish Distributed closure for snapshot '{snapshot_id}': {error}"
                    ),
                },
                error => error,
            })
    }

    /// Reconciles node-local immutable closures against canonical metadata.
    ///
    /// All canonical reads complete before any deletion. If the primary
    /// repository is unavailable, the method returns without removing local
    /// data and the server can continue startup in a conservative mode.
    pub async fn reconcile_local_artifacts(&self) -> crate::snapshot::RepositoryResult<()> {
        let Some(local) = self.local.as_ref() else {
            return Ok(());
        };
        let local_records = local.repository.list_recovery_candidates().await?;
        for local_record in &local_records {
            let _ = self.primary.repository.get_record(&local_record.id).await?;
        }

        for observed_local in local_records {
            let snapshot_id = observed_local.id;
            let Some(local_record) = local.repository.get_record(&snapshot_id).await? else {
                continue;
            };
            let local_recovery_record = local.repository.get_recovery_record(&snapshot_id).await?;
            let canonical = self
                .primary
                .repository
                .get_for_delete(&snapshot_id.to_string())
                .await?;
            let canonical_committed = match canonical.as_ref() {
                Some(record) if record.lifecycle == SnapshotLifecycle::Ready => {
                    self.primary
                        .repository
                        .get_committed_record(&snapshot_id)
                        .await?
                }
                _ => None,
            };
            match canonical {
                Some(record) if record.lifecycle == SnapshotLifecycle::Deleting => {
                    Self::purge_local_recovery_copy(local, &snapshot_id, "deleting snapshot").await;
                }
                Some(record)
                    if record.lifecycle == SnapshotLifecycle::Ready
                        && canonical_committed.is_none() =>
                {
                    warn!(
                        %snapshot_id,
                        "kept local snapshot closure whose canonical commit marker is missing"
                    );
                }
                Some(record)
                    if record.snapshot_type == SnapshotType::Distributed
                        && canonical_committed.is_some() =>
                {
                    Self::purge_local_recovery_copy(local, &snapshot_id, "Distributed snapshot")
                        .await;
                }
                Some(record)
                    if record.snapshot_type == SnapshotType::Local
                        && canonical_committed.is_some()
                        && record.owner_node_id.as_deref() == Some(self.node_id.as_str())
                        && local_recovery_record.is_some() =>
                {
                    // Canonical metadata and the physical commit marker are
                    // the only launch facts. The private record's lifecycle
                    // is not a second public state machine.
                }
                Some(record) => {
                    warn!(
                        %snapshot_id,
                        canonical_type = ?record.snapshot_type,
                        canonical_owner = ?record.owner_node_id,
                        "kept local snapshot closure with non-consumable canonical metadata"
                    );
                }
                None if local_record.lifecycle == SnapshotLifecycle::Deleting
                    || Self::local_orphan_expired(&local_record) =>
                {
                    Self::purge_local_recovery_copy(
                        local,
                        &snapshot_id,
                        "expired orphaned local snapshot closure",
                    )
                    .await;
                }
                None => {
                    warn!(
                        %snapshot_id,
                        "kept recent local snapshot closure without canonical metadata"
                    );
                }
            }
        }
        let removed = local.repository.gc_unreferenced_artifacts().await?;
        if removed > 0 {
            info!(
                removed,
                "removed unreferenced node-local snapshot managed layers"
            );
        }
        Ok(())
    }

    fn local_orphan_expired(record: &SnapshotRecord) -> bool {
        let age_ms = now_unix_ms().saturating_sub(record.updated_at_unix_ms);
        age_ms >= LOCAL_ORPHAN_GRACE_PERIOD.as_millis() as i64
    }

    async fn purge_local_recovery_copy(
        local: &SnapshotStore,
        snapshot_id: &SnapshotId,
        reason: &'static str,
    ) {
        if let Err(error) = local.repository.purge_by_id(snapshot_id).await {
            warn!(%snapshot_id, %error, reason, "failed to remove local recovery copy");
        }
    }

    /// Best effort attempt to publish snapshot artifacts to P2P.
    #[tracing::instrument(skip(self, record, manifest), fields(snapshot_id = %record.id))]
    async fn publish_p2p_artifacts(
        &self,
        record: &SnapshotRecord,
        manifest: &FirecrackerSnapshotManifest,
    ) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };
        let snapshot_id = &record.id;
        let Some(committed) = record.committed.as_ref() else {
            return;
        };

        // New and pre-namespace records use fixed artifact keys. Records from
        // the former namespaced layout must continue to resolve only through
        // their persisted OSS prefix. Content-addressed layers are safe in
        // either layout.
        let mut artifacts = Vec::new();
        if committed.legacy_artifact_namespace.is_none() {
            let manifest_bytes = serde_json::to_vec(manifest).expect("manifest should serialize");
            artifacts.push(SnapshotP2pArtifact::fixed(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
                manifest.vm_state.path.clone(),
            ));
            artifacts.push(SnapshotP2pArtifact::bytes(
                snapshot_id,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
                manifest_bytes,
            ));
        }

        // Collect any overlaybd layers referenced by this snapshot's runtime images.
        let rootfs_uuids = managed_layer_uuids(&committed.rootfs_layers);
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.rootfs.image_config_path,
            &rootfs_uuids,
        ));
        let memory_uuids = committed
            .memory_layers
            .iter()
            .filter_map(|layer| layer.uuid.clone())
            .collect();
        artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
            &manifest.memory.image_config_path,
            &memory_uuids,
        ));
        for drive in &manifest.attached_drives {
            let drive_uuids = committed
                .attached_drives
                .iter()
                .find_map(|committed_drive| match committed_drive {
                    crate::snapshot::CommittedAttachedDrive::Overlaybd {
                        drive_id, layers, ..
                    } if drive_id == &drive.drive_id => Some(managed_layer_uuids(layers)),
                    _ => None,
                })
                .unwrap_or_default();
            artifacts.extend(SnapshotP2pArtifact::local_overlaybd_layers(
                &drive.image_config_path,
                &drive_uuids,
            ));
        }

        // Publish all artifacts concurrently, but don't fail if any individual artifact fails to publish.
        stream::iter(artifacts)
            .for_each_concurrent(SNAPSHOT_P2P_PUBLISH_CONCURRENCY, |artifact| async move {
                if let Err(error) = artifact.publish(transport).await {
                    warn!(
                        key = %artifact.key,
                        source = %artifact.source,
                        error = %error,
                        "failed to publish snapshot artifact to P2P"
                    );
                }
            })
            .await;
    }

    async fn unpublish_snapshot_p2p_artifacts(&self, snapshot_id: &SnapshotId) {
        let Some(transport) = self.p2p_transport.as_ref() else {
            return;
        };

        // VM state and the Firecracker manifest use snapshot-scoped keys.
        // They are safe to reclaim without a global layer reference index;
        // managed digest/UUID keys are shared by multiple snapshots and must
        // remain advertised until a separate reference-aware GC exists.
        stream::iter([
            SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
            SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
        ])
        .for_each_concurrent(2, |name| async move {
            let key = fixed_artifact_key(snapshot_id, name);
            if let Err(error) = transport.unpublish(&key).await {
                warn!(
                    %snapshot_id,
                    %key,
                    %error,
                    "failed to unpublish deleted snapshot artifact from P2P"
                );
            }
        })
        .await;
    }

    /// Loads a snapshot record by id or alias.
    pub async fn get(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_matching(id_or_alias.as_ref(), None).await
    }

    async fn get_matching(
        &self,
        lookup: &str,
        source: Option<SnapshotSourceKind>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        let description = match source {
            Some(SnapshotSourceKind::Template) => "template",
            Some(SnapshotSourceKind::Sandbox) | None => "committed",
        };
        self.primary
            .repository
            .get(lookup)
            .await
            .map(|record| {
                record.filter(|record| source.is_none_or(|source| record.source.kind() == source))
            })
            .with_context(|| format!("load {description} snapshot '{lookup}' through repository"))
    }

    /// Loads a sandbox-captured reusable snapshot without allowing a Template
    /// record in the primary repository to shadow a local snapshot alias.
    pub async fn get_sandbox_snapshot(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_matching(id_or_alias.as_ref(), Some(SnapshotSourceKind::Sandbox))
            .await
    }

    /// Loads a template-owned snapshot from the primary repository.
    ///
    /// Template records are never published to the node-local recovery store;
    /// a local sandbox snapshot must not mask a missing or unavailable template.
    pub async fn get_template(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<SnapshotRecord>> {
        self.get_matching(id_or_alias.as_ref(), Some(SnapshotSourceKind::Template))
            .await
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.primary
            .repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        self.delete_matching(id_or_alias, None).await
    }

    /// Deletes a template record without falling back to sandbox recovery points.
    pub async fn delete_template(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        self.delete_matching(id_or_alias, Some(SnapshotSourceKind::Template))
            .await
    }

    async fn delete_matching(
        &self,
        id_or_alias: impl AsRef<str>,
        expected_source: Option<SnapshotSourceKind>,
    ) -> anyhow::Result<()> {
        let lookup = id_or_alias.as_ref();
        let (resource, delete_resource) = expected_source.map_or(("snapshot", "snapshot"), |_| {
            ("template", "template snapshot")
        });
        let Some(record) = self
            .primary
            .repository
            .get_for_delete(lookup)
            .await
            .with_context(|| format!("load {resource} '{lookup}' before delete"))?
        else {
            return Ok(());
        };
        if expected_source.is_some_and(|source| record.source.kind() != source) {
            return Ok(());
        }
        if expected_source.is_none() {
            Self::ensure_local_delete_is_supported(&record)?;
        }
        let snapshot_id = record.id.clone();
        let deleted = {
            let Some(record) = self
                .primary
                .repository
                .get_for_delete(&snapshot_id.to_string())
                .await
                .with_context(|| format!("re-read {resource} '{snapshot_id}' before delete"))?
            else {
                return Ok(());
            };
            if expected_source.is_some_and(|source| record.source.kind() != source) {
                return Ok(());
            }
            if expected_source.is_none() {
                Self::ensure_local_delete_is_supported(&record)?;
            }
            self.primary
                .repository
                .delete_by_id(&snapshot_id)
                .await
                .with_context(|| {
                    format!("delete {delete_resource} '{snapshot_id}' through repository")
                })?
        };
        if deleted {
            self.unpublish_snapshot_p2p_artifacts(&snapshot_id).await;
        }
        Ok(())
    }

    fn ensure_local_delete_is_supported(record: &SnapshotRecord) -> anyhow::Result<()> {
        if record.snapshot_type == SnapshotType::Local
            && record.lifecycle != SnapshotLifecycle::Deleting
        {
            return Err(anyhow::Error::new(RepositoryError::Unsupported {
                feature: "deleting Local snapshots before the owner-coordinated delete lifecycle is implemented"
                    .to_string(),
            }));
        }
        Ok(())
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.primary
            .repository
            .resolve_alias(alias)
            .await
            .with_context(|| {
                format!("resolve committed snapshot alias '{alias}' through repository")
            })
    }

    /// Resolves an alias only when it names a primary-owned Template record.
    pub async fn resolve_template_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.primary
            .get_template_alias(alias)
            .await
            .with_context(|| format!("load template alias target '{alias}' through repository"))
            .map(|record| record.map(|record| record.id))
    }

    /// Resolves a committed snapshot into node-local runnable artifact paths.
    pub async fn resolve_runnable(
        &self,
        snapshot: SnapshotRecord,
    ) -> anyhow::Result<RunnableSnapshot> {
        const CONTEXT: &str = "resolve committed snapshot into runnable runtime paths";
        match snapshot.snapshot_type {
            SnapshotType::Distributed => self.resolve_distributed_runtime(snapshot).await,
            SnapshotType::Local => {
                let snapshot_id = snapshot.id.clone();
                let canonical = self
                    .primary
                    .repository
                    .get_committed_record(&snapshot_id)
                    .await
                    .map_err(anyhow::Error::new)
                    .context(CONTEXT)?
                    .ok_or_else(|| {
                        anyhow::Error::new(RepositoryError::Unavailable {
                            reason: format!(
                                "canonical metadata for Local snapshot '{snapshot_id}' is unavailable"
                            ),
                        })
                    })?;
                if canonical.snapshot_type == SnapshotType::Distributed {
                    // The canonical state is now immutable Distributed metadata;
                    // remote resolution follows the same path as an initially
                    // Distributed request.
                    return self.resolve_distributed_runtime(canonical).await;
                }
                self.resolve_local_runtime(canonical)
                    .await
                    .map_err(anyhow::Error::new)
                    .context("resolve committed snapshot into runnable runtime paths")
            }
        }
    }

    /// Loads a committed snapshot and immediately resolves it into runnable state.
    #[tracing::instrument(
        skip(self, id_or_alias),
        fields(snapshot_ref = %id_or_alias.as_ref())
    )]
    pub async fn load_runnable(
        &self,
        id_or_alias: impl AsRef<str>,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self.get(id_or_alias.as_ref()).await? else {
            return Ok(None);
        };
        self.resolve_runnable(snapshot).await.map(Some)
    }

    /// Loads a runnable template by alias from primary-owned template state only.
    pub async fn load_template_alias_runnable(
        &self,
        alias: &SnapshotAlias,
    ) -> anyhow::Result<Option<RunnableSnapshot>> {
        let Some(snapshot) = self
            .primary
            .get_template_alias(alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "load template alias '{}' through repository",
                    alias.as_ref()
                )
            })?
        else {
            return Ok(None);
        };
        self.primary
            .runtime_resolver
            .resolve(Arc::new(snapshot))
            .await
            .context("resolve template snapshot into runnable runtime paths")
            .map(Some)
    }

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.primary.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.primary.repository.mark_build_error(id, reason).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::repository::backends::{PosixFsBackend, PosixFsBackendConfig};
    use crate::snapshot::{SnapshotAlias, SnapshotId, SnapshotPublishMetadata};
    use std::path::Path;
    use tempfile::TempDir;

    fn test_manager(root: &Path) -> SnapshotManager {
        let (repository, runtime_resolver) = test_store(root, "repository");
        SnapshotManager::from_parts(repository, runtime_resolver, None)
    }

    fn test_store(
        root: &Path,
        name: &str,
    ) -> (
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRuntimeResolver>,
    ) {
        let cache = root.join(format!("{name}-cache"));
        PosixFsBackend::new(PosixFsBackendConfig {
            root: root.join(name),
            cache_root: Some(cache.clone()),
            runtime_cache_root: Some(cache.join("runtime")),
        })
        .expect("posix backend")
        .into_parts()
    }

    fn test_manager_with_local(
        root: &Path,
        primary_name: &str,
        node_id: &str,
    ) -> (
        SnapshotManager,
        Arc<dyn SnapshotRepository>,
        Arc<dyn SnapshotRepository>,
    ) {
        let (primary_repository, primary_resolver) = test_store(root, primary_name);
        let (local_repository, local_resolver) = test_store(root, "local");
        let manager = SnapshotManager {
            primary: SnapshotStore::new(Arc::clone(&primary_repository), primary_resolver),
            local: Some(SnapshotStore::new(
                Arc::clone(&local_repository),
                local_resolver,
            )),
            node_id: node_id.to_string(),
            p2p_transport: None,
        };
        (manager, primary_repository, local_repository)
    }

    fn captured_metadata(
        id: SnapshotId,
        alias: Option<&str>,
        snapshot_type: SnapshotType,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            snapshot_type,
            owner_node_id: (snapshot_type == SnapshotType::Local).then(|| "node-a".to_string()),
            alias: alias.map(|alias| SnapshotAlias::parse(alias).expect("alias should parse")),
            source: SnapshotPublishSource::Sandbox {
                source_sandbox_id: "sandbox-source".to_string(),
            },
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn mock_manifest(root: &Path) -> FirecrackerSnapshotManifest {
        write_mock_built_artifacts(root)
            .expect("mock artifacts should write")
            .2
    }

    fn write_posix_record(root: &Path, store_name: &str, record: &SnapshotRecord) {
        let record_path = root
            .join(store_name)
            .join("catalog/records")
            .join(format!("{}.json", record.id));
        std::fs::write(
            record_path,
            serde_json::to_vec_pretty(record).expect("record should serialize"),
        )
        .expect("record should write");
    }

    async fn seed_built_snapshot(manager: &SnapshotManager, snapshot_id: SnapshotId, alias: &str) {
        let workspace = TempDir::new().expect("tempdir should exist");
        let (_, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            ..SnapshotPublishMetadata::mock()
        };
        manager
            .publish(metadata, manifest)
            .await
            .expect("seed publish should work");
    }

    #[tokio::test]
    async fn repository_management_methods_delegate_to_committed_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "managed").await;

        let resolved = manager
            .resolve_committed_alias("managed")
            .await
            .expect("resolve alias should work");
        assert_eq!(resolved, Some(snapshot_id.clone()));

        let loaded = manager
            .get("managed")
            .await
            .expect("load should work")
            .expect("snapshot should exist");
        assert_eq!(loaded.id, snapshot_id);

        let listed = manager
            .list(crate::snapshot::repository::SnapshotListFilter::matches_all())
            .await
            .expect("list should work");
        assert_eq!(listed.len(), 1);

        manager.delete("managed").await.expect("delete should work");
        assert!(manager
            .get("managed")
            .await
            .expect("load after delete should work")
            .is_none());
    }

    #[tokio::test]
    async fn delete_template_fences_late_publish_and_releases_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("deleted-template").expect("alias should parse");

        manager
            .create(SnapshotRecord::template_waiting(
                snapshot_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .await
            .expect("template identity should be created");
        manager
            .delete_template(snapshot_id.to_string())
            .await
            .expect("template delete should complete");

        let tombstone = manager
            .primary
            .repository
            .get_record(&snapshot_id)
            .await
            .expect("terminal identity lookup should work")
            .expect("delete should retain the terminal identity tombstone");
        assert_eq!(tombstone.lifecycle, SnapshotLifecycle::Deleting);
        assert!(tombstone.committed.is_none());

        let late_publish = manager
            .publish(
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    alias: Some(alias.clone()),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                mock_manifest(&tempdir.path().join("late-publish")),
            )
            .await
            .expect_err("late template publish must not resurrect a deleted identity");
        assert!(matches!(
            late_publish,
            RepositoryError::ConcurrentModification { .. }
        ));

        let replacement_id = SnapshotId::generate();
        manager
            .create(SnapshotRecord::template_waiting(
                replacement_id.clone(),
                Some(alias.clone()),
                Default::default(),
            ))
            .await
            .expect("a different identity should reclaim the alias");
        assert_eq!(
            manager
                .primary
                .repository
                .resolve_alias(alias.as_ref())
                .await
                .expect("rebound alias lookup should work"),
            Some(replacement_id)
        );
    }

    #[tokio::test]
    async fn load_runnable_uses_committed_snapshot_and_runtime_resolution() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let manager = test_manager(tempdir.path());
        let snapshot_id = SnapshotId::generate();
        seed_built_snapshot(&manager, snapshot_id.clone(), "runnable").await;

        let runnable = manager
            .load_runnable("runnable")
            .await
            .expect("load runnable should work")
            .expect("runnable snapshot should exist");

        assert_eq!(runnable.record().id, snapshot_id);
        assert!(runnable.manifest().rootfs.image_config_path.exists());
        assert!(runnable.manifest().vm_state.path.exists());
    }

    #[tokio::test]
    async fn local_capture_is_id_only_in_canonical_and_physical_stores() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        assert_eq!(record.snapshot_type, SnapshotType::Local);
        assert_eq!(record.owner_node_id.as_deref(), Some("node-a"));
        assert!(record.alias.is_none());
        assert_eq!(
            primary
                .get(&snapshot_id.to_string())
                .await
                .expect("canonical lookup should work")
                .expect("canonical record should exist")
                .id,
            snapshot_id
        );
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup should work")
            .is_some_and(|record| record.alias.is_none()));
        assert!(primary
            .resolve_alias("local-one")
            .await
            .expect("canonical alias lookup should work")
            .is_none());
        assert_eq!(
            manager
                .resolve_runnable(record)
                .await
                .expect("owner should resolve Local artifacts")
                .record()
                .id,
            snapshot_id
        );
    }

    #[tokio::test]
    async fn local_capture_with_alias_fails_before_any_repository_write() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("local-alias").expect("alias should parse");
        let mut metadata = captured_metadata(snapshot_id.clone(), None, SnapshotType::Local);
        metadata.alias = Some(alias.clone());

        assert!(matches!(
            manager
                .publish_captured_manifest(
                    metadata,
                    mock_manifest(&tempdir.path().join("capture")),
                )
                .await
                .expect_err("Local alias must fail before capture publication"),
            RepositoryError::InvalidRequest { .. }
        ));
        assert!(primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical lookup should work")
            .is_none());
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup should work")
            .is_none());
        assert!(primary
            .resolve_alias(alias.as_ref())
            .await
            .expect("canonical alias lookup should work")
            .is_none());
    }

    #[tokio::test]
    async fn canonical_commit_failure_never_falls_back_to_local_catalog() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        std::fs::write(tempdir.path().join("broken-primary"), b"not a directory")
            .expect("broken primary sentinel should write");
        let (manager, _, local) =
            test_manager_with_local(tempdir.path(), "broken-primary", "node-a");
        let snapshot_id = SnapshotId::generate();

        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect_err("canonical metadata failure must fail the API operation");
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup should work")
            .is_some());
        let gc_candidate = tempdir
            .path()
            .join("local/managed-layers")
            .join(format!("sha256_{}.overlaybd.commit", "f".repeat(64)));
        std::fs::write(&gc_candidate, b"orphan layer").expect("GC candidate should write");
        manager
            .reconcile_local_artifacts()
            .await
            .expect_err("primary read failure should abort reconciliation");
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup after reconciliation should work")
            .is_some());
        assert!(
            gc_candidate.exists(),
            "primary read failure must prevent managed-layer GC"
        );
        manager
            .get(&snapshot_id.to_string())
            .await
            .expect_err("public lookup must not substitute the local catalog");
    }

    #[tokio::test]
    async fn distributed_capture_commits_only_distributed_canonical_metadata() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let promoted = manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    Some("distributed-one"),
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("same-ID promotion should work");

        assert_eq!(promoted.id, snapshot_id);
        assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
        assert_eq!(promoted.owner_node_id, None);
        assert_eq!(
            promoted.alias.as_ref().map(SnapshotAlias::as_ref),
            Some("distributed-one")
        );
        assert_eq!(
            primary
                .get_record(&snapshot_id)
                .await
                .expect("canonical exact lookup should work")
                .expect("canonical record should exist")
                .snapshot_type,
            SnapshotType::Distributed
        );
        assert_eq!(
            primary
                .get("distributed-one")
                .await
                .expect("canonical lookup should work")
                .expect("canonical record should exist")
                .snapshot_type,
            SnapshotType::Distributed
        );
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup should work")
            .is_none());
        assert!(
            std::fs::read_dir(tempdir.path().join("local/managed-layers"))
                .expect("local managed layer directory should exist")
                .next()
                .is_none(),
            "promotion should garbage-collect unreferenced local managed layers"
        );
        assert_eq!(
            manager
                .resolve_runnable(promoted)
                .await
                .expect("Distributed artifacts should resolve from primary")
                .record()
                .id,
            snapshot_id
        );
    }

    #[tokio::test]
    async fn distributed_capture_failure_leaves_only_private_local_recovery() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let alias = "distributed-failure";
        std::fs::create_dir_all(tempdir.path().join("primary")).expect("primary root should exist");
        std::fs::write(tempdir.path().join("primary/managed-layers"), b"blocked")
            .expect("publication blocker should write");

        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), Some(alias), SnapshotType::Distributed),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect_err("Distributed publication should fail");

        assert!(primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .is_none());
        assert!(primary
            .resolve_alias(alias)
            .await
            .expect("canonical alias lookup should work")
            .is_none());
        let recovery = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("private local recovery should remain");
        assert_eq!(recovery.snapshot_type, SnapshotType::Local);
        assert_eq!(recovery.owner_node_id.as_deref(), Some("node-a"));
        assert!(recovery.alias.is_none());
    }

    #[tokio::test]
    async fn idempotent_promotion_removes_a_stale_local_recovery_copy() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let manifest = mock_manifest(&tempdir.path().join("capture"));
        let local_record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                manifest.clone(),
            )
            .await
            .expect("Local snapshot should publish");
        primary
            .publish(
                SnapshotManager::distributed_metadata(&local_record, None)
                    .expect("Distributed metadata should derive from Local record"),
                manifest,
            )
            .await
            .expect("canonical Distributed publication should succeed");

        let promoted = manager
            .promote(&snapshot_id.to_string())
            .await
            .expect("idempotent promotion should succeed")
            .expect("Distributed snapshot should exist");

        assert_eq!(promoted.id, snapshot_id);
        assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_none());
        assert!(
            std::fs::read_dir(tempdir.path().join("local/managed-layers"))
                .expect("local managed layer directory should exist")
                .next()
                .is_none(),
            "promotion retry should garbage-collect stale local managed layers"
        );
    }

    #[tokio::test]
    async fn promotion_failure_preserves_ready_local_canonical_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        std::fs::write(tempdir.path().join("primary/managed-layers"), b"blocked")
            .expect("promotion blocker should write");

        manager
            .promote(&snapshot_id.to_string())
            .await
            .expect_err("artifact publication should fail");
        let canonical = primary
            .get(&snapshot_id.to_string())
            .await
            .expect("canonical lookup should work")
            .expect("Local canonical record must remain visible");
        assert_eq!(canonical.id, snapshot_id);
        assert_eq!(canonical.snapshot_type, SnapshotType::Local);
        assert!(canonical.is_ready());
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local lookup should work")
            .is_some());
    }

    #[tokio::test]
    async fn local_runtime_resolution_uses_canonical_metadata_when_private_record_drifts() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let canonical = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("canonical-capture")),
            )
            .await
            .expect("Local snapshot should publish");

        // The private catalog is not a second metadata authority. A stale
        // logical field there must not make an otherwise committed closure
        // unavailable when the canonical record and physical artifacts are
        // intact.
        let mut private_record = local
            .get_record(&snapshot_id)
            .await
            .expect("private record lookup should work")
            .expect("private record should exist");
        private_record
            .committed
            .as_mut()
            .expect("Local snapshot should have committed metadata")
            .context
            .workdir = "/different".to_string();
        write_posix_record(tempdir.path(), "local", &private_record);

        let resolved = manager
            .resolve_runnable(canonical)
            .await
            .expect("canonical metadata should resolve the committed closure");
        assert_eq!(resolved.record().id, snapshot_id);

        let promoted = manager
            .promote(&snapshot_id.to_string())
            .await
            .expect("promotion should use canonical metadata and physical artifacts")
            .expect("Local snapshot should be promoted");
        assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
        let canonical = primary
            .get(&snapshot_id.to_string())
            .await
            .expect("canonical lookup should work")
            .expect("canonical Local record should remain visible");
        assert_eq!(canonical.snapshot_type, SnapshotType::Distributed);
    }

    #[tokio::test]
    async fn missing_local_commit_marker_blocks_resolve_and_promote() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        let marker = tempdir
            .path()
            .join("local/snapshots")
            .join(snapshot_id.to_string())
            .join("commit");
        assert!(
            marker.exists(),
            "local publish should create a commit marker"
        );
        std::fs::remove_file(&marker).expect("commit marker should be removable");

        let resolve_error = manager
            .resolve_runnable(record)
            .await
            .expect_err("Local resolve must reject a missing commit marker");
        assert!(resolve_error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
        assert!(matches!(
            manager
                .promote(&snapshot_id.to_string())
                .await
                .expect_err("Local promotion must reject a missing commit marker"),
            RepositoryError::Unavailable { .. }
        ));

        let canonical = primary
            .get(&snapshot_id.to_string())
            .await
            .expect("canonical lookup should work")
            .expect("canonical Local record should remain visible");
        assert_eq!(canonical.snapshot_type, SnapshotType::Local);
        assert!(canonical.is_ready());
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("raw local lookup should work")
            .is_some());
        assert!(local
            .get_committed_record(&snapshot_id)
            .await
            .expect("committed local lookup should work")
            .is_none());
    }

    #[tokio::test]
    async fn reconciliation_purges_a_fenced_local_copy_after_restart() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();

        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut local_record = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("local record should exist");
        local_record.lifecycle = SnapshotLifecycle::Deleting;
        write_posix_record(tempdir.path(), "local", &local_record);

        let mut canonical = primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .expect("canonical record should exist");
        canonical.lifecycle = SnapshotLifecycle::Deleting;
        write_posix_record(tempdir.path(), "primary", &canonical);

        // Construct fresh repositories to model a process restart after the
        // local delete fence but before its physical cleanup completed.
        let (restarted, _, restarted_local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        restarted
            .reconcile_local_artifacts()
            .await
            .expect("restart reconciliation should purge the fenced copy");

        assert!(restarted_local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup after reconcile should work")
            .is_none());
        assert!(!tempdir
            .path()
            .join("local/snapshots")
            .join(snapshot_id.to_string())
            .exists());
    }

    #[tokio::test]
    async fn reconciliation_keeps_local_copy_when_distributed_marker_is_missing() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let manifest = mock_manifest(&tempdir.path().join("capture"));
        local
            .publish(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                manifest.clone(),
            )
            .await
            .expect("local recovery copy should publish");
        primary
            .publish(
                captured_metadata(
                    snapshot_id.clone(),
                    Some("missing-distributed-marker"),
                    SnapshotType::Distributed,
                ),
                manifest,
            )
            .await
            .expect("canonical Distributed snapshot should publish");
        let marker = tempdir
            .path()
            .join("primary/snapshots")
            .join(snapshot_id.to_string())
            .join("commit");
        std::fs::remove_file(&marker).expect("canonical commit marker should be removable");

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should remain conservative");

        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_some());
        assert!(primary
            .get_committed_record(&snapshot_id)
            .await
            .expect("canonical committed lookup should work")
            .is_none());
    }

    #[tokio::test]
    async fn local_delete_fails_closed_and_preserves_both_records() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let error = manager
            .delete(snapshot_id.to_string())
            .await
            .expect_err("Local delete should fail closed");
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unsupported { .. })
        )));
        assert!(primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .is_some());
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_some());
    }

    #[tokio::test]
    async fn foreign_owner_cannot_resolve_or_promote_local_snapshot() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (owner_manager, _, _) = test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = owner_manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        let foreign_manager = SnapshotManager {
            node_id: "node-b".to_string(),
            ..owner_manager.clone()
        };

        let resolve_error = foreign_manager
            .resolve_runnable(record)
            .await
            .expect_err("foreign node must not resolve Local artifacts");
        assert!(resolve_error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
        assert!(matches!(
            foreign_manager
                .promote(&snapshot_id.to_string())
                .await
                .expect_err("foreign node must not promote Local snapshot"),
            RepositoryError::Unavailable { .. }
        ));
    }

    #[tokio::test]
    async fn distributed_resolution_does_not_fallback_to_local_artifacts() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, _, local) = test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let promoted = manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    Some("no-fallback"),
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("same-ID promotion should work");
        local
            .publish(
                captured_metadata(snapshot_id.clone(), None, SnapshotType::Local),
                mock_manifest(&tempdir.path().join("local-mirror")),
            )
            .await
            .expect("test local mirror should publish");
        std::fs::remove_file(
            tempdir
                .path()
                .join("primary/snapshots")
                .join(snapshot_id.to_string())
                .join(SNAPSHOT_ARTIFACT_LAYOUT.vm_state),
        )
        .expect("primary VM state should exist");

        let error = manager
            .resolve_runnable(promoted)
            .await
            .expect_err("missing Distributed artifact must not use local mirror");
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
    }
}
