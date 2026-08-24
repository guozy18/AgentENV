use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use dashmap::DashMap;
use futures::{stream, StreamExt};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};

use super::p2p::SnapshotP2pArtifact;
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

#[derive(Default)]
struct SnapshotLifecycleLocks {
    locks: DashMap<SnapshotId, Arc<Mutex<()>>>,
}

impl SnapshotLifecycleLocks {
    async fn acquire(self: &Arc<Self>, snapshot_id: &SnapshotId) -> SnapshotLifecycleLock {
        let lock = self
            .locks
            .entry(snapshot_id.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = lock.clone().lock_owned().await;
        SnapshotLifecycleLock {
            locks: Arc::clone(self),
            snapshot_id: snapshot_id.clone(),
            lock,
            guard: Some(guard),
        }
    }
}

struct SnapshotLifecycleLock {
    locks: Arc<SnapshotLifecycleLocks>,
    snapshot_id: SnapshotId,
    lock: Arc<Mutex<()>>,
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for SnapshotLifecycleLock {
    fn drop(&mut self) {
        drop(self.guard.take());
        self.locks
            .locks
            .remove_if(&self.snapshot_id, |_, existing| {
                Arc::ptr_eq(existing, &self.lock) && Arc::strong_count(existing) <= 2
            });
    }
}

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
    lifecycle_locks: Arc<SnapshotLifecycleLocks>,
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
            lifecycle_locks: Arc::new(SnapshotLifecycleLocks::default()),
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
            lifecycle_locks: Arc::new(SnapshotLifecycleLocks::default()),
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
        let record = {
            // Keep the repository transition and a same-process delete (or
            // another publish) in one lifecycle order. The repository still
            // supplies the cross-process fence; this lock only closes the
            // in-process gap and avoids an avoidable retry race.
            let _guard = self.lifecycle_locks.acquire(&metadata.id).await;
            self.primary
                .repository
                .publish(metadata.clone(), manifest.clone())
                .await?
        };
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
        let canonical_alias = metadata.alias.take();
        self.primary
            .repository
            .prepare_local_capture(&mut manifest)
            .await?;
        let snapshot_id = metadata.id.clone();
        let canonical_local = {
            let _guard = self.lifecycle_locks.acquire(&snapshot_id).await;
            let mut local_record = local.repository.publish(metadata, manifest).await?;

            // The configured primary repository is the only public metadata
            // authority, even when the immutable bytes remain node-local.
            local_record.alias = canonical_alias;
            self.primary.repository.commit_record(local_record).await?
        };
        match requested_type {
            SnapshotType::Local => Ok(canonical_local),
            SnapshotType::Distributed => self.promote_record(canonical_local).await,
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
        self.promote_record(record).await.map(Some)
    }

    async fn promote_record(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let snapshot_id = record.id.clone();
        let _guard = self.lifecycle_locks.acquire(&snapshot_id).await;

        // Re-read under the per-snapshot lock so a retry observes a promotion
        // completed by an earlier request instead of replaying its rollback.
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
        let metadata = Self::distributed_metadata(&record)?;
        let promoted = self
            .primary
            .repository
            .publish(metadata, runnable.manifest().clone())
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
            })?;

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
        let local_record = local
            .repository
            .get_committed_record(&snapshot_id)
            .await
            .map_err(|error| RepositoryError::Unavailable {
                reason: format!("load node-local artifacts for snapshot '{snapshot_id}': {error}"),
            })?
            .ok_or_else(|| RepositoryError::Unavailable {
                reason: format!(
                    "node '{}' no longer has artifacts for Local snapshot '{snapshot_id}'",
                    self.node_id
                ),
            })?;
        if !canonical.same_local_closure(&local_record) {
            return Err(RepositoryError::Unavailable {
                reason: format!(
                    "node-local closure for snapshot '{snapshot_id}' does not match canonical metadata"
                ),
            });
        }
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
            alias: record.alias.clone(),
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
            let _guard = self.lifecycle_locks.acquire(&snapshot_id).await;
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
                Some(record) if record.lifecycle == SnapshotLifecycle::Preparing => {
                    if let Some(mut local_record) = local_recovery_record.filter(|local| {
                        record.owner_node_id.as_deref() == Some(self.node_id.as_str())
                            && record.same_local_closure(local)
                    }) {
                        // Make the node-local recovery point visible before
                        // publishing canonical Ready metadata. If the second
                        // commit fails, the next reconciliation can safely
                        // retry from this local Ready record.
                        if local_record.lifecycle == SnapshotLifecycle::Preparing {
                            local_record.lifecycle = SnapshotLifecycle::Ready;
                            local_record = local.repository.commit_record(local_record).await?;
                        }
                        local_record.alias = record.alias;
                        self.primary.repository.commit_record(local_record).await?;
                    } else {
                        warn!(
                            %snapshot_id,
                            "kept local snapshot closure that cannot recover canonical Preparing metadata"
                        );
                    }
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
                        && local_recovery_record
                            .as_ref()
                            .is_some_and(|local| record.same_local_closure(local)) =>
                {
                    if local_record.lifecycle == SnapshotLifecycle::Preparing {
                        let mut local_record = local_recovery_record
                            .expect("matching recovery record should be present");
                        local_record.lifecycle = SnapshotLifecycle::Ready;
                        local.repository.commit_record(local_record).await?;
                    }
                }
                Some(record) => {
                    warn!(
                        %snapshot_id,
                        canonical_type = ?record.snapshot_type,
                        canonical_owner = ?record.owner_node_id,
                        "kept local snapshot closure with mismatched canonical metadata"
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

        // Fixed VM/manifest keys are safe only for the legacy layout. New OSS
        // publishes use an immutable attempt namespace per catalog commit;
        // publishing those bytes under the old snapshot-id-only P2P key would
        // let concurrent attempts overwrite one another and hydrate the wrong
        // closure on a later resolve. Content-addressed layers remain safe to
        // advertise regardless of the attempt namespace.
        let mut artifacts = Vec::new();
        if committed.artifact_namespace.is_none() {
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
        let _guard = self.lifecycle_locks.acquire(&snapshot_id).await;
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
            .with_context(|| format!("delete {delete_resource} '{snapshot_id}' through repository"))
            .map(|_| ())
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
                let _guard = self.lifecycle_locks.acquire(&snapshot_id).await;
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
    use crate::overlaybd::layer_key_from_digest;
    use crate::p2p::mock::MockTransport;
    use crate::snapshot::mock::write_mock_built_artifacts;
    use crate::snapshot::p2p::fixed_artifact_key;
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
            lifecycle_locks: Arc::new(SnapshotLifecycleLocks::default()),
            p2p_transport: None,
        };
        (manager, primary_repository, local_repository)
    }

    fn captured_metadata(
        id: SnapshotId,
        alias: &str,
        snapshot_type: SnapshotType,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            snapshot_type,
            owner_node_id: (snapshot_type == SnapshotType::Local).then(|| "node-a".to_string()),
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
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
    async fn lifecycle_lock_waiters_share_one_lock_and_cleanup_after_release() {
        let locks = Arc::new(SnapshotLifecycleLocks::default());
        let snapshot_id = SnapshotId::generate();
        let first = locks.acquire(&snapshot_id).await;
        let expected_lock = Arc::as_ptr(&first.lock) as usize;
        let (acquired_tx, mut acquired_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let mut waiters = Vec::new();

        for _ in 0..2 {
            let locks = Arc::clone(&locks);
            let snapshot_id = snapshot_id.clone();
            let acquired_tx = acquired_tx.clone();
            let release = Arc::clone(&release);
            waiters.push(tokio::spawn(async move {
                let guard = locks.acquire(&snapshot_id).await;
                acquired_tx
                    .send(Arc::as_ptr(&guard.lock) as usize)
                    .expect("test receiver should remain open");
                release
                    .acquire()
                    .await
                    .expect("test semaphore should remain open")
                    .forget();
                drop(guard);
            }));
        }
        drop(acquired_tx);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let strong_count = locks
                    .locks
                    .get(&snapshot_id)
                    .map(|entry| Arc::strong_count(entry.value()))
                    .unwrap_or_default();
                if strong_count >= 7 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both waiters should queue on the same lock");

        drop(first);
        let first_waiter_lock = tokio::time::timeout(Duration::from_secs(1), acquired_rx.recv())
            .await
            .expect("first waiter should acquire the lock")
            .expect("first waiter should report its lock");
        assert_eq!(first_waiter_lock, expected_lock);
        assert_eq!(locks.locks.len(), 1);

        release.add_permits(1);
        let second_waiter_lock = tokio::time::timeout(Duration::from_secs(1), acquired_rx.recv())
            .await
            .expect("second waiter should acquire the lock")
            .expect("second waiter should report its lock");
        assert_eq!(second_waiter_lock, expected_lock);
        assert_eq!(locks.locks.len(), 1);

        release.add_permits(1);
        for waiter in waiters {
            waiter.await.expect("waiter task should finish");
        }
        assert!(locks.locks.is_empty());
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
    async fn publish_advertises_snapshot_artifacts_to_p2p_after_commit() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let backend = PosixFsBackend::new(PosixFsBackendConfig {
            root: tempdir.path().join("repository"),
            cache_root: Some(tempdir.path().join("runtime-cache")),
            runtime_cache_root: Some(tempdir.path().join("runtime-cache").join("runtime")),
        })
        .expect("posix backend");
        let (repository, runtime_resolver) = backend.into_parts();
        let p2p = Arc::new(MockTransport::default());
        let manager = SnapshotManager::from_parts(repository, runtime_resolver, Some(p2p.clone()));

        let workspace = TempDir::new().expect("tempdir should exist");
        let (rootfs_lower, _, manifest) =
            write_mock_built_artifacts(workspace.path()).expect("mock artifacts should write");
        let snapshot_id = SnapshotId::generate();
        let metadata = SnapshotPublishMetadata {
            id: snapshot_id.clone(),
            ..SnapshotPublishMetadata::mock()
        };

        manager
            .publish(metadata, manifest)
            .await
            .expect("publish should commit");

        let vm_state_key = fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
        let manifest_key =
            fixed_artifact_key(&snapshot_id, SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest);
        let rootfs_layer_digest = crate::digest::FileDigest::describe(&rootfs_lower)
            .await
            .expect("describe rootfs lower");
        let rootfs_layer_key = layer_key_from_digest(&rootfs_layer_digest.sha256);

        assert!(p2p
            .lookup(&vm_state_key)
            .await
            .expect("lookup vm state")
            .is_some());
        assert!(p2p
            .lookup(&manifest_key)
            .await
            .expect("lookup manifest")
            .is_some());
        assert!(p2p
            .lookup(&rootfs_layer_key)
            .await
            .expect("lookup rootfs layer")
            .is_some());
    }

    #[tokio::test]
    async fn local_capture_keeps_public_alias_out_of_the_physical_store() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let stale_id = SnapshotId::generate();
        local
            .publish(
                captured_metadata(stale_id.clone(), "local-one", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("stale-capture")),
            )
            .await
            .expect("stale local recovery copy should publish");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "local-one", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        assert_eq!(record.snapshot_type, SnapshotType::Local);
        assert_eq!(record.owner_node_id.as_deref(), Some("node-a"));
        assert_eq!(
            primary
                .get("local-one")
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
        assert_eq!(
            local
                .resolve_alias("local-one")
                .await
                .expect("stale local alias should remain readable"),
            Some(stale_id)
        );
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
    async fn canonical_commit_failure_never_falls_back_to_local_catalog() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        std::fs::write(tempdir.path().join("broken-primary"), b"not a directory")
            .expect("broken primary sentinel should write");
        let (manager, _, local) =
            test_manager_with_local(tempdir.path(), "broken-primary", "node-a");
        let snapshot_id = SnapshotId::generate();

        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "orphan", SnapshotType::Local),
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
            .get("orphan")
            .await
            .expect_err("public lookup must not substitute the local catalog");
    }

    #[tokio::test]
    async fn distributed_capture_promotes_the_same_id_and_removes_local_recovery_copy() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let promoted = manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    "distributed-one",
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
    async fn idempotent_promotion_removes_a_stale_local_recovery_copy() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let manifest = mock_manifest(&tempdir.path().join("capture"));
        let local_record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "retry-cleanup", SnapshotType::Local),
                manifest.clone(),
            )
            .await
            .expect("Local snapshot should publish");
        primary
            .publish(
                SnapshotManager::distributed_metadata(&local_record)
                    .expect("Distributed metadata should derive from Local record"),
                manifest,
            )
            .await
            .expect("canonical Distributed publication should succeed");

        let promoted = manager
            .promote("retry-cleanup")
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
                captured_metadata(snapshot_id.clone(), "retryable", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        std::fs::write(tempdir.path().join("primary/managed-layers"), b"blocked")
            .expect("promotion blocker should write");

        manager
            .promote("retryable")
            .await
            .expect_err("artifact publication should fail");
        let canonical = primary
            .get("retryable")
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
    async fn mismatched_local_closure_cannot_resolve_or_promote() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, _) = test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let canonical = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "mismatched", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("canonical-capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut mismatched_local = canonical.clone();
        mismatched_local
            .committed
            .as_mut()
            .expect("Local snapshot should have committed metadata")
            .context
            .workdir = "/different".to_string();
        write_posix_record(tempdir.path(), "local", &mismatched_local);

        let resolve_error = manager
            .resolve_runnable(canonical)
            .await
            .expect_err("mismatched Local closure must not resolve");
        assert!(resolve_error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
        assert!(matches!(
            manager
                .promote("mismatched")
                .await
                .expect_err("mismatched Local closure must not promote"),
            RepositoryError::Unavailable { .. }
        ));
        let canonical = primary
            .get("mismatched")
            .await
            .expect("canonical lookup should work")
            .expect("canonical Local record should remain visible");
        assert_eq!(canonical.snapshot_type, SnapshotType::Local);
    }

    #[tokio::test]
    async fn missing_local_commit_marker_blocks_resolve_and_promote() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "missing-marker", SnapshotType::Local),
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
                .promote("missing-marker")
                .await
                .expect_err("Local promotion must reject a missing commit marker"),
            RepositoryError::Unavailable { .. }
        ));

        let canonical = primary
            .get("missing-marker")
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
    async fn missing_primary_commit_marker_blocks_stale_local_resolve_and_promote() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    "missing-primary-marker",
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        let marker = tempdir
            .path()
            .join("primary/snapshots")
            .join(snapshot_id.to_string())
            .join("commit");
        assert!(
            marker.exists(),
            "primary publish should create a commit marker"
        );
        std::fs::remove_file(&marker).expect("primary commit marker should be removable");

        let resolve_error = manager
            .resolve_runnable(record.clone())
            .await
            .expect_err("stale Local resolve must reject a missing primary marker");
        assert!(resolve_error.chain().any(|cause| matches!(
            cause.downcast_ref::<RepositoryError>(),
            Some(RepositoryError::Unavailable { .. })
        )));
        assert!(matches!(
            manager
                .promote("missing-primary-marker")
                .await
                .expect_err("stale Local promotion must reject a missing primary marker"),
            RepositoryError::Unavailable { .. }
        ));

        let canonical = primary
            .get_record(&snapshot_id)
            .await
            .expect("raw canonical lookup should work")
            .expect("canonical identity should remain durable");
        assert!(canonical.is_ready());
        assert!(primary
            .get_committed_record(&snapshot_id)
            .await
            .expect("committed canonical lookup should work")
            .is_none());
        assert!(local
            .get_committed_record(&snapshot_id)
            .await
            .expect("local committed lookup should work")
            .is_some());
    }

    #[tokio::test]
    async fn reconciliation_applies_one_grace_period_to_local_orphans() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, _, local) = test_manager_with_local(tempdir.path(), "primary", "node-a");
        let cases = [
            ("recent-ready", SnapshotLifecycle::Ready, false),
            ("expired-ready", SnapshotLifecycle::Ready, true),
            ("recent-preparing", SnapshotLifecycle::Preparing, false),
            ("expired-preparing", SnapshotLifecycle::Preparing, true),
        ];
        let mut records = Vec::new();
        for (alias, lifecycle, expired) in cases {
            let id = SnapshotId::generate();
            let mut record = local
                .publish(
                    captured_metadata(id.clone(), alias, SnapshotType::Local),
                    mock_manifest(&tempdir.path().join(alias)),
                )
                .await
                .expect("local orphan should publish");
            record.lifecycle = lifecycle;
            if expired {
                record.updated_at_unix_ms = 0;
            }
            write_posix_record(tempdir.path(), "local", &record);
            records.push((id, alias, expired));
        }

        let gc_candidate = tempdir
            .path()
            .join("local/managed-layers")
            .join(format!("sha256_{}.overlaybd.commit", "f".repeat(64)));
        std::fs::write(&gc_candidate, b"unreferenced layer").expect("GC candidate should write");

        manager
            .reconcile_local_artifacts()
            .await
            .expect("healthy primary reconciliation should work");

        for (id, alias, expired) in records {
            assert_eq!(
                local
                    .get_record(&id)
                    .await
                    .expect("local orphan lookup should work")
                    .is_none(),
                expired,
                "unexpected reconciliation result for {alias}"
            );
        }
        assert!(!gc_candidate.exists());
    }

    #[tokio::test]
    async fn reconciliation_only_recovers_preparing_metadata_on_its_owner() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (owner_manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        owner_manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "recoverable", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        let mut preparing = primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .expect("canonical record should exist");
        preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "primary", &preparing);

        let foreign_manager = SnapshotManager {
            node_id: "node-b".to_string(),
            lifecycle_locks: Arc::new(SnapshotLifecycleLocks::default()),
            ..owner_manager.clone()
        };
        foreign_manager
            .reconcile_local_artifacts()
            .await
            .expect("foreign reconciliation should remain conservative");
        assert_eq!(
            primary
                .get_record(&snapshot_id)
                .await
                .expect("canonical exact lookup should work")
                .expect("Preparing record should remain")
                .lifecycle,
            SnapshotLifecycle::Preparing
        );
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_some());

        owner_manager
            .reconcile_local_artifacts()
            .await
            .expect("owner reconciliation should recover metadata");
        assert_eq!(
            primary
                .get("recoverable")
                .await
                .expect("public canonical lookup should work")
                .expect("recovered record should be public")
                .lifecycle,
            SnapshotLifecycle::Ready
        );
    }

    #[tokio::test]
    async fn reconciliation_recovers_committed_preparing_local_and_canonical_records() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    "recover-both-preparing",
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut local_preparing = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("local record should exist");
        local_preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "local", &local_preparing);

        let mut canonical_preparing = primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .expect("canonical record should exist");
        canonical_preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "primary", &canonical_preparing);

        assert!(local
            .get_recovery_record(&snapshot_id)
            .await
            .expect("local recovery lookup should work")
            .is_some());
        assert!(local
            .get_committed_record(&snapshot_id)
            .await
            .expect("local public committed lookup should work")
            .is_none());

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should recover both Preparing records");

        assert_eq!(
            primary
                .get_record(&snapshot_id)
                .await
                .expect("canonical exact lookup after recovery should work")
                .expect("canonical record should remain")
                .lifecycle,
            SnapshotLifecycle::Ready
        );
        assert_eq!(
            local
                .get_record(&snapshot_id)
                .await
                .expect("local exact lookup after recovery should work")
                .expect("local record should remain")
                .lifecycle,
            SnapshotLifecycle::Ready
        );
    }

    #[tokio::test]
    async fn reconciliation_does_not_resurrect_a_deleting_local_record() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "do-not-resurrect", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut local_deleting = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("local record should exist");
        local_deleting.lifecycle = SnapshotLifecycle::Deleting;
        write_posix_record(tempdir.path(), "local", &local_deleting);

        let mut canonical_preparing = primary
            .get_record(&snapshot_id)
            .await
            .expect("canonical exact lookup should work")
            .expect("canonical record should exist");
        canonical_preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "primary", &canonical_preparing);

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should remain conservative");

        assert_eq!(
            primary
                .get_record(&snapshot_id)
                .await
                .expect("canonical exact lookup after reconciliation should work")
                .expect("canonical record should remain")
                .lifecycle,
            SnapshotLifecycle::Preparing
        );
        assert_eq!(
            local
                .get_record(&snapshot_id)
                .await
                .expect("local exact lookup after reconciliation should work")
                .expect("local record should remain")
                .lifecycle,
            SnapshotLifecycle::Deleting
        );
        assert!(local
            .get_recovery_record(&snapshot_id)
            .await
            .expect("local recovery lookup should work")
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
                captured_metadata(snapshot_id.clone(), "fenced-local", SnapshotType::Local),
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
    async fn reconciliation_removes_distributed_local_recovery_copy() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let manifest = mock_manifest(&tempdir.path().join("capture"));
        local
            .publish(
                captured_metadata(
                    snapshot_id.clone(),
                    "reconciled-distributed",
                    SnapshotType::Local,
                ),
                manifest.clone(),
            )
            .await
            .expect("stale local recovery copy should publish");
        primary
            .publish(
                captured_metadata(
                    snapshot_id.clone(),
                    "reconciled-distributed",
                    SnapshotType::Distributed,
                ),
                manifest,
            )
            .await
            .expect("canonical Distributed snapshot should publish");
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_some());

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should work");
        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .is_none());
        assert_eq!(
            primary
                .get("reconciled-distributed")
                .await
                .expect("canonical lookup should work")
                .expect("Distributed record should remain")
                .snapshot_type,
            SnapshotType::Distributed
        );
    }

    #[tokio::test]
    async fn reconciliation_repairs_preparing_local_copy_for_ready_canonical() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    "repair-local-preparing",
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut local_preparing = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("local record should exist");
        local_preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "local", &local_preparing);

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should repair the local lifecycle");

        assert_eq!(
            local
                .get_record(&snapshot_id)
                .await
                .expect("local exact lookup after reconciliation should work")
                .expect("local record should remain")
                .lifecycle,
            SnapshotLifecycle::Ready
        );
        assert!(local
            .get_committed_record(&snapshot_id)
            .await
            .expect("local committed lookup after reconciliation should work")
            .is_some());
        assert!(primary
            .get_committed_record(&snapshot_id)
            .await
            .expect("canonical committed lookup should work")
            .is_some());
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
                captured_metadata(
                    snapshot_id.clone(),
                    "missing-distributed-marker",
                    SnapshotType::Local,
                ),
                manifest.clone(),
            )
            .await
            .expect("local recovery copy should publish");
        primary
            .publish(
                captured_metadata(
                    snapshot_id.clone(),
                    "missing-distributed-marker",
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
    async fn reconciliation_purges_preparing_local_copy_for_a_canonical_tombstone() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    "tombstoned-preparing",
                    SnapshotType::Local,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let mut local_preparing = local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup should work")
            .expect("local record should exist");
        local_preparing.lifecycle = SnapshotLifecycle::Preparing;
        write_posix_record(tempdir.path(), "local", &local_preparing);

        primary
            .delete_by_id(&snapshot_id)
            .await
            .expect("canonical delete should leave an identity tombstone");
        assert_eq!(
            primary
                .get_for_delete(&snapshot_id.to_string())
                .await
                .expect("canonical lifecycle lookup should work")
                .expect("canonical tombstone should remain")
                .lifecycle,
            SnapshotLifecycle::Deleting
        );
        assert!(manager
            .promote(&snapshot_id.to_string())
            .await
            .expect("promotion of a terminal tombstone should be idempotent")
            .is_none());

        manager
            .reconcile_local_artifacts()
            .await
            .expect("reconciliation should purge the fenced local closure");

        assert!(local
            .get_record(&snapshot_id)
            .await
            .expect("local exact lookup after reconciliation should work")
            .is_none());
        assert!(!tempdir
            .path()
            .join("local/snapshots")
            .join(snapshot_id.to_string())
            .exists());
    }

    #[tokio::test]
    async fn local_delete_fails_closed_and_preserves_both_records() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, local) =
            test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        manager
            .publish_captured_manifest(
                captured_metadata(snapshot_id.clone(), "undeletable", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");

        let error = manager
            .delete("undeletable")
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
                captured_metadata(snapshot_id, "owner-bound", SnapshotType::Local),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Local snapshot should publish");
        let foreign_manager = SnapshotManager {
            node_id: "node-b".to_string(),
            lifecycle_locks: Arc::new(SnapshotLifecycleLocks::default()),
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
                .promote("owner-bound")
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
                    "no-fallback",
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("same-ID promotion should work");
        local
            .publish(
                captured_metadata(snapshot_id.clone(), "no-fallback", SnapshotType::Local),
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
