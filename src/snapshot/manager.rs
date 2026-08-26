use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Context;
use futures::{stream, StreamExt};
use tracing::warn;

use super::p2p::SnapshotP2pArtifact;
use super::types::{now_unix_ms, SNAPSHOT_ARTIFACT_LAYOUT};
use crate::p2p::P2pTransport;
use crate::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerSnapshotManifest,
};
use crate::snapshot::repository::backends::{
    build_local_snapshot_backend, build_snapshot_backend, PosixFsArtifactStore,
};
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
use crate::snapshot::repository::{RepositoryError, SnapshotListFilter};
use crate::snapshot::{
    OverlaybdLayerRef, RunnableSnapshot, SnapshotAlias, SnapshotId, SnapshotPublishMetadata,
    SnapshotPublishSource, SnapshotRecord, SnapshotSource, SnapshotType,
};

/// Concurrency limit for publishing snapshot artifacts to P2P after commit.
const SNAPSHOT_P2P_PUBLISH_CONCURRENCY: usize = 8;
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
/// Coordinates committed snapshot lifecycle operations over repository-backed state.
///
/// Distributed snapshot reachability is owned by the [`SnapshotRepository`]
/// (PosixFS `managed-layers/`, OSS object storage, or the source registry).
/// Local snapshots are Pod-scoped physical closures and are only usable while
/// their owner Pod remains available. The node-local overlaybd layer cache
/// (`image-cache/commits/`) is reclaimable, so this manager records no local
/// image ref pins.
pub struct SnapshotManager {
    repository: Arc<dyn SnapshotRepository>,
    runtime_resolver: Arc<dyn SnapshotRuntimeResolver>,
    local_artifacts: Option<Arc<PosixFsArtifactStore>>,
    local_runtime_resolver: Option<Arc<dyn SnapshotRuntimeResolver>>,
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
        let (local_artifacts, local_runtime_resolver) = build_local_snapshot_backend()?;
        Ok(Self {
            repository,
            runtime_resolver,
            local_artifacts: Some(local_artifacts),
            local_runtime_resolver: Some(local_runtime_resolver),
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
            repository,
            runtime_resolver,
            local_artifacts: None,
            local_runtime_resolver: None,
            node_id: String::new(),
            p2p_transport,
        }
    }

    pub async fn create(
        &self,
        record: SnapshotRecord,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.create(record).await
    }

    #[tracing::instrument(skip(self, metadata, manifest), fields(snapshot_id = %metadata.id))]
    pub async fn publish(
        &self,
        metadata: SnapshotPublishMetadata,
        manifest: FirecrackerSnapshotManifest,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        let record = self.repository.publish(metadata, manifest.clone()).await?;
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
        if metadata.snapshot_type == SnapshotType::Distributed {
            // Distributed capture follows the original repository path: the
            // configured primary backend owns artifact publication and the
            // canonical Ready record. A node-local artifact copy is not part
            // of the Distributed create contract.
            return self.publish(metadata, manifest).await;
        }
        let Some(local_artifacts) = self.local_artifacts.as_ref() else {
            return Err(RepositoryError::Unsupported {
                feature: "node-local snapshot store is not configured".to_string(),
            });
        };

        metadata.owner_node_id = Some(self.node_id.clone());
        metadata
            .validate()
            .map_err(|reason| RepositoryError::InvalidRequest { reason })?;
        self.repository.prepare_local_capture(&mut manifest).await?;
        let committed = local_artifacts
            .commit_local(metadata.clone(), manifest)
            .await?;
        // The configured primary repository is the only public metadata
        // authority, even when the immutable bytes remain node-local.
        self.repository
            .commit_record(SnapshotRecord::new_committed(
                &metadata,
                committed,
                now_unix_ms(),
            ))
            .await
    }

    /// Promotes one reusable Local snapshot to Distributed without recapture.
    ///
    /// The lookup is always resolved by the canonical repository. A missing or
    /// foreign-owner Local snapshot is never substituted from node-local state.
    pub async fn promote(
        &self,
        id_or_alias: &str,
    ) -> crate::snapshot::RepositoryResult<Option<SnapshotRecord>> {
        let Some(record) = self.repository.get(id_or_alias).await? else {
            return Ok(None);
        };
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
        if record.snapshot_type == SnapshotType::Distributed {
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
        Ok(promoted)
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
        let local_runtime_resolver =
            self.local_runtime_resolver
                .as_ref()
                .ok_or_else(|| RepositoryError::Unavailable {
                    reason: "node-local snapshot store is not configured".to_string(),
                })?;
        // The canonical record owns identity and logical artifact
        // references.  The local resolver owns the physical commit marker,
        // fixed artifacts, managed-layer files, and runtime materialization
        // checks.  Reading a second full SnapshotRecord here only duplicated
        // metadata authority without validating any artifact bytes.
        local_runtime_resolver
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
        self.runtime_resolver
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
        self.repository
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

        let mut artifacts = Vec::new();
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
        self.repository.get(id_or_alias.as_ref()).await
    }

    /// Lists snapshot records that match the given filter.
    pub async fn list(&self, filter: SnapshotListFilter) -> anyhow::Result<Vec<SnapshotRecord>> {
        self.repository
            .list(filter)
            .await
            .context("list committed snapshots through repository")
    }

    /// Deletes a snapshot by id or alias.
    ///
    /// Returns `Ok(())` on success. The operation is idempotent:
    /// if the snapshot does not exist, it is still considered success.
    pub async fn delete(&self, id_or_alias: impl AsRef<str>) -> anyhow::Result<()> {
        self.repository
            .delete(id_or_alias.as_ref())
            .await
            .with_context(|| {
                format!(
                    "delete snapshot '{}' through repository",
                    id_or_alias.as_ref()
                )
            })
    }

    /// Resolves an alias to its committed snapshot id.
    pub async fn resolve_committed_alias(&self, alias: &str) -> anyhow::Result<Option<SnapshotId>> {
        self.repository.resolve_alias(alias).await.with_context(|| {
            format!("resolve committed snapshot alias '{alias}' through repository")
        })
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
                    .repository
                    .get(&snapshot_id.to_string())
                    .await
                    .map_err(anyhow::Error::new)
                    .context(CONTEXT)?
                    .filter(|record| record.committed.is_some())
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

    /// Atomically transitions one template build from waiting to building.
    pub async fn try_start_build(
        &self,
        id: &SnapshotId,
    ) -> crate::snapshot::RepositoryResult<SnapshotRecord> {
        self.repository.try_start_build(id).await
    }

    /// Marks one template build as failed.
    pub async fn mark_build_error(
        &self,
        id: &SnapshotId,
        reason: crate::snapshot::TemplateBuildErrorReason,
    ) -> crate::snapshot::RepositoryResult<()> {
        self.repository.mark_build_error(id, reason).await
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
        std::path::PathBuf,
    ) {
        let (primary_repository, primary_resolver) = test_store(root, primary_name);
        let (_, local_resolver) = test_store(root, "local");
        let local_root = root.join("local");
        let local_artifacts = Arc::new(PosixFsArtifactStore::new(local_root.clone()));
        let manager = SnapshotManager {
            repository: Arc::clone(&primary_repository),
            runtime_resolver: primary_resolver,
            local_artifacts: Some(Arc::clone(&local_artifacts)),
            local_runtime_resolver: Some(local_resolver),
            node_id: node_id.to_string(),
            p2p_transport: None,
        };
        (manager, primary_repository, local_root)
    }

    fn local_commit_marker(root: &Path, id: &SnapshotId) -> std::path::PathBuf {
        root.join("snapshots").join(id.to_string()).join("commit")
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
        assert!(local_commit_marker(&local, &snapshot_id).is_file());
        assert!(!tempdir
            .path()
            .join("primary/snapshots")
            .join(snapshot_id.to_string())
            .exists());
        assert_eq!(
            manager
                .list(SnapshotListFilter::matches_all())
                .await
                .expect("Local snapshot should be listed")
                .len(),
            1
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
    async fn canonical_commit_failure_leaves_local_physical_closure() {
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
        assert!(local_commit_marker(&local, &snapshot_id).is_file());
        manager
            .get(&snapshot_id.to_string())
            .await
            .expect_err("public lookup must not substitute local physical artifacts");
    }

    #[tokio::test]
    async fn distributed_capture_commits_distributed_canonical_metadata_directly() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let (manager, primary, _) = test_manager_with_local(tempdir.path(), "primary", "node-a");
        let snapshot_id = SnapshotId::generate();
        let record = manager
            .publish_captured_manifest(
                captured_metadata(
                    snapshot_id.clone(),
                    Some("distributed-one"),
                    SnapshotType::Distributed,
                ),
                mock_manifest(&tempdir.path().join("capture")),
            )
            .await
            .expect("Distributed capture should publish directly");

        assert_eq!(record.id, snapshot_id);
        assert_eq!(record.snapshot_type, SnapshotType::Distributed);
        assert_eq!(record.owner_node_id, None);
        assert_eq!(
            record.alias.as_ref().map(SnapshotAlias::as_ref),
            Some("distributed-one")
        );
        assert_eq!(
            primary
                .get(&snapshot_id.to_string())
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
        assert_eq!(
            manager
                .resolve_runnable(record)
                .await
                .expect("Distributed artifacts should resolve from primary")
                .record()
                .id,
            snapshot_id
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
        assert!(local_commit_marker(&local, &snapshot_id).is_file());
        assert!(!tempdir
            .path()
            .join("primary/snapshots")
            .join(snapshot_id.to_string())
            .exists());
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
        let marker = local_commit_marker(&local, &snapshot_id);
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
        assert!(!local_commit_marker(&local, &snapshot_id).exists());
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
        PosixFsArtifactStore::new(local.clone())
            .commit_local(
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
