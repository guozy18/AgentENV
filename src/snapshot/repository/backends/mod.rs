pub(crate) mod common;
pub(crate) mod oss;
pub(crate) mod posixfs;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cfg::{ConfigManager, SnapshotImageStoragePolicy, SnapshotRepositoryBackendKind};
use crate::image::cache::local_image_services_from_app_config;
use crate::p2p::P2pTransport;
use crate::snapshot::artifact_cache::LocalArtifactCache;
use crate::snapshot::repository::interfaces::{SnapshotRepository, SnapshotRuntimeResolver};
pub use oss::OssBackend;
pub use posixfs::{PosixFsBackend, PosixFsBackendConfig};

/// Builds the configured snapshot repository backend and its matching runtime resolver from the global configuration.
pub fn build_snapshot_backend(
    p2p_transport: Option<Arc<dyn P2pTransport>>,
) -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    let config = ConfigManager::global_config();
    let shared_cache_root = shared_runtime_cache_root();
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    match config.snapshot.repository_backend {
        SnapshotRepositoryBackendKind::PosixFs => {
            let root = config
                .backend
                .posix_fs
                .as_ref()
                .context("backend.posix_fs config is required when repository_backend = posix_fs")?
                .snapshot_store
                .join("repository");
            let cache = LocalArtifactCache::new(shared_cache_root.clone(), None)?;
            Ok(PosixFsBackend::from_parts(
                PosixFsBackendConfig {
                    root,
                    cache_root: Some(shared_cache_root.clone()),
                    runtime_cache_root: Some(shared_cache_root.join("runtime")),
                },
                overlaybd_layers,
                cache,
            )
            .into_parts())
        }
        SnapshotRepositoryBackendKind::Oss => {
            let oss_config = config
                .backend
                .oss
                .as_ref()
                .context("backend.oss config is required when repository_backend = oss")?;
            let snapshot_image_storage = if config.snapshot.image_publish.enabled {
                SnapshotImageStoragePolicy::SourceRegistry
            } else {
                SnapshotImageStoragePolicy::ObjectStorage
            };
            let cache =
                LocalArtifactCache::new(shared_cache_root.clone(), oss_config.cache_max_size_gb)?;
            Ok(OssBackend::from_parts(
                oss_config,
                snapshot_image_storage,
                cache,
                shared_cache_root.join("runtime"),
                overlaybd_layers,
                p2p_transport,
            )?
            .into_parts())
        }
    }
}

/// Builds the durable node-local POSIX repository used for sandbox recovery
/// points. It is deliberately independent of the configured publication
/// backend, so an OSS outage cannot prevent a local snapshot from committing.
pub(crate) fn build_local_snapshot_backend() -> Result<(
    Arc<dyn SnapshotRepository>,
    Arc<dyn SnapshotRuntimeResolver>,
)> {
    let config = ConfigManager::global_config();
    let overlaybd_layers = local_image_services_from_app_config(config).overlaybd_layers;
    let root = config.home_path.join("snapshot-local-store");
    let cache_root = root.join("cache");
    let cache = LocalArtifactCache::new(cache_root.clone(), None)?;
    Ok(PosixFsBackend::from_parts(
        PosixFsBackendConfig {
            root: root.clone(),
            cache_root: Some(cache_root),
            runtime_cache_root: Some(root.join("runtime")),
        },
        overlaybd_layers,
        cache,
    )
    .into_parts())
}

pub(crate) fn shared_runtime_cache_root() -> PathBuf {
    ConfigManager::global_config()
        .snapshot
        .local_cache_path
        .clone()
}
