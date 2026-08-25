use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use agentenv::cfg::{ConfigManager, OssBackendConfig};
use agentenv::sandbox::FirecrackerSnapshotManifest;
use agentenv::snapshot::mock::write_mock_built_artifacts;
use agentenv::snapshot::repository::backends::OssBackend;
use agentenv::snapshot::{
    CommittedSnapshot, OverlaybdLayerRef, RepositoryError, SnapshotAlias, SnapshotId,
    SnapshotLifecycle, SnapshotListFilter, SnapshotPublishMetadata, SnapshotPublishSource,
    SnapshotRecord, SnapshotRuntimeVersions, SnapshotSource, SnapshotType,
    SNAPSHOT_ARTIFACT_LAYOUT,
};
use agentenv::types::SandboxResources;
use agentenv_test_support::minio::{MinioFixture, MINIO_PASS, MINIO_USER};
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use overlaybd::backend::local::LocalFile;
use overlaybd::index_file::{CommitArgs, LSMTFile};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn test_runtime_versions() -> SnapshotRuntimeVersions {
    SnapshotRuntimeVersions {
        kernel_version: "kernel".to_string(),
        firecracker_version: "fc".to_string(),
        envd_version: "envd".to_string(),
        tools_drive_version: "0.1.0".to_string(),
    }
}

fn test_publish_metadata(id: SnapshotId, alias: Option<SnapshotAlias>) -> SnapshotPublishMetadata {
    SnapshotPublishMetadata {
        id,
        snapshot_type: SnapshotType::Distributed,
        owner_node_id: None,
        alias,
        source: SnapshotPublishSource::Template,
        context: agentenv::snapshot::CommandContext::default(),
        startup: None,
        resources: SandboxResources::default(),
        runtime_versions: test_runtime_versions(),
        virtualization_mode: ConfigManager::global_config().virtualization_mode,
        image_configs: agentenv::types::ImageConfigs::new(),
        custom_extension_params: None,
    }
}

/// Write a real ZFile-compressed sealed LSMT layer, mirroring the memory
/// snapshot output when `[memory_snapshot].compression_enabled = true`.
async fn write_zfile_memory_lower(path: &Path) -> Result<()> {
    let data = Arc::new(LocalFile::new(path.with_extension("data"))?);
    let index = Arc::new(LocalFile::new(path.with_extension("index"))?);
    let layer = LSMTFile::create(data, Some(index), 2 * 4096, false).await?;
    layer.write_at(0, &[0x5A; 4096]).await?;
    layer.write_at(4096, &[0xA5; 4096]).await?;
    let output = Arc::new(LocalFile::new(path)?);
    let compress_args = CompressArgs::new(CompressOptions::new(
        CompressOptions::LZ4,
        CompressOptions::DEFAULT_BLOCK_SIZE,
        0,
    ));
    let writer = Arc::new(ZFileCompactWriter::new(output, &compress_args).await?);
    layer
        .commit_with_args(CommitArgs::from_writer(writer))
        .await?;
    Ok(())
}

/// Returns the rootfs/memory lower digests, the memory lower path, and the
/// manifest. The memory lower is a real ZFile layer referenced without
/// digest/size in the image config, matching the production publish path for
/// freshly written memory lowers (digest is derived from the physical bytes).
async fn write_built_artifacts(
    root: &Path,
) -> Result<(String, String, PathBuf, FirecrackerSnapshotManifest)> {
    let (rootfs_lower, _, manifest) = write_mock_built_artifacts(root)?;
    let memory_lower = root.join("mem.zfile.commit");
    write_zfile_memory_lower(&memory_lower).await?;
    std::fs::write(
        &manifest.memory.image_config_path,
        format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, memory_lower.display()),
    )?;
    Ok((
        digest_for_file(&rootfs_lower)?,
        digest_for_file(&memory_lower)?,
        memory_lower,
        manifest,
    ))
}

fn digest_for_file(path: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(std::fs::read(path)?);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn ensure_test_config() -> Result<()> {
    static INIT: OnceLock<()> = OnceLock::new();
    static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = INIT_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
    if INIT.get().is_some() {
        return Ok(());
    }

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let config_root = workspace_root
        .join("target")
        .join("snapshot-oss-e2e-config");
    let deps_path = config_root.join("env");
    let local_cache = config_root.join("snapshot-local-cache");
    std::fs::create_dir_all(&config_root)?;
    let config_path = workspace_root.join("config").join("default.toml");
    std::env::set_var("AENV_CONFIG_PATH", &config_path);
    std::env::set_var("AENV_HOME_PATH", &config_root);
    std::env::set_var("AENV_DEPS_PATH", &deps_path);
    std::env::set_var("AENV_SNAPSHOT_LOCAL_CACHE_PATH", &local_cache);

    let manager = ConfigManager::init_global()?;
    let overlaybd_global = manager.config().ublk.overlaybd.global_config_path.clone();
    let overlaybd_dir = overlaybd_global
        .parent()
        .context("overlaybd global config must have parent")?;
    std::fs::create_dir_all(overlaybd_dir)?;
    std::fs::write(&overlaybd_global, "{}")?;

    let _ = INIT.set(());
    Ok(())
}

fn test_oss_config(fixture: &MinioFixture, prefix: &str) -> OssBackendConfig {
    OssBackendConfig {
        endpoint: fixture.endpoint.clone(),
        bucket: fixture.bucket.clone(),
        prefix: Some(prefix.to_string()),
        credential_process: None,
        access_key_id: Some(MINIO_USER.to_string()),
        access_key_secret: Some(MINIO_PASS.to_string()),
        security_token: None,
        region: Some(fixture.region.clone()),
        addressing_style: None,
        cache_max_size_gb: Some(1),
    }
}

fn prefixed_key(prefix: &str, relative: &str) -> String {
    format!("{prefix}/{relative}")
}

fn artifact_key_for_record(prefix: &str, record: &SnapshotRecord, relative: &str) -> String {
    let artifact_path = record
        .committed
        .as_ref()
        .and_then(|committed| committed.legacy_artifact_namespace.as_deref())
        .map(|namespace| format!("artifacts/{}/{namespace}/{relative}", record.id))
        .unwrap_or_else(|| format!("artifacts/{}/{relative}", record.id));
    prefixed_key(prefix, &artifact_path)
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_publish_and_resolve_remote_managed_layers() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/e2e";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let cache_root = workspace.path().join("oss-cache");
    let (repository, resolver) = OssBackend::new(&oss, cache_root)?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (rootfs_digest, memory_digest, memory_lower_path, manifest) =
        write_built_artifacts(&artifacts_root).await?;
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            test_publish_metadata(
                snapshot_id.clone(),
                Some(SnapshotAlias::parse("oss-e2e").expect("alias should parse")),
            ),
            manifest,
        )
        .await?;

    let committed = stored
        .committed
        .as_ref()
        .expect("published snapshot should be committed");
    assert!(matches!(
        committed.rootfs_layers.as_slice(),
        [OverlaybdLayerRef::Managed(_)]
    ));
    assert_eq!(committed.memory_layers.len(), 1);
    let source_zfile_bytes = std::fs::read(&memory_lower_path)?;
    let managed_memory = &committed.memory_layers[0];
    assert_eq!(managed_memory.digest, memory_digest);
    assert_eq!(managed_memory.size, source_zfile_bytes.len() as u64);

    assert!(
        fixture
            .object_exists(&format!("{prefix}/managed-layers/{rootfs_digest}"))
            .await?
    );
    assert!(
        fixture
            .object_exists(&format!("{prefix}/managed-layers/{memory_digest}"))
            .await?
    );
    // The uploaded memory layer object must be byte-identical to the
    // published ZFile: committed digest/size describe the physical bytes.
    let object_bytes = fixture
        .client
        .get_object()
        .bucket(&fixture.bucket)
        .key(format!("{prefix}/managed-layers/{memory_digest}"))
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    assert_eq!(object_bytes.as_ref(), source_zfile_bytes.as_slice());
    assert!(
        fixture
            .object_exists(&artifact_key_for_record(
                prefix,
                &stored,
                SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
            ))
            .await?
    );
    assert!(
        fixture
            .object_exists(&artifact_key_for_record(
                prefix,
                &stored,
                SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
            ))
            .await?
    );

    let runnable = resolver.resolve(Arc::new(stored.clone())).await?;
    assert!(runnable.manifest().vm_state.path.exists());
    assert!(runnable.manifest().memory.image_config_path.exists());
    assert!(runnable.manifest().rootfs.image_config_path.exists());

    let rootfs_config: serde_json::Value = serde_json::from_slice(&std::fs::read(
        runnable.manifest().rootfs.image_config_path.as_path(),
    )?)?;
    assert_eq!(
        rootfs_config["repoBlobUrl"],
        format!("s3://{}/{prefix}/managed-layers", fixture.bucket)
    );
    assert_eq!(rootfs_config["lowers"][0]["digest"], rootfs_digest);
    assert_eq!(rootfs_config["lowers"][0]["file"], "");

    let mem_config: serde_json::Value = serde_json::from_slice(&std::fs::read(
        runnable.manifest().memory.image_config_path.as_path(),
    )?)?;
    assert_eq!(
        mem_config["repoBlobUrl"],
        format!("s3://{}/{prefix}/managed-layers", fixture.bucket)
    );
    assert_eq!(mem_config["lowers"][0]["digest"], memory_digest);
    assert_eq!(mem_config["lowers"][0]["file"], "");
    assert_eq!(
        mem_config["lowers"][0]["size"],
        source_zfile_bytes.len() as u64
    );

    // Upgrade compatibility: old records must keep resolving and deleting
    // their exact persisted attempt prefix without touching fixed siblings.
    drop(runnable);
    let legacy_namespace = "attempt-old";
    for relative in [
        SNAPSHOT_ARTIFACT_LAYOUT.vm_state,
        SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
    ] {
        let fixed_key = prefixed_key(prefix, &format!("artifacts/{snapshot_id}/{relative}"));
        let bytes = fixture
            .client
            .get_object()
            .bucket(&fixture.bucket)
            .key(&fixed_key)
            .send()
            .await?
            .body
            .collect()
            .await?
            .into_bytes();
        fixture
            .client
            .put_object()
            .bucket(&fixture.bucket)
            .key(prefixed_key(
                prefix,
                &format!("artifacts/{snapshot_id}/{legacy_namespace}/{relative}"),
            ))
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await?;
        fixture
            .client
            .delete_object()
            .bucket(&fixture.bucket)
            .key(fixed_key)
            .send()
            .await?;
    }
    let mut legacy = stored;
    legacy
        .committed
        .as_mut()
        .expect("snapshot should remain committed")
        .legacy_artifact_namespace = Some(legacy_namespace.to_string());
    fixture
        .client
        .put_object()
        .bucket(&fixture.bucket)
        .key(prefixed_key(
            prefix,
            &format!("catalog/records/{snapshot_id}.json"),
        ))
        .body(ByteStream::from(serde_json::to_vec(&legacy)?))
        .send()
        .await?;
    let legacy_record = repository
        .get_record(&snapshot_id)
        .await?
        .expect("legacy record should remain readable");
    let legacy_runnable = resolver.resolve(Arc::new(legacy_record)).await?;
    assert!(legacy_runnable.manifest().vm_state.path.exists());
    drop(legacy_runnable);

    let fixed_sibling = prefixed_key(prefix, &format!("artifacts/{snapshot_id}/fixed-sibling"));
    fixture
        .client
        .put_object()
        .bucket(&fixture.bucket)
        .key(&fixed_sibling)
        .body(ByteStream::from_static(b"fixed sibling"))
        .send()
        .await?;
    assert!(repository.delete_by_id(&snapshot_id).await?);
    assert!(
        !fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!(
                    "artifacts/{snapshot_id}/{legacy_namespace}/{}",
                    SNAPSHOT_ARTIFACT_LAYOUT.vm_state
                ),
            ))
            .await?
    );
    assert!(fixture.object_exists(&fixed_sibling).await?);
    assert!(repository.delete_by_id(&snapshot_id).await?);
    assert!(!fixture.object_exists(&fixed_sibling).await?);

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_local_metadata_promotes_same_identity_to_distributed() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/local-promotion";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, resolver) =
        OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (rootfs_digest, memory_digest, _, manifest) =
        write_built_artifacts(&artifacts_root).await?;
    let snapshot_id = SnapshotId::generate();
    let source_sandbox_id = "sandbox-local-promotion".to_string();
    let context = agentenv::snapshot::CommandContext::default();
    let resources = SandboxResources::default();
    let runtime_versions = test_runtime_versions();
    let virtualization_mode = ConfigManager::global_config().virtualization_mode;
    let image_configs = agentenv::types::ImageConfigs::new();
    let local = SnapshotRecord {
        id: snapshot_id.clone(),
        snapshot_type: SnapshotType::Local,
        owner_node_id: Some("node-a".to_string()),
        lifecycle: SnapshotLifecycle::Ready,
        alias: None,
        source: SnapshotSource::Sandbox {
            source_sandbox_id: source_sandbox_id.clone(),
        },
        resources,
        created_at_unix_ms: 1,
        updated_at_unix_ms: 1,
        committed: Some(CommittedSnapshot {
            context: context.clone(),
            startup: None,
            runtime_versions: runtime_versions.clone(),
            virtualization_mode,
            image_configs: image_configs.clone(),
            rootfs_layers: Vec::new(),
            attached_drives: Vec::new(),
            memory_layers: Vec::new(),
            disk_publications: Vec::new(),
            legacy_artifact_namespace: None,
            custom_extension_params: None,
        }),
    };

    let local = repository.commit_record(local).await?;
    assert_eq!(local.id, snapshot_id);
    assert_eq!(local.snapshot_type, SnapshotType::Local);
    assert_eq!(local.owner_node_id.as_deref(), Some("node-a"));
    assert!(local.alias.is_none());
    assert_eq!(local.lifecycle, SnapshotLifecycle::Ready);

    let exact_local = repository
        .get_record(&snapshot_id)
        .await?
        .expect("Local canonical metadata should be readable by exact id");
    assert_eq!(exact_local.snapshot_type, SnapshotType::Local);
    assert_eq!(exact_local.owner_node_id.as_deref(), Some("node-a"));
    assert!(exact_local.alias.is_none());

    let record_key = prefixed_key(prefix, &format!("catalog/records/{snapshot_id}.json"));
    let record_json = fixture
        .client
        .get_object()
        .bucket(&fixture.bucket)
        .key(&record_key)
        .send()
        .await?
        .body
        .collect()
        .await?
        .into_bytes();
    let record_json = String::from_utf8(record_json.to_vec())?;
    assert!(
        !record_json.contains(workspace.path().to_string_lossy().as_ref()),
        "canonical Local metadata must not contain node-local artifact paths"
    );

    let vm_state_key = artifact_key_for_record(prefix, &local, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
    let manifest_key = artifact_key_for_record(
        prefix,
        &local,
        SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
    );
    assert!(!fixture.object_exists(&vm_state_key).await?);
    assert!(!fixture.object_exists(&manifest_key).await?);

    let promoted = repository
        .publish(
            SnapshotPublishMetadata {
                id: snapshot_id.clone(),
                snapshot_type: SnapshotType::Distributed,
                owner_node_id: None,
                alias: None,
                source: SnapshotPublishSource::Sandbox { source_sandbox_id },
                context,
                startup: None,
                resources,
                runtime_versions,
                virtualization_mode,
                image_configs,
                custom_extension_params: None,
            },
            manifest,
        )
        .await?;

    assert_eq!(promoted.id, snapshot_id);
    assert_eq!(promoted.snapshot_type, SnapshotType::Distributed);
    assert!(promoted.owner_node_id.is_none());
    assert!(promoted.alias.is_none());
    assert_eq!(promoted.lifecycle, SnapshotLifecycle::Ready);
    let exact_promoted = repository
        .get_record(&snapshot_id)
        .await?
        .expect("promoted metadata should remain visible by exact id");
    assert_eq!(exact_promoted.id, snapshot_id);
    assert_eq!(exact_promoted.snapshot_type, SnapshotType::Distributed);
    assert!(exact_promoted.owner_node_id.is_none());
    assert!(exact_promoted.alias.is_none());

    let promoted_vm_state_key =
        artifact_key_for_record(prefix, &promoted, SNAPSHOT_ARTIFACT_LAYOUT.vm_state);
    let promoted_manifest_key = artifact_key_for_record(
        prefix,
        &promoted,
        SNAPSHOT_ARTIFACT_LAYOUT.firecracker_manifest,
    );
    assert!(fixture.object_exists(&promoted_vm_state_key).await?);
    assert!(fixture.object_exists(&promoted_manifest_key).await?);
    assert!(
        fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!("managed-layers/{rootfs_digest}")
            ))
            .await?
    );
    assert!(
        fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!("managed-layers/{memory_digest}")
            ))
            .await?
    );

    let committed = promoted
        .committed
        .as_ref()
        .expect("promoted snapshot should carry its distributed closure");
    assert!(matches!(
        committed.rootfs_layers.as_slice(),
        [OverlaybdLayerRef::Managed(layer)] if layer.digest == rootfs_digest
    ));
    assert_eq!(committed.memory_layers.len(), 1);
    assert_eq!(committed.memory_layers[0].digest, memory_digest);

    let runnable = resolver.resolve(Arc::new(promoted)).await?;
    assert!(runnable.manifest().vm_state.path.exists());
    assert!(runnable.manifest().memory.image_config_path.exists());
    assert!(runnable.manifest().rootfs.image_config_path.exists());

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_resolve_reports_missing_managed_layer() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/missing-managed-layer";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, resolver) =
        OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (rootfs_digest, _, _, manifest) = write_built_artifacts(&artifacts_root).await?;
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(test_publish_metadata(snapshot_id, None), manifest)
        .await?;

    fixture
        .client
        .delete_object()
        .bucket(&fixture.bucket)
        .key(prefixed_key(
            prefix,
            &format!("managed-layers/{rootfs_digest}"),
        ))
        .send()
        .await?;

    let error = resolver
        .resolve(Arc::new(stored))
        .await
        .expect_err("resolve should fail when a managed layer is missing");
    match error {
        RepositoryError::ArtifactNotFound { artifact } => {
            assert!(artifact.contains("managed layer"));
            assert!(artifact.contains(&rootfs_digest));
        }
        other => panic!("expected ArtifactNotFound, got {other:?}"),
    }

    Ok(())
}

#[tokio::test]
#[ignore = "requires docker"]
async fn snapshot_oss_delete_by_alias_removes_manifest_and_listing() -> Result<()> {
    let fixture = MinioFixture::start().await?;
    let workspace = TempDir::new()?;
    let prefix = "snapshots/delete-alias";
    let oss = test_oss_config(&fixture, prefix);
    ensure_test_config()?;

    let (repository, _) = OssBackend::new(&oss, workspace.path().join("oss-cache"))?.into_parts();
    let artifacts_root = workspace.path().join("local-artifacts");
    let (_, _, _, manifest) = write_built_artifacts(&artifacts_root).await?;
    let alias = SnapshotAlias::parse("delete-me").expect("alias should parse");
    let snapshot_id = SnapshotId::generate();

    let stored = repository
        .publish(
            test_publish_metadata(snapshot_id.clone(), Some(alias.clone())),
            manifest,
        )
        .await?;

    repository.delete(alias.as_ref()).await?;

    assert!(repository.get(alias.as_ref()).await?.is_none());
    assert_eq!(repository.resolve_alias(alias.as_ref()).await?, None);
    assert!(repository
        .list(SnapshotListFilter::matches_all())
        .await?
        .is_empty());
    assert!(
        !fixture
            .object_exists(&prefixed_key(
                prefix,
                &format!(
                    "artifacts/{}/{}",
                    stored.id, SNAPSHOT_ARTIFACT_LAYOUT.vm_state
                )
            ))
            .await?
    );

    Ok(())
}
