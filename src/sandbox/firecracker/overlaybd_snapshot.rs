//! Overlaybd-specific snapshot helpers for Firecracker sandboxes.
//!
//! Immutable checkpoint capture seals writable state through
//! [`restack_snapshot_overlaybd_rootfs`]. Temporal pause instead stages the
//! mutable upper without sealing it or extending the immutable lower chain.
//!
use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{validate_upper_config, ImageConfig, LayerConfig};
use overlaybd::index::{Segment, SegmentMapping};
use overlaybd::index_file::{compact_to, create_mappings_from_sparse};
use overlaybd::layer_metadata::resolve_local_layer_path;
use overlaybd::virtual_file::VirtualFile;
use tracing::{debug, warn};

use super::process_vm_reader::ProcessVmReader;
use super::sandbox::managed_snapshot_base;
use crate::cfg::ConfigManager;
use crate::sandbox::ublk::UblkDevice;
use crate::sandbox::ublk::{
    compact_layers, create_commit_args, OverlaybdCompactOutput, UblkDeviceManager,
};
use crate::sandbox::SandboxCaptureError;

/// Default maximum number of overlaybd snapshot layers before compaction triggers.
/// Well below the hard limit of 255 (`MAX_STACK_LAYERS`) in overlaybd.
const DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS: usize = 32;
const INHERITED_LAYERS_DIR: &str = "inherited-layers";
const MANAGED_BASE_LAYER_FILE: &str = "managed-base.commit";
const FIRECRACKER_DIRTY_PAGE_SIZE: u64 = 4096;
const OVERLAYBD_ALIGNMENT: u64 = 512;
const DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY: usize = 32;

enum LiveOverlaybdSnapshotState {
    ReadOnly,
    Restacked(PathBuf),
}

impl LiveOverlaybdSnapshotState {
    fn snapshot_layer_path(&self) -> Option<&Path> {
        match self {
            Self::ReadOnly => None,
            Self::Restacked(path) => Some(path.as_path()),
        }
    }

    fn finish_staging(self, staged_snapshot: Result<PathBuf>) -> Result<PathBuf> {
        match self {
            Self::ReadOnly => staged_snapshot,
            Self::Restacked(_) => staged_snapshot.map_err(into_terminal_snapshot_failure),
        }
    }
}

fn into_terminal_snapshot_failure(err: anyhow::Error) -> anyhow::Error {
    SandboxCaptureError::terminal(err).into()
}

fn local_layer_config(path: &Path) -> LayerConfig {
    local_layer_config_with_descriptor(path, None)
}

fn local_layer_config_with_descriptor(
    path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> LayerConfig {
    LayerConfig {
        file: path.display().to_string(),
        digest: descriptor
            .map(|descriptor| descriptor.digest.clone())
            .unwrap_or_default(),
        size: descriptor.map(|descriptor| descriptor.size).unwrap_or(0),
        ..Default::default()
    }
}

fn canonicalized_runtime_owned_roots() -> &'static [PathBuf] {
    static RUNTIME_OWNED_ROOTS: std::sync::OnceLock<Vec<PathBuf>> = std::sync::OnceLock::new();
    RUNTIME_OWNED_ROOTS
        .get_or_init(|| {
            let config = ConfigManager::global_config();
            [
                managed_snapshot_base(),
                config.orchestrator.persisted_sandbox_store_path.clone(),
            ]
            .into_iter()
            .map(|root| fs::canonicalize(&root).unwrap_or(root))
            .collect::<Vec<_>>()
        })
        .as_slice()
}

fn runtime_owned_roots_for_live_config(image_config_path: &Path) -> Vec<PathBuf> {
    let mut roots = canonicalized_runtime_owned_roots().to_vec();
    if let Some(runtime_dir) = image_config_path.parent() {
        let runtime_dir =
            fs::canonicalize(runtime_dir).unwrap_or_else(|_| runtime_dir.to_path_buf());
        if !roots.contains(&runtime_dir) {
            roots.push(runtime_dir);
        }
    }
    roots
}

/// Split a list of overlaybd lowers into two parts: the prefix of stable lowers and the suffix of runtime-owned lowers.
///
/// Runtime-created local lowers are appended during resume/fork/pause.
/// Published or cache-owned layers must be materialized before that
/// suffix; otherwise adopting only the tail would either leave a dangling
/// runtime artifact reference or change lower precedence.
fn split_runtime_suffix(
    mut lowers: Vec<LayerConfig>,
    runtime_owned_roots: &[PathBuf],
) -> (Vec<LayerConfig>, Vec<LayerConfig>) {
    let Some(first_runtime_owned_index) = lowers
        .iter()
        .position(|lower| layer_references_runtime_owned_artifact(lower, runtime_owned_roots))
    else {
        return (lowers, Vec::new());
    };

    let runtime_owned_suffix = lowers.split_off(first_runtime_owned_index);

    debug_assert!(lowers
        .iter()
        .all(|lower| !layer_references_runtime_owned_artifact(lower, runtime_owned_roots)));

    (lowers, runtime_owned_suffix)
}

fn local_path_ownership(path: &Path, runtime_owned_roots: &[PathBuf]) -> (bool, bool, PathBuf) {
    let resolved = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let reference_owned = runtime_owned_roots
        .iter()
        .any(|root| path.starts_with(root));
    let target_owned = runtime_owned_roots
        .iter()
        .any(|root| resolved.starts_with(root));
    (reference_owned, target_owned, resolved)
}

fn layer_references_runtime_owned_artifact(
    lower: &LayerConfig,
    runtime_owned_roots: &[PathBuf],
) -> bool {
    let source = resolve_local_layer_path(lower).or_else(|| {
        (!lower.dir.is_empty() && lower.file.is_empty()).then(|| PathBuf::from(&lower.dir))
    });
    source.is_some_and(|path| {
        let (reference_owned, target_owned, _) = local_path_ownership(&path, runtime_owned_roots);
        reference_owned || target_owned
    })
}

fn link_or_copy_file_blocking(source: &Path, destination: &Path) -> std::io::Result<()> {
    if source == destination {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    match fs::remove_file(destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    if let Err(error) = fs::hard_link(source, destination) {
        warn!(
            source = %source.display(),
            destination = %destination.display(),
            error = %error,
            "hard-link runtime artifact failed; falling back to sparse copy"
        );
    } else {
        return Ok(());
    }

    let mut cmd = std::process::Command::new("cp");
    if cfg!(target_os = "linux") {
        cmd.arg("--reflink=auto").arg("--sparse=always");
    }
    let status = cmd.arg(source).arg(destination).status();
    match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => {
            let _ = fs::remove_file(destination);
            Err(std::io::Error::other(format!(
                "sparse copy command failed with status {status}"
            )))
        }
        Err(error) => {
            let _ = fs::remove_file(destination);
            Err(error)
        }
    }
}

pub(super) async fn link_or_copy_runtime_artifact(source: &Path, destination: &Path) -> Result<()> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let description = format!(
        "link or copy runtime artifact {} -> {}",
        source.display(),
        destination.display()
    );
    tokio::task::spawn_blocking(move || link_or_copy_file_blocking(&source, &destination))
        .await
        .context("link runtime artifact task failed")?
        .with_context(|| description)
}

async fn prepare_specific_snapshot_layer_path(snapshot_layer_path: &Path) -> Result<()> {
    match tokio::fs::remove_file(&snapshot_layer_path).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| {
                format!(
                    "remove stale overlaybd snapshot layer {}",
                    snapshot_layer_path.display()
                )
            })
        }
    }
    Ok(())
}

fn load_existing_image_config(
    image_config_path: Option<&Path>,
    description: &str,
) -> Result<ImageConfig> {
    let Some(image_config_path) = image_config_path else {
        return Ok(ImageConfig::default());
    };

    let image_config =
        overlaybd::config::load_image_config(image_config_path).with_context(|| {
            format!(
                "load {description} image config {}",
                image_config_path.display()
            )
        })?;
    Ok(image_config)
}

fn restack_target_upper_data_path(live_runtime_image_config_path: &Path) -> Result<PathBuf> {
    let image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    if image_config.upper.data.is_empty() {
        anyhow::bail!(
            "restack snapshot requires writable upper.data in live runtime config {}",
            live_runtime_image_config_path.display()
        );
    }
    Ok(PathBuf::from(image_config.upper.data))
}

fn on_same_filesystem(src_path: &Path, dst_dir: &Path) -> Result<bool> {
    let src_meta = fs::metadata(src_path)
        .with_context(|| format!("stat restack source path {}", src_path.display()))?;
    let dst_meta = fs::metadata(dst_dir)
        .with_context(|| format!("stat restack destination dir {}", dst_dir.display()))?;
    Ok(src_meta.dev() == dst_meta.dev())
}

fn write_bytes_atomically(path: &Path, bytes: &[u8], description: &str) -> Result<()> {
    let parent = path.parent().with_context(|| {
        format!(
            "{description} path has no parent directory: {}",
            path.display()
        )
    })?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    temp.write_all(bytes)
        .with_context(|| format!("write temp {description} {}", temp.path().display()))?;
    temp.as_file()
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp.path().display()))?;
    let temp_path = temp.path().to_path_buf();
    temp.persist(path).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            path.display()
        ))
    })?;
    Ok(())
}

async fn copy_file_atomically(src: &Path, dst: &Path, description: &str) -> Result<()> {
    let parent = dst.parent().with_context(|| {
        format!(
            "{description} destination has no parent directory: {}",
            dst.display()
        )
    })?;
    let temp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temp {description} in {}", parent.display()))?;
    let temp_path = temp.path().to_path_buf();
    tokio::fs::copy(src, &temp_path).await.with_context(|| {
        format!(
            "copy {description} {} -> {}",
            src.display(),
            temp_path.display()
        )
    })?;
    std::fs::File::open(&temp_path)
        .with_context(|| format!("open temp {description} {}", temp_path.display()))?
        .sync_all()
        .with_context(|| format!("sync temp {description} {}", temp_path.display()))?;
    temp.persist(dst).map_err(|error| {
        anyhow::Error::new(error.error).context(format!(
            "persist {description} {} -> {}",
            temp_path.display(),
            dst.display()
        ))
    })?;
    Ok(())
}

async fn rewrite_live_runtime_config_for_restack(
    live_runtime_image_config_path: &Path,
    snapshot_layer_path: &Path,
    descriptor: Option<&overlaybd::LayerDescriptor>,
) -> Result<()> {
    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live overlaybd runtime config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    image_config.lowers.push(local_layer_config_with_descriptor(
        snapshot_layer_path,
        descriptor,
    ));
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize rewritten live overlaybd runtime config")?;
    write_bytes_atomically(
        live_runtime_image_config_path,
        &bytes,
        "live overlaybd runtime config",
    )?;
    Ok(())
}

pub(super) async fn stage_overlaybd_snapshot_from_live_runtime(
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    snapshot_layer_path: Option<&Path>,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load overlaybd rootfs image config {}",
                live_runtime_image_config_path.display()
            )
        })?;

    let appended_layer = if let Some(snapshot_layer_path) = snapshot_layer_path {
        let expected_snapshot_path = fs::canonicalize(snapshot_layer_path)
            .unwrap_or_else(|_| snapshot_layer_path.to_path_buf());
        let Some(latest_lower) = image_config.lowers.last() else {
            anyhow::bail!(
                "restack snapshot expected latest lower to be the newly claimed sealed upper layer"
            );
        };
        let actual_snapshot_path = fs::canonicalize(&latest_lower.file)
            .unwrap_or_else(|_| PathBuf::from(&latest_lower.file));
        anyhow::ensure!(
            actual_snapshot_path == expected_snapshot_path,
            "restack snapshot expected latest lower {} but found {}",
            expected_snapshot_path.display(),
            actual_snapshot_path.display()
        );
        Some(image_config.lowers.pop().expect("checked latest lower"))
    } else {
        None
    };

    let runtime_owned_roots = runtime_owned_roots_for_live_config(live_runtime_image_config_path);
    let rewritten_lowers = rewrite_lowers_with_runtime_roots(
        image_config.lowers,
        output_dir,
        appended_layer,
        MANAGED_BASE_LAYER_FILE,
        &runtime_owned_roots,
        // Rootfs layers must stay raw: only memory snapshots may be compressed.
        OverlaybdCompactOutput::Raw,
    )
    .await?;
    image_config.lowers = rewritten_lowers;
    image_config.upper = Default::default();
    image_config.result_file = "./result.txt".to_string();

    let output_path = output_dir.join("image.json");
    let bytes = serde_json::to_vec_pretty(&image_config)
        .context("serialize overlaybd rootfs image config with inherited runtime layers")?;
    write_bytes_atomically(&output_path, &bytes, "overlaybd rootfs image config")?;
    Ok(output_path)
}

async fn rewrite_lowers_with_owned_runtime_suffix(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    compaction_output: OverlaybdCompactOutput,
) -> Result<Vec<LayerConfig>> {
    rewrite_lowers_with_runtime_roots(
        existing_lowers,
        output_dir,
        appended_layer,
        compaction_output_name,
        canonicalized_runtime_owned_roots(),
        compaction_output,
    )
    .await
}

async fn rewrite_lowers_with_runtime_roots(
    existing_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    appended_layer: Option<LayerConfig>,
    compaction_output_name: &'static str,
    runtime_owned_roots: &[PathBuf],
    compaction_output: OverlaybdCompactOutput,
) -> Result<Vec<LayerConfig>> {
    let (mut lowers, runtime_owned_lowers) =
        split_runtime_suffix(existing_lowers, runtime_owned_roots);

    // If the total number of lowers exceeds the default maximum, try to compact the
    // runtime-owned suffix and appended layers into a single layer.
    let compactable_count = runtime_owned_lowers.len() + usize::from(appended_layer.is_some());
    if compactable_count > 1
        && lowers.len() + compactable_count > DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS
    {
        let mut runtime_suffix = runtime_owned_lowers;
        if let Some(layer) = appended_layer {
            runtime_suffix.push(layer);
        }
        if runtime_suffix.iter().any(|lower| lower.file.is_empty()) {
            anyhow::bail!("cannot compact runtime-owned overlaybd suffix with remote lowers");
        }
        if let Some(compacted_path) = compact_layers(
            &runtime_suffix,
            &output_dir.join(compaction_output_name),
            compaction_output,
        )
        .await?
        {
            lowers.push(local_layer_config(&compacted_path));
        }
        return Ok(lowers);
    }

    let mut lowers = adopt_runtime_owned_lowers(
        lowers,
        runtime_owned_lowers,
        output_dir,
        runtime_owned_roots,
    )
    .await?;
    if let Some(layer) = appended_layer {
        lowers.push(layer);
    }
    Ok(lowers)
}

async fn adopt_runtime_owned_lowers(
    mut lowers: Vec<LayerConfig>,
    mut runtime_owned_lowers: Vec<LayerConfig>,
    output_dir: &Path,
    runtime_owned_roots: &[PathBuf],
) -> Result<Vec<LayerConfig>> {
    let inherited_layers_dir = output_dir.join(INHERITED_LAYERS_DIR);
    for (index, lower) in runtime_owned_lowers.iter_mut().enumerate() {
        let layer_dir = inherited_layers_dir.join(format!("{index:04}"));
        if let Some(source) = resolve_local_layer_path(lower) {
            let (reference_owned, target_owned, resolved) =
                local_path_ownership(&source, runtime_owned_roots);
            if target_owned {
                let destination = layer_dir.join(
                    resolved
                        .file_name()
                        .unwrap_or_else(|| OsStr::new("runtime-layer.commit")),
                );
                link_or_copy_runtime_artifact(&resolved, &destination).await?;
                lower.file = destination.display().to_string();
                lower.dir.clear();
            } else if reference_owned {
                lower.file = resolved.display().to_string();
                lower.dir.clear();
            }
        } else if !lower.dir.is_empty() {
            let (reference_owned, target_owned, _) =
                local_path_ownership(Path::new(&lower.dir), runtime_owned_roots);
            if reference_owned || target_owned {
                anyhow::bail!(
                    "runtime-owned overlaybd lower dir has no commit or sealed artifact: {}",
                    lower.dir
                );
            }
        }
    }
    lowers.extend(runtime_owned_lowers);
    Ok(lowers)
}

async fn stage_mutable_upper_path(
    field: &mut String,
    output_dir: &Path,
    file_name: &str,
) -> Result<()> {
    if field.is_empty() {
        return Ok(());
    }
    let source = PathBuf::from(field.as_str());
    let destination = output_dir.join(file_name);
    link_or_copy_runtime_artifact(&source, &destination).await?;
    *field = destination.display().to_string();
    Ok(())
}

/// Preserve a live OverlayBD runtime as a mutable, single-writer Temporal
/// continuation without sealing its upper or changing its immutable lowers.
pub(super) async fn stage_temporal_overlaybd_device(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    kind: &'static str,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create temporal overlaybd dir {}", output_dir.display()))?;

    if !read_only {
        UblkDeviceManager::global()
            .sync_for_checkpoint(ublk_device)
            .await
            .with_context(|| format!("sync {kind} overlaybd upper for temporal checkpoint"))?;
    }

    let mut image_config = overlaybd::config::load_image_config(live_runtime_image_config_path)
        .with_context(|| {
            format!(
                "load live {kind} overlaybd config {}",
                live_runtime_image_config_path.display()
            )
        })?;
    let runtime_owned_roots = runtime_owned_roots_for_live_config(live_runtime_image_config_path);
    let (stable_lowers, runtime_owned_lowers) =
        split_runtime_suffix(image_config.lowers, &runtime_owned_roots);
    image_config.lowers = adopt_runtime_owned_lowers(
        stable_lowers,
        runtime_owned_lowers,
        output_dir,
        &runtime_owned_roots,
    )
    .await?;

    let has_writable_upper = validate_upper_config(&image_config.upper)?;
    anyhow::ensure!(
        read_only || has_writable_upper,
        "writable {kind} runtime is missing an OverlayBD upper"
    );
    if read_only {
        image_config.upper = Default::default();
    } else {
        stage_mutable_upper_path(&mut image_config.upper.data, output_dir, "upper.data").await?;
        stage_mutable_upper_path(&mut image_config.upper.index, output_dir, "upper.index").await?;
    }
    image_config.result_file = "./result.txt".to_string();

    let output_path = output_dir.join("image.json");
    let bytes = serde_json::to_vec_pretty(&image_config)
        .with_context(|| format!("serialize temporal {kind} overlaybd config"))?;
    write_bytes_atomically(&output_path, &bytes, "temporal overlaybd image config")?;
    Ok(output_path)
}

async fn capture_live_overlaybd_snapshot(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    kind: &'static str,
) -> Result<LiveOverlaybdSnapshotState> {
    if read_only {
        debug!(
            output_dir = %output_dir.display(),
            "staging overlaybd snapshot from read-only runtime config"
        );
        return Ok(LiveOverlaybdSnapshotState::ReadOnly);
    }

    let live_upper_data_path = restack_target_upper_data_path(live_runtime_image_config_path)
        .context("resolve restack source upper path")?;
    let snapshot_layer_path = output_dir.join("snapshot.commit");
    prepare_specific_snapshot_layer_path(&snapshot_layer_path).await?;
    let live_snapshot_layer_path = if on_same_filesystem(&live_upper_data_path, output_dir)
        .context("validate restack snapshot filesystem precondition")?
    {
        snapshot_layer_path.clone()
    } else {
        let live_snapshot_layer_path = live_upper_data_path
            .parent()
            .context("restack source upper path has no parent directory")?
            .join("snapshot.commit");
        prepare_specific_snapshot_layer_path(&live_snapshot_layer_path)
            .await
            .with_context(|| {
                format!(
                    "prepare same-filesystem restack snapshot layer {}",
                    live_snapshot_layer_path.display()
                )
            })?;
        live_snapshot_layer_path
    };

    let descriptor = UblkDeviceManager::global()
        .restack_snapshot_device(ublk_device, &live_snapshot_layer_path, kind)
        .await
        .context("request overlaybd restack snapshot from ublk device")?;

    // The daemon has sealed the old upper and opened a new one. Every error
    // after this mutation boundary is terminal until the live config is
    // rewritten to describe that new stack.
    async {
        if live_snapshot_layer_path != snapshot_layer_path {
            copy_file_atomically(
                &live_snapshot_layer_path,
                &snapshot_layer_path,
                "restack snapshot layer",
            )
            .await
            .context("copy restack snapshot layer into managed snapshot dir")?;
        }

        if let Some(descriptor) = descriptor.as_ref() {
            let copied_size = tokio::fs::metadata(&snapshot_layer_path)
                .await
                .with_context(|| {
                    format!(
                        "read restack snapshot layer metadata {}",
                        snapshot_layer_path.display()
                    )
                })?
                .len();
            if copied_size != descriptor.size {
                anyhow::bail!(
                    "restack snapshot descriptor size mismatch for {}: descriptor says {}, file has {}",
                    snapshot_layer_path.display(),
                    descriptor.size,
                    copied_size
                );
            }
        }

        rewrite_live_runtime_config_for_restack(
            live_runtime_image_config_path,
            &snapshot_layer_path,
            descriptor.as_ref(),
        )
        .await
        .context("rewrite live runtime config after restack snapshot")
    }
    .await
    .map_err(into_terminal_snapshot_failure)?;

    Ok(LiveOverlaybdSnapshotState::Restacked(snapshot_layer_path))
}

pub(super) async fn build_mem_snapshot_image_config(
    resume_mem_image_config_path: Option<&Path>,
    new_layer_path: &Path,
    output_dir: &Path,
    memory_output: OverlaybdCompactOutput,
) -> Result<ImageConfig> {
    let inherited_image_config =
        load_existing_image_config(resume_mem_image_config_path, "memory snapshot")?;
    let new_layer = local_layer_config(new_layer_path);
    let lowers = rewrite_lowers_with_owned_runtime_suffix(
        inherited_image_config.lowers,
        output_dir,
        Some(new_layer),
        "mem_compacted.commit",
        memory_output,
    )
    .await?;

    Ok(ImageConfig {
        repo_blob_url: inherited_image_config.repo_blob_url,
        lowers,
        ..Default::default()
    })
}

/// Stage a persisted snapshot config from the current live overlaybd runtime.
///
/// Writable runtimes are first restacked in-place so the sealed old upper
/// becomes the newest lower. Read-only runtimes skip the restack phase and
/// only stage the persisted snapshot image config.
pub(super) async fn restack_snapshot_overlaybd_device(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    output_dir: &Path,
    kind: &'static str,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create overlaybd snapshot dir {}", output_dir.display()))?;

    let live_snapshot = capture_live_overlaybd_snapshot(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        output_dir,
        kind,
    )
    .await?;

    let staged_snapshot = stage_overlaybd_snapshot_from_live_runtime(
        live_runtime_image_config_path,
        output_dir,
        live_snapshot.snapshot_layer_path(),
    )
    .await;

    live_snapshot.finish_staging(staged_snapshot)
}

pub(super) async fn restack_snapshot_overlaybd_rootfs(
    ublk_device: &UblkDevice,
    read_only: bool,
    live_runtime_image_config_path: &Path,
    snapshot_root: &Path,
) -> Result<PathBuf> {
    restack_snapshot_overlaybd_device(
        ublk_device,
        read_only,
        live_runtime_image_config_path,
        &snapshot_root.join("rootfs"),
        "rootfs",
    )
    .await
}

async fn publish_memory_overlaybd_layer(
    src_layers: &[Arc<dyn VirtualFile>],
    mappings: &[SegmentMapping],
    virtual_size: u64,
    output_path: &Path,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<()> {
    let lower_tmp = output_path.with_extension("commit.tmp");
    let build_result = async {
        let output_file: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::new(&lower_tmp)
                .with_context(|| format!("create temp lower failed: {}", lower_tmp.display()))?,
        );
        let commit_args = create_commit_args(output_file, mode, concurrency).await?;
        compact_to(src_layers, mappings, virtual_size, commit_args)
            .await
            .context("compact memory layer")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "move sealed memory lower into place failed: {}",
                    output_path.display()
                )
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result
}

fn checked_i64_to_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} must be non-negative, got {value}"))
}

fn dirty_ranges_to_segment_mappings(
    dirty_ranges: &DirtyMemoryRanges,
) -> Result<(Vec<SegmentMapping>, u64)> {
    ensure!(
        dirty_ranges.page_size == FIRECRACKER_DIRTY_PAGE_SIZE as i32,
        "dirty memory page_size must be {FIRECRACKER_DIRTY_PAGE_SIZE}, got {}",
        dirty_ranges.page_size
    );
    let memory_size = checked_i64_to_u64(dirty_ranges.memory_size, "memory_size")?;
    ensure!(
        memory_size % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
        "dirty memory_size {memory_size} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
    );

    let mut mappings = Vec::new();
    for range in &dirty_ranges.ranges {
        let mut base_host_virt_addr =
            checked_i64_to_u64(range.base_host_virt_addr, "base_host_virt_addr")?;
        let mut image_offset = checked_i64_to_u64(range.image_offset, "image_offset")?;
        let length = checked_i64_to_u64(range.length, "length")?;
        ensure!(length > 0, "dirty memory range length must be positive");
        ensure!(
            base_host_virt_addr % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range base_host_virt_addr {base_host_virt_addr:#x} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range image_offset {image_offset} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            length % FIRECRACKER_DIRTY_PAGE_SIZE == 0,
            "dirty range length {length} is not {FIRECRACKER_DIRTY_PAGE_SIZE}-byte aligned"
        );
        ensure!(
            image_offset
                .checked_add(length)
                .is_some_and(|end| end <= memory_size),
            "dirty range [{image_offset}, {}) exceeds memory_size {memory_size}",
            image_offset.saturating_add(length)
        );

        let mut remaining_sectors = length / OVERLAYBD_ALIGNMENT;
        while remaining_sectors > 0 {
            let sector_count = remaining_sectors.min(Segment::MAX_LENGTH as u64);
            mappings.push(SegmentMapping::new(
                image_offset / OVERLAYBD_ALIGNMENT,
                sector_count as u32,
                base_host_virt_addr / OVERLAYBD_ALIGNMENT,
                false,
                0,
            ));
            let bytes = sector_count * OVERLAYBD_ALIGNMENT;
            base_host_virt_addr += bytes;
            image_offset += bytes;
            remaining_sectors -= sector_count;
        }
    }

    mappings.sort_unstable();
    // `offset` and `length` describe destination memory-image sectors, while
    // `moffset` carries the source Firecracker HVA in sector units. This check
    // intentionally rejects overlapping destination ranges, not overlapping
    // source virtual addresses.
    for pair in mappings.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        let previous_end = previous
            .offset()
            .checked_add(u64::from(previous.length()))
            .context("dirty memory segment end overflow")?;
        ensure!(
            previous_end <= next.offset(),
            "overlapping dirty memory destination ranges detected at image sectors {} and {}",
            previous.offset(),
            next.offset()
        );
    }
    Ok((mappings, memory_size))
}

/// Convert dirty Firecracker memory ranges directly into a sealed overlaybd layer.
pub(crate) async fn convert_dirty_memory_to_overlaybd(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<(PathBuf, u64)> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;

    let data_path = output_dir.join("overlaybd.commit");
    let (mappings, memory_size) = dirty_ranges_to_segment_mappings(dirty_ranges)?;
    let source_file: Arc<dyn VirtualFile> = Arc::new(ProcessVmReader::new(firecracker_pid));
    let src_layers = vec![source_file];
    publish_memory_overlaybd_layer(
        &src_layers,
        &mappings,
        memory_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await
    .context("compact dirty memory ranges as overlaybd layer")?;

    Ok((data_path, memory_size))
}

/// Convert a complete memory file into a sealed OverlayBD layer.
pub(crate) async fn convert_memory_file_to_overlaybd(
    memory_path: &Path,
    output_dir: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<(PathBuf, u64)> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .with_context(|| format!("create mem overlaybd dir: {}", output_dir.display()))?;
    let data_path = output_dir.join("overlaybd.commit");
    let virtual_size = tokio::fs::metadata(memory_path)
        .await
        .with_context(|| format!("stat memory snapshot: {}", memory_path.display()))?
        .len();
    let source: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(memory_path)?);
    let mappings = create_mappings_from_sparse(&source, 0).await?;
    publish_memory_overlaybd_layer(
        &[source],
        &mappings,
        virtual_size,
        &data_path,
        mode,
        DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await?;
    let _ = tokio::fs::remove_file(memory_path).await;
    Ok((data_path, virtual_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    use async_trait::async_trait;
    use bytes::Bytes;
    use firecracker_client::models::DirtyMemoryRange;
    use serde_json::json;

    #[test]
    fn dirty_ranges_to_segment_mappings_splits_large_ranges() {
        let first_chunk = Segment::MAX_LENGTH as i64 * OVERLAYBD_ALIGNMENT as i64;
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 4096,
            memory_size: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: first_chunk + OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let (mappings, memory_size) =
            dirty_ranges_to_segment_mappings(&dirty_ranges).expect("convert mappings");

        assert_eq!(memory_size, dirty_ranges.memory_size as u64);
        assert_eq!(mappings.len(), 2);
        assert_eq!(mappings[0].offset(), 0);
        assert_eq!(mappings[0].length(), Segment::MAX_LENGTH);
        assert_eq!(mappings[0].moffset, 0x1000_0000 / OVERLAYBD_ALIGNMENT);
        assert_eq!(mappings[1].offset(), Segment::MAX_LENGTH as u64);
        assert_eq!(mappings[1].length(), 1);
        assert_eq!(
            mappings[1].moffset,
            0x1000_0000 / OVERLAYBD_ALIGNMENT + Segment::MAX_LENGTH as u64
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_4k_page_size() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: 2048,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: Vec::new(),
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty memory page_size must be 4096"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_non_page_aligned_range() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
            ranges: vec![DirtyMemoryRange {
                base_host_virt_addr: 0x1000_0000,
                image_offset: 0,
                length: OVERLAYBD_ALIGNMENT as i64,
            }],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("dirty range length 512 is not 4096-byte aligned"),
            "{err:#}"
        );
    }

    #[test]
    fn dirty_ranges_to_segment_mappings_rejects_overlaps() {
        let dirty_ranges = DirtyMemoryRanges {
            page_size: FIRECRACKER_DIRTY_PAGE_SIZE as i32,
            memory_size: (FIRECRACKER_DIRTY_PAGE_SIZE * 3) as i64,
            ranges: vec![
                DirtyMemoryRange {
                    base_host_virt_addr: 0x1000_0000,
                    image_offset: 0,
                    length: (FIRECRACKER_DIRTY_PAGE_SIZE * 2) as i64,
                },
                DirtyMemoryRange {
                    base_host_virt_addr: 0x2000_0000,
                    image_offset: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                    length: FIRECRACKER_DIRTY_PAGE_SIZE as i64,
                },
            ],
        };

        let err = dirty_ranges_to_segment_mappings(&dirty_ranges).unwrap_err();
        assert!(
            err.to_string()
                .contains("overlapping dirty memory destination ranges detected"),
            "{err:#}"
        );
    }

    #[test]
    fn split_runtime_suffix_preserves_stable_lowers() {
        let config = ConfigManager::global_config();
        let snapshot_store_root = config
            .backend
            .posix_fs
            .as_ref()
            .map(|posix_fs| posix_fs.snapshot_store.clone())
            .unwrap_or_else(|| std::env::temp_dir().join("test_snapshot_store"));
        let persisted_sandbox_store_root = config.orchestrator.persisted_sandbox_store_path.clone();
        let snapshot_store_lower = snapshot_store_root
            .join("managed-layers")
            .join("sha256_base.overlaybd.commit");
        let persisted_store_lower = persisted_sandbox_store_root
            .join("artifacts")
            .join("sandbox-parent")
            .join("1700000000000")
            .join("rootfs")
            .join("snapshot.commit");
        let managed_base_lower = managed_snapshot_base().join("sandbox/1700000000001/mem.commit");
        let lowers = vec![
            LayerConfig {
                file: String::new(),
                digest: "sha256:remote".to_string(),
                size: 42,
                ..Default::default()
            },
            local_layer_config(&snapshot_store_lower),
            local_layer_config(&persisted_store_lower),
            local_layer_config(&managed_base_lower),
        ];

        let (preserved, managed) =
            split_runtime_suffix(lowers, canonicalized_runtime_owned_roots());

        assert_eq!(preserved.len(), 2);
        assert_eq!(preserved[0].file, "");
        assert_eq!(preserved[0].digest, "sha256:remote");
        assert_eq!(PathBuf::from(&preserved[1].file), snapshot_store_lower);
        assert_eq!(managed.len(), 2);
        assert_eq!(PathBuf::from(&managed[0].file), persisted_store_lower);
        assert_eq!(PathBuf::from(&managed[1].file), managed_base_lower);
    }

    #[tokio::test]
    async fn snapshot_adopts_persisted_runtime_suffix_by_link() {
        let temp = tempfile::tempdir().expect("tempdir");
        let artifacts_root = temp.path().join("persisted-sandboxes").join("artifacts");
        let output_dir = artifacts_root.join("sandbox-c").join("gen1").join("rootfs");
        let parent_root = artifacts_root.join("sandbox-b").join("gen1").join("rootfs");
        let cache_root = temp.path().join("image-cache").join("commits");
        std::fs::create_dir_all(&parent_root).expect("create parent rootfs");
        std::fs::create_dir_all(&cache_root).expect("create cache root");

        let parent_base = parent_root.join("managed-base.commit");
        let parent_snapshot = parent_root.join("snapshot.commit");
        let cache_layer = cache_root.join("sha256-base").join("overlaybd.commit");
        std::fs::create_dir_all(cache_layer.parent().unwrap()).expect("create cache layer dir");
        std::fs::write(&parent_base, b"parent base").expect("write parent base");
        std::fs::write(&parent_snapshot, b"parent snapshot").expect("write parent snapshot");
        std::fs::write(&cache_layer, b"cache base").expect("write cache layer");

        let lowers = vec![
            LayerConfig {
                file: cache_layer.display().to_string(),
                digest: "sha256:base".to_string(),
                size: 10,
                ..Default::default()
            },
            local_layer_config(&parent_base),
            local_layer_config(&parent_snapshot),
        ];
        let runtime_owned_roots = [artifacts_root.canonicalize().unwrap()];

        let rewritten = rewrite_lowers_with_runtime_roots(
            lowers,
            &output_dir,
            None,
            MANAGED_BASE_LAYER_FILE,
            &runtime_owned_roots,
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("rewrite inherited runtime layers");

        assert_eq!(rewritten.len(), 3);
        assert_eq!(PathBuf::from(&rewritten[0].file), cache_layer);
        assert_eq!(rewritten[0].digest, "sha256:base");
        assert_eq!(rewritten[0].size, 10);

        for (index, (rewritten_lower, source)) in rewritten[1..]
            .iter()
            .zip([parent_base, parent_snapshot])
            .enumerate()
        {
            let adopted = PathBuf::from(&rewritten_lower.file);
            let expected_parent = output_dir
                .join(INHERITED_LAYERS_DIR)
                .join(format!("{index:04}"));
            assert!(adopted.starts_with(expected_parent));
            assert!(!adopted.starts_with(&parent_root));
            assert_eq!(adopted.file_name(), source.file_name());
            assert!(rewritten_lower.digest.is_empty());
            assert_eq!(rewritten_lower.size, 0);

            let source_metadata = std::fs::metadata(&source).expect("source metadata");
            let adopted_metadata = std::fs::metadata(&adopted).expect("adopted metadata");
            assert_eq!(source_metadata.dev(), adopted_metadata.dev());
            assert_eq!(source_metadata.ino(), adopted_metadata.ino());
        }
    }

    #[tokio::test]
    async fn live_runtime_lower_is_adopted_before_runtime_directory_is_deleted() {
        let temp = tempfile::tempdir().expect("tempdir");
        let stable_dir = temp.path().join("stable");
        let live_dir = temp.path().join("live");
        let source_lowers_dir = live_dir.join("source-lowers");
        let output_dir = temp.path().join("paused").join("rootfs");
        std::fs::create_dir_all(&stable_dir).expect("create stable dir");
        std::fs::create_dir_all(&source_lowers_dir).expect("create live lower dir");

        let stable_lower = stable_dir.join("base.commit");
        let runtime_lower = source_lowers_dir.join("continued.commit");
        std::fs::write(&stable_lower, b"stable").expect("write stable lower");
        std::fs::write(&runtime_lower, b"continued").expect("write runtime lower");
        let live_config = live_dir.join("image.json");
        std::fs::write(
            &live_config,
            serde_json::to_vec_pretty(&json!({
                "lowers": [
                    {
                        "file": stable_lower.display().to_string(),
                        "digest": "sha256:stable",
                        "size": 6
                    },
                    {
                        "file": runtime_lower.display().to_string(),
                        "digest": "sha256:continued",
                        "size": 9
                    }
                ],
                "upper": {},
                "resultFile": ""
            }))
            .expect("serialize live config"),
        )
        .expect("write live config");

        let staged_config =
            stage_overlaybd_snapshot_from_live_runtime(&live_config, &output_dir, None)
                .await
                .expect("stage live runtime");
        let staged =
            overlaybd::config::load_image_config(&staged_config).expect("load staged config");

        assert_eq!(staged.lowers.len(), 2);
        assert_eq!(PathBuf::from(&staged.lowers[0].file), stable_lower);
        assert_eq!(staged.lowers[0].digest, "sha256:stable");
        let adopted = PathBuf::from(&staged.lowers[1].file);
        assert!(adopted.starts_with(output_dir.join(INHERITED_LAYERS_DIR)));
        assert_eq!(staged.lowers[1].digest, "sha256:continued");
        assert_eq!(
            std::fs::read(&adopted).expect("read adopted lower"),
            b"continued"
        );
        assert!(!output_dir.join("snapshot.commit").exists());

        std::fs::remove_dir_all(&live_dir).expect("remove source runtime");
        assert_eq!(
            std::fs::read(&adopted).expect("read adopted lower after source removal"),
            b"continued"
        );
    }

    #[tokio::test]
    async fn stage_live_runtime_preserves_restacked_layer_descriptor() {
        let temp = tempfile::tempdir().expect("tempdir");
        let live_dir = temp.path().join("live");
        let output_dir = temp.path().join("out");
        std::fs::create_dir_all(&live_dir).expect("create live dir");
        std::fs::create_dir_all(&output_dir).expect("create output dir");

        let snapshot_lower = output_dir.join("snapshot.commit");
        std::fs::write(&snapshot_lower, b"snapshot").expect("write snapshot lower");
        let live_runtime_image_config_path = live_dir.join("image.json");
        std::fs::write(
            &live_runtime_image_config_path,
            serde_json::to_vec_pretty(&json!({
                "lowers": [
                    {
                        "file": snapshot_lower,
                        "digest": "sha256:descriptor",
                        "size": 8
                    }
                ],
                "upper": {},
                "resultFile": live_dir.join("result.txt"),
                "download": {}
            }))
            .expect("serialize live image config"),
        )
        .expect("write live image config");

        let output_path = stage_overlaybd_snapshot_from_live_runtime(
            &live_runtime_image_config_path,
            &output_dir,
            Some(&snapshot_lower),
        )
        .await
        .expect("stage snapshot");

        let staged =
            overlaybd::config::load_image_config(&output_path).expect("load staged image config");
        let latest = staged.lowers.last().expect("latest lower");
        assert_eq!(PathBuf::from(&latest.file), snapshot_lower);
        assert_eq!(latest.digest, "sha256:descriptor");
        assert_eq!(latest.size, 8);
    }

    #[tokio::test]
    async fn build_mem_snapshot_image_config_skips_compaction_for_remote_lowers() {
        let temp = tempfile::tempdir().expect("tempdir");
        let parent_config_path = temp.path().join("parent-mem-image.json");
        let repo_blob_url = "s3://agentenv-oss-validation/validation/managed-layers";
        let remote_lowers: Vec<_> = (0..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS)
            .map(|index| {
                json!({
                    "digest": format!("sha256:parent-{index}"),
                    "size": 4096
                })
            })
            .collect();
        std::fs::write(
            &parent_config_path,
            serde_json::to_vec_pretty(&json!({
                "repoBlobUrl": repo_blob_url,
                "lowers": remote_lowers,
                "upper": {},
                "resultFile": "",
                "download": {}
            }))
            .expect("serialize parent image config"),
        )
        .expect("write parent image config");

        let new_layer = temp.path().join("mem_overlaybd").join("overlaybd.commit");
        std::fs::create_dir_all(new_layer.parent().unwrap()).expect("create mem layer dir");
        std::fs::write(&new_layer, b"memory delta").expect("write mem layer");
        let image_config = build_mem_snapshot_image_config(
            Some(&parent_config_path),
            &new_layer,
            temp.path(),
            OverlaybdCompactOutput::Raw,
        )
        .await
        .expect("build memory image config");

        assert_eq!(image_config.repo_blob_url, repo_blob_url);
        assert_eq!(
            image_config.lowers.len(),
            DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS + 1
        );
        assert_eq!(image_config.lowers[0].digest, "sha256:parent-0");
        assert!(image_config.lowers[..DEFAULT_MAX_OVERLAYBD_SNAPSHOT_LAYERS]
            .iter()
            .all(|lower| lower.file.is_empty()));
        let latest = image_config.lowers.last().expect("latest mem lower");
        assert_eq!(PathBuf::from(&latest.file), new_layer);
        assert!(latest.digest.is_empty());
        assert_eq!(latest.size, 0);
        assert!(image_config.download_override.is_none());
    }

    struct FailingSource;

    #[async_trait]
    impl VirtualFile for FailingSource {
        async fn read_at(&self, _offset: u64, _len: usize) -> Result<Bytes> {
            anyhow::bail!("injected memory source read failure")
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("injected memory source write failure")
        }

        async fn size(&self) -> Result<u64> {
            Ok(OVERLAYBD_ALIGNMENT)
        }
    }

    fn one_page_mapping() -> Vec<SegmentMapping> {
        vec![SegmentMapping::new(0, 1, 0, false, 0)]
    }

    #[tokio::test]
    async fn publish_memory_layer_failure_removes_temp_and_preserves_final() {
        let temp = tempfile::tempdir().expect("tempdir");
        let output = temp.path().join("memory.commit");
        let lower_tmp = output.with_extension("commit.tmp");
        let existing = b"existing final";
        tokio::fs::write(&output, existing)
            .await
            .expect("write existing final");
        let source: Arc<dyn VirtualFile> = Arc::new(FailingSource);
        publish_memory_overlaybd_layer(
            &[source],
            &one_page_mapping(),
            OVERLAYBD_ALIGNMENT,
            &output,
            OverlaybdCompactOutput::Raw,
            1,
        )
        .await
        .expect_err("copy failure should be returned");

        assert!(!lower_tmp.exists());
        assert_eq!(
            tokio::fs::read(&output).await.expect("read final"),
            existing
        );
    }
}
