use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use agentenv::cfg::ConfigManager;
use agentenv::sandbox::{
    CapturedSandboxSnapshot, FirecrackerCapturedSnapshot, FirecrackerPausedState,
    FirecrackerSandbox, ProcessOpts, SandboxBackend, SandboxExecutor, SandboxLaunchConfig,
};
use agentenv::snapshot::{
    SnapshotAlias, SnapshotId, SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord,
    SnapshotRuntimeVersions,
};
use agentenv::template::TemplateBuildSpec;
use agentenv::types::{SandboxId, SandboxResources};
use anyhow::{anyhow, bail, Context, Result};
use tempfile::tempdir;

use crate::common;

fn sample_runtime_versions() -> SnapshotRuntimeVersions {
    SnapshotRuntimeVersions {
        kernel_version: "kernel".to_string(),
        firecracker_version: "fc".to_string(),
        envd_version: "envd".to_string(),
        tools_drive_version: ConfigManager::global_config()
            .resolved_tools_version()
            .to_string(),
    }
}

async fn write_guest_file(sandbox: &FirecrackerSandbox, path: &str, contents: &str) -> Result<()> {
    let escaped_path = path.replace('\'', "'\\''");
    let escaped_contents = contents.replace('\'', "'\\''");
    let command = format!(
        "mkdir -p \"$(dirname '{escaped_path}')\" && printf '%s' '{escaped_contents}' > '{escaped_path}'"
    );
    let output = sandbox.run_command("sh", &["-lc", &command]).await?;
    if output.exit_code != 0 {
        bail!("write guest file {path} failed: {}", output.stderr);
    }
    Ok(())
}

async fn assert_guest_file(
    sandbox: &FirecrackerSandbox,
    path: &str,
    expected_contents: &str,
) -> Result<()> {
    let output = sandbox.run_command("cat", &[path]).await?;
    if output.exit_code != 0 {
        bail!(
            "read guest file {path} failed (exit {}): {}",
            output.exit_code,
            output.stderr
        );
    }
    assert_eq!(
        output.stdout, expected_contents,
        "guest file {path} contents changed"
    );
    Ok(())
}

async fn assert_guest_path_absent(sandbox: &FirecrackerSandbox, path: &str) -> Result<()> {
    let output = sandbox.run_command("test", &["!", "-e", path]).await?;
    assert_eq!(
        output.exit_code, 0,
        "guest path {path} unexpectedly exists: {}",
        output.stderr
    );
    Ok(())
}

async fn assert_guest_process_env(
    sandbox: &FirecrackerSandbox,
    pid: u32,
    key: &str,
    value: &str,
) -> Result<()> {
    let expected = format!("{key}={value}");
    let command = format!(
        "test -r /proc/{pid}/environ && tr '\\000' '\\n' < /proc/{pid}/environ | grep -Fqx '{expected}'"
    );
    let output = sandbox.run_command("sh", &["-c", &command]).await?;
    if output.exit_code != 0 {
        bail!(
            "guest process {pid} lost environment entry {expected}: {}",
            output.stderr
        );
    }
    Ok(())
}

async fn publish_captured_snapshot_for_test(
    snapshot_manager: &agentenv::snapshot::SnapshotManager,
    alias: &str,
    source_sandbox_id: SandboxId,
    captured_snapshot: CapturedSandboxSnapshot,
) -> Result<SnapshotRecord> {
    Ok(snapshot_manager
        .publish_captured(
            SnapshotPublishMetadata {
                id: SnapshotId::generate(),
                snapshot_type: agentenv::snapshot::SnapshotType::Distributed,
                owner_node_id: None,
                alias: Some(SnapshotAlias::parse(alias)?),
                source: SnapshotPublishSource::Sandbox {
                    source_sandbox_id: source_sandbox_id.to_string(),
                },
                context: agentenv::snapshot::CommandContext::default(),
                startup: None,
                resources: SandboxResources {
                    cpu_count: 1,
                    memory_mib: 128,
                    disk_size_mib: 0,
                },
                runtime_versions: sample_runtime_versions(),
                virtualization_mode: agentenv::cfg::ConfigManager::global_config()
                    .virtualization_mode,
                image_configs: agentenv::types::ImageConfigs::new(),
                custom_extension_params: None,
            },
            captured_snapshot,
        )
        .await?)
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn index(&mut self, len: usize) -> usize {
        ((self.next_u64() >> 33) as usize) % len
    }
}

enum ScenarioSandboxRuntime {
    Running(Box<FirecrackerSandbox>),
    Paused(Box<agentenv::sandbox::FirecrackerSnapshotConfig>),
}

struct ScenarioSandbox {
    name: String,
    sandbox_id: SandboxId,
    runtime: ScenarioSandboxRuntime,
    expected_files: HashMap<String, String>,
    captures: usize,
    ever_paused_after_capture: bool,
    captured_snapshot_deleted_after_child_start: bool,
}

struct ScenarioSnapshot {
    alias: String,
    record: SnapshotRecord,
    expected_files: HashMap<String, String>,
    deleted: bool,
    is_template: bool,
    source_sandbox_id: Option<SandboxId>,
    child_starts: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RandomLifecycleOperation {
    WriteMarker,
    CaptureSnapshot,
    StartSandbox,
    PauseSandbox,
    ResumeSandbox,
    DeleteSandbox,
    DeleteSnapshotOrTemplate,
}

#[derive(Default)]
struct LifecycleCoverage {
    repeated_capture_same_sandbox: bool,
    child_started_from_captured_snapshot: bool,
    captured_snapshot_deleted_after_child_start: bool,
    captured_sandbox_paused: bool,
    captured_sandbox_resumed_after_snapshot_delete: bool,
}

async fn assert_expected_files(
    sandbox: &FirecrackerSandbox,
    expected_files: &HashMap<String, String>,
) -> Result<()> {
    for (path, contents) in expected_files {
        assert_guest_file(sandbox, path, contents).await?;
    }
    Ok(())
}

fn running_sandbox_indices(sandboxes: &[ScenarioSandbox]) -> Vec<usize> {
    sandboxes
        .iter()
        .enumerate()
        .filter_map(|(index, sandbox)| match sandbox.runtime {
            ScenarioSandboxRuntime::Running(_) => Some(index),
            ScenarioSandboxRuntime::Paused(_) => None,
        })
        .collect()
}

fn paused_sandbox_indices(sandboxes: &[ScenarioSandbox]) -> Vec<usize> {
    sandboxes
        .iter()
        .enumerate()
        .filter_map(|(index, sandbox)| match sandbox.runtime {
            ScenarioSandboxRuntime::Paused(_) => Some(index),
            ScenarioSandboxRuntime::Running(_) => None,
        })
        .collect()
}

fn live_snapshot_indices(snapshots: &[ScenarioSnapshot]) -> Vec<usize> {
    snapshots
        .iter()
        .enumerate()
        .filter_map(|(index, snapshot)| (!snapshot.deleted).then_some(index))
        .collect()
}

fn push_weighted(
    operations: &mut Vec<RandomLifecycleOperation>,
    operation: RandomLifecycleOperation,
    weight: usize,
) {
    operations.extend(std::iter::repeat_n(operation, weight));
}

#[tokio::test]
async fn built_and_derived_snapshot_can_be_launched() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (builder, snapshot_manager, _) = common::snapshot_test_parts(store.path());
    let base_alias = unique_alias_for_test("snapshot_showcase");
    let derived_alias = unique_alias_for_test("snapshot_showcase_derived");

    let build_spec = common::default_rootfs_template_build_spec()
        .alias(base_alias.clone())
        .resources(1, 128)
        .run("mkdir -p /workspace")
        .workdir("/workspace")
        .env("TEMPLATE_MARK", "snapshot-ready")
        .run("printf '%s' \"$TEMPLATE_MARK\" > env.txt && pwd > cwd.txt")
        .run("echo base > base.txt");
    let base_snapshot = builder
        .build_and_publish(&snapshot_manager, build_spec)
        .await?;

    let loaded_base = snapshot_manager
        .get(&base_alias)
        .await?
        .ok_or_else(|| anyhow!("snapshot should exist"))?;
    assert_eq!(loaded_base.id.to_string(), base_snapshot.id.to_string());

    let runnable = snapshot_manager
        .resolve_runnable(loaded_base.clone())
        .await?;
    let derived_id = SnapshotId::generate();
    builder
        .build_from_snapshot_and_publish(
            &snapshot_manager,
            TemplateBuildSpec::new()
                .alias(derived_alias.clone())
                .resources(1, 128)
                .run("echo rebuilt > rebuilt.txt"),
            derived_id.clone(),
            &runnable,
        )
        .await?;

    let derived = snapshot_manager
        .get(&derived_alias)
        .await?
        .ok_or_else(|| anyhow!("derived snapshot should exist"))?;
    assert_eq!(derived.id, derived_id);
    assert_eq!(
        derived.alias.as_ref().map(ToString::to_string),
        Some(derived_alias.clone())
    );

    let runnable = snapshot_manager.resolve_runnable(derived.clone()).await?;
    let mut sandbox =
        FirecrackerSandbox::from_snapshot(&runnable, &SandboxLaunchConfig::default())?;
    sandbox.start().await?;
    let version_out = sandbox
        .run_command("sh", &["-lc", "envd --version 2>&1 || envd -version 2>&1"])
        .await?;
    assert_eq!(version_out.exit_code, 0);
    assert!(version_out.stdout.contains(
        &derived
            .committed
            .as_ref()
            .unwrap()
            .runtime_versions
            .envd_version
    ));
    let env_out = sandbox.run_command("cat", &["/workspace/env.txt"]).await?;
    assert_eq!(env_out.exit_code, 0);
    assert_eq!(env_out.stdout.trim(), "snapshot-ready");

    let cwd_out = sandbox.run_command("cat", &["/workspace/cwd.txt"]).await?;
    assert_eq!(cwd_out.exit_code, 0);
    assert_eq!(cwd_out.stdout.trim(), "/workspace");

    let base_out = sandbox.run_command("cat", &["/workspace/base.txt"]).await?;
    assert_eq!(base_out.exit_code, 0);
    assert_eq!(base_out.stdout.trim(), "base");

    let rebuilt_out = sandbox
        .run_command("cat", &["/workspace/rebuilt.txt"])
        .await?;
    assert_eq!(rebuilt_out.exit_code, 0);
    assert_eq!(rebuilt_out.stdout.trim(), "rebuilt");

    sandbox.stop().await?;
    Ok(())
}

#[tokio::test]
async fn persistent_snapshot_lifecycle_preserves_original_pause_resume_state() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (_, snapshot_manager, _) = common::snapshot_test_parts(store.path());

    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.vcpu_count = 1;
    sandbox_config.mem_size_mib = 128;
    let mut original = FirecrackerSandbox::new(sandbox_config)?;
    original.start().await?;
    let source_sandbox_id = SandboxId::new();

    write_guest_file(&original, "/tmp/agentenv-lifecycle/base.txt", "base").await?;
    let first_alias = unique_alias_for_test("lifecycle_first");
    let first_capture = SandboxBackend::snapshot(&mut original).await?;
    assert_guest_file(&original, "/tmp/agentenv-lifecycle/base.txt", "base").await?;
    write_guest_file(
        &original,
        "/tmp/agentenv-lifecycle/after-first.txt",
        "after-first",
    )
    .await?;
    let first_snapshot = publish_captured_snapshot_for_test(
        &snapshot_manager,
        &first_alias,
        source_sandbox_id,
        first_capture,
    )
    .await?;

    let second_alias = unique_alias_for_test("lifecycle_second");
    let second_capture = SandboxBackend::snapshot(&mut original).await?;
    assert_guest_file(&original, "/tmp/agentenv-lifecycle/base.txt", "base").await?;
    assert_guest_file(
        &original,
        "/tmp/agentenv-lifecycle/after-first.txt",
        "after-first",
    )
    .await?;
    let second_snapshot = publish_captured_snapshot_for_test(
        &snapshot_manager,
        &second_alias,
        source_sandbox_id,
        second_capture,
    )
    .await?;

    let first_runnable = snapshot_manager
        .resolve_runnable(first_snapshot.clone())
        .await?;
    let second_runnable = snapshot_manager
        .resolve_runnable(second_snapshot.clone())
        .await?;
    let mut children = Vec::new();
    for (index, runnable) in [&first_runnable, &first_runnable, &second_runnable]
        .into_iter()
        .enumerate()
    {
        let launch_config = SandboxLaunchConfig {
            sandbox_id: SandboxId::new(),
            snapshot_id: runnable.record().id.to_string(),
            env_vars: None,
            network: None,
            extra_mmds: serde_json::Map::new(),
            custom_extension_params: None,
            envd_access_token: None,
        };
        let mut child = FirecrackerSandbox::from_snapshot(runnable, &launch_config)?;
        child.start().await?;
        assert_guest_file(&child, "/tmp/agentenv-lifecycle/base.txt", "base").await?;
        if runnable.record().id == first_snapshot.id {
            assert_guest_path_absent(&child, "/tmp/agentenv-lifecycle/after-first.txt").await?;
        } else {
            assert_guest_file(
                &child,
                "/tmp/agentenv-lifecycle/after-first.txt",
                "after-first",
            )
            .await?;
        }
        write_guest_file(
            &child,
            &format!("/tmp/agentenv-lifecycle/child-{index}.txt"),
            "child",
        )
        .await?;
        children.push(child);
    }

    let paused_original = original.capture_immutable_checkpoint().await?;
    original.stop().await?;

    // RunnableSnapshot holds the shared repository lease through the
    // Firecracker/ublk start window. All children are fully started above, so
    // release those launch leases before deleting the source records.
    drop(first_runnable);
    drop(second_runnable);
    snapshot_manager.delete(&first_alias).await?;
    snapshot_manager.delete(&second_alias).await?;
    for child in &mut children {
        child.stop().await?;
    }
    drop(children);
    drop(first_snapshot);
    drop(second_snapshot);

    let mut resumed_original =
        FirecrackerSandbox::resume_from_snapshot_config(&paused_original).await?;
    assert_guest_file(
        &resumed_original,
        "/tmp/agentenv-lifecycle/base.txt",
        "base",
    )
    .await?;
    assert_guest_file(
        &resumed_original,
        "/tmp/agentenv-lifecycle/after-first.txt",
        "after-first",
    )
    .await?;
    resumed_original.stop().await?;
    Ok(())
}

#[tokio::test]
async fn temporal_runtime_can_publish_an_independent_reusable_snapshot() -> Result<()> {
    const TOKEN_ENV: &str = "AGENTENV_TEMPORAL_REUSABLE_TOKEN";
    const BEFORE_PATH: &str = "/tmp/agentenv-temporal-reusable-before.txt";
    const AFTER_PATH: &str = "/tmp/agentenv-temporal-reusable-after.txt";

    common::setup().await;
    let store = tempdir()?;
    let (_, snapshot_manager, _) = common::snapshot_test_parts(store.path());

    let mut sandbox_config = common::default_sandbox_config()?;
    sandbox_config.vcpu_count = 1;
    sandbox_config.mem_size_mib = 128;
    let mut source = FirecrackerSandbox::new(sandbox_config)?;
    source.start().await?;

    let alias = unique_alias_for_test("temporal_reusable");
    let token = format!("token-{alias}");
    let process = source
        .start_process(
            "sleep",
            &["1000000"],
            &ProcessOpts::new().with_envs(HashMap::from([(TOKEN_ENV.to_string(), token.clone())])),
        )
        .await?;
    let pid = process.pid();
    write_guest_file(&source, BEFORE_PATH, "before").await?;

    let artifact_root = store.path().join("temporal-paused-artifacts");
    let paused_state = SandboxBackend::pause(&mut source, Some(&artifact_root)).await?;
    let encoded = paused_state.encode()?;
    source.stop().await?;
    drop(process);
    drop(paused_state);

    let decoded = FirecrackerPausedState::decode(artifact_root.clone(), encoded)?;
    let mut source = FirecrackerSandbox::from_snapshot_config(decoded.snapshot_config())?;
    source.start().await?;
    drop(decoded);
    tokio::fs::remove_dir_all(&artifact_root)
        .await
        .with_context(|| {
            format!(
                "remove consumed temporal artifacts {}",
                artifact_root.display()
            )
        })?;

    write_guest_file(&source, AFTER_PATH, "after").await?;
    let sync = source.run_command("sync", &[]).await?;
    if sync.exit_code != 0 {
        bail!("sync Temporal source files failed: {}", sync.stderr);
    }
    let expected_files = HashMap::from([
        (BEFORE_PATH.to_string(), "before".to_string()),
        (AFTER_PATH.to_string(), "after".to_string()),
    ]);

    let captured = SandboxBackend::snapshot(&mut source).await?;
    let firecracker_capture = captured
        .downcast_ref::<FirecrackerCapturedSnapshot>()
        .context("expected Firecracker captured snapshot")?;
    let memory_image = overlaybd::config::load_image_config(
        &firecracker_capture.manifest().memory.image_config_path,
    )?;
    assert_eq!(
        memory_image.lowers.len(),
        1,
        "capturing a Temporal runtime should seal its complete memory into one immutable lower"
    );
    assert!(
        !overlaybd::config::validate_upper_config(&memory_image.upper)?,
        "published reusable memory must not retain a writable upper"
    );

    let source_sandbox_id = SandboxId::new();
    let snapshot =
        publish_captured_snapshot_for_test(&snapshot_manager, &alias, source_sandbox_id, captured)
            .await?;

    assert_guest_process_env(&source, pid, TOKEN_ENV, &token).await?;
    assert_expected_files(&source, &expected_files).await?;

    let runnable = snapshot_manager.resolve_runnable(snapshot.clone()).await?;
    let launch_config = SandboxLaunchConfig {
        sandbox_id: SandboxId::new(),
        snapshot_id: runnable.record().id.to_string(),
        env_vars: None,
        network: None,
        extra_mmds: serde_json::Map::new(),
        custom_extension_params: None,
        envd_access_token: None,
    };
    let mut child = FirecrackerSandbox::from_snapshot(&runnable, &launch_config)?;
    child.start().await?;
    assert_guest_process_env(&child, pid, TOKEN_ENV, &token).await?;
    let drop_caches = child
        .run_command("sh", &["-c", "sync && echo 3 > /proc/sys/vm/drop_caches"])
        .await?;
    if drop_caches.exit_code != 0 {
        bail!(
            "drop child page cache before reusable snapshot verification failed: {}",
            drop_caches.stderr
        );
    }
    assert_expected_files(&child, &expected_files).await?;

    child.stop().await?;
    source.stop().await?;
    drop(runnable);
    drop(snapshot);
    snapshot_manager.delete(&alias).await?;
    Ok(())
}

#[tokio::test]
async fn randomized_snapshot_lifecycle_operations_preserve_artifact_ownership() -> Result<()> {
    common::setup().await;
    let store = tempdir()?;
    let (builder, snapshot_manager, _) = common::snapshot_test_parts(store.path());
    let base_alias = unique_alias_for_test("random_lifecycle_base");
    let seed = 0xa6e5_2026_0505_0283;
    let mut rng = DeterministicRng::new(seed);
    let mut operation_log = Vec::new();

    let base_snapshot = builder
        .build_and_publish(
            &snapshot_manager,
            common::default_rootfs_template_build_spec()
                .alias(base_alias.clone())
                .resources(1, 128)
                .run(
                    "mkdir -p /tmp/agentenv-random && printf base > /tmp/agentenv-random/base.txt",
                ),
        )
        .await?;
    let mut expected_files = HashMap::new();
    expected_files.insert(
        "/tmp/agentenv-random/base.txt".to_string(),
        "base".to_string(),
    );
    let mut snapshots = vec![ScenarioSnapshot {
        alias: base_alias,
        record: base_snapshot,
        expected_files: expected_files.clone(),
        deleted: false,
        is_template: true,
        source_sandbox_id: None,
        child_starts: 0,
    }];
    let mut sandboxes = Vec::new();
    let mut coverage = LifecycleCoverage::default();
    operation_log.push(format!("seed={seed:#x}"));

    let step_count = 75;
    for step in 0..step_count {
        let mut operations = Vec::new();
        if !running_sandbox_indices(&sandboxes).is_empty() {
            push_weighted(&mut operations, RandomLifecycleOperation::WriteMarker, 3);
            push_weighted(
                &mut operations,
                RandomLifecycleOperation::CaptureSnapshot,
                8,
            );
            push_weighted(&mut operations, RandomLifecycleOperation::PauseSandbox, 5);
            push_weighted(&mut operations, RandomLifecycleOperation::DeleteSandbox, 1);
        }
        if !paused_sandbox_indices(&sandboxes).is_empty() {
            push_weighted(&mut operations, RandomLifecycleOperation::ResumeSandbox, 6);
            push_weighted(&mut operations, RandomLifecycleOperation::DeleteSandbox, 1);
        }
        if !live_snapshot_indices(&snapshots).is_empty() {
            push_weighted(&mut operations, RandomLifecycleOperation::StartSandbox, 8);
            if snapshots
                .iter()
                .filter(|snapshot| !snapshot.deleted)
                .count()
                > 1
            {
                push_weighted(
                    &mut operations,
                    RandomLifecycleOperation::DeleteSnapshotOrTemplate,
                    4,
                );
            }
        }
        let operation = operations[rng.index(operations.len())];
        operation_log.push(format!("step {step}: selected {operation:?}"));

        let step_result: Result<()> = async {
            match operation {
                RandomLifecycleOperation::WriteMarker => {
                    let candidates = running_sandbox_indices(&sandboxes);
                    let sandbox_index = candidates[rng.index(candidates.len())];
                    let sandbox_name = sandboxes[sandbox_index].name.clone();
                    let scenario = &mut sandboxes[sandbox_index];
                    let ScenarioSandboxRuntime::Running(sandbox) = &scenario.runtime else {
                        unreachable!("candidate must be running");
                    };
                    let path = format!("/tmp/agentenv-random/{sandbox_name}-step-{step}.txt");
                    let contents = format!("{sandbox_name}-step-{step}");
                    write_guest_file(sandbox, &path, &contents).await?;
                    scenario.expected_files.insert(path, contents);
                    assert_expected_files(sandbox, &scenario.expected_files).await?;
                    operation_log.push(format!(
                        "step {step}: completed write marker in {sandbox_name}"
                    ));
                    Ok(())
                }
                RandomLifecycleOperation::CaptureSnapshot => {
                    let candidates = running_sandbox_indices(&sandboxes);
                    let sandbox_index = candidates[rng.index(candidates.len())];
                    let alias = unique_alias_for_test(&format!("random_lifecycle_capture_{step}"));
                    let sandbox_name = sandboxes[sandbox_index].name.clone();
                    let source_sandbox_id = sandboxes[sandbox_index].sandbox_id;
                    let expected_files_at_capture = sandboxes[sandbox_index].expected_files.clone();
                    let ScenarioSandboxRuntime::Running(sandbox) =
                        &mut sandboxes[sandbox_index].runtime
                    else {
                        unreachable!("candidate must be running");
                    };
                    let captured = SandboxBackend::snapshot(sandbox.as_mut()).await?;
                    assert_expected_files(sandbox, &expected_files_at_capture).await?;
                    let record = publish_captured_snapshot_for_test(
                        &snapshot_manager,
                        &alias,
                        source_sandbox_id,
                        captured,
                    )
                    .await?;
                    sandboxes[sandbox_index].captures += 1;
                    if sandboxes[sandbox_index].captures >= 2 {
                        coverage.repeated_capture_same_sandbox = true;
                    }
                    snapshots.push(ScenarioSnapshot {
                        alias,
                        record,
                        expected_files: expected_files_at_capture,
                        deleted: false,
                        is_template: false,
                        source_sandbox_id: Some(source_sandbox_id),
                        child_starts: 0,
                    });
                    operation_log.push(format!("step {step}: completed capture {sandbox_name}"));
                    Ok(())
                }
                RandomLifecycleOperation::StartSandbox => {
                    let candidates = live_snapshot_indices(&snapshots);
                    let snapshot_index = candidates[rng.index(candidates.len())];
                    let runnable = snapshot_manager
                        .resolve_runnable(snapshots[snapshot_index].record.clone())
                        .await?;
                    let sandbox_id = SandboxId::new();
                    let launch_config = SandboxLaunchConfig {
                        sandbox_id,
                        snapshot_id: runnable.record().id.to_string(),
                        env_vars: None,
                        network: None,
                        extra_mmds: serde_json::Map::new(),
                        custom_extension_params: None,
                        envd_access_token: None,
                    };
                    let mut sandbox = FirecrackerSandbox::from_snapshot(&runnable, &launch_config)?;
                    sandbox.start().await?;
                    assert_expected_files(&sandbox, &snapshots[snapshot_index].expected_files)
                        .await?;
                    snapshots[snapshot_index].child_starts += 1;
                    if !snapshots[snapshot_index].is_template {
                        coverage.child_started_from_captured_snapshot = true;
                    }
                    let name = format!("sandbox-{step}");
                    sandboxes.push(ScenarioSandbox {
                        name: name.clone(),
                        sandbox_id,
                        runtime: ScenarioSandboxRuntime::Running(Box::new(sandbox)),
                        expected_files: snapshots[snapshot_index].expected_files.clone(),
                        captures: 0,
                        ever_paused_after_capture: false,
                        captured_snapshot_deleted_after_child_start: false,
                    });
                    operation_log.push(format!(
                        "step {step}: completed start {name} from snapshot {snapshot_index}"
                    ));
                    Ok(())
                }
                RandomLifecycleOperation::PauseSandbox => {
                    let candidates = running_sandbox_indices(&sandboxes);
                    let sandbox_index = candidates[rng.index(candidates.len())];
                    let mut scenario = sandboxes.swap_remove(sandbox_index);
                    let ScenarioSandboxRuntime::Running(mut sandbox) = scenario.runtime else {
                        unreachable!("candidate must be running");
                    };
                    let paused = sandbox.capture_immutable_checkpoint().await?;
                    sandbox.stop().await?;
                    if scenario.captures > 0 {
                        scenario.ever_paused_after_capture = true;
                        coverage.captured_sandbox_paused = true;
                    }
                    let name = scenario.name.clone();
                    scenario.runtime = ScenarioSandboxRuntime::Paused(Box::new(paused));
                    sandboxes.push(scenario);
                    operation_log.push(format!("step {step}: completed pause {name}"));
                    Ok(())
                }
                RandomLifecycleOperation::ResumeSandbox => {
                    let candidates = paused_sandbox_indices(&sandboxes);
                    let sandbox_index = candidates[rng.index(candidates.len())];
                    let mut scenario = sandboxes.swap_remove(sandbox_index);
                    let ScenarioSandboxRuntime::Paused(paused) = scenario.runtime else {
                        unreachable!("candidate must be paused");
                    };
                    let sandbox =
                        FirecrackerSandbox::resume_from_snapshot_config(paused.as_ref()).await?;
                    assert_expected_files(&sandbox, &scenario.expected_files).await?;
                    if scenario.ever_paused_after_capture
                        && scenario.captured_snapshot_deleted_after_child_start
                    {
                        coverage.captured_sandbox_resumed_after_snapshot_delete = true;
                    }
                    let name = scenario.name.clone();
                    scenario.runtime = ScenarioSandboxRuntime::Running(Box::new(sandbox));
                    sandboxes.push(scenario);
                    operation_log.push(format!("step {step}: completed resume {name}"));
                    Ok(())
                }
                RandomLifecycleOperation::DeleteSandbox => {
                    // Deleting a sandbox is valid from either running or paused state,
                    // so choose from the full tracked sandbox set.
                    let sandbox_index = rng.index(sandboxes.len());
                    let scenario = sandboxes.swap_remove(sandbox_index);
                    let name = scenario.name;
                    match scenario.runtime {
                        ScenarioSandboxRuntime::Running(mut sandbox) => sandbox.stop().await?,
                        ScenarioSandboxRuntime::Paused(_) => {}
                    }
                    operation_log.push(format!("step {step}: completed delete sandbox {name}"));
                    Ok(())
                }
                RandomLifecycleOperation::DeleteSnapshotOrTemplate => {
                    let candidates = live_snapshot_indices(&snapshots);
                    let snapshot_index = candidates[rng.index(candidates.len())];
                    snapshot_manager
                        .delete(&snapshots[snapshot_index].alias)
                        .await?;
                    if !snapshots[snapshot_index].is_template
                        && snapshots[snapshot_index].child_starts > 0
                    {
                        coverage.captured_snapshot_deleted_after_child_start = true;
                        if let Some(source_sandbox_id) = snapshots[snapshot_index].source_sandbox_id
                        {
                            for sandbox in &mut sandboxes {
                                if sandbox.sandbox_id == source_sandbox_id {
                                    sandbox.captured_snapshot_deleted_after_child_start = true;
                                }
                            }
                        }
                    }
                    snapshots[snapshot_index].deleted = true;
                    operation_log.push(format!(
                        "step {step}: completed delete snapshot {snapshot_index}"
                    ));
                    Ok(())
                }
            }
        }
        .await;

        step_result.with_context(|| {
            format!(
                "random lifecycle operation failed with seed {seed:#x}; steps:\n{}",
                operation_log.join("\n")
            )
        })?;
    }

    for sandbox in sandboxes {
        match sandbox.runtime {
            ScenarioSandboxRuntime::Running(mut sandbox) => {
                sandbox.stop().await.with_context(|| {
                    format!(
                        "random lifecycle cleanup failed with seed {seed:#x}; steps:\n{}",
                        operation_log.join("\n")
                    )
                })?;
            }
            ScenarioSandboxRuntime::Paused(_) => {}
        }
    }
    for snapshot in snapshots {
        if snapshot.deleted {
            continue;
        }
        snapshot_manager
            .delete(&snapshot.alias)
            .await
            .with_context(|| {
                format!(
                    "random lifecycle snapshot cleanup failed with seed {seed:#x}; steps:\n{}",
                    operation_log.join("\n")
                )
            })?;
    }

    assert!(
        coverage.repeated_capture_same_sandbox,
        "random lifecycle seed {seed:#x} did not capture the same sandbox twice; steps:\n{}",
        operation_log.join("\n")
    );
    assert!(
        coverage.child_started_from_captured_snapshot,
        "random lifecycle seed {seed:#x} did not start a child from a captured snapshot; steps:\n{}",
        operation_log.join("\n")
    );
    assert!(
        coverage.captured_snapshot_deleted_after_child_start,
        "random lifecycle seed {seed:#x} did not delete a captured snapshot after child start; steps:\n{}",
        operation_log.join("\n")
    );
    assert!(
        coverage.captured_sandbox_paused,
        "random lifecycle seed {seed:#x} did not pause a sandbox after capture; steps:\n{}",
        operation_log.join("\n")
    );
    assert!(
        coverage.captured_sandbox_resumed_after_snapshot_delete,
        "random lifecycle seed {seed:#x} did not resume a captured-then-paused sandbox after snapshot delete; steps:\n{}",
        operation_log.join("\n")
    );

    Ok(())
}

fn unique_alias_for_test(prefix: &str) -> String {
    format!(
        "{prefix}_{:x}{:x}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}
