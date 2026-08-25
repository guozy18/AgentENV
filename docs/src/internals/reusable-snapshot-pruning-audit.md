# Reusable Snapshot / Temporal 分支精简审查记录

> 状态：工作区审查记录，2026-08-26
>
> 基线：`origin/main`
>
> 当前分支：`feat/agentic-rl-snapshot-lifecycle`
>
> 目的：记录本分支中被删除或暂不保留的机制、删除边界、必须保留的不变量，以及未来什么证据可以重新引入某个机制。本文不把当前分支既有实现视为设计真相。

## 1. 审查边界

本记录覆盖 reusable snapshot、Local/Distributed promotion、Gateway placement、POSIX/OSS repository、OverlayBD runtime 和 Temporal pause/resume。Temporal pause/resume 仍表示同一个 sandbox 的 durable continuation；它不是新的 reusable snapshot 类型。

本轮没有修改以下两个受保护文件：

- `docs/src/SUMMARY.md`
- `docs/src/internals/snapshot-lifecycle-architecture.md`

本记录是独立新增的审查文档，不通过修改受保护文档来记录决策。

本阶段受保护文档清单：

- `docs/src/SUMMARY.md`
- `docs/src/internals/snapshot-lifecycle-architecture.md`
- `docs/src/internals/reusable-snapshot-defense-reproduction-cases.md`

复现手册中的场景、故障注入点和证据门槛属于审查输入；后续清理代码时不得为了让实现更容易删除而改写这些记录。

本轮继续精简（未提交）：删除只验证缓存取消/失败内部状态、Gateway header 投影、durable rollback 阻塞顺序、OSS 手工 Local promotion 和固定 sibling 清理的测试；移除未被实际调用的 `get_record`/`delete_by_id` repository API 以及 OSS 中未使用的 managed-layer canonicalize helper。保留 Local owner/commit marker、promotion 保留本地闭包、Distributed 不 fallback、Temporal 物理闭包和缓存 pin 等产品边界测试。

## 2. 当前基线和产品不变量

审查以 `origin/main` 的调用链和职责为唯一基线。当前分支历史上的功能提交包括：

```text
0e2720b  reusable snapshot lifecycle
2e367d6  routing and artifact hardening
34cdbbe  durable Temporal continuation
9dbe52f  lifecycle/test simplification
92fde59  local artifact lifecycle simplification
c4ff210  redundant lifecycle state removal
b80feee  proxy lifecycle drain removal
```

必须继续保持的产品语义：

1. Local snapshot 是 Pod-scoped 的物理闭包。Pod 被删除后 Local 可以直接不可达，不做新 Pod owner rebind。
2. Local snapshot 不允许 alias。
3. Local launch 必须精确路由到 canonical record 中的 owner；owner 不可用时 fail-closed，返回 503，不 fallback 到其他节点。
4. Distributed snapshot 的 canonical metadata 和可跨节点 artifact 由 primary repository/OSS 持有。
5. Local capture 先完成 node-local physical closure，再提交 canonical Local record；canonical record 不能在物理闭包完成前声明可用。
6. Local → Distributed promotion 保持 snapshot ID；promotion 成功提交前不能丢失 Local 恢复能力。
7. canonical metadata 只有一个权威来源；local physical artifact 和 runtime-resolved `RunnableSnapshot` 不是第二份 metadata authority。
8. Temporal pause/resume 保存同一个 sandbox 的 mutable continuation，不增加 `SnapshotType::Temporal`、alias 或 reusable snapshot API。
9. 同一 snapshot ID 没有并发写者；客户端超时重试使用新的 snapshot ID；promotion 是偶发操作。

## 3. 删除决策台账

| 机制 | 与 `origin/main` 的关系 | 本轮处理 | 删除边界 / 保留边界 | 重新引入所需证据 |
|---|---|---|---|---|
| `SandboxGate`、proxy read guard、pause/snapshot drain | 当前分支新增；`origin/main` 使用 route-first，不等待在途 proxy | 已删除 | 删除 gate、HTTP/WS body guard、pause/snapshot 等待和专用测试；保留普通 route/state CAS | 真实数据面并发导致 pause 产生可复现的数据损坏，且不能由已有 runtime 生命周期解决 |
| `SourceStateStrategy::Clone` | 当前分支为 Temporal resume 新增；`origin/main` 没有 mutable continuation 隔离问题 | 已删除 | runtime device 恢复统一使用原有 Reuse；删除 upper clone、lower adopt、`source-lowers/`、hard-link/reflink/copy 分支及协议字段；保留 Temporal memory/rootfs/drive continuation | 故障注入证明 resume 失败会修改旧 paused upper/lower，且旧 generation 在失败后必须可重试；并证明只 clone 必要的 mutable 部分 |
| dirty-memory sparse/full fallback 状态机 | `origin/main` 有 dirty-range capture，但失败直接返回；当前分支新增 fallback、consumed parent 和 full 状态 | 已删除 | immutable capture 保留直接 `diff → dirty ranges → OverlayBD`；删除 sparse fallback、`needs_full_memory_snapshot`、`consumed_memory_snapshot_config_path` 和相关故障测试；Temporal pause 仍保存完整 regular memory file | Moonshot/KVM 运行或故障注入证明 dirty-range API 在目标环境会失败，并且 sparse/full fallback 能恢复而不发布不完整 snapshot |
| POSIX repository-wide shared/exclusive lock | 当前分支新增；`origin/main` 没有全局 lock/lease | 已删除 | 删除 `repository.lock`、publish/delete 全局锁和 runtime resolver 全局 lease；保留 per-record lock、alias lock、原子文件写入；暂不做 managed-layer GC | 真实并发测试证明跨 snapshot 删除会破坏正在使用的 artifact，并且 per-record lock 无法覆盖该故障 |
| `SnapshotStore` / `LocalSnapshotStore` wrapper | 当前分支新增；只包装 repository/resolver/artifact，没有独立生命周期语义 | 已删除 | manager 直接持有 canonical repository、distributed resolver、local artifact store、local resolver | 只有在出现第二个真实 store 实现且共享行为不适合直接字段时才重新抽象 |
| Template/Sandbox 专用 manager API | `SnapshotSource` 在 main 已存在；本分支新增多套 API 层重复过滤 | 已删除 | 统一使用 `get`、`delete`、`load_runnable`、`resolve_committed_alias`；保留 `SnapshotSource` 数据模型和 repository filter 能力 | 真实脏数据或公共 API 误用证明 generic API 会把资源类型错误地暴露给另一类 endpoint |
| Gateway placement 外围防御 | placement/owner routing 是当前分支新增产品语义；部分 header/error/size 防御是附加层 | 部分删除 | 保留 Schedule → placement → owner `GetNode` → READY → owner-only route → 503；删除非认证 tracing header 转发和 8 KiB placement response 限制 | 真实安全审计或链路追踪要求证明这些 header/size wrapper 是必要边界 |
| ACR manifest digest HEAD/GET retry | 当前分支新增；产品约束是同 ID 无并发写者、超时重试用新 ID | 已删除并恢复简单 tag-exists 检查 | 保留 `origin/main` 的 tag 已存在即拒绝；删除 HEAD/GET body hash、同 digest retry 和 fake-server 矩阵 | 外部调用方实际复用同一 snapshot ID，且 registry PUT 超时会造成可见的错误重试问题 |
| ublk pool 参数条件变化 | 与 snapshot 无关的分支改动 | 已恢复 `origin/main` | pool 参数逻辑回到原有 `app_config.is_none()` 条件 | 独立的 ublk 配置 bug/测试证明需要另开变更 |
| disk rate limiter reconciliation | 对比后确认 `origin/main` 已经存在，不是本分支新增 | 保留 | 不作为本 PR 的精简对象 | 另一个独立限流需求，不应与 snapshot 生命周期混改 |
| Local physical closure → canonical commit | 当前分支新增，但直接对应 canonical authority 不变量 | 保留 | 先写本地文件和 commit marker，再 `commit_record`；OSS 失败时保留本地闭包 | 只有产品改成允许 canonical record 指向远端异步状态时才重新设计 |
| owner READY / exact owner / fail-closed | 当前 Local snapshot 产品语义新增 | 保留核心 | 不允许 owner replacement fallback；可继续简化外围错误包装和 endpoint 重复校验 | 明确改变 Local 可迁移或可 fallback 的产品语义 |
| OverlayBD `CacheHandle` pin | cache 是 node-local derived state；pin 保护运行时仍使用的 image config | 暂不删除 | 保留运行时使用期间不被 LRU 删除；不新增 cache policy、复制或 GC | 真实磁盘压力、命中率和 eviction 故障数据证明当前 pin 机制过重或不必要 |
| 缓存取消/失败、Gateway header、rollback 阻塞顺序等测试矩阵 | 只验证旧实现的内部时序或 mock 状态，不对应已确认的用户契约 | 已删除 | 保留缓存基本去重/evict、owner 路由和实际 resume 失败结果；不保留内部 barrier、header 投影和 partial mock 矩阵 | 真实故障注入证明简化实现产生用户可见的数据损坏、错误路由或不可恢复 |
| `get_record` / `delete_by_id` repository API | 为 UUID 形态 alias 的假设场景增加接口和双实现；当前 manager 已持有 canonical record，产品约束没有该调用 | 已删除 | manager 使用已有 `get`/`delete`；若未来出现跨仓库精确删除需求，应先提供真实 alias 冲突案例 | 真实调用链出现 UUID alias 与同 ID 删除歧义，且无法在 repository 内用已有解析结果解决 |

## 4. 最终简化后的关键调用链

### Local capture

```text
capture sandbox state
  → local POSIX physical store 写入固定 artifact / managed layer
  → local commit marker
  → primary repository commit_record(canonical Local metadata)
```

### Local launch

```text
Gateway Schedule
  → placement 查询得到 snapshotType + ownerNodeID
  → Scheduler.GetNode(ownerNodeID)
  → READY + endpoint 检查
  → 只转发到 owner；失败返回 503
  → owner node 读取 canonical record
  → local runtime resolver 解析本节点 physical closure
  → build/start sandbox
```

### Distributed promotion

```text
canonical Local record
  → owner resolver 解析 local closure
  → primary/OSS publish 同一 snapshot ID
  → canonical record 变为 Distributed
  → promotion 成功前保留 Local closure
```

### Temporal pause/resume

```text
pause
  → Firecracker pause
  → 保存 vm state、regular memory、mutable rootfs、mutable drives
  → persist_paused

resume
  → 读取同一个 sandbox 的 paused state
  → 使用现有 runtime materialization（不再额外 Clone/adopt source lower）
  → start/wait ready
```

## 5. 当前仍保留但需要未来证据的机制

以下机制没有在本轮删除，不代表已经证明它们是最终形态：

1. Temporal mutable memory 的 full checkpoint 和原子 generation 替换。它是 pause/resume 可恢复性的直接实现，但还需要 Linux/Firecracker 故障注入验证失败后的 artifact 状态。
2. `classify_after_live_mutation` 和 terminal capture failure。它们防止 live OverlayBD 已被 restack 后又把运行时当成可安全恢复的 Running 状态；需要真实 restack 失败证据验证错误粒度。
3. Local runtime resolver 对 commit marker、固定 artifact 和 managed layer 的完整检查。Local 删除后不可达的语义允许缺失，但在 record 仍可见时不能返回不可运行的 runtime。
4. Local/Distributed record transition 校验、CAS/原子写入和 alias lock。它们直接保护唯一 canonical metadata 和 promotion identity。
5. `LocalArtifactCache` 的 per-key lock、atomic staging、pin/evict。当前没有性能证据，不应进一步扩大，也不应在没有 eviction 故障证据时重建复杂 GC。

未来若要恢复被删除机制，必须先提供：

- 可重放的故障序列，而不是只验证当前实现自己的状态机；
- 失败前后 artifact、record、runtime path 的实际状态；
- 证明已有层无法解决该故障的原因；
- focused regression test；
- Linux/KVM/Firecracker/ublk/OSS 或 Moonshot-test 的运行证据（如果问题属于这些环境）。

## 6. 验证记录

本轮已执行：

- `cargo fmt --all -- --check`：通过；
- `git diff --check`：通过；
- `cargo metadata --no-deps --format-version 1`：通过；
- `go test ./...`（`services/`）：通过；
- Gateway/Scheduler Go tests：通过；此前一次 Gateway 运行曾受限于 `httptest.NewServer` 监听权限，但重跑 `go test ./...` 已通过；
- Rust 完整编译：macOS 原生目标缺少 Linux `io-uring`/ublk；交叉编译目标另缺 `x86_64-linux-gnu-gcc`、Linux OpenSSL/zstd/lz4 sysroot，未将环境失败伪装成代码通过；
- 残留扫描：已删除的 Clone、dirty fallback、repository-wide lock、source-specific manager wrapper 和 proxy drain 标识不再存在。

本轮额外删除的低价值测试/逻辑包括：缓存取消和失败的内部状态矩阵、Gateway placement header 投影测试、durable rollback 阻塞顺序测试、OSS 手工构造 Local promotion E2E，以及未被实际调用的 `get_record`/`delete_by_id` 接口。保留了缓存去重/pin、owner fail-closed、Local commit marker、promotion 保留本地闭包和实际 Temporal/Firecracker 数据路径测试。

当前工作区变化（相对上一提交 `b80feee`，含本轮未提交清理）：

```text
+292 / -2104
```

当前相对 `origin/main` 的有效 diff：

```text
+6396 / -2400
```

本记录和代码清理均未提交、未 push。两个受保护文档未被修改、暂存、提交、覆盖或删除。
