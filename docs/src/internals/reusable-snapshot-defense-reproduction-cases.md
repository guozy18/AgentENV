# 已删除防御机制的真实故障复现手册

> 状态：待验证，2026-08-26
>
> 基线：`origin/main` (`39bfa34`)
>
> 当前分支：`feat/agentic-rl-snapshot-lifecycle`
>
> 目的：记录本 PR 中已删除、但曾用于防御某类故障的代码，以及如何用真实故障注入判断该故障是否确实存在。本文不是当前实现说明，也不以旧单元测试通过作为恢复代码的证据。

本文与 `docs/src/SUMMARY.md`、`docs/src/internals/snapshot-lifecycle-architecture.md` 一起列为本阶段受保护审查文档：后续 agent 可以追加复现结果，但不得删除、覆盖或为了迁就当前实现而改写既有记录。

## 1. 使用方法和证据门槛

相关历史节点：

```text
34cdbbe  durable Temporal continuation 的初始实现
c4ff210  仍包含 SandboxGate、paused artifact 独立性和额外 durable fence
b80feee  已删除 proxy drain 和一批 durable cleanup 状态矩阵
当前工作区  继续删除 Clone、dirty fallback、全局 POSIX lease 等候选机制
```

建议在独立 worktree 中对比，不要切换或覆盖当前脏工作区：

```bash
git worktree add /tmp/agentenv-defense-c4ff210 c4ff210
git worktree add /tmp/agentenv-defense-b80feee b80feee
```

每个复现至少保留以下证据：

1. 精确的 commit、配置、Firecracker/ublk/OverlayBD/内核版本和部署形态；
2. 故障注入发生前后的 API 返回值、进程状态和日志时间线；
3. canonical record、paused record、artifact root、runtime directory 的路径清单、大小、inode 和 digest；
4. 节点进程重启后的实际可恢复性；
5. 简化实现和旧防御实现使用相同输入、相同故障点的对照结果；
6. 一个只保护用户可见契约的 focused regression test。

以下结果不足以证明旧机制必要：

- mock 只证明某个函数按旧状态机顺序被调用；
- 测试直接持有旧实现内部的 guard，再断言另一个 future 被阻塞；
- 只断言 VM 启动成功，不验证 guest memory、rootfs 和 attached drive 数据；
- 只触发错误分支，没有证明简化实现造成额外的数据损坏、错误路由或不可恢复；
- 为了触发故障而违反已确认的产品不变量，例如人为制造同一 snapshot ID 的并发写者。

当前统一结论是：这些场景的触发条件有些可构造，但真实危害尚未验证，因此机制处于 **QUARANTINE**。在达到各节的“恢复门槛”前，不应把旧实现或旧测试直接加回。

## 2. `SandboxGate` 和 pause/snapshot proxy drain

### 旧代码防御的场景

历史位置：`c4ff210` 的 `src/orchestrator/service.rs`、`src/api/proxy.rs` 和 `src/orchestrator/tests.rs`。

旧实现认为存在以下竞态：

```text
请求 R 在 sandbox 仍为 Running 时取得 proxy route
  → pause/snapshot CAS 把 metadata 改成 Pausing/Snapshotting
  → CAS 阻止之后的新请求，但 R 已经进入数据面
  → pause/snapshot 在 R 仍修改 guest state 时保存 memory/disk
```

`SandboxGate.data_plane` 的 read guard 跟随 HTTP body 或 WebSocket 生命周期；pause/snapshot 取得 write guard，等所有已准入请求退出后才操作 VM。旧测试
`pause_drains_in_flight_proxy_and_releases_gate_after_recoverable_failure` 只证明了这把锁会等待，并没有证明不等待会产生真实故障。

### 复现步骤

1. 在 guest 中启动一个测试服务。请求进入后先持久化 `phase=entered`，随后阻塞在用户控制的 barrier；barrier 释放后再同时修改一段内存 token 和 rootfs 文件，最后返回响应。
2. 通过真实 Gateway/node proxy 发起请求 R，确认 guest 已写入 `phase=entered`，但不要释放 barrier。
3. 并发调用 pause；另做一轮调用 reusable snapshot capture。
4. 在简化实现中确认 pause/capture 已经开始操作 Firecracker，而 R 尚未完成。
5. 释放 barrier，记录 R 是完成、断连还是超时；随后 resume/launch 捕获结果。
6. 在 guest 中同时校验 memory token、rootfs 文件和请求幂等标记，不能只检查 VM ready。
7. 在 `c4ff210` 重复同一序列，确认差异是否确实来自 drain。
8. 对普通 HTTP、上传中的 request body 和长寿命 WebSocket 分开测试。WebSocket 还要记录旧 drain 是否会无限阻止 pause。

### 什么才算真实故障

- pause/capture 返回成功，但恢复结果包含无法由请求提交点解释的 memory/disk 混合状态；
- 产品契约明确保证已经准入的请求必须完成，而简化实现稳定地丢失该请求；
- 该问题不能由现有 route detach、sandbox state CAS 或 Firecracker pause 原子性解决。

单纯“pause 期间在途请求收到断连”不自动算故障；如果 pause 的契约允许数据面请求失败，drain 没有必要。恢复代码前还必须给长寿命连接定义上限，不能重新引入无界等待。

### 最小恢复方向

若场景成立，先验证“CAS 后拒绝新请求 + 对短请求做有界 drain”是否足够。不要直接恢复 per-sandbox 双锁、HTTP/WS body guard、auto-resume follower 和 delete completion 的完整矩阵。

## 3. POSIX repository-wide lock 和 runtime lease

### 旧代码防御的场景

历史位置：`b80feee` 的：

- `src/snapshot/repository/backends/posixfs/layout.rs`：`repository.lock`；
- `catalog.rs`：publish/commit 取得 shared lock，delete 取得 exclusive lock；
- `runtime.rs`：`PosixRuntimeArtifactLease` 在 `RunnableSnapshot` 生命周期内持有 shared lock；
- `backend.rs`：`resolved_runnable_lease_blocks_delete_until_drop`。

它防御的是 resolver 返回的路径仍被 runtime 使用时，delete 或未来 GC 把这些路径删掉。问题在于锁是 repository-wide：S1 的 runnable 会阻塞完全无关的 S2 删除，并把“已 resolve 一个对象”近似成“真实 runtime 仍在使用 artifact”。

### 复现步骤

1. 在同一 POSIX root 发布 S1、S2，记录两个 record 和 artifact closure 的路径、inode、digest。
2. resolve S1 并保持 `RunnableSnapshot` 存活，但不启动 VM；并发删除 S2。
3. 在 `b80feee` 验证 S2 是否被 S1 的 lease 阻塞；在简化实现验证 S2 是否能立即删除。
4. 接着从 S1 真正启动 OverlayBD/ublk 和 Firecracker，在 guest 内持续随机读 rootfs/drive，并记录校验和。
5. runtime 运行期间删除 S1 的 record/artifact；继续做读写，再重启 ublk daemon 或触发一次需要重新按路径打开文件的动作。
6. 分别测试“只删 record”“删 snapshot closure”“删 managed layer”。不要用 repository 全清来代替 per-record delete。
7. runtime 停止后重复删除，确认 per-record lock 和原子写入仍能保护 publish/delete 同一 ID 的并发。

### 什么才算真实故障

- 删除 S1 后，已经运行的 S1 出现 I/O 错误、数据校验失败，或 ublk reconnect 无法重新打开必要路径；
- S1 和 S2 共享同一个物理 managed layer，删除 S2 会破坏 S1，并且引用关系没有其他 owner；
- 该故障在当前明确支持的 delete/GC 调用路径中可达，而不是尚未实现的未来 GC。

旧测试只证明 runnable 的任意生命周期会挡住全仓删除，不证明这个粒度正确。如果故障只影响 S1，恢复方向应是 per-snapshot 或 per-layer pin；不得据此恢复 repository-wide lease。

## 4. Temporal resume 的 `SourceStateStrategy::Clone`

### 旧代码防御的场景

历史位置：`b80feee` 的 Firecracker resume、extra drive、ublk protocol/runtime。

Temporal resume 曾对 rootfs/attached drive 选择 `SourceStateStrategy::Clone`：

- clone writable upper 的 data/index，避免新 runtime 直接修改 paused generation；
- 把 paused artifact root 内的 lower 收养到新 runtime 的 `source-lowers/`；
- 外部 immutable lower 继续引用原路径；
- resume 成功后即使旧 generation 被清理，新 runtime 仍不依赖它；
- resume 在 `wait_for_ready` 等后期失败时，旧 paused generation 理论上仍可重试。

删除后的简化实现统一走已有 Reuse 路径。需要验证的不是“clone 出来的 inode 不同”，而是 Reuse 是否真的会破坏 retry 或成功后的运行。

### 复现步骤

1. 在 Linux/KVM 环境创建 sandbox；向 rootfs 和每个 attached drive 写入不同 token，并运行一个持续校验 memory token 的 guest 进程。
2. pause，记录 paused generation 中所有 upper data/index、source-owned lower、image config 和 memory/state 文件的 inode、size、digest。
3. 开始 resume，在 device materialization 完成之后、sandbox ready 之前注入失败。优先使用真实故障：让 envd readiness 超时、在正确时刻终止 Firecracker，或让网络初始化失败；不要只让 mock `WaitForReady` 返回错误。
4. 失败后再次记录 paused generation，确认哪些文件被修改、截断、移动或删除。
5. 不重新 pause，直接 retry resume；在 guest 中校验 rootfs、drive、memory token，并执行实际读写。
6. 做另一轮成功 resume：成功路径清理旧 paused generation后，继续运行 guest I/O，再重启 ublk daemon，检查 runtime config 是否仍引用已删除的旧 generation。
7. 在 `b80feee` 的 Clone 版本重复相同故障点，对比旧 paused generation 和 retry 结果。

### 什么才算真实故障

- 失败的 resume 修改了唯一 paused upper，导致第二次 resume 丢数据或不可启动；
- 成功 resume 清理旧 generation 后，正在运行的 ublk/OverlayBD 仍需要按旧路径重开文件并失败；
- source-owned lower 的生命周期确实短于 live runtime，且现有 open-file 语义或 cache pin 不能覆盖。

只有 inode 不同、旧测试能写坏 clone、或理论上“隔离更安全”都不够。如果只有某一种 mutable upper 需要隔离，最小修复应只处理它，不能恢复跨 protocol、rootfs、drive 和 lower adoption 的完整 strategy。

## 5. Firecracker dirty-memory sparse/full fallback

### 旧代码防御的场景

历史位置：`b80feee` 的 `src/sandbox/firecracker/sandbox.rs` 和 `tests/integration/fc.rs`。

旧状态机覆盖两类担忧：

1. 自定义 `/vm/dirty-memory-ranges` 不可用时，改走 Firecracker 标准 sparse diff memory file，再转 OverlayBD；
2. 标准 diff 已经消费 dirty bitmap、但后续转换或持久化失败时，把下一次 checkpoint 强制为 Full，避免 retry 漏掉已消费的 dirty pages。

相关状态包括 `needs_full_memory_snapshot`、`consumed_memory_snapshot_config_path`、parent config 选择和 `diff_failure_round` 故障矩阵。简化实现中 immutable capture 的 dirty-range 请求或转换失败会直接失败；Temporal pause 每次保存完整 regular memory file。

### 复现步骤

1. 在 guest 内启动多个进程，每个进程在不同内存页保存随机 token，并持续输出 token hash；同时修改 rootfs，避免只验证 memory。
2. 对 immutable capture 分别注入：
   - `/vm/dirty-memory-ranges` 返回 404/500 或连接中断；
   - dirty ranges 返回成功，但读 `/proc/<firecracker-pid>/mem` 或 OverlayBD conversion 中途失败；
   - artifact 已生成但 publish/commit 前失败。
3. 对每个故障点记录 Firecracker dirty bitmap 是否已经被消费，然后恢复依赖并重试一次 capture。
4. launch retry 成功的 snapshot，逐一核对所有 guest token；不能只看 API 200 或 VM ready。
5. 另测 Temporal pause 的 full regular memory 路径，确认它不再依赖这套 immutable sparse fallback。
6. 在 `b80feee` 启用旧 fallback 重复测试，比较它是否真正把失败变成正确结果，而不是发布能启动但缺页的 snapshot。
7. 在 Moonshot-test 记录目标 Firecracker 版本是否实际支持 dirty-range endpoint；若所有生产版本都支持，404 compatibility fallback 的前提不成立。

### 什么才算真实故障

- 目标生产 Firecracker 确实缺少或不稳定地提供 dirty-range endpoint，而产品要求 capture 自动兼容，不能把失败返回给调用方；
- 简化实现第一次失败后，第二次 capture 返回成功但稳定漏页；
- 旧 full/sparse fallback 在同一故障点能恢复全部 token，且不会发布不完整 artifact。

如果简化实现明确失败、retry 后数据完整，则额外 fallback 没有价值。若只有“bitmap 已消费后 retry”成立，应只修复该消费边界，不恢复所有 parent/fallback 状态。

## 6. Template/Sandbox source-specific manager API

### 旧代码防御的场景

历史位置：`b80feee` 的 `src/snapshot/manager.rs` 及 template/snapshot API 调用点。

已删除的 API 包括 `get_template`、`get_sandbox_snapshot`、`delete_template`、`resolve_template_alias`、`load_template_alias_runnable` 和内部 `get_matching`。它们防御 Template record 和 Sandbox record 共用 canonical repository/ID/alias namespace 时，一个产品 API 误读、误删或启动另一类 source：

```text
GET /templates/{sandbox-snapshot-id} 返回 sandbox snapshot
DELETE /templates/{sandbox-snapshot-id} 删除 sandbox snapshot
snapshot API 把 Template record 当成 sandbox capture
alias 指向错误 source，遮蔽预期资源
```

简化后 manager 使用通用 `get`、`delete`、`resolve_committed_alias` 和 `load_runnable`；`SnapshotSource` 仍保留在 canonical record 中。

### 复现步骤

1. 在同一 repository 创建 Template record T（带 alias A）和 Sandbox record S；Local sandbox snapshot 保持无 alias。
2. 通过公开 API 交叉调用：template get/delete/launch 使用 S，snapshot get/delete/promote 使用 T，template alias endpoint 使用指向不同 source 的 A。
3. 每次调用后读取 canonical record，确认是否发生错误删除、错误 promotion、错误 owner routing 或不应有的信息暴露。
4. 对照 public OpenAPI、现有客户端和产品约定，先明确预期是 404/no-op、类型错误，还是允许按通用 snapshot 使用。没有产品契约就不能把“返回另一 source”直接算 bug。
5. 若 API 边界确实要求隔离，在 API handler 加一个 source check 做 focused test，再与旧 manager 方法矩阵比较代码量和调用深度。

### 什么才算真实故障

- 一个公开 endpoint 能删除或变更另一类资源；
- launch/promote 因 source 混淆使用错误 artifact 或错误生命周期；
- 已发布 API 明确承诺类型隔离，而 generic manager 违反该承诺。

若 source 只是 provenance，或公开 API 本来允许把 Template 和 sandbox capture 都当 reusable snapshot 使用，则专用 manager API 是重复过滤。即使故障成立，最小修复优先放在产品 API 边界，不恢复整套 manager wrapper。

## 7. Gateway placement 的外围 header/size 防御

### 旧代码防御的场景

历史位置：`b80feee` 的 `services/gateway/internal/snapshot_route.go` 和测试。

已删除的是：

- 向内部 placement 请求复制 `Traceparent`、`Tracestate`、`Baggage`、`X-Request-ID`；
- 对 placement JSON 添加独立的 8 KiB response body 上限。

这些分别防御链路追踪丢失和异常/恶意 node 返回巨大 body 导致 Gateway 内存压力。当前只转发鉴权所需的 `X-API-Key`。

以下核心没有删除，也不属于本节候选：owner node ID 必须精确匹配、状态必须 READY、endpoint 必须可用、owner 不可用返回 503、Local 不 fallback。

### 复现步骤

1. placement server 记录收到的 headers；通过真实 Gateway 发起 Local launch/promote，确认没有 trace headers 时是否仍能用既有 request/snapshot ID 串起日志和 trace。
2. 如果可观测平台明确依赖 W3C trace context，比较有/无 header forwarding 的 trace 断裂情况，并记录实际排障影响。
3. 让受控 placement server 分别返回 8 KiB、1 MiB、64 MiB 的合法前缀加 padding；并发发起请求，记录 Gateway RSS、延迟、GC 和其他请求可用性。
4. 同时验证共享 `http.Client`、上游 server limit、context timeout 是否已经提供边界；不要只断言旧常量能拒绝 8193 bytes。
5. 用异常 owner response 验证核心 fail-closed 语义仍在：different owner ID、not READY、空/非法 endpoint 都必须 503 且不回退 scheduled node。

### 什么才算真实故障

- 生产 tracing/SLO 明确要求这条内部调用保留 context，且缺少 header 造成不可接受的排障盲区；
- placement endpoint 的信任边界允许异常大响应，且实测能显著影响 Gateway 可用性，现有通用限制不能覆盖。

如果需要 body limit，应优先使用 Gateway 的统一上游响应策略，而不是给两个字段的单个 endpoint 建一套 error/type/test 矩阵。

## 8. ACR manifest same-ID retry

### 旧代码防御的场景

历史位置：`b80feee` 的：

- `common/acr/client.rs::manifest_digest`；
- `common/acr/publisher.rs::source_registry_manifest_retry_reuses_same_digest_and_rejects_conflict`。

旧实现处理“registry 已经提交 manifest，但成功响应丢失”：调用方以相同 snapshot ID 重试时，若 tag 的 digest 与期望相同则当作成功；digest 不同才报冲突。为此增加 HEAD header、GET body hash fallback 和 fake registry 测试矩阵。

当前产品约束是同一 snapshot ID 没有并发写者，客户端超时重试生成新的 snapshot ID，因此必须先证明生产调用链真的会以同一 ID retry。

### 复现步骤

1. 在 registry 代理或 fake registry 中，让 manifest PUT 完整落盘后立即断开连接，不把成功响应发回 publisher。
2. 记录此时 ACR tag/digest、OSS canonical record 和本地 staging 状态。
3. 沿真实 API retry 行为重试：先确认客户端究竟复用旧 ID 还是申请新 ID，不能在测试中擅自固定旧 ID。
4. 如果真实客户端生成新 ID，验证新 capture 是否成功，以及旧 tag 是否只是可回收 orphan；记录 orphan 的实际成本。
5. 如果真实内部逻辑会自动复用旧 ID，再比较简化实现和 `b80feee`：相同 digest 是否能幂等完成 canonical commit，冲突 digest 是否仍被拒绝。
6. 对 Local → Distributed promotion 单独测试，因为 promotion 按产品语义保持 snapshot ID；确认现有 repository/promotion retry 是否会进入同一个 ACR tag 路径。

### 什么才算真实故障

- 正式调用链确实复用相同 ID；
- PUT-response-loss 在目标 registry/网络中可发生；
- 简化实现导致用户无法完成 capture/promotion，而旧 digest 比对能安全完成同一个 canonical commit；
- 不存在更小的 repository-level reconciliation 或 orphan cleanup 方案。

仅证明 fake server 支持 same-ID retry 不够。如果真实客户端总是新 ID，旧机制防御的是违反产品画像的请求。

## 9. paused artifact independence 和额外 durable cleanup fence

### 旧代码防御的场景

历史位置：`c4ff210` 的 `src/orchestrator/service.rs`、`src/sandbox/backend.rs`、Firecracker paused state 和 orchestrator tests；这批额外分支在 `b80feee` 被删除。

旧实现用 `artifacts_are_independent_after_resume()` 决定成功 resume 后：

- 新 runtime 已 clone/adopt，删除 paused record 和 artifact；或
- 新 runtime 仍引用旧 artifact，只删 record，保留 artifact。

它还在 pause persist 失败、delete stop 失败、resume completion 等路径加入额外 record fence 和 fail-closed cleanup，防御以下问题：

1. pause record 已经可见但 API 返回失败，进程重启后又加载出一份 stale Paused sandbox；
2. resume 成功后删除旧 generation，却破坏仍引用旧路径的 live runtime；
3. stop 部分执行后失败，却恢复 route/Running metadata 指向已经被破坏的 runtime；
4. record rollback 失败时，内存状态和重启后状态各自声明不同事实。

这里记录的是已删除的 artifact-independence/cleanup 状态矩阵；当前仍存在的 `mark_resuming`、`rollback_resuming` 及 paused record 原子持久化不能仅凭本节删除。

### 复现步骤

1. 使用真实 `FileBackedSandboxPersister`，在 atomic record rename 成功之后、`persist_paused` 返回之前杀死进程或注入 I/O 响应失败；重启 node，记录加载出的 sandbox 状态和 artifact root。
2. 对比另一故障点：artifact 写完、record rename 之前杀进程。确认 orphan artifact 和 loadable record 的区别。
3. pause 后以 Reuse resume；在 sandbox ready 后删除旧 paused record/artifact，继续做 rootfs/drive I/O，并重启 ublk daemon，判断 live runtime 是否仍依赖旧路径。
4. 在 resume 的 `mark_resuming`、build、wait-ready、rollback 各边界杀进程；每次重启后只依据磁盘 record 恢复，记录是 Paused、Resuming、Running 还是不可加载。
5. 对 delete 注入真实部分失败：Firecracker 已停止但 ublk cleanup 失败，或反过来；观察恢复 route 后请求是否指向已死亡/半销毁 runtime。
6. 对照 `c4ff210`，验证额外 fence 是否真正避免重复恢复或数据损坏，而不是只改变 `RecordingPersister.calls()` 的顺序。

### 什么才算真实故障

- API 失败和进程重启组合能产生两个可写 runtime、错误恢复 stale generation，或丢失唯一可恢复 generation；
- 成功 resume 后的清理稳定破坏 live I/O；
- delete 失败后恢复 Running/route 会把流量发给不可用或数据不完整的 backend；
- 简化状态机无法通过现有原子 record、状态 CAS 和“失败即不可用”语义处理。

如果问题只发生在一个明确的持久化线性化点，应在 `FileBackedSandboxPersister` 修复该点，而不是恢复 orchestrator 中按 mock call order展开的所有组合。

## 10. 复现结果模板

其他 agent 完成任一场景后，应追加一条独立结果，至少包含：

```text
Subject:        <机制和具体故障点>
Mode:           Retrospective
Commits:        <简化版本> vs <旧防御版本>
Environment:    <kernel/KVM/Firecracker/ublk/OverlayBD/OSS or ACR>
Necessity:      Pass | Fail
Necessity note: <不违反产品不变量的真实触发序列>
User impact:    <数据损坏/错误路由/不可恢复/仅请求失败/无影响>
Artifacts:      <record、artifact、runtime 前后路径和 digest/inode>
Focused test:   <测试名和命令>
Evidence:       <日志、API、guest token、重启结果的位置>
Verdict:        KEEP-old | RESTORE-minimal | KEEP-simplified
Minimal alt:    <若成立，能覆盖故障的最小修复>
Revisit when:   <若未复现，未来重新评估的可测触发条件>
```

若结论是恢复机制，还必须回答：为什么现有 owner 层、record 原子性、per-record lock、runtime pin 或明确失败返回无法解决。没有这个答案，复现只证明“发生过一次错误”，不能证明旧架构值得恢复。
