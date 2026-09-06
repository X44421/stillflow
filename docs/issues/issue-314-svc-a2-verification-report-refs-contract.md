# SVC-A2 — Verification Report ArtifactRefs Frozen Implementation Contract

> Status: Frozen 2026-09-06 · Part of [#314](https://github.com/X44421/stillflow/issues/314)（SVC-A2 · verification 报告产物的 control-plane ArtifactRef 补建）
> 风险级别：L3（产物发布面语义；control-plane 持久化行为；不改线上 wire 格式）
> 上位合同：[SVC-A1 HTTP Service Entry Contract](issue-303-svc-a1-http-service-entry-contract.md)（§3.2 状态映射、§6.1 typed-binary wire）· E4 storage 产物/摘要法律（`docs/data-ingestion-architecture.md`）
> 来源：SVC-A1 收口 PR #310 的 closing-PR 发现（`read_artifact_content` 不可达成功），按 SVC-A1 §2.5/§9 停止条件留作本独立任务。

## 1. Objective and risk class

Verification bundle 的三个报告产物（ValidationReport / RejectedRows / DeduplicationReport）是唯一以 Arrow section 形式存储内容的产物类别，但生成路径从不创建 control-plane `ArtifactRef`，导致 §6.1 冻结的 `artifact.content` 路由对它们恒 404（fail-closed 留痕于 PR #310 e2e）。本合同冻结补建语义：报告产物在 bundle 提交后、Run 终态提交时获得与其余产物类别**同一纪律**的 ref 生命周期（staged → committed/failed，事件由 Run stream 持有）。

风险 L3：改变 control-plane 持久化行为与引擎发布面。**不改**：任何线上 wire 格式（§6.1 原样）、`open_artifact_section` 寻址规则、既有四类产物（Profile/Quality/Export/Drift）的 ref 行为、E4 存储格式与摘要法律。

## 2. Authorized public changes (additive only)

1. `stillflow-engine`：Verification 操作在 bundle 提交后为报告成员 stage `ArtifactRefDraft`（新增调用，不改既有调用点）。
2. `stillflow-storage::control_plane`：
   - `validate_verification_bundle_output` 增加 staged refs 与成员集的**精确匹配校验**（计数、身份、kind、digest、无重复、无 canonical body）；
   - `commit_staged_terminal_artifacts` 将 `TerminalOutputRef::VerificationBundle` 的 members 与顶层 `Artifact` 输出**同等翻正**（staged → committed + 每产物确定性 `ArtifactCommitted` 事件）。
3. `stillflow-service` e2e：PR #310 的 fail-closed 断言翻转为成功路径断言 + 未知 id 的 404 fail-closed 断言。

其余公共面（包括 `read_artifact_content`/`list_artifact_metadata`/`open_artifact_section`/wire 模块）零修改。

## 3. Ref staging semantics (frozen)

对每次成功提交的 VerificationBundle，引擎在 Run 处于 running 时、终态发布之前 stage 与 bundle 成员一一对应的 ArtifactRef（2 条恒有：ValidationReport、DeduplicationReport；bundle 含 rejected section 时加第 3 条 RejectedRows）：

| 字段 | 冻结值 |
| --- | --- |
| `artifact_id` / `artifact_kind` / `content_digest` | 与 bundle membership / report manifest / report provenance 完全一致（即 typed member 集合） |
| `external_ref_kind` | `Artifact` |
| `external_ref_id` | `bundle_id` |
| `metadata.artifactType` | `validation_report` / `deduplication_report` / `rejected_rows` |
| `metadata.artifactBodyVersion` | `1` |
| `metadata.bundleId` | bundle id |
| `metadata.bundleVersionDigest` | bundle version digest（64 位 hex） |
| `metadata.verificationContractVersion` | bundle provenance 的 contract version |
| canonical body | **禁止**（内容在 bundle 存储 section 中；`cp_artifact_bodies` 不得有对应行） |

staging 失败（含任何一条失败）→ Run 失败，不得部分发布；staged refs 随既有失败路径翻 `failed`。

## 4. Terminal validation and commit semantics (frozen)

1. **校验**（终态事务内、翻正前）：staged refs 计数 == members 计数；逐成员 staged 行与 typed member 的 workspace/kind/digest 一致、`external_ref_kind = Artifact`、无重复 id、无 body 行。任一不满足 → `InvalidDraft` → Run 失败且不发布引用（沿用既有 fallback）。
2. **翻正**：成员与顶层 Artifact 输出同等走 staged → committed（`committed_at_utc` = Run 终态时间），并按既有确定性规则为每成员发 `ArtifactCommitted` 事件（Run stream 持有）。
3. **可读性**：`get_artifact_ref`/`list_artifact_refs` 既有 committed-only 过滤不变——staged/failed 的报告 ref 对读路径不可见（fail-closed 保持）。

## 5. Invariants and bounds

- 报告 ref 的 digest = bundle 内 report provenance 的 content_digest（不重算、不引入第二摘要）。
- 不产生任何 canonical JSON body 行（三类报告 kind 均为无 body 类，与 ExportArtifact 同域）。
- `read_artifact_content` 的 §6.1 Arrow 流路径、分页（maxRows/maxBytes/afterPartitionSequence）与 §3.2 错误映射零修改；未知/失败/未提交产物 id 仍 404 `notFound`。
- E4 摘要法律不变：ref 不参与 bundle/section 摘要的任何 preimage。

## 6. Objective acceptance tests

1. **e2e（TCP，`http_entry_e2e` T2 扩展）**：verification job 成功后——
   - `GET /v1/runs/{runId}/artifacts` 列出 bundle 全部成员的 committed ref（id/kind 匹配）；
   - `GET /v1/artifacts/content?...sectionId=validation-rule-summary` 返回 200 + §6.1 media type + 可解码流（读者规则生效）；
   - 未知 artifact id → 404 `notFound`（fail-closed 留痕延续）。
2. **引擎全链路**：既有 V-suite（真实 bundle 物化）在新 staging/校验/翻正纪律下保持全绿——纪律不匹配即红（fail-closed 回归网）。
3. **五项门禁**（AGENTS Required verification）全绿；exact-head CI 6/6。

## 7. Dependencies

无新增 crate/feature（`arrow-*`、axum 等既有）。

## 8. Risks and stop conditions

- 若 staged 纪律与 reconciliation/恢复路径（crash window、stale claim）冲突，停止并回合同评审——不引入第二套 ref 生命周期。
- 若发现 rejected-rows 缺席场景（零拒绝）与成员计数的既有断言冲突，停止并回合同评审（预期：`validate_verification_bundle_output` 既有 expected_member_count 逻辑已覆盖 2/3 两态）。
- `taskctl` 在实现环境不可用（沿 PR-1/#310 同一披露，合同 §7.8 惯例）：Registry claim `engine:verification`（实现面含 control_plane 终态提交路径）以 PR 正文披露代替 CAS 行，单写者窗口由本 PR 独占。
