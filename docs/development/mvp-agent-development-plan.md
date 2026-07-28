# StillFlow AI Data Cleaning MVP Development Plan

## 1. 当前基线

仓库：

```text
X44421/stillflow
```

已完成并合并：

```text
#4  Rust Workspace 与 CI
#5  Arrow Connector 契约与领域基础类型
```

当前 crate：

```text
stillflow-core
stillflow-connectors
stillflow-engine
stillflow-api
```

下一项任务从 Issue #6 本地表格文件 Connector 开始。

## 2. MVP 目标

实现以下完整闭环：

```text
Import
→ Profile
→ Detect Issues
→ Generate Rule Draft
→ Preview Changes
→ Execute
→ Validate
→ Export AI-ready Dataset
```

最终验收数据：

```text
customer_support.jsonl
```

必须完成：

* 导入 CSV / JSONL / Parquet
* 检测空文本、重复、PII、Token 异常
* 生成结构化规则草案
* 显示 Before/After
* Polars 确定性执行
* 生成 Rejected Dataset
* 生成新的 Dataset Version
* 输出 JSONL
* 整个流程可重复执行

## 3. 强制架构边界

```text
stillflow-api
      ↓
stillflow-engine
      ↓
stillflow-connectors
      ↓
stillflow-core
```

### Core

只允许包含：

* 领域类型
* 稳定错误
* Arrow 协议
* Repository/Provider trait
* 事件与请求上下文

禁止：

* Polars
* Axum
* SQLx
* 文件系统操作
* AI Provider SDK

### Connectors

只负责：

```text
External Source → Arrow RecordBatch
```

禁止负责：

* Profiling
* Rule 执行
* Quality Score
* Dataset Version 生命周期
* AI 调用

### Engine

负责：

* Profiling
* Issue Detection
* Rule Validation
* Polars 执行
* Compare
* Pipeline Run
* Quality Report
* Export

Polars 类型不得泄漏到公共 API 或 Core。

### API

负责：

* Axum 路由
* DTO
* SQLite Adapter
* Local Artifact Adapter
* ModelProvider Adapter
* SSE 运行进度
* 依赖组装

API 层不得实现清洗语义。

## 4. MVP 冻结原则

所有子 Agent 必须遵守：

1. 原始 Dataset 永远不可覆盖。
2. 每次正式执行产生新的 `DatasetVersion`。
3. Preview 不产生正式版本。
4. Run 必须绑定确定的 Pipeline Revision。
5. AI 只能生成 `CleaningPlanDraft`。
6. AI 不得直接执行规则或修改数据。
7. 所有规则必须表示为结构化 `RuleSpec`。
8. Connector 边界只能使用 Arrow。
9. Polars 只存在于 Connector 实现和 Engine 内部。
10. 错误行必须进入 Rejected Dataset。
11. Secret、PII 样本和连接凭据不得进入日志或事件。
12. 所有 I/O 必须支持取消、deadline 和有界读取。
13. 不得通过 `clone`、`Arc`、`Box` 单纯绕过所有权设计问题。
14. 不得修改无关前端样式。
15. 不得引入契约未批准的依赖。

## 5. MVP 工作包

### WP-01：本地文件 Connector

对应 Issue #6。

范围：

```text
CSV
TSV
JSON
JSONL
Parquet
```

交付：

* `LocalTabularConnector`
* allowed roots
* 文件发现
* 格式识别
* Schema 推断
* bounded preview
* projection
* Arrow BatchStream
* 编码、分隔符和损坏行警告
* 路径穿越防护

允许修改：

```text
backend/Cargo.toml
backend/Cargo.lock
backend/crates/stillflow-connectors/**
backend/crates/stillflow-core/**
```

只有确实无法实现时才能修改已经冻结的 Connector 公共接口。

验收：

```text
discover
inspect
preview
read_batches
```

全部覆盖 CSV、JSONL、Parquet fixture。

---

### WP-02：Dataset Version 与持久化

交付领域对象：

```text
DatasetVersion
ArtifactReference
Pipeline
PipelineRevision
PipelineRun
RejectedDataset
QualityReport
ExportArtifact
```

持久化：

```text
SQLite
Local Artifact Storage
Parquet Snapshot
```

文件布局：

```text
.stillflow/
├── metadata.db
├── objects/
├── datasets/{dataset_id}/versions/{version_id}/
├── runs/
└── exports/
```

必须保持：

```text
DatasetVersion != DatasetSnapshot
Session != DatasetVersion
```

---

### WP-03：Profiling 与问题检测

通用指标：

* Null
* Unique
* Duplicate
* Min/Max
* Top values
* 字符串长度
* 类型异常

AI 数据指标：

* 空文本
* Token 长度
* 重复文本
* 标准化后重复
* 特殊字符
* HTML 残留
* PII
* 标签分布

每个 `IssueFinding` 必须包含：

```text
issue_type
column
affected_rows
affected_ratio
sample_row_locators
severity
evidence
suggested_rule_types
```

禁止只返回不可解释的总分。

---

### WP-04：Rule AST 与 Polars Runtime

第一批规则：

```text
Trim
NormalizeUnicode
NormalizeWhitespace
Replace
RegexReplace
CastType
FillNull
DropNull
Filter
MapValues
Deduplicate
RenameColumn
StripHtml
LengthFilter
TokenLengthFilter
RedactPii
Validate
```

每条规则必须支持：

* 参数校验
* enabled
* failure policy
* 来源和原因
* 可重复执行
* 影响统计
* serde round-trip

Engine 内实现：

```text
Arrow Batch
→ Polars
→ Rule
→ Arrow Batch
```

---

### WP-05：Preview 与 Compare

交付：

* 单规则 Preview
* 全 Pipeline Preview
* Before/After 行级样本
* 指标变化
* 删除行数
* 修改行数
* 错误行数
* Preview 截断信息

Preview 必须：

* 使用样本
* 不创建正式版本
* 不修改源文件
* 不写入正式 Dataset
* 使用和正式 Run 相同的规则执行器

---

### WP-06：Pipeline Run 与 Rejected Dataset

Run 状态机：

```text
Queued
→ Profiling
→ Running
→ Validating
→ Materializing
→ Succeeded
```

失败状态：

```text
Cancelling
Cancelled
Failed
```

Rejected Row：

```text
original_row
source_row_locator
rule_id
error_code
error_reason
node_index
timestamp
```

必须支持：

* 进度
* 取消
* deadline
* 执行事件
* 失败恢复
* 输入输出统计
* 有界内存

---

### WP-07：Quality 与 Export

Quality Report：

```text
Schema validity
Completeness
Duplicate rate
Text validity
Privacy risk
Token health
Label balance
```

导出：

```text
CSV
JSONL
Parquet
Instruction JSONL
Chat Messages JSONL
```

ExportArtifact 必须记录：

* Dataset Version
* Pipeline Revision
* Source Hash
* Quality Report
* Schema
* Export time
* Artifact Hash

---

### WP-08：AI Cleaning Assistant

定义：

```rust
trait ModelProvider {
    async fn generate_cleaning_plan(
        &self,
        context: CleaningContext,
    ) -> Result<CleaningPlanDraft>;
}
```

模型输入仅允许：

* Schema
* Profile
* IssueFinding
* 脱敏样本
* 可用 Rule 类型
* Rule JSON Schema

模型输出：

```text
CleaningPlanDraft
└── Vec<RuleDraft>
```

执行前必须经过：

```text
Deserialize
→ RuleValidator
→ Schema Validation
→ Preview
→ User Confirmation
→ Pipeline Run
```

禁止模型输出任意可执行代码。

---

### WP-09：API 与 UI 接线

最小 API：

```text
POST /api/datasets/import
GET  /api/versions/{id}/preview
GET  /api/versions/{id}/profile
GET  /api/versions/{id}/issues

POST /api/cleaning-plans/generate
POST /api/pipelines
POST /api/pipelines/{id}/preview

POST /api/runs
GET  /api/runs/{id}
POST /api/runs/{id}/cancel
GET  /api/runs/{id}/compare

GET  /api/versions/{id}/quality
POST /api/versions/{id}/exports
```

UI 只接入现有 Workspace：

```text
Data
Profile
Workflow
Compare
Quality
```

禁止新增一级页面或修改现有视觉系统。

## 6. 子 Agent 并行规则

不能并行修改公共协议。

推荐顺序：

```text
WP-01 Connector
      ↓
WP-02 领域模型和持久化协议冻结
      ↓
┌───────────────┬────────────────┐
│ WP-03 Profile │ WP-04 Rules    │
└───────────────┴────────────────┘
      ↓
WP-05 Preview
      ↓
┌────────────────┬────────────────┐
│ WP-06 Runtime  │ WP-07 Export   │
└────────────────┴────────────────┘
      ↓
WP-08 AI
      ↓
WP-09 API/UI/E2E
```

只有满足以下条件才能并行：

* 不修改同一公共 trait
* 不修改同一领域对象
* 不修改同一 Cargo 依赖区域
* 各自有明确文件所有权
* 有已经冻结的输入输出契约

## 7. 每个子 Agent 的工作流程

```text
1. 阅读 AGENTS.md
2. 阅读对应 Implementation Contract
3. 检查 origin/main 和依赖 Issue
4. 只修改授权路径
5. 先写失败测试
6. 最小实现
7. 运行局部测试
8. 运行 Workspace 全量测试
9. 检查 Diff 范围
10. 提交报告，不自行扩大任务
```

分支命名：

```text
agent/mvp-006-local-tabular
agent/mvp-007-version-storage
agent/mvp-008-profiling
agent/mvp-009-rule-runtime
```

## 8. 统一验收命令

```bash
cd backend
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

前端被修改时额外运行：

```bash
npm ci
npm run build
```

禁止通过以下方式让测试"通过"：

* 删除失败测试
* 添加全局 `allow`
* 忽略错误
* 用 `unwrap`/`expect` 绕过生产错误
* 用无限制 collect 读取完整文件
* 降低安全或路径校验
* 静默丢弃损坏行

## 9. 子 Agent 交付报告格式

```markdown
## Result

## Modified files

## Implemented behavior

## Public API changes

## New dependencies

## Tests added

## Verification results

## Contract deviations

## Remaining risks

## TODO / unwrap / expect audit

## Suggested follow-ups
```

## 10. 下一位子 Agent 的直接提示词

```text
你负责 StillFlow Issue #6：本地结构化文件 Connector。

开始前必须阅读：
- AGENTS.md
- docs/data-ingestion-architecture.md
- docs/development/mvp-agent-development-plan.md
- Issue #6
- stillflow-core 与 stillflow-connectors 当前公共 API

目标：
实现 LocalTabularConnector，使本地 CSV、TSV、JSON、JSONL 和
Parquet 支持 discover、inspect、preview 和 read_batches，并通过现有
Arrow SourceConnector 边界工作。

必须保持：
1. 不修改 crate 依赖方向。
2. 不向 core 泄漏 Polars 类型。
3. 不重新设计 SourceConnector，除非当前契约确实无法表达需求。
4. Preview 必须同时受 row_limit 和 byte_limit 限制。
5. read_batches 内存必须受 batch_size 限制。
6. 只允许读取配置的 allowed roots。
7. 拒绝路径穿越、符号链接逃逸和非法 URI。
8. 保留原始列名，规范化名称只能作为独立 metadata。
9. 所有失败使用现有 ConnectorError。
10. 所有操作传播 cancellation 和 deadline。
11. 不实现 Excel、S3、SQL、DuckDB、Profiling、规则或 UI。
12. 不修改无关文件。

测试至少覆盖：
- CSV、TSV、JSONL、Parquet
- Schema 推断
- bounded preview
- preview truncation
- projection
- batch size
- 损坏行
- 空文件
- 非 UTF-8 或编码警告
- allowed-root 拒绝
- ../ 路径穿越
- 符号链接逃逸
- cancellation
- deadline
- Arrow schema 与行数保持

先只分析代码并输出 Implementation Contract，不修改文件。
Contract 获得确认后再实现。

最终运行：
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace

完成后按照统一交付报告格式汇报。
```

这份方案可以作为所有子 Agent 的共同上层约束；每个具体 Issue 再配一份更短的 `issue-xxx-implementation-contract.md`，避免子 Agent一边设计公共协议，一边铺开实现。
