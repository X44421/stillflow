# O0-D0：后端代码精简与热路径事实盘点

- Issue: #75（`backend: inventory code complexity and hot-path costs before optimization`）
- 分支: `agent/issue-075-backend-optimization-inventory`
- 基线: `main@684b0ab0cdb8c62666bcdd7d16f95c53ee220972`（PR #68 合并点，含 #67；提交时间 2026-08-22T10:14:15+08:00）
- 性质声明: 本任务只调查、分类和设计测量方案——不修改生产代码、不冻结新 API、不宣称未经测量的性能收益、不实施重构或优化。
- 只读引用对象: PR #53 / PR #71 / PR #74、`experiment/e4-vertical-slice`。
- 标注体系:【事实】= 可由 path:symbol:line 直接复核；【静态推断】= 由代码结构推导的复杂度/行为结论，未经运行测量；【待测假设】= 需 §6 基准验证的猜想；【建议】= 后续任务候选动作。

## 1. 基线与测量方法

### 1.1 总量【事实】

| 指标 | 值 |
| --- | ---: |
| Rust 文件数 | 82 |
| 源码总体积 | 1,063,601 字节（≈1.04 MiB） |
| 总行数 | 31,147 |
| 生产代码行数 | 20,979 |
| 测试代码行数 | 10,168 |

### 1.2 各 crate 明细【事实】

| crate | 文件数 | 生产 LOC | 测试 LOC |
| --- | ---: | ---: | ---: |
| stillflow-api | 1 | 15 | 9 |
| stillflow-connector-local-tabular | 14 | 3,897 | 1,851 |
| stillflow-connector-object-store | 9 | 2,649 | 1,048 |
| stillflow-connector-workbook | 13 | 3,255 | 932 |
| stillflow-connectors | 5 | 374 | 381 |
| stillflow-core | 20 | 3,716 | 1,847 |
| stillflow-engine | 12 | 4,404 | 3,084 |
| stillflow-plan | 3 | 581 | 332 |
| stillflow-storage | 5 | 2,088 | 684 |
| **合计** | **82** | **20,979** | **10,168** |

### 1.3 统计命令与规则【事实】

命令：

```text
find backend -name '*.rs' | wc -l                      # 文件数
find backend/crates -name '*.rs' -exec wc -c {} + | tail -1   # 字节体积
wc -l <files>                                          # 行数
grep -n '#\[cfg(test)\]' <file>                        # 测试模块定位
```

计数规则：

1. `tests/`、`benches/` 目录下的文件整体计为测试代码。
2. `src` 文件内顶层 `#[cfg(test)] mod … { … }` 以花括号配对定界，块内计为测试。配对使用一个临时词法脚本（处理行注释、嵌套块注释、字符串字面量、raw string），脚本未提交，存放在仓库外工作区目录，最终 diff 不包含它。
3. 经 lib.rs 的 `#[cfg(test)] mod tests;` 声明的整文件计为测试——`backend/crates/stillflow-engine/src/tests.rs`（2,956 行）属于此类；若直接对该文件扫 `#[cfg(test)]` 会误判为生产代码，§1.2 数字已修正（engine：7,360−2,956=4,404 生产；128+2,956=3,084 测试）。
4. 抽查锚点【事实】：`store.rs` cfg(test)@1511（生产 1,510/测试 669）；`batch.rs` @724（723/396）；`access.rs` @821（820/165）；`read.rs`、`preflight.rs`、`predict.rs`、`remainder.rs` 文件内无 cfg(test)，全部为生产代码（其测试分别位于 `tests/` 目录与 engine `src/tests.rs`）。

## 2. 大文件与长函数清单

### 2.1 主表【事实：行数由 wc/词法脚本得出；职责数与风险为人工判读】

| 路径/符号 | 生产行数 | 测试行数 | 职责数量 | 风险 | 是否仅结构问题 |
| --- | ---: | ---: | --- | --- | --- |
| `backend/crates/stillflow-storage/src/store.rs` | 1,510 | 669 | 6 | 高 | 否（含双遍 I/O 与连接生命周期等运行时事实，§3.3） |
| `backend/crates/stillflow-core/src/batch.rs` | 723 | 396 | 4 | 高 | 基本是（体积来自逐类型合同映射，§4） |
| `backend/crates/stillflow-connector-local-tabular/src/read.rs` | 1,008 | 0 | 5 | 中高 | 否（双解析为运行时行为，§3.2） |
| `backend/crates/stillflow-connector-object-store/src/access.rs` | 820 | 165 | 4 | 中 | 基本是 |
| `backend/crates/stillflow-engine/src/preflight.rs` | 849 | 0 | 6 | 高 | 结构为主（含 schema 传播耦合） |
| `backend/crates/stillflow-engine/src/predict.rs` | 636 | 0 | 4 | 中高 | 否（含重复计算运行时事实，§3.1） |
| `backend/crates/stillflow-engine/src/remainder.rs` | 822 | 0 | 4 | 高 | 待重估（正被 #53/#71 改写，合并后基线变化） |

职责数明细【事实：由符号清单归纳】：

- `store.rs`（6）：快照生命周期（staging/commit/recovery/GC）；Parquet 分区编解码 I/O；摘要与完整性校验；SQLite manifest/journal/publications；维护操作（recover/gc/orphan-scan/activity）；并发锁与上限校验。
- `batch.rs`（4）：`BatchEnvelope` 构造与边界校验（:630–641）；`LogicalSchema`↔Arrow 双向映射与指纹（:299–578）；规范元数据编解码（:588–628）；错误分类（:670+）。
- `read.rs`（5）：格式分派与 reader 准备（:80–261）；流式填充与分批（:319–485）；CSV/JSON 手工值校验（:487–571、:849–965）；投影重排（:584）；错误映射（:966–1010）。
- `access.rs`（4）：对象存储 open 与配置（:101）；流式 GET（:409）；upload（:480）；错误映射。
- `preflight.rs`（6）：计划校验与预算（:36–75）；线性化（:191–251）；`CompiledStep` 编译（:106–158）；schema 传播与 `apply_rule_schema`（:447–621）；paused 类型/表达式门禁（:294–337）；表达式深度/节点数迭代校验（:690–785）。
- `predict.rs`（4）：k 二分搜索（:143–170）；逐 rule/step 字节预测（:223–433）；Arrow 物理字节度量（:505–636）；导出转换模型（:477–503）。
- `remainder.rs`（4）：rebatch 打包与 freeze 编排（`CanonicalRebatcher` :19）；逐类型精确 sink（:203–708）；位图位打包（`BitPackedSink` :385）；内存记账接口。

### 2.2 长函数清单（≥50 行，词法工具输出）【事实】

| 文件 | 函数 | 行号 | 行数 |
| --- | --- | ---: | ---: |
| connector-local-tabular/src/read.rs | `prepare_reader` | L80 | 182 |
| engine/src/predict.rs | `predict_rule` | L256 | 178 |
| engine/src/preflight.rs | `preflight` | L36 | 154 |
| engine/src/preflight.rs | `apply_rule_schema` | L468 | 153 |
| storage/src/store.rs | `read_partition` | L1125 | 139 |
| storage/src/store.rs | `load_manifest_inner` | L989 | 135 |
| connector-local-tabular/src/read.rs | `fill_pending` | L359 | 126 |
| connector-object-store/src/access.rs | `upload` | L480 | 117 |
| storage/src/store.rs | `commit_manifest` | L888 | 85 |
| storage/src/store.rs | `append_inner` | L379 | 84 |
| connector-object-store/src/access.rs | `stream` | L409 | 70 |
| engine/src/preflight.rs | `linearize` | L191 | 61 |
| storage/src/store.rs | `migrate_to_version_one` | L660 | 50 |

（`batch.rs` 生产代码无 ≥50 行函数——其体积由大量小型逐类型映射函数构成，佐证"体积=合同面"判读。）

### 2.3 文件大 ≠ 性能差【建议】

不得以行数推断性能：`batch.rs` 的体积是逐类型合同映射（§4"可生成重复"候选，非性能问题）；`read.rs` 的规模则对应真实运行时双解析行为（§3.2）；`store.rs` 兼有结构问题与 I/O 事实。性能结论只能出自 §6 基准。

## 3. 三条当前主线热路径

执行链总览【事实】：`ExecutionEngine::execute`（engine.rs:205–227，逐 envelope）→ `consume_envelope`（engine.rs:237–288）→ :251 `largest_feasible_k`（每切片一次）→ Polars lowering :258–280 → `CanonicalRebatcher::push` :283。

### 3.1 Engine prediction：`largest_feasible_k → predict → predict_step / predict_rule`

**F1【事实】predict 调用次数 = 1 + I，其中 0 ≤ I ≤ ⌈log₂ remaining⌉**：`largest_feasible_k`（predict.rs:143–170）先做单行探针 `predict(1,…)`（:154），再在 `[low, high]`（high 初始化为当轮 remaining）上二分，每轮调用一次 `predict(mid,…)`（:163）。I 为实际迭代轮数，随分支路径与数据变化；⌈log₂ remaining⌉ 只是该轮 remaining 的最坏情况上界，不是所有切片的恒等式。每轮都是完整重算，结果间不共享中间量。

**F2【事实】PredictedSchema 克隆次数 = 每 predict 一次 + 每 step/rule 一次**：
- 每次 predict 入口克隆：predict.rs:106 `let mut working = schema.clone();`
- Project 分支 :233、Filter 分支 :245 各克隆一次；每条规则入口 :264 `let mut next = working.clone();`
- `PredictedColumn` 含 `String name` 与 `LogicalType`（List/Struct 内含 Vec），clone 非浅拷贝【静态推断：成本随列数线性】。
- 推断公式【静态推断】：每切片 schema 克隆次数 = (1 + R) × (1 + I)——1 + I 即该切片 predict 的实际调用次数（上界见 F1）。

**F3【事实】ColumnId 线性查找**：`column()`/`column_mut()`（predict.rs:70–75/77–82）实现为 `iter().find(...)`，O(cols)；Rename :267、Trim :276/:281、ReplaceLiteral :320、FillNull :348、Cast :380/:399 均经此路径；DeriveColumn 另有全表唯一性扫描 :292–295。

**F4【事实】Project 的 retain + sort_by_key(position)**：predict.rs:232–240——`retain` O(cols×proj)；`sort_by_key` 的 key 闭包内执行 `columns.iter().position(...)`（:236–239），key 总计算量 O(cols×proj)，排序 O(cols·log cols)。1024 列投影时为百万级比较操作【静态推断】。

**F5【事实】column_physical_sum 在多数 rule 之后全量重算**：调用点 :241(Project)、:272(DropColumn)、:282(Trim)、:325/:332/:342(ReplaceLiteral 三分支)、:362/:370(FillNull)、:420(Cast)；每次 O(cols)，且变宽源列触发 O(k) 行级扫描（见 F7）。

**F6【事实】Cast→Utf8 表达式每次重建 LogicalSchema**：`expression_max_value_bytes` 中 :452 `working.to_logical_schema()?` + :453 `type_check_expr` ——为一个类型检查分配全部字段对象；该函数在每次 predict 的 DeriveColumn-Cast 路径都会执行。

**F7【事实】行级扫描热点**：
- `refresh_source_widths` :172–186 对每个变宽源列调用 `max_variable_width` :567–582：无 offsets 快路径，逐行 `value_width`。
- `value_width` :602–625：每行最多 4 次 `as_any().downcast_ref` 类型尝试。
- `variable_data_bytes` :584–600：StringArray 有 offsets 差值快路径 :586–593；Binary/LargeString 等走逐行累加 :595–599。

**F8【事实】导出转换模型对每列字节双重计算**：`predict_export_transition` :477–503 的第一循环 :484–487 与第二循环 :491–500 各调一次 `column_physical_bytes`，同一列同一参数算两遍。

**I1【静态推断】放大公式（按切片求和的上界形式）**：设 envelope N 行、变宽列数 V、规则总数 R；切片 i 的行数为 kᵢ、剩余行为 remainingᵢ = N − offsetᵢ、预测轮数 Iᵢ 满足 0 ≤ Iᵢ ≤ ⌈log₂ remainingᵢ⌉（F1）：
- 行访问量（width 刷新主导项）≈ Σᵢ (1+Iᵢ) × V × kᵢ
- schema 克隆 ≈ Σᵢ (1+R) × (1+Iᵢ)
- column_physical_sum 全量重算 ≈ Σᵢ (1+Iᵢ) × (R+c)
k̄ 平均化表述弃用——轮数上限跟随每轮各自的 remaining。1024 列 × 128 规则 × 万行 envelope 时各项为乘性放大；确切量级须 §6 B1 实测。

**H1【待测假设】** 二分搜索中相邻 predict(mid) 的计算高度重叠，可被"编译期规则游标 + 增量宽度表"消除且不改变准入字节结果（§7 候选 1）。验证门：B1 基准 + 内存律测试 t43/t46/t47/t52/t55/t56（tests.rs:2114/2225/2278/2541/2677/2747）全绿。

调用点上下文【事实】：engine.rs:191–195 rebatcher 每次 execute 新建；:197 `PredictedSchema::from_scan_output` 每次 execute 一次；:251 位于 `while offset < row_count` 循环体内，即每个切片各跑一轮二分。

### 3.2 Local Tabular ingestion

读取链【事实】：lib.rs:123 `prepare_reader` → read.rs:268 `into_raw_stream` → :320 `next_output_frame` → :359 `fill_pending`。产出为 arrow-rs `RecordBatch` 包 `BatchEnvelope`（bridge/mod.rs:49 rechunk 转换）；Polars DataFrame 不跨越 connector 边界【事实，负发现】。

**C1【事实】CSV 全程双解析 + 有界第三次推理解析**：
- Pass A 推理（有界）：read.rs:94–95 `inspect_opened_asset` → inference.rs:34 `read_bounded`（默认 8 MiB / 上限 64 MiB，config.rs:12–13），自有 `csv::Reader` 于 Cursor（inference.rs:133–138），采样 ≤ inference_rows 默认 10,000（config.rs:10）。
- Pass B 校验器（与解码全程锁步）：read.rs:156–162 在同一文件的独立句柄上开第二个 `csv::Reader`；prepare 阶段仅比对 header（:163–183）；该 reader 存入 `CsvState.validator`（read.rs:67）存活整个流。
- Pass C Polars 解码：:184–188 mmap 句柄 + `CsvReadOptions.batched(None)`；schema 显式提供 :141（无 Polars 侧推理）；chunk_size = min(4,096, batch_size) :144。
- 锁步校验：:377 `next_batches(1)` 之后 :379 `validate_rows` → :488–531 `validator.read_record`（:492）逐行重读同一段文本：宽度检查 :513、逐单元格 `csv_value_matches` :520–528（日期走 chrono 解析 :559–571）。
- 结论：完整读取（无 max_rows 截断）时每个 CSV 字节约被解析两遍——Polars 解码与 csv 校验器对同一范围锁步消费；限行读取时两条解析器只消费对应前缀、对该已消费前缀进行双解析；另有有界的推理第三遍【事实，按实际读取范围限定】。Pass B/C 结果不共享——校验只产出布尔/错误，解码产出类型化帧【事实】。同一次 prepare 中同一文件被打开三次：read.rs:94、:121、:156【事实】。

**C2【事实】JSON 每行的处理阶段——语义 JSON 解析：NDJSON 两遍 / 顶层数组形三遍；另有 framing 与序列化各一遍**（计数口径：括号扫描与行装配属 framing 遍历，不计为语义解析；`serde_json::to_writer` 是序列化输出，不是解析）：
1. framing 遍历：json_stream.rs:92–239——NDJSON 行装配 :148–174；顶层数组形逐字节括号扫描 :196–239（单对象字节上限 MAX_BATCH_BYTES :295–303）。
2. （仅数组形）`IgnoredAny` 语法预解析 json_stream.rs:122 → :314–321：语义解析第 1 遍，产物只是一个被丢弃的合法位。
3. serde_json 投影+校验解析 read.rs:862–885（visitor :614–651）：语义解析再下一遍，每行物化为 `Value` 树。
4. 序列化：read.rs:436 函数局部 `Vec` 缓冲 + :449–456 `serde_json::to_writer` 写回 NDJSON 字节。
5. Polars JsonLines 语义解析：read.rs:464–477 `JsonReader::new(Cursor::new(encoded))`。
缓冲按 chunk 有界【事实】，但为函数局部变量，每次 refill 重新分配。

**C3【事实】Parquet 每批次重建 reader**：read.rs:397–411 = `file.try_clone()` + `ParquetReader::new` + `set_metadata(Arc clone)` :405 + `projection.clone()` :407 + `(offset, rows)` 切片 :408。footer 元数据在 prepare 时已取并缓存（:216–237），故 footer 不随 chunk 重析【事实，对照 vendored polars-io 0.46 reader.rs:181–197 的 set_metadata 语义】；但 reader 对象与读取计划每 chunk 重建。同一请求内 Parquet magic 校验 ×2（inspect.rs:24; read.rs:215）、footer/schema 读取 ×2（inspect.rs:26–41; read.rs:216–233）【事实】。

**C4【事实】缓冲与 vstack**：`vstack_mut` 单点 read.rs:351，目标行数 :328 `min(batch_size, remaining)`——按批有界；帧切分零拷贝 :343–347。全 crate 读路径无整文件 collect/mmap/read_to_end【负发现】。

**C5【事实】分批与上限语义**：发射仅按行数封顶（INTERNAL_ROWS=4,096，read.rs:33；batch_size ∈ [1,65,536]，core/src/domain/read.rs:26–27）；64 MiB 信封字节上限在解码完成后由 envelope_factory.try_build 强制（read.rs:289–298）——超限批次报错而非拆分。

**C6 分类汇总**：

| pass | 证据 | 分类 |
| --- | --- | --- |
| 文本 schema 推理 | inference.rs:34,133–138 | contract-mandated（其每次 read 无条件重跑、不缓存：needs-verification） |
| CSV header 复查 | read.rs:156–176 | needs-verification（同调用内与推理期 header 校验重复） |
| CSV Polars 解码 | read.rs:184–188,377 | contract-mandated |
| CSV 锁步第二全文解析器 | read.rs:157–162,379,487–531 | 行为 mandated（tests/local_tabular.rs:255 固化 typed drift）；实现策略 likely-incidental |
| JSON framing | json_stream.rs:92–239 | contract-mandated |
| JSON IgnoredAny 预解析 | json_stream.rs:122,314–321 | likely-incidental |
| JSON 投影+校验解析 | read.rs:862–885 | contract-mandated |
| JSON 重序列化 + Polars 第二次语义解析 | read.rs:436–477 | likely-incidental |
| Parquet magic ×2 / footer ×2 | inspect.rs:24,26–41; read.rs:215–237 | likely-incidental 重复 |
| Parquet 每 chunk reader 重建 | read.rs:397–411 | needs-verification |
| vstack 累积 / bridge rechunk+空值掩码 | read.rs:351; bridge/mod.rs:49,104–114 | contract-mandated |

负发现【事实】：无 digest/integrity pass；无 LazyFrame/LazyCsvReader；无 payload 帧 clone（clone 均为 schema/元数据/索引向量）；CSV/JSON 游标跨批持续、无按批归零重启。

**H2【待测假设】** 单遍解码（校验信息从解码帧派生，或验证融合进解码管线）可在保持 typed-drift 错误分类与 RSS 门的前提下消除 CSV 第二全文解析，并把 JSON 语义解析遍数降为数组形 3→1、NDJSON 2→1，同时省去中间 NDJSON 缓冲。验证门：B2 基准 + tests/local_tabular.rs + tests/memory_bound.rs:15–16,107–110（64 MiB 源峰值增量 ≤32 MiB 的既有硬门）。

### 3.3 Snapshot storage

**S1【事实】写路径摘要需要第二次全文件读**：`write_partition`（store.rs:797–833）序列 = `create_new` 打开 staging 文件 :803–808 → `ArrowWriter::try_new(file,…)` 直写 + SNAPPY / row group=MAX_BATCH_ROWS :810–817 → `into_inner()`+`sync_all()` :818–822 → 字节量取 fstat（:823–826，非重读）→ `seek(SeekFrom::Start(0))` :827 + `digest_file` 全量重读 :829（digest.rs:64–77，64 KiB 栈块）；行数取自内存 envelope :830–831。commit 为纯 rename + 目录 fsync（:850–861）加单个 SQLite 事务（:888–972）。成因【事实】：最终文件名内容寻址 `{seq}-{sha256}.parquet`（:624–628），摘要必须在 install 前存在。

**S2【事实·逻辑读取两遍；物理 I/O 另计】**：`read_partition` :1125–1263 的逻辑读取顺序 = symlink/长度 stats :1132–1171 → `digest_file` 对全文做一次完整顺序读取 :1175（失败即 `IntegrityFailure::DigestMismatch` :1176–1182）→ rewind :1183 → Parquet footer + seek/range 解码并 drain 至尽 :1192–1242（单分区单批不变量 :1236–1242）。无解密。代码可证明的是**逻辑读取两遍**（digest 顺序读 + 解码 range 读）；真实块设备 I/O 量取决于 OS page cache 命中与 Parquet 访问模式，不能由调用顺序静态断言为恰好 2× 物理读。`verify_snapshot` :197–205 对全部分区重复该双遍逻辑读取流程。

**S3【事实】manifest 与分区遍历无 N+1 读**：manifest 存于 SQLite 行而非文件；`load_manifest_inner` :989–1123 = 单行 header 查询 :994–1000 + 一条有序分区查询 :1087–1090 + 容量预分配 Vec :1103–1105；读取侧分区推进为内存索引 :533。serde_json 仅用于 schema/lineage 两列（:1034、:1063）。

**S4【事实】连接生命周期与 SQLite**：`open_connection` :631–647 每操作新建连接（symlink 探测 + busy_timeout(5000) + foreign_keys/WAL/synchronous=FULL 四项 PRAGMA :640–644），无池化；append 全程零 SQL（:379–462）；Immediate 事务共五处（:215、:327、:662、:767、:902）；维护类操作 recover/GC/orphan-scan 每候选新建连接（:1282、:1401、:1378→1292、:325–328），候选上限 1,024（manifest.rs:17，:1460–1467 校验）——有界 N+1 式连接抖动。

**S5 重复 I/O 分类**：

| pass | 证据 | 分类 |
| --- | --- | --- |
| 写后 digest 全文重读 | store.rs:827–829 | likely-replaceable：编码时未做哈希 tee（:813 将裸 `File` 交给 ArrowWriter），以哈希包装 `Write` 即可在飞行中得到含 footer 的同一摘要，消除该 pass；字节量已用 fstat 规避重读 |
| 读前 digest 全文校验 | :1175–1182 | contract-required 语义 / 实现可替换 → needs-verification：损坏检测是被测试固化的行为（:1914–1976），tee 化会把"先验后读"变为"边读边验"，失败时机改变，必须保持 fail-closed 与错误分类不变 |
| 解码后 drain-to-end | :1236–1242 | contract-required（单分区单 row group 单批不变量，manifest.rs:237–241）|
| 维护操作每候选新建连接 | :1282、:1401、:325–328 | likely-replaceable（每 sweep 复用一个连接）；候选 ≤1,024 有界 |

**S6 负发现**【事实】：写字节量非重读（fstat）；行数/统计非回读（无 Parquet footer 回读）；读路径无 N+1 SQL；manifest 非整文件解析；每侧恰一次 digest 调用（无二次哈希）；全 crate 无 read_to_end/mmap/BufReader/BufWriter；append 零 SQL；零行 envelope 不创建分区文件（:414–416）。

**H3【待测假设】** 写路径 tee 化可省一次全文逻辑读（该重读常被页缓存吸收，收益集中在冷缓存与大分区场景）；读路径 tee 化把逻辑读取从两遍降为一遍——物理 I/O 的实际降幅取决于缓存命中与访问模式，须经 B3 实测。两者均须 B3 基准与 checksum fail-closed 故障注入测试（store.rs:1914–1976）守护。

## 4. 禁止盲目精简的代码

以下部分的可审计性优先于精简【建议】，任何改动须保持外部可观测语义：

1. **canonical encoding / digest preimage**：batch.rs 的元数据规范编解码（encode_metadata/decode_metadata/ensure_exact_metadata_keys，:588–628）与指纹（fingerprint_bytes :646）——键序与内容即摘要前像；storage digest.rs 对精确写入字节序列（含 Parquet footer）求 SHA-256。任何流化/tee 必须覆盖完全相同的字节序列。
2. **Arrow 类型显式 sink 与位图逻辑**：remainder.rs `BitPackedSink` :385、`ExactPrimitiveSink<T>` :439、`ExactBooleanSink` :520、`VariableBytes` :588——canonical buffer layout 是准入声明与信封字节数的根基。
3. **symlink、长度、摘要及 schema 完整性检查**：store.rs:1132–1171；connector read.rs 的 schema-drift 守卫（:227–233）；CSV/JSON typed drift 校验。
4. **内存律与 failure-injection 测试**：memory.rs 相位分配计数器与 MemoryTracker；engine tests t43/t46/t47/t52/t55/t56（tests.rs:2114/2225/2278/2541/2677/2747）；local-tabular tests/memory_bound.rs:15–16,107–110。
5. **错误分类与有界错误文本**：error.rs 的 ErrorCategory/retryable 映射与 sanitized fallback summary（:209–290）；connector 的定界错误消息构造。

四类重复的区分与实例：

| 类别 | 定义 | 本仓库实例 |
| --- | --- | --- |
| 必要重复 | 正确性/合同直接要求，不可去除 | 读路径 digest 校验语义（store.rs:1175）；LogicalSchema↔Arrow 边界双表示（batch.rs:299–578）；CSV typed-drift 校验行为 |
| 可生成重复 | 机械逐类型样板，可用宏/泛型生成 | ffi.rs map_i8…map_u64 十个逐类型映射（:191–241）；remainder.rs ColumnSink 的逐 primitive 分支（:203+） |
| 偶然重复 | 同一请求内的冗余防御或重复计算 | Parquet magic ×2（inspect.rs:24; read.rs:215）与 footer ×2（inspect.rs:26–41; read.rs:216–233）；predict_export_transition 双循环（predict.rs:486/:492）；CSV header 复查 vs 推理期 header 校验 |
| 热路径重复 | 主数据通路上的乘性重复处理 | CSV 锁步第二解析（与解码同范围，read.rs:487–531）；JSON 多阶段处理链（framing + 至多三次语义解析 + 一次序列化，read.rs:436–477、json_stream.rs:92–239）；PredictedSchema 每 step 克隆 × 二分（predict.rs:106/264×:163）；rule 后 column_physical_sum 全量重算 |

区分原则【建议】：偶然/热路径重复是精简候选；必要重复的实现策略仅在外部可观测语义（错误类别、失败时机、fail-closed 行为）保持不变时可优化；可生成重复属低风险清理。

## 5. 活跃 PR 依赖矩阵

状态核对【事实，2026-08-22】：#53 OPEN/Draft、head `c50e3c937b3494f56ed6cda19c47a83aff36de93`；#71 OPEN/Draft、head `2b9fdcb716a66a0ff92cedd82825f533c16b6250`（CI run 32545765540 双工具链全绿）；#74 OPEN/Draft、head 分支 `agent/issue-073-e4-artifact-verification-bundle-storage`。三者均未合入 main。

| 候选优化 | 当前 owner | 被哪个 PR 阻塞 | 可启动条件 |
| --- | --- | --- | --- |
| Preview/remainder 精简 | #53/#71 | #71 尚未合入 #53 | #53 合并 main |
| VerificationBundle accounting | #74 | #74 尚未通过 | #74 合并 main |
| Connector 单次解析 | main | 无代码冲突 | O0-D0 批准 |
| Snapshot 单次 I/O | main | 需保持存储合同 | 基准与内存证明完成 |

注：

1.【事实】#74 的 `VerificationBundle` 实现仅存在于其分支——在 base `main@684b0ab` 的 backend 内 grep "VerificationBundle" 为 0 处。不得把该分支的实现当作 main 已有事实。
2.【事实】#71 的 diff 面 = preview.rs/memory.rs/remainder.rs/tests.rs（engine），不含 predict.rs/preflight.rs/engine.rs；#53 改写 consume_envelope 一带的执行循环。因此 §7 候选 1（predict 游标化）与候选 4 的 engine 文件部分须在 #53 合并后做行号级冲突复查。
3.【事实】storage crate 同时是 #74 的扩展面：store.rs 的结构拆分必须排在 #74 合并之后或与其协调，否则必然冲突。

## 6. 可复现基准设计

本节只设计测量方案，不提交任何基准代码【建议】。

### 6.0 通用指标采集设计

| 指标 | 采集方式（设计） |
| --- | --- |
| wall time | `std::time::Instant` 环绕 scoped 区间；≥30 次取 P50/P95 |
| CPU time | getrusage(RUSAGE_SELF) utime+stime 差分，或 `/usr/bin/time -v` |
| peak RSS | ru_maxrss（getrusage） |
| allocation count/bytes | 全局分配器挂钩计数（仿 stillflow-engine 现有 test_alloc::PhasedAlloc 模式，lib.rs:301），以 cargo feature / cfg(test) 门控启用 |
| 文件实际读取字节数 | /proc/<pid>/io read_bytes 差分；或计数 `Read` wrapper |
| 调用次数（predict invocation / reader construction / schema clone） | feature 门控的计数插桩，仅基准构建启用 |

### 6.1 B1：Engine prediction

- fixture：进程内构造 RecordBatch（不经 connector）。列数 ∈ {64, 256, 1024}（一半 utf8@64B 变长值、一半 i64）；规则数 ∈ {1, 32, 128}（DeriveColumn-Utf8 / Cast / Rename / DropColumn 混合）；每 envelope 10,000 行；plan 形态 scan → ApplyRules → materialize。
- command 形态：`cargo test -p stillflow-engine --release <pred_bench> -- --ignored --nocapture`（未来实现时的形态）。
- 预期观测量：largest_feasible_k 的 wall/CPU；predict 调用次数（含逐切片迭代轮数 I 的分布——检验 F1 上界而非恒等式）；PredictedSchema 克隆次数；column_physical_sum 执行次数；peak RSS；alloc count/bytes。用于检验 §3.1 I1 公式的实测形状。
- 通过/停止条件：先记录基线；后续优化 PR 必须给出相对基线的实测对比，且 t43/t46/t47/t52/t55/t56 内存律测试全绿；任一回归或 peak RSS 越过 MAX_ENGINE_PEAK_BYTES 即停止并回退。

### 6.2 B2：Connector ingestion

- fixture：本地生成的 CSV/JSONL/Parquet 文件各一；列数 ∈ {10, 100}；行数 ∈ {100,000, 1,000,000}；utf8 列含 32–96 字节变长值；生成脚本置于一次性目录，不提交。
- command 形态：`cargo test -p stillflow-connector-local-tabular --release <read_bench> -- --ignored`。
- 预期观测量：端到端 read_batches 的 wall/CPU/RSS/alloc；实际读取字节（CSV 应 ≈ 文件大小 × 解析遍数——直接检验 C1 的"2×"）；解析 pass 计数；reader 构造计数（Parquet 每 chunk 重建假设）。
- 通过/停止条件：记录基线；单遍解码候选须保持 typed-drift 错误分类不变且 memory_bound RSS 门（64 MiB 源峰值增量 ≤32 MiB）不回退；否则停止。

### 6.3 B3：Snapshot storage

- fixture：独立 store 目录；分区数 ∈ {1, 1,024, 16,384}，每分区一个批次（≤65,536 行 × 混合列型）；分别测 append、read、verify_snapshot 三相。
- command 形态：`cargo test -p stillflow-storage --release <storage_bench> -- --ignored`。
- 预期观测量：分相 wall/CPU/RSS；逻辑读取字节数（计数 `Read` wrapper）与 `/proc/<pid>/io read_bytes` 差分并列记录——后者受页缓存影响、反映物理 I/O，不得单独作为解析遍数或逻辑字节数的判定依据；SQLite 语句/连接计数；fsync 次数（可选 strace）。用于检验 S2 的双遍逻辑读取假设及 tee 化后的物理 I/O 变化。
- 通过/停止条件：记录基线；单次有界读取候选须保持 checksum fail-closed 测试（store.rs:1914–1976）与 recovery/GC 测试组全绿；失败即停。

## 7. 后续任务排序

最多四个独立候选 PR（本任务不设计公共 API）：

| # | 候选 PR | 预期收益（结构性假设，须经基准证实） | 合同风险 | 验证方法 | 启动依赖 |
| --- | --- | --- | --- | --- | --- |
| 1 | Engine compiled-rule / reusable prediction cursor | 消除 O(log k) 次全量重算与每 step schema 克隆，预测降为单次增量扫描【待测假设 H1】 | 中高：准入 oracle 必须逐字节等价于现有 predict | B1 + 内存律测试组 t43/t46/t47/t52/t55/t56 | predict.rs 归 main 所有；启动前与 #53 对 consume_envelope 的改动作行号级冲突复查 |
| 2 | Connector single-pass decode | CSV 移除第二全文解析器；JSON 语义解析遍数数组形 3→1、NDJSON 2→1，并移除中间 NDJSON 缓冲【待测假设 H2】 | 中：typed-drift 错误行为已被测试固化 | B2 + tests/local_tabular.rs + tests/memory_bound.rs | O0-D0 批准后即可启动；与 #53/#71/#74 零代码重叠（不同 crate） |
| 3 | Snapshot single-pass bounded verification | 写路径省一次全文逻辑读；读路径逻辑读取由两遍降为一遍（物理 I/O 降幅实测）【待测假设 H3】 | 中：读侧校验的失败时机变化需合同注记；持久化格式不变 | B3 + checksum fail-closed + recovery/GC 测试组 | B3 基线 + 内存证明完成；storage crate 与 #74 重叠，须串行或协调 |
| 4 | 模块拆分与测试外移 | store.rs(2,179 行)/preflight.rs(849)/read.rs(1,008) 职责解耦，缩小后续改动冲突面 | 低：纯结构，逐 crate 小步 | 全套测试 + git diff 面审查 | store.rs 拆分排在 #74 之后或协调；engine 文件拆分在 #53 合并后进行 |

排序建议【建议】：2 → 1 → 3 → 4（按冲突面从小到大；候选 2 与三条活跃 PR 零重叠，可在本盘点获批后立即启动）。

## 8. 验收自检

- [x] 每个结论绑定 path:symbol:line（§2.1–§2.2、§3.1–§3.3 的引用清单）
- [x] 事实 / 静态推断 / 待测假设 / 建议四类标注贯穿全文
- [x] 无未基准支持的"显著提升"类表述——所有收益均标注为结构性假设并指向 B1–B3 验证门
- [x] 无任何未完成事项标记或占位符；无空表格
- [x] 最终 diff 仅含本文档；生产代码、Cargo、CI、前端与其他文档零变更
- [x] Draft PR 保持 Draft；不标记 Ready；不合并
