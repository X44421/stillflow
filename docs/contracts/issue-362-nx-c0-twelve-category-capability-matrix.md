# Issue #362: twelve-category NodeGraph capability matrix and compatibility contract

> Status: Candidate frozen contract for acceptance by #362
> Risk: L1 — documentation and contract only; no Rust, executor, storage, API, schema, workflow or OpenShip change
> Parent: #361 (Epic: complete the twelve NodeGraph data-processing capabilities)
> Predecessor contracts, normative and **unchanged** by this document: NG-C0 #324, NX-C0 #335, NX-C1 #340, NX-V0 #342
> Designs consumed: NX-D0 #345, NX-P0 #346 (both merged on `main`)
> Base: `main@cae3d5f4f95e0999fd71f927e535c47869040158`
> Suggested branch: `agent/issue-362-nx-c0-twelve-capability-matrix`

This document converts the twelve data-processing capabilities of Epic #361 into an executable
capability matrix: for every category it freezes the input arity, configuration surface, output-schema
law, error and diagnostic surface, resource treatment, compatibility duty and execution routing — and
assigns the extension mechanism that the corresponding child issue must use. It authorizes no runtime
change by itself. Implementation issues remain blocked until this contract is accepted and their own
risk-level gates are satisfied.

Naming note: this issue is the **second** contract named "NX-C0". The first is #335
(`docs/contracts/issue-335-nx-c0-node-extension-contract.md`), which stays normative for the extension
base. Throughout this document, and in any follow-up work, capabilities are identified by **issue
number**, never by the bare label `NX-C0`/`NX-C1`/`NX-N1`/`NX-N2`, which name closed predecessor issues
(#335, #340, #337, #341).

---

## 1. Decision and authority boundary

### 1.1 What this contract freezes

1. The twelve-category capability matrix in §2, including each category's configuration surface,
   output-schema law, error surface and routing.
2. The extension mechanism for each capability (§3): which capabilities are already expressible, which
   extend the `Rule`/`Expr` AST, which require a new `PlanNodeKind`, which merely *admit* an existing
   verification rule, and which stay a separate report runtime.
3. The paused-capability ledger (§4): every currently paused compile-time capability that a wave issue
   depends on, and therefore every "un-pause" decision this wave must take explicitly.
4. Materialization, Preview and Verification routing (§5), consistent with #346.
5. The compatibility duties (§6) that the eleven atomic built-ins, published `PlanVersion` records,
   canonical bytes, and `GraphRevision` migration must retain.
6. The error and diagnostic vocabulary (§7) and the resource laws (§8) that child issues must reuse
   rather than reinvent.
7. The per-child execution boundary (§9): authorized paths, dependencies, gates, test boundary and
   **actual execution risk level** for #363–#379 and #381.

### 1.2 Authority

`stillflow_plan::LogicalPlan` (canonical bytes + fingerprint) remains the single logical execution
authority; `Rule`/`Expr` remain the only logical languages; `NodeDefinition`/`NodeRegistry` remain
declarative and closed. This contract adds no executor, no second AST, no second canonicalizer, no
registry channel and no publication path. Rust stays authoritative: where this document and the code
diverge, the code plus the frozen predecessor contracts win, and the divergence is a contract defect.

This contract does **not** authorize any of the following, all of which require their own accepted
contract at their own risk level: un-pausing a paused capability (§4), widening a frozen bound (§8),
changing `PlanNodeKind` or plan serialization (§3.3), admitting `Validate`/`Deduplicate` to ordinary
materialization (§3.4), opening multi-source/DAG execution (§3.3, #375), or lifting the #93 XR HOLD.

### 1.3 Relationship to #335 (append-only)

`docs/contracts/issue-335-nx-c0-node-extension-contract.md` is not edited, superseded or re-interpreted
by this document. This contract extends it additively: everything #335 froze (versioning policy, the
machine-readable config-constraint vocabulary, the diagnostics field set, request order and deadline,
catalog surface, module boundaries, the version-1 compatibility baseline for the eleven built-ins)
continues to apply to every capability added by this wave.

### 1.4 Catalog counting (decision D5)

* **Eleven** atomic built-in product kinds are registered in
  `stillflow-core/src/node_graph/definitions/` — `stillflow.node.source`, `.select`, `.filter`,
  `.rename`, `.trim`, `.cast`, `.replace-literal`, `.fill-null`, `.drop-column`, `.derive-column`,
  `.output` (`definitions/mod.rs:31-43`; frozen table NG-C0 #324 §4).
* The **deployed catalog is twelve** entries: `NodeRegistry::deployed()` additionally loads the
  compiled-in composite package `stillflow.composite.trim-clean` (`node_graph/composite.rs`);
  `GET /v1/node-types` serves the deployed catalog.
* Therefore: the capability matrix in §2 is keyed by **atomic** kinds and capabilities; any statement
  about "the catalog" in UI, API or acceptance text means `NodeRegistry::deployed()`. The four
  hard-coded catalog counts (`registry.rs:32`, `definitions/mod.rs:296`, `node_graph/mod.rs:1167`,
  `http_entry_e2e.rs:419`) move with the implementation issue that adds a kind, not with this contract.

---

## 2. Capability matrix

### 2.1 Column definitions

| Column | Meaning |
| --- | --- |
| Input arity | Number of logical inputs the capability consumes on the version-1 profile (exactly one unless stated) |
| Config surface | The wire-visible configuration; every field must be typed and machine-readable per #335 |
| Output-schema law | How field identity, order, logical type and nullability evolve |
| Routing | Where the capability may execute: `materialize`, `verification` (E4 only), or `report` (no data output) |
| Bucket | Extension mechanism from §3: `A` config-only, `B` Rule/Expr AST, `C` new plan node kind, `D` verification-rule admission, `E` separate report runtime |
| Child | The issue that owns implementation |
| Risk | Actual execution risk level for the child (decision D3), independent of the GitHub label |

### 2.2 Master matrix

| # | Category | Child | Input arity | Config surface | Output-schema law | Routing | Bucket | Risk |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 字段整理 — field shaping | #365 | 1 | ordered `select` list; `rename` mapping; `drop-column`; optional batch mapping | subset and order exactly as declared; `ColumnId` preserved by rename/drop; only `name` changes | materialize | A | L2 |
| 2 | 值清洗 — value cleaning | #366 | 1 | target column + operation + operation parameters | `Utf8` in → `Utf8` out; nullability preserved; NULL never becomes a value | materialize | B | L3 |
| 3 | 类型转换 — type conversion and date parsing | #367 | 1 | target `dataType`; `onFailure` = `error`/`setNull`; temporal format, timezone, numeric separators and precision | declared target type; nullability widens iff `setNull`; no implicit timezone or implicit conversion | materialize | B + §4 lifts | L3 |
| 4 | 缺失值处理 — missing-value definition and fill | #369 (+#371) | 1 | per-field missing definition (NULL / empty string / whitespace-only / custom markers); fixed fill value; forward fill additionally requires an order | fill value must match the field type; custom markers never alter undeclared fields; nullability narrows only when explicitly declared | materialize | B (fixed) / C (forward fill) | L3 |
| 5 | 筛选 — filtering | #365 | 1 | predicate `Expr`; keep or exclude mode | schema unchanged | materialize | A | L2 |
| 6 | 去重 — deduplication | #372 | 1 | ordered, non-empty, typed key tuple; keep-first/last; explicit ordering key | schema unchanged; row count reduces; duplicates are reported, never silently dropped | verification | D | L3 |
| 7 | 校验 — validation and failure handling | #373 | 1 | rule predicate + severity (`warning`/`error`) + message | schema unchanged; error rows are removed **and** routed to rejected rows | verification | D | L3 |
| 8 | 派生计算 — derived computation | #368 | 1 | output field id, name, logical type, nullability; expression | one appended field with declared identity; existing fields untouched | materialize | B + §4 lifts | L3 |
| 9 | 结构变换 — structural transforms | #378 (+#379) | 1 | array path; empty-array and NULL policy; unpivot column set; expansion bound | row-multiplying: every output row keeps a stable source-record association; field identity and conflicts declared | materialize | C + §4 lifts | L3 |
| 10 | 关联／合并 — join and union | #376 | 2+ | join type; ordered left/right key expressions; explicit output mapping; union member order | Join: left fields then right fields, and only if all ids/names stay unique — otherwise fail before connector reads. Union: identity and order of member 0 | materialize | C | L3 |
| 11 | 聚合 — aggregation | #377 | 1 | group keys; metrics (`count`, `sum`, `avg`, `min`, `max`, `count-distinct`); output metric naming | grouped schema; deterministic types and nullability; results invariant under batch size | materialize | C | L3 |
| 12 | 分析 — analysis and findings | #374 | 1 | distribution, frequency, null-rate, rule-anomaly and IQR rules; full or sampled scope | **no modification of materialized output**; produces a report artifact only | report | E | L3 |

### 2.3 Per-category normative notes

**(1) Field shaping — #365.** Batch name mapping is realized as an ordered sequence of rename steps; it
must not be expressed as an unordered map, because field order and identity must be reproducible. An
empty target list, a duplicate entry, an unknown `ColumnId`, or an attempt to drop the last remaining
field is rejected (`NG_INVALID_CONFIG` / `NG_UNKNOWN_COLUMN`), not repaired.

**(2) Value cleaning — #366.** Every operation declares its target, its parameters, its NULL policy and
its non-text policy. Operations act on values only; they never change field identity, order or type.
Unicode normalization form and the definition of "whitespace" are part of the configuration, not
implicit behaviour; the same input must produce the same output across restarts and batch boundaries.

**(3) Type conversion — #367.** `onFailure` = `error` and `error|setNull` semantics are preserved
exactly (NX-C0 #335 baseline; `CastFailurePolicy`). Date/time parsing must declare format and timezone
explicitly; an ambiguous or absent timezone fails under `error` or yields NULL under `setNull`, never a
guess. This category is additionally constrained by §4 (`DateToUtf8CastPaused`,
`TimestampSecondPaused`).

**(4) Missing values — #369 and #371.** Missingness is *declared*, never inferred: `0`, `false` and
legitimate text are never treated as missing unless the configuration says so. Fixed fill is a bounded,
row-local operation (#369). Forward and grouped fill are **stateful across batches** and therefore
require the shared ordering/stateful substrate of #370 before they may be implemented (#371); group
boundaries must be explicit and fill must never cross a group.

**(5) Filtering — #365.** Combined AND/OR/NOT is expressed with the existing `Expr::Binary` and
`Expr::Unary` nodes; no new predicate language is introduced. Keep-mode is the predicate, exclude-mode
is its logical negation. The predicate must type-check to `Boolean`; a NULL predicate result removes the
row (three-valued logic), and this must be stated in the node's config documentation and covered by
real-data tests. Ordered comparison on incomparable types fails closed
(`OrderedComparisonIncompatible`), which is the same contract that OpenShip#35 surfaces in the client.

**(6) Deduplication — #372.** `Rule::Deduplicate` already exists and is admitted only on the E4
verification path (#346 §7.3). This wave **admits** it; it does not build it. Keys are an ordered,
non-empty, typed tuple of existing `ColumnId`s; equality is exact and typed including E4 NULL, NaN,
signed-zero, timestamp, Utf8, Binary and timezone laws; keep-first uses ascending logical Scan output
ordinal; duplicate findings and rejected rows use E4 outputs only. Ordinary materialization must
continue to reject the rule rather than silently switch modes.

**(7) Validation — #373.** `Rule::Validate` already exists and is verification-only (#346 §7.2). Its
mapping is frozen: `true` keeps the row with no finding; `false` with `warning` keeps the row with one
warning finding; `false` with `error` removes the row with one error finding plus permitted rejected-row
payload; `NULL` behaves as `false`. Rules run in declared order and a terminal error cannot be
re-admitted by a later rule. Findings must locate the failure by rule, `nodeId`, `columnId` and field
path, and the report must bind to `PlanVersion`, `Run` and the input version (see §5.3).

**(8) Derived computation — #368.** `Rule::DeriveColumn` already declares `id`, unique `name`,
`dataType`, `nullable` and an `Expr`; the wave extends the **expression** surface, not the field
identity rules. A derivation must never silently change an existing field, and expression depth, node
count and byte budgets stay under the existing analyzer bounds. Arithmetic, `contains` and text
extraction are currently paused (§4) and must be un-paused by an explicit decision before #368 can
promise the scope in its own issue text.

**(9) Structural transforms — #378 and #379.** Array explode multiplies rows; every output row must
carry a stable association to its source record, and the expansion must be bounded (an oversized
expansion is rejected or cancelled, never unbounded). Unpivot converts columns to rows with explicit
output identity. Both require nested/list execution, which is paused today (§4, `ListStructPaused`), and
both need a new plan-node kind (§3.3). Pivot (#379) is built on aggregation (#377) and structural
transform (#378) and must bound its output column set explicitly.

**(10) Join and union — #376.** The logical plan IR already models `Join` and `Union`
(`PlanNodeKind::Join { join_type, keys }`, `PlanNodeKind::Union`, six `JoinType`s) and the engine
rejects both at preflight; the version-1 product graph cannot express them. This category therefore
requires three things together: the #345-based multi-source execution contract (folded into #363,
decision D1), bounded multi-source execution (#375), and a `NodeGraph`/`GraphRevision` format decision
for non-linear graphs (§6.4). The semantic slots are already frozen in the #345 design and are adopted
here as the F-ENG1 requirements: explicit ordered key pairs with no inference, NULL never equal to NULL,
type-incompatible keys rejected before connector reads, output = left fields then right fields only when
ids and names stay unique, deterministic left-major row order with unmatched rows appended for
Right/Full, and batch-boundary invariance. Union requires an ordered member list of at least two members
with exactly equal ordered `ColumnId` sequence, logical types and names; it never deduplicates, sorts or
coalesces.

**(11) Aggregation — #377.** A new plan-node kind is required; no aggregate operator exists today.
Results must be invariant under input batch size, NULL group keys and NULL metrics must have declared
behaviour, `count-distinct` must respect the memory law, and output field identity, naming, type and
nullability must be deterministic and reproducible in Preview, `PlanVersion`, `Run`, Artifact and
export.

**(12) Analysis — #374.** Profiling, quality and drift already exist as job/artifact-driven runtimes
(`engine/src/{profile,quality,drift}.rs`) that are **not** reachable from a graph. This category must
therefore decide, in #364, whether analysis is exposed as a report-only node over the existing runtime
or stays a separate job surface. Analysis never fabricates data and never changes materialized output;
sampled results must carry the sampling marker, scope and input fingerprint, and every report must bind
to input, `PlanVersion`, `Run` and rule version.

---

## 3. Extension mechanism freeze

### 3.1 Bucket A — configuration and catalog only (#365)

Capabilities 1 and 5 are expressible with the existing eleven built-ins and the existing `Rule`/`Expr`
surface. Their work is configuration shape, validation, catalog metadata, schema propagation tests and
client contract — not new execution semantics. They stay L2.

**Limit interaction.** Batch rename mapping conflicts with the frozen per-node rule bound
`MAX_RULES_PER_NODE = 1` (`core/src/node_graph/mod.rs:35`; restated in NX-C1 #340 §8). The contract
therefore rules: **either** the mapping is expressed as an ordered chain of single-rule nodes, **or**
#365 raises the per-node rule bound for this node kind under an explicit limit amendment in this
contract's §8. It must not be implemented by silently accepting multiple rules in one node.

Note that the engine declares its own `MAX_RULES_PER_NODE = 256` (`engine/src/lib.rs:94`). That is an
engine-internal capacity bound; per #335 it must never be adopted as, or confused with, the product
bound, and no child issue may cite it as authorization to emit multiple rules per node.

### 3.2 Bucket B — `Rule`/`Expr` AST extension (#366, #367, #368, #369, part of #371)

Extending `Rule` (currently 10 variants: Rename, Cast, Trim, ReplaceLiteral, FillNull, DropColumn,
DeriveColumn, FilterRows, Deduplicate, Validate) or `Expr` (currently 7 variants: Column, Literal,
Unary, Binary, IsNull, Cast, Coalesce) is an **L3** change per AGENTS.md ("expression/rule AST"), and it
is a compile-forced edit across the exhaustive matches in `stillflow-plan` (`semantics.rs`,
`rule.rs`) and `stillflow-engine` (`lower.rs`, `incremental.rs`, `preflight.rs`, `predict.rs`).

Rules for any bucket-B change:

1. **Additive only.** New variants are added; no existing variant's name, wire tag, field meaning or
   ordering changes. Version-1 serialized plans must continue to decode to the same `Rule`/`Expr`.
2. **Wire tags are frozen once published.** A new variant's serde tag is part of the public wire
   contract and cannot be renamed later without a migration decision.
3. **Schema law first.** Each new variant declares its effect in `semantics.rs` before it is admitted
   by the engine; capability gates (§4) are evaluated in the shared analyzer, not duplicated in the
   engine.
4. **One node, one rule.** The `MAX_RULES_PER_NODE = 1` bound stays unless §3.1's amendment is taken.
5. **Catalog agreement.** A new configuration field must appear identically in the definition's
   machine-readable constraints (`required`, `valueKind`, `enumValues`, `minLength`/`maxLength`,
   `minItems`/`maxItems`, `uniqueItems`, `ordered`, `nonEmpty`, `nonNull`, `byteBound`) and in the
   catalog entry (#335 rule); #335's catalog↔validator agreement test must cover it. A capability that
   is type-limited (for example text cleaning, which requires `Utf8`) must declare it with the existing
   per-port support conditions — `requiresType`, `requiresNonNullable`/`requiresNullable`,
   `forbidsType`, `requiresExecutableType`, `minFields` (`core/src/node_graph/definition.rs:194-206`) —
   rather than in prose, so the client can disable an incompatible configuration before submitting it.
6. **Risk level.** Children #366, #367, #368, #369 are executed at **L3** even though their GitHub
   labels currently say L2 (decision D3). #365 stays L2.

### 3.3 Bucket C — new plan-node kinds and non-linear execution (#370, #376, #377, #378, #379)

New capabilities in this bucket change `PlanNodeKind` (today: Scan, Project, Filter, ApplyRules, Join,
Union, Materialize) and therefore plan serialization, the compile-time work budget, the preflight
shape validation, the engine lowering and the memory laws. Consequences that child issues must carry:

* `Sort` (#370), `Aggregate` (#377), explode/unpivot (#378) and pivot (#379) each need a new variant
  **and** a new `NodeLoweringTarget` (the enum is closed at five targets) plus catalog advertisement.
* Join/Union (#376) need no new variant — the variants exist and are rejected at preflight
  (`preflight.rs:156-158`, `303-305`) — but un-rejecting them is an execution-semantics change of the
  same class.
* Every new relational/stateful operator must declare its memory law, spill or reject policy,
  cancellation behaviour and atomic-publication behaviour (§8).
* Plan serialization changes require a version decision: existing stored plans and their canonical
  bytes stay valid and unchanged; a new plan version may only be introduced with its own compatibility
  statement.

### 3.4 Bucket D — admitting existing verification rules (#372, #373)

`Deduplicate` and `Validate` are **already implemented** as rules executed on the E4 verification path,
with artifact kinds, report sections, rejected-row routing and memory laws. The wave's work is
admission, not construction: #364 freezes the target/context, report binding, rejected-row policy and
the explicit statement that ordinary materialization never infers verification from a rule's presence.
The three independent product-path gates (plan semantics `RuleNotAdmitted`, engine `UnsupportedRule` in
lowering/incremental/predict, preflight verification-only admission) are removed only by the issue that
#364 authorizes, and only under E4 laws.

### 3.5 Bucket E — report runtime boundary (#374)

Analysis stays report-only. Either it is exposed through a graph-reachable node that produces a report
artifact without altering materialized output, or it remains the existing job/artifact surface. #364
decides which, and the decision must preserve the existing profile/quality/drift contracts rather than
creating a parallel report authority.

---

## 4. Paused-capability ledger

The compile-time capability gate (`stillflow-plan/src/semantics.rs`, `pub mod capability`) is frozen
behaviour: its own comment states the paused set "must never be unlocked, relaxed, or reordered here",
and NX-C1 #340 refuses "unpausing any capability" (X-4). Several wave issues nevertheless depend on
paused capabilities. This ledger makes the dependency explicit; **lifting any row requires its own
contracted decision** and is not authorized by this document.

| Paused item (code) | Semantic kind | Blocks | Wave issue | Required decision |
| --- | --- | --- | --- | --- |
| Arithmetic: `+ - * / %`, unary negate | `CheckedArithmeticPaused` | numeric derivation, aggregation metrics | #368, #377 | Contract must define checked-overflow, division-by-zero and precision semantics before un-pausing |
| `contains` | `ContainsPaused` | text extraction / matching in derivation | #368 | Decide whether `contains` is admitted or replaced by an explicit predicate |
| List and Struct types | `ListStructPaused` | array explode, nested unpivot/join keys | #378, #376 | Nested-type execution contract (memory, nullability, comparison, serialization) |
| Timestamp with second unit | `TimestampSecondPaused` | date parsing with second precision | #367 | Decide unit coverage and canonical representation |
| Date/Timestamp → Utf8 cast | `DateToUtf8CastPaused` | date formatting / round-trip | #367 | Decide formatting contract and locale independence |
| Binary casts | `BinaryCastUnauthorized` | binary parsing/formatting | #367 (only if claimed) | Keep paused unless a concrete capability requires it |
| Ordered comparison on incomparable types | `OrderedComparisonIncompatible` | filters and join keys with mixed types | #365, #376 | Keep fail-closed; it is the intended behaviour (cf. OpenShip#35) |

Any un-pause is an execution-semantics change (L3), must name the exact semantic kind it lifts, and must
ship with differential tests proving that previously compiling graphs still compile to byte-identical
plans.

---

## 5. Materialization, Preview and Verification routing

### 5.1 Routing classes

| Class | Meaning | Categories |
| --- | --- | --- |
| `materialize` | Executes on the ordinary path in Preview and in Run; publishes a snapshot/artifact through the existing path | 1, 2, 3, 4, 5, 8, 9, 10, 11 |
| `verification` | Executes only with an explicit verification target/context under E4 laws; never inferred from a rule's presence | 6, 7 |
| `report` | Produces a report artifact; never alters materialized output | 12 |
| `preview-only` | Diagnostic evaluation of an incomplete draft; no Job/Run/Artifact side effects | none in this wave (see #346 §8.1) |

### 5.2 Preview

Preview keeps the existing read-only guarantees: no `Job`, `Run` or `Artifact` side effects, and no
release plan is implied by a successful preview. A preview of a capability that requires verification
may display the verification dependency as **unexecuted**; it may not execute or publish E4 artifacts
(#346 §8.2).

### 5.3 Input identity and provenance

Every report, rejected-row set and verification output produced by categories 6, 7 and 12 must bind to
the input version through the authorized input-version digest named by the #346 design
(`VerificationExecutionContext.authorizedInputVersionDigest`). Decision D4 records that:

1. the unmerged digest implementation (`f3451f6`, adding `stillflow.e4.logical-input.v1` and
   `AssetMetadata.version_digest`) is landed as its own L3 PR, and
2. #364 owns the frozen digest domain and the report-provenance semantics, including the missing
   client-visible asset version digest (`SourceAssetView` carries none today).

A placeholder or all-zero digest must never reach a committed report: OpenShip's current placeholder is
explicitly called out as false provenance in `X44421/openship` →
`docs/integration/stillflow-nodegraph-integration.md` ("Known gaps"), and acceptance evidence for
#373/#374/#381 may not be produced on top of it.

---

## 6. Compatibility freeze

### 6.1 Version-1 graph and plan invariants (unchanged)

* The version-1 product graph remains exactly one source, exactly one output, one connected linear path,
  `edges == nodes - 1`, and one incoming/outgoing edge per transform node. Branches, merges, cycles,
  multiple sources and multiple outputs stay rejected with the frozen topology errors.
* Every graph that compiles today must keep compiling to a **byte-identical** `LogicalPlan` — NX-C1 #340
  R-12 ("Strictly additive") and §9 row "Atomic-only v1 graph → compiles (frozen baseline) →
  byte-identical plan". No capability added by this wave may change the canonical bytes or the
  fingerprint of an existing graph.
* `LogicalPlan::canonical_bytes()` and the fingerprint algorithm `stillflow-fnv1a64x4-v1`
  (`stillflow-plan/src/plan.rs:15`) are not modified by this wave.
* A published `PlanVersion` never needs the graph, a package or a revision to execute (#340
  "Published plans never depend on packages"; #342 authority row "Revisions never alter plans; the
  association is bookkeeping only").

### 6.2 Built-in nodes, configuration versions and the cast law

* The eleven atomic kinds, their `configVersion` (1) and their exact `(type_id, config_version)`
  registry lookup are the compatibility baseline; there is no fallback decoding and no shim (#335
  baseline, #340 X-6 analog).
* `cast` keeps `onFailure` = `error` / `setNull` with unchanged semantics: `setNull` widens
  nullability, `error` fails the run; no implicit conversion and no implicit timezone is introduced by
  #367.
* Existing persisted node configurations must keep decoding; new configuration fields are additive and
  optional unless a config-version change is explicitly contracted.

### 6.3 Composite and declarative-package compatibility (NX-C1 #340 §9)

| Case | Compile | Plan | Notes |
| --- | --- | --- | --- |
| Atomic-only v1 graph | compiles (frozen baseline) | byte-identical plan | unchanged, executable without packages |
| Graph referencing `stillflow.composite.*` before deployment | `NG_UNKNOWN_NODE_TYPE` | `NG_UNKNOWN_NODE_TYPE` | fail closed; no fallback |
| Same graph after explicit deployment | expands deterministically | published form is a standard plan | executable with the package removed |
| Package with unknown format/version/operator | deployment refused | n/a | closed admissible-operator set |
| Package content changed under same version | deployment/compile refused (`NG_INVALID_CONFIG`) | published plans unaffected | content digest mismatch |

Composite expansion stays ≤ 4 steps, depth 1, admissible operators limited to the nine transform kinds,
and expansion is accounted **before** work limits so no bound is circumvented (#340 §8).

### 6.4 `GraphRevision` format and migration

* `format_version` is `1` and is **independent** of the graph wire `version`, which stays `1` while the
  graph shape is v1 (#342 §4.1/§5). Known formats: exactly `{1}`; there is no shipped migration.
* Bump rule (#342): "A future format change bumps `format_version` and ships an explicit migration
  `v(n) → v(n+1)`". A revision whose format is newer than the binary knows fails closed
  ("graph revision format is newer than this service"); downgrades are not supported; there is **no**
  auto-migration (§10 Must-not).
* Migration duties, which any format bump in this wave must satisfy: pure, deterministic,
  side-effect-free; dry-run writes nothing and returns the per-node diff in the NX-C0 §7.1 diagnostic
  shape (`nodeId`, `fieldPath`, `expected`, `actual`) plus the would-be `graph_digest`; apply fully
  validates against the target format and appends a new revision with `parent_revision_id` set; history
  is never rewritten; applying at the target format is idempotent; interruption leaves no partial state
  and is retryable.
* **Consequence for #375/#378/#379.** Multi-source/DAG shapes are an explicit non-goal of NX-V0 (#342
  §11) and are forbidden under the current graph law (§6.1). Any change to the graph's topology
  therefore implies a `format_version` bump plus a migration of exactly the kind above, contracted in
  #375's own delivery — not a silent extension of format 1.

### 6.5 Catalog compatibility

`GET /v1/node-types` serves `NodeRegistry::deployed()`. Adding composites is additive: an
atomic-only client keeps working, and a client that cannot resolve a new kind must fail visibly rather
than silently drop it. §1.4 fixes the counting rule (eleven atomic, twelve deployed) so contract, API
and client stay in agreement.

### 6.6 Never-change list

Canonical-byte encoding, fingerprint algorithm, `ColumnId` identity across rename/select/drop, the
`NG_*` code strings already published, the diagnostics field set, the frozen resource bounds (unless a
§8 amendment is explicitly contracted), and every published `PlanVersion`. The contract's own
"stop and return to contract review" trigger applies: if a child issue needs an unlisted public field,
a new persistence format, a new executor-owned semantic decision, an unbounded operation, or a second
runtime/publication authority, work stops and returns here.

---

## 7. Error and diagnostic vocabulary

### 7.1 Reused codes

Child issues reuse the frozen `NodeGraphErrorCode` set (`core/src/node_graph/mod.rs:38-62`, wire strings
`NG_*`) instead of inventing parallel codes:

| Family | Codes |
| --- | --- |
| Version / identity | `NG_UNSUPPORTED_GRAPH_VERSION`, `NG_UNSUPPORTED_CONFIG_VERSION`, `NG_UNKNOWN_NODE_TYPE` |
| Configuration | `NG_INVALID_CONFIG`, `NG_INVALID_PORT`, `NG_SOURCE_BINDING` |
| Topology | `NG_INVALID_TOPOLOGY`, `NG_UNSUPPORTED_TARGET` |
| Schema / typing | `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` |
| Bounds | `NG_LIMIT_GRAPH_BYTES`, `NG_LIMIT_NODES`, `NG_LIMIT_EDGES`, `NG_LIMIT_CONFIG_BYTES`, `NG_LIMIT_METADATA_BYTES`, `NG_LIMIT_STRING_BYTES`, `NG_LIMIT_NESTING_DEPTH`, `NG_LIMIT_RULES_PER_NODE`, `NG_LIMIT_RULES`, `NG_LIMIT_COMPILE_WORK` |
| Plan / internal | `NG_PLAN_INVALID`, `NG_INTERNAL` |

Rule- and expression-level failures keep their existing logical semantics (`SemanticKind`, `RuleError`)
and surface through `NG_INCOMPATIBLE_TYPE`, `NG_UNKNOWN_COLUMN` or `NG_INVALID_CONFIG` as appropriate; a
capability that is still paused (§4) surfaces as a capability error, never as a silent no-op.

**Recorded divergence.** NG-C0 names `NG_UNSUPPORTED_COMPILER_VERSION` for a mismatched compiler stamp
(`issue-324-ng-c0-nodegraph-compiler-contract.md:145`) but that code does **not** exist in the Rust
`NodeGraphErrorCode` enum (`core/src/node_graph/mod.rs:38-62`). This contract rules accordingly: the
compiler-stamp check must either reuse an existing frozen code or have the missing code added
additively under §7.3 by the issue that first needs it. No child issue may assume the code is already
emitted, and no child issue may rename the stamp constant (`NODE_GRAPH_COMPILER_VERSION =
"ng-nodegraph-compiler-v1"`).

### 7.2 Diagnostic location and shape

The compile diagnostic shape is frozen and reused
(`plan/src/node_graph_compiler.rs:76-84`):

```text
CompileDiagnostic { code, node_id, column_id, field_path, expected, actual, message }
```

Every capability in §2 must be able to locate a failure to at least node, column and field path, and
must populate `expected`/`actual` where a value or type was rejected. Diagnostics are bounded
(`MAX_DIAGNOSTICS = 64`, `MAX_DIAGNOSTIC_BYTES = 1024`) and sanitized: no raw row values, no secrets,
no unbounded paths. Engine-side failures map into the same location payload rather than a second
diagnostic vocabulary.

The surrounding rules are inherited unchanged from #335 and apply to every child issue:

* **Envelope.** `ApiErrorResponse { meta: { apiVersion, requestId }, error: { code, message,
  diagnostics? } }`; `diagnostics` is omitted, not `null`, when there is nothing to report.
* **Status mapping.** `invalidRequest`/400, except `NG_SOURCE_BINDING` → `notFound`/404 (an
  unauthorized or foreign source stays indistinguishable from an absent one), `NG_LIMIT_*` →
  `limitExceeded`/413, `NG_PLAN_INVALID`/`NG_INTERNAL` → `internal`/500.
* **Ordering and determinism.** Failures follow the frozen stage precedence; within a stage,
  graph-level precedes node-level, nodes are ordered by ascending `NodeId` (UUID byte order) rather than
  array order, and edges by `(from.nodeId, from.port, to.nodeId, to.port)`. A failed compile returns
  exactly one primary diagnostic; a successful compile returns none. Determinism excludes
  `meta.requestId`.
* **Sanitization.** Diagnostics may carry codes, ids, bounded paths, type names, port ids, counts and
  bounds — never raw config JSON, cell values, display names, credentials, connector or filesystem
  paths, third-party error text or backtraces. A rejection never echoes the offending value, and
  `ensure_no_secret_fields` applies.
* **Budgets.** Diagnostics and response bytes are counted inside the existing 2 MiB response bound and
  enforced before allocation; no child issue may widen these bounds (§8.4).

### 7.3 New codes

A child issue that needs a new code must (a) add it additively to the frozen enum family, (b) keep the
`NG_` prefix and SCREAMING_SNAKE form, (c) document it in this contract's §9 row or the child's own
contract before use, and (d) never renumber, rename or reuse an existing code string. Adding a code is
an L3 change because the strings are published wire values.

Two failure classes have **no** `NG_*` code today — secret-safety rejection (NG-C0 uses
`ensure_no_secret_fields` without a code) and an invalid preview target (NG-C0 §9 states the rule but
assigns no code). A child issue that must report either class adds a code under this section rather
than overloading an unrelated existing one.

---

## 8. Resource laws

### 8.1 Frozen graph, configuration and schema bounds

Reused unchanged by every child issue (`core/src/node_graph/mod.rs:25-36`,
`core/src/logical.rs:14-17`, `plan/src/node_graph_compiler.rs:20-32`):

| Bound | Value |
| --- | --- |
| Graph document | 2 MiB (`MAX_GRAPH_BYTES`) |
| Nodes / edges | 64 / 63 (`MAX_NODES`, `MAX_EDGES = MAX_NODES - 1`) |
| Config per node / total | 64 KiB / 1 MiB |
| String / metadata | 4 KiB / 64 KiB |
| Nesting depth | 64 |
| Expression nodes / depth | 1 024 / 64 |
| Rules per node / total rules | **1** / 64 |
| Schema fields / schema nesting | 4 096 / 64 |
| Compile work | 2 000 000 (`nodes + edges + config_bytes + 4·expr_nodes + 4·schema_fields + 16·metadata_entries`) |
| Diagnostics | 64 diagnostics × 1 024 bytes |
| Schema snapshot estimate | 2 MiB |
| Batch bytes | 64 MiB (`MAX_BATCH_BYTES`, unchanged) |

Composite expansion is accounted against the **expanded** form, so no bound is circumvented by
expansion (#340 §8).

### 8.2 Stateful and relational operator laws

Every bucket-C capability (#370, #371, #376, #377, #378, #379) must declare, before implementation:

1. its **memory law** — a named bound, not "as needed"; over-limit behaviour is spill or an explicit
   rejection, never unbounded growth;
2. **batch-boundary invariance** — changing input batch size may not change values, field identity,
   field order, nullability, row order or group boundaries;
3. **cancellation** — cooperative cancellation with temporary-resource cleanup and no partial
   publication;
4. **atomic publication** — a failed or cancelled run publishes nothing;
5. **determinism** — no hash-iteration, clock or environment dependence in output ordering; ties are
   resolved by a declared rule.

### 8.3 Verification-path laws (categories 6, 7, 12)

Verification and report capabilities reuse the E4 laws rather than defining their own
(`engine/src/verification.rs:33-50`): `MAX_DEDUP_KEY_COLUMNS = 64`,
`VERIFICATION_MAX_LIVE_COLUMNAR_PAYLOADS = 6`, `VERIFICATION_MAX_COMPILED_PLAN_BYTES = 3 MiB`,
`VERIFICATION_MAX_ROUTING_STATE_BYTES = 512 KiB`, `MAX_VALIDATION_FINDINGS_PER_ROW = MAX_RULES_PER_NODE`,
`MAX_VALIDATION_MESSAGE_BYTES = 1024`, plus the engine peak-byte law. Canonical dedup key bytes are
bounded before index insertion (#346 §7.3).

### 8.4 Amendment policy

A child issue that needs to exceed any bound above must request an explicit amendment in **this**
contract (or a successor contract that names the bound it changes), state the compatibility impact, and
carry differential evidence that previously compiling graphs still compile to byte-identical plans.
§3.1's batch-rename case is the only pre-identified candidate and is **not** granted here.

---

## 9. Per-child execution boundary

Every child issue can be executed independently against this table. "Paths" are the authorized write
surface; anything outside them is a stop-and-return condition. Every child must also satisfy the
repo's own gates (scoped Registry claim for L2/L3, Draft PR, exact-head CI, independent review or
compliant acceptance receipt).

| Child | Bucket | Authorized paths | Gates / dependencies | Minimum test boundary | Risk |
| --- | --- | --- | --- | --- | --- |
| #363 C1 | contract | `docs/contracts/`, `docs/issues/` | #362 accepted; consumes design #345 | docs-only; complete legal/illegal port and topology matrix; must carry the **F-ENG1** logical Join/Union contract (D1) and state which capabilities still need their own L3 runtime contract | L1 delivery freezing L3-class decisions |
| #364 C2 | contract | `docs/contracts/`, `docs/issues/` | #362 accepted; consumes design #346 | docs-only; validation/dedup/analysis input, output, error, report and failure matrix; owns the **input-version digest** contract (D4) | L1 delivery freezing L3-class decisions |
| #365 N1 | A | `core/src/node_graph/definitions/**`, `definitions/mod.rs`, `plan/src/{node_graph_compiler,semantics}.rs`, catalog counts, API/service tests | #362; §3.1 limit amendment if batch mapping is used | real-data select/rename/drop/filter; NULL-predicate rows removed; duplicate/empty/unknown column rejected; old node config regression | L2 |
| #366 N2 | B | `plan/src/rule.rs`, `plan/src/semantics.rs`, `engine/src/{lower,incremental,preflight,predict}.rs`, `core/src/node_graph/definitions/**` | #362; additive-AST rules (§3.2) | Chinese/Unicode/empty/NULL/non-text behaviour; operation ordering; wire-tag stability; old `trim`/`replace-literal` regression | L3 |
| #367 N3 | B | as #366, plus the cast law | #362; §4 decisions for `TimestampSecondPaused` and `DateToUtf8CastPaused` | legal/illegal dates, timezone ambiguity, numeric overflow and precision; `error`/`setNull` and nullability agreement; old `cast` and canonical-plan compatibility | L3 |
| #368 N4 | B | `core/src/expression.rs`, `plan/src/semantics.rs`, `engine/src/{lower,incremental,preflight,predict}.rs` | #362; §4 decisions for `CheckedArithmeticPaused` and `ContainsPaused` | multi-field input, NULL, division by zero, overflow, type inference; expression depth/byte budgets; old `derive-column` regression | L3 |
| #369 N5 | B | `plan/src/rule.rs` (fill rule extension), definitions, engine rule paths | #362 | `0`/`false`/legitimate text never treated as missing; fill type and nullability agreement; undeclared fields untouched; old `fill-null` regression | L3 |
| #370 S0 | C | `plan/src/plan.rs` (new `Sort` kind), `plan/src/{semantics,node_graph_compiler}.rs`, `engine/src/{preflight,lower}.rs`, definitions, catalog counts | #363 accepted | result invariance under batch size; NULL/ties/group-boundary stability; over-limit, cancellation and temporary-resource cleanup; no second publication path | L3 |
| #371 N6 | C | stateful fill operator, engine state path | #363, #369, #370 | no cross-group fill; group-head missing-value policy; batch continuity; memory limit and cancellation | L3 |
| #372 N7 | D | E4 admission at the three product-path gates plus semantics | #364, #370 | real row reduction with verifiable keep policy; NULL keys; cross-batch duplicates; report/ArtifactRef bound to Run and version; no silent row loss | L3 |
| #373 N8 | D | as #372 | #364, #370 | per-row pass/reject verification; diagnostics locate rule, `nodeId`, `columnId`, field path; report bound to input, PlanVersion, Run; ordinary materialization does not switch modes | L3 |
| #374 N9 | E | report runtime boundary per #364 | #364, #370 | NULL/empty/non-numeric and denominator rules; sampling marker, scope and input fingerprint; no fabricated data; no materialized-output change | L3 |
| #375 D0-R | C | typed ports, multi-source authorization, graph/plan topology, `GraphRevision` format bump (§6.4) | #363 accepted (includes F-ENG1); #345 design requirements converted to an accepted runtime contract | v1 graphs still compile/preview/run unchanged; illegal topology rejected before connector I/O; per-input identity/permission/schema/version explicit; failure publishes nothing; no second executor; XR HOLD untouched | L3 |
| #376 N10 | C | `Join`/`Union` admission at preflight, multi-input node graph + compiler, definitions | #375 | empty key never matches; duplicate keys, NULL, unmatched rows, left/right order; no implicit key conversion; schema uniqueness or fail before reads; output blow-up/memory/cancel bounded | L3 |
| #377 N11 | C | new `Aggregate` plan kind, semantics, compiler, engine | #370 | empty input, NULL group/metric, overflow and precision; batch-size invariance; `count-distinct` and resource limits; provenance in Preview/Run/Artifact/export | L3 |
| #378 N12 | C + §4 | explode/unpivot operator plus the `ListStructPaused` decision | #363 | row-count change, nested paths, empty-array/NULL policy, stable source-record association, expansion bound, old linear-graph regression | L3 |
| #379 N13 | C | pivot operator | #377, #378 | duplicate-cell aggregation; new column values, order, field identity and schema stability; output column and memory bounds; NULL/empty input/cancellation | L3 |
| #381 G0 | acceptance | report + test evidence only | every child above plus `X44421/openship#36` | the four required flows; compatibility of old graphs, PlanVersions, runs and errors; Preview without Job/Run/Artifact side effects; cancel/retry/permission/limit evidence; compile schema equals real output | L3 |

**Cross-cutting staging.** A child may not be claimed while its gate contract is unmerged. #363 and
#364 share the same write surface as #362 and must be executed one at a time; capabilities that share
`rule.rs`, the node catalog or the engine rule matches must also be serialized (§3.2, R4 of the triage).

**Version-1 regression corpus.** The frozen corpus at
`backend/crates/stillflow-plan/tests/fixtures/nx-v1/` is the compatibility evidence for every row above.
It was captured before the predecessor wave modified any authorized file, and per #335 it is never
regenerated from a post-change head: new surfaces get their own fixtures (the `nx-next/` convention),
and changing a version-1 fixture requires a contract that explicitly authorizes a change to version-1
execution identity. "The old graph still compiles to byte-identical canonical bytes" is therefore
demonstrated against this corpus, not asserted.

---

## 10. Acceptance matrix for this delivery

| Issue #362 acceptance item | Where satisfied |
| --- | --- |
| Every category has an input, config, output, error, resource and compatibility entry | §2.2 matrix and §2.3 notes; §6 compatibility; §7 errors; §8 resources |
| Every follow-up issue's interface, file range and test boundary is independently executable | §9 |
| Compatibility decisions for old graphs, old PlanVersions, canonical bytes and existing execution behaviour are explicit | §6.1–§6.6 |
| Documentation links, terminology and JSON wire values are consistent; no Rust/executor/storage change | this document and its `docs/issues/` pointer only; wire values quoted from the frozen contracts; no code touched |

Verification for this delivery is document-level and objectively checkable: every referenced issue
number and document path resolves; every claimed wire value (`NG_*` codes, `format_version`,
`MAX_RULES_PER_NODE`, `MAX_COMPILE_WORK`, `stillflow-fnv1a64x4-v1`, artifact kinds) matches the frozen
contract or the code; every §9 row names a real path; and the dependency arrows of §9 do not contradict
Epic #361.

---

## 11. Non-goals and stop conditions

This delivery does **not**:

* register Join, Union, aggregation, structural transforms, or `Validate`/`Deduplicate` on the ordinary
  materialization path;
* un-pause any capability listed in §4 — it only records which decisions the wave must take;
* add or change any executor, queue, scheduler, canonicalizer, fingerprint or publication path;
* widen a frozen bound, lift `MAX_NODES`, or change `MAX_RULES_PER_NODE` (except through the explicit
  §3.1 amendment, which this document does not itself grant);
* change `PlanNodeKind`, plan serialization, `Rule`/`Expr` variants, the NodeGraph wire format,
  `GraphRevision.format_version`, storage schema, API routes or error codes;
* modify `docs/contracts/issue-324-ng-c0-*`, `issue-335-nx-c0-*`, `issue-340-nx-c1-*`,
  `issue-342-nx-v0-*`, `issue-346-nx-p0-*` or the #345 design;
* change Openship production code, or lift the #93 XR HOLD;
* run, or claim to have run, any Rust build, test, clippy or fmt.

**Stop and return to contract review** if a child issue needs an unlisted public field, a new
persistence format, a new executor-owned semantic decision, an unbounded operation, a second
runtime/publication authority, or an un-pause not recorded in §4.

---

## 12. Decision record

| ID | Decision | Ruling |
| --- | --- | --- |
| D1 | Where the F-ENG1 logical Join/Union contract lives | Folded into **#363**; this contract only marks it as the gate for #375/#376 |
| D2 | Whether #362 revises #335's contract | **No**: new document; `issue-335-nx-c0-node-extension-contract.md` stays unchanged (append-only) |
| D3 | Risk levels for #366–#369 | By authorized surface: #365 stays **L2**, #366–#369 execute at **L3**; GitHub labels are the maintainer's to update |
| D4 | Input-version digest | Land `f3451f6` as its own L3 PR **and** let #364 freeze the digest domain and report provenance, including the missing `SourceAssetView` field |
| D5 | Node counting | **Eleven atomic built-ins; deployed catalog twelve**; clients follow `NodeRegistry::deployed()` |
