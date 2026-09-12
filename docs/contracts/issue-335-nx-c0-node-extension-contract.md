# Issue #335: NX-C0 node-extension foundation, compatibility, and diagnostics contract

> Status: Candidate frozen contract for acceptance by #335
> Risk: L1 documentation delivery. Every public contract change it names is implemented at L3 by the owning issue.
> Parent: #334
> Predecessor: #324 NG-C0 — [`docs/contracts/issue-324-ng-c0-nodegraph-compiler-contract.md`](issue-324-ng-c0-nodegraph-compiler-contract.md)
> Authorized base: `main@5bff563cc4d33ef11f0cff4935aec02e92985166`
> Suggested implementation branch: `agent/issue-335-nx-c0`

This document freezes the extension foundation for the node system before any
statement about it changes. It records, item by item against NG-C0, what stays
frozen, what this series revises, and what later issues own. It defines the
module and dependency boundaries for built-in node definitions, the shared
semantic analysis result, the machine-readable configuration/catalog contract,
the safe diagnostic contract, the request-pipeline order and single-deadline
law, and the version-1 compatibility baseline for the eleven built-in nodes.

### Document location

The canonical normative text is this file under `docs/contracts/`, matching the
NG-C0 precedent (`docs/contracts/issue-324-ng-c0-nodegraph-compiler-contract.md`)
and #324's pre-declaration that the contract document lives under
`docs/architecture/` or `docs/contracts/`. A pointer stub is kept at
`docs/issues/issue-335-nx-c0-node-extension-contract.md` so that the
`docs/issues/` contract-index convention in `AGENTS.md` resolves without
duplicating normative text. Later issues in this series link this contract
through the stub.

It authorizes no runtime change by itself. NX-S1 (#336), NX-N1 (#337), NX-A1
(#338), NX-B1 (#339), NX-C1 (#340), NX-N2 (#341), NX-V0 (#342), and NX-V1
(#343) implement their own slices under their own risk level. This document is
normative only for the boundaries named in section 13.

The invariants of the epic remain in force: Rust is authoritative; the existing
`stillflow_plan::LogicalPlan`, `Rule`, and `stillflow_core::Expr` remain the only
logical execution language; preview, `PlanVersion`, `JobRuntime`, and the
snapshot/artifact publication path are reused rather than duplicated; already
published version-1 plans keep their canonical bytes, digest, column identity,
and execution behavior.

## 1. Decision, authority, and current-state evidence

NG-C0 section 1 fixes the authority table. NX-C0 does not change it:

| Concern | Authority | NX-C0 rule |
| --- | --- | --- |
| Product graph identity, arrangement, UI metadata | `NodeGraph` | Still product data; never controls execution. |
| Node type/config metadata and validation | `NodeDefinition` / `NodeRegistry` | Still declarative, built-in only, deterministic, transport-neutral. |
| Graph topology and schema checks | NG compiler | Still fail closed before connector execution or materialization. |
| Execution IR, logical meaning, canonical bytes, plan fingerprint | `stillflow-plan::LogicalPlan` | Still the single execution authority. |
| Durable plan identity and lifecycle | Existing `PlanVersion` APIs | Unchanged; NG adds no lifecycle. |
| Preview and formal execution | Existing Preview / `JobRuntime` / `ExecutionEngine` | Unchanged; NG adds no executor. |

The problem NX-C0 answers is that the surfaces *above* that table have grown
awkward to extend and to diagnose:

1. node definitions, per-node config allowlists, catalog metadata, compiler
   branches, and engine checks are each maintained separately, so adding one
   node requires edits in several authorities that can silently diverge;
2. the compiler and the engine each derive expression type, nullability, cast
   restrictions, and post-rule schema independently;
3. compile failures lose their node/field location when they cross the API
   boundary, and pure-graph validation happens after the data source has
   already been inspected.

The evidence below is the observed state at the authorized base. It is recorded
so that every "frozen" or "revised" statement in this contract is falsifiable
from the repository, not from an issue draft.

| Observed fact | Evidence |
| --- | --- |
| Built-in node definitions and their per-kind config validation live in one file, `stillflow-core/src/node_graph.rs`. | `backend/crates/stillflow-core/src/node_graph.rs:995` (`validate_node_config`), `:1367`–`:1520` (the eleven definitions) |
| Config allowlists are hand-written per kind at each call site, not derived from `ConfigSchema`. | `backend/crates/stillflow-core/src/node_graph.rs:1567` (`parse_config`), `:1549` (`additional_properties: false` is declared but not consulted by validation) |
| `ConfigField` carries only `name`, `valueKind`, `required`; `NodeSupportStatus` has a single `Supported` variant. | `backend/crates/stillflow-core/src/node_graph.rs:835`–`:882` |
| Unknown fields in the graph/node/edge envelope are ignored: `NodeGraph`, `NodeConfig`, `NodeEdge`, `NodePort` do not use `deny_unknown_fields`. | `backend/crates/stillflow-core/src/node_graph.rs:193`–`:244`, `:347`–`:388`; absence of the attribute repo-wide for these types |
| Expression typing/nullability is implemented in the engine. | `backend/crates/stillflow-engine/src/typing.rs:8`–`:22` (`type_check_expr`, `infer_type`), `:102` (`infer_type`) |
| Execution-capability (paused) restrictions are implemented separately in the engine and in the NG compiler. | `backend/crates/stillflow-engine/src/typing.rs:34`–`:100`; `backend/crates/stillflow-plan/src/node_graph_compiler.rs:951`, `:1019`, `:1037` |
| Post-rule schema propagation is implemented in the engine and re-implemented in the compiler. | `backend/crates/stillflow-engine/src/incremental.rs:220` (`IncrementalSchema::apply_rule`, production path), `backend/crates/stillflow-engine/src/preflight.rs:733` (`apply_rule_schema`), `backend/crates/stillflow-plan/src/node_graph_compiler.rs:575`–`:960` (`compile_transform` and its schema helpers) |
| A test-only legacy rule-schema path exists as the differential reference. | `backend/crates/stillflow-engine/src/preflight.rs:754` (`apply_rule_schema_legacy`, `#[cfg(test)]`) |
| The compiled result declares a bounded diagnostic list, but the compiler never populates it and fails fast with a single error. | `backend/crates/stillflow-plan/src/node_graph_compiler.rs:108`–`:113`, `:279` (`diagnostics: Vec::new()`), `:1142` (`type_error`) |
| The API error is a two-field record with no location and no request identity. | `backend/crates/stillflow-api/src/error.rs:25`–`:31`; `backend/crates/stillflow-api/src/envelope.rs:114`–`:124` |
| Compile errors are flattened to a code-only message and the node ID is discarded. | `backend/crates/stillflow-api/src/service.rs:4079`–`:4101` |
| The typed routes return errors with a nil request ID even when the envelope decoded successfully. | `backend/crates/stillflow-service/src/adapter.rs:29`–`:55`, `:87`–`:95` |
| Body decoding collapses every shape mismatch into one generic message with no field path. | `backend/crates/stillflow-service/src/adapter.rs:66`–`:96` |
| The compile/preview request inspects the connector before any pure-graph validation runs. | `backend/crates/stillflow-api/src/service.rs:2393`–`:2395`, `:2476`–`:2507` (`registry.inspect`) |
| The preview path creates a second request deadline for the engine stage. | `backend/crates/stillflow-api/src/service.rs:3645` (`request_context`), `:2429`, `:2452` |
| The existing HTTP request bound is 2 MiB. | `backend/crates/stillflow-api/src/limits.rs:18`; enforced at `backend/crates/stillflow-service/src/process.rs:129` (`DefaultBodyLimit`) |
| The API route/schema manifest is a hand-maintained seed whose schema entries carry a version. | `backend/crates/stillflow-api/src/manifest.rs:11`–`:52`, `:446`–`:461`, `:770`–`:772` |
| A catalog read surface already exists and returns the registry catalog with the compiler version. | `backend/crates/stillflow-service/src/routes.rs:114`, `:209`–`:214`; `backend/crates/stillflow-api/src/manifest.rs:328`–`:334`; `backend/crates/stillflow-api/src/service.rs:1098`–`:1103`, `:1322`–`:1334` |
| The API already declares a `{code, nodeId, message}` diagnostic record that is never populated. | `backend/crates/stillflow-api/src/service.rs:1105`–`:1111`, `:4071`–`:4077`; `backend/crates/stillflow-plan/src/node_graph_compiler.rs:279` |
| The cast-failure policy enum exists twice and is re-mapped between the copies. | `backend/crates/stillflow-core/src/node_graph.rs:1201`–`:1206`; `backend/crates/stillflow-plan/src/rule.rs:8`–`:13`; re-mapped at `backend/crates/stillflow-plan/src/node_graph_compiler.rs:648`–`:651` |
| The engine's rules-per-node bound (256) is looser than the product bound (1), so the product path is the stricter authority. | `backend/crates/stillflow-engine/src/lib.rs:93`; `backend/crates/stillflow-core/src/node_graph.rs:23` |
| The engine rejects a preview whose deadline exceeds 30 s, while the API default request deadline is 300 s. | `backend/crates/stillflow-engine/src/lib.rs:123`, `backend/crates/stillflow-engine/src/preview.rs:66`–`:75`; `backend/crates/stillflow-api/src/service.rs:3645`–`:3653` |

## 2. NG-C0 delta ledger

Every NG-C0 clause is classified exactly once. `P` items are preserved without
change. `R` items are revised by a named issue and carry an explicit
compatibility decision plus an objective test. `D` items are deferred to a
later issue. `X` items are compatibility explicitly refused.

### 2.1 Preserved (no change authorized)

| ID | NG-C0 clause | Preserved statement |
| --- | --- | --- |
| P-1 | §1 | The authority table and the "no second executor / no second plan language" rule. |
| P-2 | §2.1 | `NodeGraph` version-1 wire shape: `version`, `graphId`, `sourceNodeId`, `outputNodeId`, `nodes`, `edges`, `metadata`; node `id`/`typeId`/`configVersion`/`config`/`metadata`; edge `from`/`to` with `nodeId`/`port`. |
| P-3 | §2.1 | Structural validation: non-nil UUIDs, unique node IDs, unique edge tuples, single source and single output, exactly one connected path, `edges == nodes - 1`, array order is not execution order. |
| P-4 | §2.2 | Port IDs `in` / `out`, case-sensitive and definition-owned; source identity is the authorized `SourceAsset.id`; output identity is node ID plus `outputLabel`. |
| P-5 | §2.3 | Fail-closed versioning: graph version exactly `1`, config version exactly the definition's version, exact `(typeId, configVersion)` registry lookup, no best-effort decode and no version fallback. |
| P-6 | §3 | `NodeDefinition` responsibilities and prohibitions; closed deterministic registration; catalog sorted by `typeId` then `configVersion`; registry content and order are compiler inputs. |
| P-7 | §4 | The eleven built-in type IDs, their config fields, their lowering targets, the one-product-node-to-one-plan-node rule, and the "one rule per `ApplyRules` node" rule. |
| P-8 | §4 | `Rule::Validate` and `Rule::Deduplicate` stay outside the product path; Join, Union, multiple scans, arbitrary DAGs, variadic ports, SQL, and plugin nodes stay unregistered and fail closed. |
| P-9 | §5.2 | Deterministic lowering sequence and independence from array order, registry insertion order, hash order, wall clock, UUID generation, locale, environment, and connector behavior. |
| P-10 | §5.3 | The closed logical target set `Scan`, `Project`, `Filter`, `ApplyRules`, `Materialize` and the reuse of existing `Expr`/`Rule`/`LogicalSchema` contracts. |
| P-11 | §6 | Every per-node schema/identity propagation rule, including rename keeping identity, drop removing it, derived identity being caller-supplied, and display-name lookup never substituting for `ColumnId` lookup. |
| P-12 | §7 | All frozen resource numbers and the compile-work inequality. NX-B1 may add accounting, never widen these. |
| P-13 | §8.1 | The stable `NG_*` code vocabulary. NX-A1 adds safe location fields, not new failure classes. |
| P-14 | §8.2 | The secret-safety policy, including `ensure_no_secret_fields`, rejection without echoing the value, and fingerprints not being a redaction or permission mechanism. |
| P-15 | §9 | Preview compiles first and delegates to the existing preview path; a persisted plan never needs the graph to execute. |
| P-16 | §10 | The non-goal list, including #93 XR HOLD, SQL connector #9, and DuckDB executor #10. |
| P-17 | §11 | NG-R1/NG-R2 entry criteria remain the gate that was satisfied by #325/#326. |

### 2.2 Revised (each with a compatibility decision and an objective test)

| ID | NG-C0 clause | Revision | Owner | Compatibility decision | Objective test |
| --- | --- | --- | --- | --- | --- |
| R-1 | §2.1 last bullet, §2.3 | Unknown fields at the graph, node, edge, and port envelope levels are rejected instead of ignored. NG-C0 froze this for the node config object; NX-C0 extends the same fail-closed rule to the envelope levels, under §2.3's ban on "ignore what this runtime does not understand". | NX-A1 | Tightening only, and a new decision rather than a conformance fix at the envelope levels. No previously *successful* plan changes, because the previously tolerated keys had no defined meaning. | Decode a graph with one extra key at each level; each fails closed with a stable code and the offending field path (`NG_INVALID_CONFIG` for objects decoded inside the graph, `invalidRequest` plus field path for envelope-level rejects); a graph without extra keys still compiles to identical bytes. |
| R-2 | §3, §4 | Node definitions, config constraints, catalog metadata, and support conditions are generated from one definition source per node type instead of separate hand-written lists. | NX-N1 | Additive on the wire: existing `configSchema` fields keep their names and meaning. Rust-side additions use defaults so existing constructors keep compiling where the workspace allows; new constraint records are additive. | For each of the eleven nodes, positive and negative config samples agree mechanically between the catalog constraints and the actual validator; the definitions can be reordered without changing catalog order or compile output. |
| R-3 | §4 | Per-node supported input conditions become machine-readable in the catalog (for example `trim` requires `Utf8`) instead of being implicit in compiler branches. | NX-N1 | Additive. `supportStatus` keeps `Supported` for all eleven; any new status value must be additive and must never promote a paused capability. | Each condition declared in the catalog is enforced by the compiler with the same code, and no condition is enforced only by the catalog. |
| R-4 | §5.1 | The bounded diagnostic list in the compile result is populated with the failed compile's primary diagnostic instead of staying unconditionally empty; a successful compile still returns zero diagnostics. | NX-A1 | Strictly additive and behavior-preserving: exactly one primary diagnostic on failure, still no partial plan. | Failed compile returns one diagnostic carrying code and node ID; successful compile returns an empty list; both counts stay within the frozen caps. |
| R-5 | §5.2, §6 | Expression type/nullability analysis and post-rule schema effects have one shared implementation that both the compiler and engine preflight call; the lower layer never depends on `stillflow-engine`. | NX-S1 | Behavior-preserving for previously successful plans. Inputs that compiled but were rejected by the engine may now be rejected at compile time with a typed diagnostic; this is recorded as an intentional error-surface change. | Differential corpus over all eleven nodes, executable expressions, and the paused set: both entry points produce the same schema or an equivalent rejection; version-1 canonical bytes and fingerprints are unchanged for every previously successful fixture. |
| R-6 | §7 | Compile accounting gains an explicit schema/response cost surface so schema fan-out and response bytes are bounded before allocation. | NX-B1 | Tightening only, and measured before it is enforced. No engine or storage bound may be widened or replaced. | Boundary cases at and above each input/intermediate/output bound are rejected without panic or partial plans; before/after canonical bytes, schemas, errors, and actual results are identical for accepted graphs. |
| R-7 | §8.1 | Diagnostics gain bounded safe location fields (`nodeId`, `columnId`, `fieldPath`, `expected`, `actual`) and the request ID is preserved in the error envelope. | NX-A1 | Additive on the wire. `code` and `message` keep their current meaning and casing; new fields are optional. | An authorized invalid request identifies the failing node and field and echoes the original `requestId`; foreign or unauthorized resources remain indistinguishable from absent ones. |
| R-8 | §8.1 | Failure ordering becomes explicitly two-level: stage precedence is unchanged, and intra-stage fault order becomes canonical (graph-level before node-level; nodes by ascending `NodeId`; edges by endpoint tuple) instead of caller array order. Count and byte budgets become testable. | NX-A1 | A single-fault graph and every successful compile are unchanged; a multi-fault graph may report the smallest-`NodeId` fault instead of the first-array-order fault. This intentional error-ordering change is recorded here and must be listed with its minimal cases. | Repeated runs over permuted `nodes`/`edges` arrays produce byte-identical `code` and `diagnostics` given identical request metadata (`meta.requestId` is echoed input and is excluded or fixed); diagnostics never exceed the count/byte caps. |
| R-9 | §2.3 | Catalog and configuration-constraint versioning is added alongside graph/config/compiler versioning. | NX-N1 | Additive. `NodeGraph.version` and `configVersion` stay exactly `1`; the catalog remains a version-1 surface. | Unknown catalog or constraint versions are rejected; a v1 client that ignores new catalog fields still compiles identical plans. |
| R-10 | §7 | The pipeline order and the single-request deadline law are frozen explicitly (section 8), including the rule that no data source is inspected before pure-graph validation passes. | NX-A1 | Strictly better ordering. Success responses are unchanged; some previously invalid requests now fail earlier and cheaper. | For a decodable graph with an unknown node, a bad port, a bad topology, or a mismatched source binding, the connector inspect/read counters are zero. |
| R-11 | §7 (engine bounds), §8.2 | The timeout law is frozen case by case: an explicit `timeoutSeconds` is accepted as given or rejected with `limitExceeded`, never clamped; an absent value uses the operation default, itself bounded by the strictest consumer cap (30 s for preview, 300 s otherwise). | NX-A1 | Two intentional error-surface changes: an absent preview timeout succeeds with a 30 s deadline instead of failing inside the engine; an explicit over-cap preview value is rejected up front as `limitExceeded`/413, not `invalidRequest`/400 after the source was inspected. `0` and above-ceiling values keep their current outcome, and no previously successful request changes. | The six table cases (absent, `0`, `30`, `31`, `300`, `301`) produce the frozen outcome on both the preview and compile paths; a boundary rejection performs zero connector calls; exactly one context is created per request and shared by all stages. |

### 2.3 Deferred (owned by a later issue, not decided here)

| ID | Deferred decision | Owner |
| --- | --- | --- |
| D-1 | One-to-many expansion of a product node into a bounded linear sequence of existing logical operators, the stable expansion mapping, internal ID determinism, conflict detection, and diagnostic mapping. | NX-C1 (#340), implemented by NX-N2 (#341) |
| D-2 | Declarative node packages: namespace, config/semantic version, immutable content digest, exact dependency and allowed-operator set, controlled parameter binding, and the first approved composite sample. | NX-C1 (#340), implemented by NX-N2 (#341) |
| D-3 | Editable `GraphRevision` persistence, save idempotency, optimistic concurrency, immutability of history, explicit non-destructive configuration migration, and database upgrade/rollback. | NX-V0 (#342), implemented by NX-V1 (#343) |
| D-4 | Typed ports, fan-in/fan-out cardinality, Join/Union semantics, schema reconciliation, and staged DAG execution boundaries. | NX-D0 (#345) under the F-ENG1 / #93 boundary |
| D-5 | Draft upstream preview and verification-node admission boundaries. | NX-P0 (#346) |
| D-6 | Whether a physical executor extraction (the historical #144/#149/#152/#164 line) is ever worth doing. | Refused here (X-5); would require its own contract |

### 2.4 Compatibility explicitly refused

| ID | Refused compatibility | Reason |
| --- | --- | --- |
| X-1 | Runtime, user-supplied, or dynamically loaded node registration; declarative packages that may execute callbacks, SQL, or downloaded content. | NG-C0 §3/§10; D-2 permits only composition of already approved operators. |
| X-2 | A second expression/rule AST, graph-specific `PlanNodeKind`, optimizer, or canonicalizer. | NG-C0 §5.3, §10. |
| X-3 | "Ignore what this runtime does not understand" forward compatibility for graph, node, edge, port, or config fields. | NG-C0 §2.3; R-1 tightens it. |
| X-4 | Unpausing or widening any paused capability (checked arithmetic, `contains`, list/struct execution, timestamp-second unit, date/timestamp-to-utf8 cast, binary cast) in this series. | NG-C0 §4, epic non-goals; NX-C0 freezes the current paused set as behavior. |
| X-5 | Enlarging API, engine, or storage bounds, or replacing an engine/storage limit with a product limit. | NG-C0 §7; `ApiLimits::bounded` may only cap. |
| X-6 | Best-effort or fallback decoding of an unknown graph, config, compiler, catalog, or constraint version. | NG-C0 §2.3. |
| X-7 | Merging, cherry-picking, or casually enabling the historical experimental branches #144/#149/#152/#164. | Epic non-goals; they remain read-only references. |
| X-8 | Changing the canonical-byte or fingerprint algorithm, or persisting a graph-level canonical form as a substitute for plan canonical bytes. | NG-C0 §5.2, §9. |

## 3. Public surface change inventory

The NX series changes public Rust items, wire JSON, and the API manifest. Each
change is classified; nothing else in the public surface may change.

### 3.1 Rust API

| Crate | Item | Change | Kind | Decision | Owner |
| --- | --- | --- | --- | --- | --- |
| `stillflow-plan` | new `semantics` module: expression analysis, rule effect, capability gate, typed semantic error | added | additive | New public surface. Types are named indicatively; the semantics in section 5 are normative. | NX-S1 |
| `stillflow-engine` | `typing` / `preflight` / `incremental` semantic entry points | re-pointed to the shared analyzer; internal optimization layers keep an explicit non-sharable justification | behavior-preserving | Public `stillflow-engine` items keep their signatures; internal `pub(crate)` items may change. | NX-S1 |
| `stillflow-engine` | new public `semantics` facade: `analyze_expr`, `rule_effect`, `project_effect`, `ExprSemantics` | added | additive | Thin public seam over the re-pointed preflight entries. Section 11's differential battery must run without a connector, API, service, or filesystem dependency, and integration tests cannot reach `pub(crate)` entries, so the battery needs this one public seam; the facade adds no decision logic. (Amended by NX-S1 #336 during implementation.) | NX-S1 |
| `stillflow-core` | `ConfigField`, `ConfigSchema`, `NodeCatalogEntry`, `NodeSupportStatus`, `NodeDefinition` | constraint and support-condition metadata added | additive with defaults | `NodeSupportStatus` may gain variants; adding an enum variant breaks exhaustive matches, so every match site is updated in the same PR and the change is listed in the PR body. | NX-N1 |
| `stillflow-core` | `NodeGraph`, `NodeConfig`, `NodeEdge`, `NodePort` decoding | unknown-field rejection added | tightening | Wire-visible only for inputs that were previously meaningless. | NX-A1 |
| `stillflow-core` | `NodeGraphError`, `NodeGraphErrorCode` | safe location fields added; code vocabulary unchanged | additive | Existing codes keep their strings. | NX-A1 |
| `stillflow-plan` | `CompileDiagnostic`, `NodeGraphCompileError` | safe location fields added; `diagnostics` populated with the primary diagnostic | additive | The error remains a single typed failure; no partial plan. | NX-A1 |
| `stillflow-plan` | compile budget surface | schema/response accounting added | additive | Accounting is internal unless it must be reported; any reported field is optional. | NX-B1 |
| `stillflow-api` | `ApiError`, `ApiErrorBody`, `ApiErrorCode` | optional `requestId` and `diagnostics` added | additive | `ApiErrorCode` variants are unchanged; new location data never replaces `code`/`message`. | NX-A1 |
| `stillflow-api` | `NodeGraphCompileRequest`/`View`, `NodeGraphPreviewRequest` | optional response fields / compact schema option | additive | Request-side additions must be optional with defaults to stay version 1. | NX-B1 |
| `stillflow-api` | `manifest::{E5_A1_ROUTES, E5_A1_SCHEMAS}` | updated in place | additive | Route paths and operation IDs are unchanged, including the existing catalog route. | NX-N1, NX-A1, NX-B1 |
| `stillflow-api` | `NodeCatalogEntry` fields (through `NodeCatalogView` on `GET /v1/node-types`), `NodeGraphDiagnosticView` | constraint/support/diagnostic fields added | additive | No new route: the catalog route already exists. New fields are optional and omitted when absent. | NX-N1, NX-A1 |

### 3.2 Wire JSON

The wire compatibility rule is frozen:

1. Adding an optional field to a **response** body is version-1 compatible. It
   must be omitted (not `null`) when absent, via
   `#[serde(default, skip_serializing_if = "Option::is_none")]` or an equivalent.
2. Adding an optional, defaulted field to a **request** body is version-1
   compatible.
3. Adding a required request field, removing any field, changing a field's type,
   changing its casing, or changing the meaning of an existing value is a
   breaking change. It requires a new schema version in the manifest and an
   explicit contract; NX-C0 authorizes none.
4. Unknown **input** fields stay rejected (X-3). Clients must ignore unknown
   output fields.
5. `ApiError.code` and `ApiError.message` keep their current spelling and
   meaning; `meta.requestId` stays a UUID and becomes populated on typed-route
   failures whenever the envelope decoded.

### 3.3 Manifest and schema versions

`SchemaSpec.version` stays `1` for every change listed in section 3.1. A
version increment is reserved for the breaking class in section 3.2 rule 3.
Declaring a schema without implementing it, or implementing it without
declaring it, is a defect; the manifest and the typed handler must move
together.

### 3.4 Surfaces with no authorized change

No change is authorized to: dependency manifests or the lockfile; persistence
or database migrations; the `PlanVersion`/`JobRuntime`/snapshot/artifact
lifecycle; the canonical-byte or fingerprint algorithm; the engine or storage
limit law; secret handling; HTTP route paths; and the Openship production
codebase.

## 4. Module responsibility and dependency direction

### 4.1 Frozen responsibilities

| Layer | Owns | Must not own |
| --- | --- | --- |
| `stillflow-core` | `NodeGraph`/`NodeConfig`/port/edge wire model and decoding; `NodeId`/`PortId`/`ColumnId` identity; structural validation and bounds; per-type definition records, config allowlists, catalog records; `Expr`/`LogicalSchema`/`LogicalType` contracts and their shape validation | Rule semantics; graph traversal; plan assembly; any physical engine object |
| `stillflow-plan` | Graph traversal and lowering; plan assembly; schema/identity propagation; the shared semantic analysis module; compile diagnostics; canonical bytes and fingerprint via the existing `LogicalPlan` | Connector or credential access; data inspection; `PlanVersion` allocation; execution |
| `stillflow-engine` | Execution preflight; runtime capability enforcement; physical lowering and execution; existing preview and job runtime | The compile-time semantic authority; graph model ownership |
| `stillflow-api` | Authorization, workspace scoping, request/response DTOs, API error mapping, manifest | Graph semantics; connector inspection policy beyond the frozen pipeline order |
| `stillflow-service` | Transport, body limits, envelope decoding, status mapping | Domain semantics; a second validation authority |

A built-in node's definition, typed config, validation, and tests are one unit
that NX-N1 may organize as one module per node type. The registry may then be
reduced to deterministic lookup and ordering; it must not be the place where a
node's rules are written.

### 4.2 Dependency direction

The frozen arrows are unchanged. `AGENTS.md` is the normative source for the
workspace direction; `stillflow-service` is the transport crate above
`stillflow-api` (added by #303), and the observed arrows are:

```text
stillflow-service -> stillflow-api, stillflow-engine, stillflow-plan,
                     stillflow-connectors, stillflow-storage, stillflow-core
stillflow-api -> stillflow-engine, stillflow-plan, stillflow-connectors,
                 stillflow-storage, stillflow-core
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

Additional rules for this series:

- The shared semantic analysis lives at or below `stillflow-plan`; it must not
  depend on `stillflow-engine`, `stillflow-api`, `stillflow-service`,
  `stillflow-connectors`, `stillflow-storage`, Polars, DuckDB, SQLx, or Axum.
- `stillflow-core` must not gain a dependency on `stillflow-plan`. If a shared
  result type needs `Rule`, that type lives in `stillflow-plan`.
- `stillflow-engine` keeps its runtime capability enforcement; it calls the
  shared analyzer rather than maintaining a parallel semantic path.
- No module in this series may introduce a dependency in the reverse direction
  of the arrows above.
- The repository is the authority for the arrow list; a PR that changes it must
  change this section and the `AGENTS.md` diagram together.

### 4.3 Independent-crate assessment (evaluated, not mandated)

An independent `stillflow-nodes` crate (definitions depending only on
`stillflow-core`, with `stillflow-plan` depending on it) was evaluated:

| Criterion | Extraction benefit | Extraction cost |
| --- | --- | --- |
| Ownership clarity | One crate boundary for node definitions | A new public boundary to version and review |
| Compile isolation | Rebuilding definitions does not rebuild the plan crate | Marginal: definitions are small and already compiled together |
| Dependency hygiene | Makes a core-only definition layer mechanically checkable | Must not become `core -> nodes -> plan`; the direction has to be enforced by review |
| Change surface | None: NX-N1 already keeps the change local to node modules | New `Cargo.toml`, workspace member, CI entry, and lockfile churn, all of which this series refuses (section 3.4) |

Decision: **no new crate is required or authorized by NX-C0.** NX-N1 satisfies
the modularity goal with modules inside `stillflow-core` and a stable public
re-export surface. A future extraction requires its own contract, must keep the
arrows in section 4.2, and must not change the wire contract. Modularity is a
code-organization goal here, not a crate-count goal.

Node definitions are also not a physical executor extraction: this series does
not move `PolarsExecutor` code, and X-7 stands.

## 5. Shared semantic analysis contract (NX-S1)

### 5.1 What is shared

One transport-neutral analysis entry point becomes the only implementation of
these semantics for both the node-graph compiler and engine preflight:

- column identity resolution against the working schema, by `ColumnId` only;
- expression result type and nullability inference;
- expression shape and reference validation;
- the post-rule schema effect of every `Rule` variant that the product path
  admits;
- classification of a semantic failure into the frozen `NG_*` vocabulary, with
  the safe location fields of section 7.

An equivalent result shape is:

```text
ExprAnalysis {
  dataType: LogicalType,
  nullable: bool
}

RuleEffect {
  schema: LogicalSchema      // the complete schema after the rule
}

SemanticError {
  code: NG_UNKNOWN_COLUMN | NG_INCOMPATIBLE_TYPE | NG_INVALID_CONFIG
        | NG_LIMIT_COMPILE_WORK,
  columnId:  ColumnId?,      // the exact field involved, when one is identifiable
  fieldPath: String?,        // config path (for example "predicate" / "value")
  expected:  String?,        // bounded type or kind name
  actual:    String?         // bounded type or kind name
}
```

The names are indicative; the semantics are normative. In particular:

- `nullable` is the inferred nullability of the expression result, not the
  declared nullability of a column.
- A schema effect is the whole resulting schema; a rule that changes nothing
  returns the unchanged schema, and identity, order, type, nullability,
  metadata, and column IDs are preserved exactly as NG-C0 §6 requires.
- Analysis never mutates the input schema, never reads a connector, never
  allocates a `PlanVersion`, and never depends on wall clock, randomness, hash
  order, locale, or environment.
- Failure is a typed error. No partial result, no warning-and-continue path,
  and no panicking path is permitted.

Nullability is part of the analysis result because it is currently derived in
more than one place and is observable in the compiled schema. It is therefore
frozen as a first-class output rather than an implementation detail.

### 5.2 Boundary with execution-capability checks

Two authorities stay separate, and the contract names them:

1. **Semantic analysis** answers "what does this mean given the schema".
2. **Capability enforcement** answers "is this executable on this head".

The currently paused set is frozen as behavior and must not be unlocked,
relaxed, or reordered by this series (X-4):

| Paused surface | Current enforcement | Evidence |
| --- | --- | --- |
| Unary negation, `Add`/`Subtract`/`Multiply`/`Divide`/`Modulo` (checked arithmetic) | engine and compiler | `stillflow-engine/src/typing.rs:34`, `:131`; `stillflow-plan/src/node_graph_compiler.rs:1019` |
| `BinaryOperator::Contains` | engine and compiler | `stillflow-engine/src/typing.rs:58` |
| `List` and `Struct` types | engine and compiler | `stillflow-engine/src/typing.rs:87` |
| `Timestamp` with `TimeUnit::Second` | engine and compiler | `stillflow-engine/src/typing.rs:92` |
| `Date32`/`Timestamp` to `Utf8` cast | engine and compiler | `stillflow-engine/src/typing.rs:147`, `stillflow-engine/src/preflight.rs:913`; `stillflow-plan/src/node_graph_compiler.rs:1037` |
| Cast to or from `Binary` | engine and compiler | `stillflow-engine/src/preflight.rs:913`; `stillflow-plan/src/node_graph_compiler.rs:1050` |
| Paused casts nested inside an expression | engine and compiler | `stillflow-engine/src/preflight.rs:931`; `stillflow-plan/src/node_graph_compiler.rs:951` |

Rules:

- A capability rejection keeps its current classification
  (`NG_INCOMPATIBLE_TYPE` for the compile path) so existing error handling does
  not change class; it gains location fields only.
- Capability checks may be called *after* semantic analysis, but a capability
  rejection must never be reported as a semantic success, and semantic success
  must never imply executability.
- The engine's runtime gate remains the final authority. The compiler must not
  become more permissive than the engine. Where the compiler is currently
  stricter, that strictness is preserved.
- Unlocking any row of the table requires a separate contract that names the
  semantics, limits, and tests.

### 5.3 Divergence law

The compiler and the engine do not have to be identical today; they must stop
diverging silently. The law is:

1. Build a differential corpus covering all eleven built-in nodes, every
   supported expression form, boundary values (NULL, numeric edges, `Binary`,
   paused casts), and rename/delete-then-reference sequences.
2. Run both entry points over the corpus and record every disagreement as a
   minimal case.
3. Resolve each disagreement by contract, not by picking the convenient side:
   - if the engine rejects and the compiler accepts, the compile path adopts
     the rejection (fail earlier, same or stricter outcome);
   - if the compiler rejects and the engine accepts, the corpus records the
     case and the compiler behavior is preserved until a contract authorizes
     relaxing it;
   - if both accept with different schemas, the engine's execution-visible
     result is authoritative for compatibility, and the shared analyzer must
     reproduce it.
4. Every previously successful version-1 fixture must keep identical canonical
   bytes, fingerprint, schema, and results.

Consequence, stated explicitly: an input that compiled successfully but could
never execute may now fail at compile time. That is an error-surface change and
must be listed in the NX-S1 PR body with the affected minimal cases.

### 5.4 Paths, tests, and non-sharable exceptions

- Shared analysis: `backend/crates/stillflow-plan/src/semantics.rs` (or a
  `semantics/` module with per-concern files), re-exported from
  `stillflow-plan/src/lib.rs`.
- Compiler call sites: `backend/crates/stillflow-plan/src/node_graph_compiler.rs`.
- Engine call sites: `backend/crates/stillflow-engine/src/typing.rs`,
  `preflight.rs`; `incremental.rs` may remain as an engine-internal
  representation.
- Analysis tests: `backend/crates/stillflow-plan/tests/nx_s1_semantics.rs` plus
  the differential battery
  `backend/crates/stillflow-engine/tests/nx_s1_differential.rs`.
- Compatibility fixtures: `backend/crates/stillflow-plan/tests/fixtures/nx-v1/`.

An engine-internal implementation may stay non-shared only when it is a
performance representation with the same observable result and is differentially
tested against the shared analyzer (the `IncrementalSchema` path and the
`#[cfg(test)]` legacy reference are the existing precedent). The exception must
be named in the PR body; there is no silent second authority.

## 6. Machine-readable config constraints and catalog (NX-N1)

### 6.1 One definition source

For each built-in type, one definition source produces all of: the typed config
record, the validation behavior, the catalog entry, the support conditions, and
the validity samples used by tests. Adding a node type must not require editing
a generic graph traversal, a central enum, or a second allowlist.

The directory may describe a node; it must never become the execution
authority. Client-facing hints are advisory; the validator decides.

### 6.2 Frozen constraint vocabulary

`ConfigField` gains machine-readable constraints sufficient to express what the
hand-written validators express today. `name`, `valueKind`, and `required` keep
their current positions and meanings; the additional constraints are carried in
a `constraints` object on the same field record, so the record stays additive.
The vocabulary is frozen as:

| Constraint | Meaning | Example in the built-in set |
| --- | --- | --- |
| `required` | Existing flag; a missing value fails | `stillflow.node.filter.predicate` |
| `valueKind` | Existing discriminator; extended to name the accepted JSON shape. Its value is a **catalog wire token**, not a Rust type name (see the wire table below) | `columnId`, `expression`, `logicalType`, `scalarValue`, `uuid`, `string`, `boolean`, `columnIdList`, `castFailurePolicy` |
| `enumValues` | Closed value set, expressed in **wire values** (camelCase), not Rust variant names | `onFailure` → `["setNull", "error"]` |
| `minLength` / `maxLength` | UTF-8 byte bounds on a string | node `to`, `name`, `outputLabel` |
| `minItems` / `maxItems` | List length bounds | `projection`, `columns` |
| `uniqueItems` | No duplicate entries | `columns`, `projection` |
| `ordered` | Caller order is semantic and preserved | `columns`, `projection` |
| `nonEmpty` | Rejects empty strings and empty lists after trimming rules | `to`, `outputLabel` |
| `nonNull` | Rejects `null` even where the kind would allow it | `fill-null.value` |
| `byteBound` | Per-value byte ceiling from the frozen limit table | strings, config objects |

Wire tokens are frozen as the serde camelCase forms of the existing enums; the
Rust type is named only for implementers:

| Catalog `valueKind` wire token | Constrains (Rust) | Accepted JSON shape |
| --- | --- | --- |
| `uuid` | `Uuid` | UUID string |
| `columnId` | `ColumnId` | UUID string |
| `columnIdList` | `Vec<ColumnId>` | ordered array of UUID strings |
| `string` | `String` | JSON string |
| `boolean` | `bool` | JSON boolean |
| `expression` | `Expr` | existing expression object |
| `logicalType` | `LogicalType` | `{ "kind": <camelCase variant>, "value": … }`, e.g. `{"kind":"utf8"}` |
| `scalarValue` | `ScalarValue` | existing scalar-literal shape |
| `castFailurePolicy` | `CastFailurePolicy` | `"setNull"` or `"error"` |

A serialized sample, so that a client cannot generate a config the validator
rejects. Catalog entry (abridged) and the matching valid request node:

```json
{
  "typeId": "stillflow.node.cast",
  "configVersion": 1,
  "configSchema": {
    "fields": [
      { "name": "column",    "valueKind": "columnId",          "required": true },
      { "name": "dataType",  "valueKind": "logicalType",       "required": true },
      { "name": "onFailure", "valueKind": "castFailurePolicy", "required": true,
        "constraints": { "enumValues": ["setNull", "error"] } }
    ],
    "additionalProperties": false
  }
}
```

```json
{ "id": "…", "typeId": "stillflow.node.cast", "configVersion": 1,
  "config": { "column": "…", "dataType": { "kind": "utf8" }, "onFailure": "setNull" } }
```

The catalogue must be mechanically round-trippable: every value the constraints
advertise must be accepted by the validator, and every rejection reason the
validator produces must be expressible by a constraint. The positive/negative
sample test for all eleven nodes is the enforcement of this rule.

Constraint data is descriptive of the validator, not a substitute for it. A
constraint that the validator does not enforce, or a validator rule the
constraints cannot express, is a defect for the positive/negative sample test.

### 6.3 Support conditions

Support conditions are machine-readable statements about when a node is
applicable to the working schema. The frozen form is a bounded list of
conditions per input port, each naming one requirement:

| Condition kind | Meaning | Built-in example |
| --- | --- | --- |
| `requiresType` | Input field must have this logical type | `stillflow.node.trim` requires `Utf8` |
| `requiresNonNullable` / `requiresNullable` | Nullability requirement | reserved; none of the eleven requires it today |
| `forbidsType` | Input field must not have this type | reserved; none of the eleven forbids one today |
| `requiresExecutableType` | The shared capability gate must accept the type | every node |
| `minFields` | Minimum working-schema width | `drop-column` may not remove the last field |

A condition is checked before the node's plan node is emitted, and rejection
uses the same `NG_*` code the compiler uses today. `supportStatus` stays
`Supported` for all eleven nodes; a future status value must be additive and
must not advertise a paused capability as supported.

### 6.4 Catalog delta

`NodeCatalogEntry` keeps every current field name and adds only:

- per-field constraint metadata from section 6.2;
- per-port support conditions from section 6.3;
- the definition's config version (already present) and, if needed, a
  constraint-version marker.

Catalog ordering stays `typeId`, then `configVersion`, and must be stable under
registration reordering, hash-map iteration, and repeated construction. Unknown
catalog versions and duplicate `(typeId, configVersion)` keys fail closed.

The eleven type IDs, their config field names, their ports, their lowering
targets, and their config versions remain exactly as frozen by NG-C0 §4.

### 6.5 Catalog read surface (existing, extended in place)

A catalog read surface already exists at the authorized base and is **not**
being introduced by this series:

```text
operationId: node-types.list
GET /v1/node-types
request:  EmptyRequest
response: NodeCatalogView { compilerVersion, nodes: [NodeCatalogEntry] }
```

Evidence: route `backend/crates/stillflow-service/src/routes.rs:114`, handler
`:209`–`:214`; manifest `backend/crates/stillflow-api/src/manifest.rs:328`–`:334`;
handler `backend/crates/stillflow-api/src/service.rs:1322`–`:1334`; DTO
`:1098`–`:1103`.

Frozen requirements for NX-N1:

- no new route and no route rename; `nodes` stays sorted by `typeId` then
  `configVersion`;
- `compilerVersion` (currently `NODE_GRAPH_COMPILER_VERSION`,
  `ng-nodegraph-compiler-v1`) is the catalog's version marker; it changes only
  with a breaking catalog or compiler change, and a client that does not
  recognize it must not partially interpret the catalog;
- the response carries no connector data, credentials, source values, or
  per-workspace information;
- adding fields to `NodeCatalogEntry` is additive; removing or renaming one, or
  changing the meaning of `supportStatus`, is a breaking change requiring a new
  schema version and an explicit contract;
- the catalog remains descriptive. It never gains the authority to admit or
  reject a graph.

## 7. Safe diagnostic contract (NX-A1)

### 7.1 Frozen fields

A diagnostic is transport-neutral and bounded. The compile view already
declares a diagnostic record with `{ code, nodeId, message }`
(`backend/crates/stillflow-api/src/service.rs:1105`–`:1111`, mapper
`:4071`–`:4077`); it is never populated today. NX-A1 keeps those three fields
and adds the safe location fields:

```text
Diagnostic {
  code:      string,     // frozen NG_* vocabulary, section 2.1 P-13
  nodeId:    UUID?,      // the product node that failed
  columnId:  UUID?,      // the logical column involved, when one is identifiable
  fieldPath: string?,    // bounded config path, never a value
  expected:  string?,    // bounded logical type or kind name
  actual:    string?,    // bounded logical type or kind name
  message:   string      // sanitized, bounded, human-readable
}
```

`fieldPath` is a structural path such as `predicate`, `columns[2]`, `value`, or
`expression.left`. It never contains a caller-supplied scalar, a column name, a
connection string, or a filesystem path.

The HTTP error envelope gains, additively:

```text
ApiErrorResponse {
  meta:  { apiVersion, requestId },          // requestId now populated when decoded
  error: { code, message, diagnostics? }     // diagnostics optional, bounded
}
```

`error.code` and `error.message` keep their current values for the current
failure classes. `diagnostics` is omitted when empty.

### 7.2 Ordering, count, and byte law

Precedence is frozen at two levels: the stage order below is unchanged, and the
intra-stage fault order becomes canonical.

Stage precedence (a fault in an earlier stage always wins over a later stage):

1. shape/work estimate bound (`check_shape_work`);
2. authorized source-schema validity;
3. structural graph validation, per-node registry lookup, config validation,
   and the single-source/single-output count check (`validated_configs`);
4. compile-work budget (`check_compile_work`);
5. unique source-to-output path (`unique_path`);
6. source-asset binding (`NG_SOURCE_BINDING`);
7. preview-target validity (`NG_UNSUPPORTED_TARGET`);
8. per-node schema propagation and capability checks in path order;
9. `LogicalPlan::new`, `canonical_bytes`, `fingerprint` (`NG_PLAN_INVALID`).

Intra-stage fault order, frozen so that caller array order cannot select the
reported error:

- within a stage, a graph-level fault precedes a node-specific fault;
- node-specific faults are ordered by ascending `NodeId` (UUID byte order), not
  by position in the `nodes` array;
- edge faults are ordered by `(from.nodeId, from.port, to.nodeId, to.port)`;
- the primary diagnostic is the first fault in that order.

This is a deliberate change. Today stage 3 iterates the caller's `nodes` array
(`backend/crates/stillflow-core/src/node_graph.rs:464`–`:496`), so a multi-fault
graph can report a different error when its array is permuted. The compatibility
decision, recorded as R-8: a graph with exactly one fault, and every successful
compile, is unchanged; a multi-fault graph may now report the smallest-`NodeId`
fault instead of the first-array-order fault.

- A failed compile returns exactly one primary diagnostic, and the compile
  result's `diagnostics` list contains exactly that entry.
- A successful compile returns an empty diagnostics list.
- If a later contract admits additional diagnostics, they are ordered
  deterministically by the same intra-stage rule followed by `code` and
  `fieldPath` — never by hash order, iteration order, or arrival order.
- Count and size caps: at most `MAX_DIAGNOSTICS` (64) per response, at most
  `MAX_DIAGNOSTIC_BYTES` (1024) of diagnostic text per item, and the aggregate
  diagnostic payload stays within the existing response byte bound
  (`ApiLimits::max_response_bytes`, 2 MiB).
- Caps are enforced before allocation, not after.
- Determinism claim, stated precisely: two runs over the same logical graph with
  permuted `nodes`/`edges` arrays, different map insertion order, and different
  process state must produce byte-identical `code` and `diagnostics` payloads
  **given identical request metadata**. `meta.requestId` is echoed from the
  request, so a byte comparison either fixes the same request metadata or
  excludes `meta.requestId`; it is never part of the diagnostic-ordering claim.

### 7.3 Sanitization law

Diagnostics and logs may contain: stable codes, node IDs, column IDs, bounded
field paths, logical type names, port IDs, counts, and numeric bounds. They must
never contain: raw config JSON, cell values, column display names, credentials
or secret-like strings, connector or filesystem paths, third-party error text,
or backtraces. Rejection never echoes the offending value. The existing
`ensure_no_secret_fields` policy applies to diagnostics exactly as it applies to
config and metadata.

### 7.4 Code and status mapping

The mapping from graph/compile codes to the API error code is frozen:

| Graph/compile code | API code | HTTP status (advisory) |
| --- | --- | --- |
| `NG_UNSUPPORTED_GRAPH_VERSION`, `NG_UNKNOWN_NODE_TYPE`, `NG_UNSUPPORTED_CONFIG_VERSION` | `invalidRequest` | 400 |
| `NG_INVALID_CONFIG`, `NG_INVALID_PORT`, `NG_INVALID_TOPOLOGY`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE`, `NG_UNSUPPORTED_TARGET` | `invalidRequest` | 400 |
| `NG_SOURCE_BINDING` | `notFound` | 404 |
| `NG_LIMIT_*` | `limitExceeded` | 413 |
| `NG_PLAN_INVALID`, `NG_INTERNAL` | `internal` | 500 |

`NG_SOURCE_BINDING` deliberately maps to `notFound` so that an unauthorized or
foreign source stays indistinguishable from an absent one. This preserves the
current behavior (`backend/crates/stillflow-api/src/service.rs:4095`).

### 7.5 Strict decoding law

- The request envelope, graph, node, edge, and port objects reject unknown
  fields (R-1).
- Node config unknown fields stay rejected, as they are today.
- Wrong primitive types, `null` where a value is required, missing required
  fields, duplicate node IDs, duplicate edge tuples, non-object configs, and
  oversized or over-deep nested values are typed failures with a location.
- A decode failure that can be attributed to a field reports that field; a
  failure that cannot must not invent one.
- Duplicate object keys inside a single JSON object are a rejection candidate:
  NX-A1 must determine the current decoder behavior with a test and, if
  duplicate keys are currently accepted with last-wins semantics, reject them
  for graph/node/config objects, recording the change under R-1/R-8.

## 8. Request pipeline order, deadline, and bounds (NX-A1)

### 8.1 Frozen order

For node-graph compile and preview, the order is:

| Stage | Work | May touch a data source? |
| --- | --- | --- |
| 1 | Transport and body bound (existing 2 MiB `DefaultBodyLimit`) | no |
| 2 | Envelope/meta decode, API version, strict shape decoding | no |
| 3 | Authorization, workspace scope, and request-binding checks that need no source schema | no |
| 4 | Pure graph validation: structural checks, registry lookup, config constraints, ports, topology, source-binding-to-request binding, and all frozen bounds | no |
| 5 | Authorized source schema resolution (exactly the existing `inspect` path) | `inspect` only, at most once |
| 6 | Semantic compile: expression typing, capability gate, schema propagation | no |
| 7 | Preview or persisted execution (may `read`) | yes |

Consequences, each mechanically testable:

- A request that is decodable but fails stage 4 must produce **zero** connector
  `inspect` and `read` calls. This is the acceptance criterion owned by NX-A1.
- Stage 4 binds the graph's source node to the request's `assetId`/`connectionId`
  before stage 5, so an unauthorized asset is rejected without an inspect call
  and without revealing existence.
- Stage 5 resolves the authorized schema through the existing `inspect` path and
  must not call the execution read path (`Connector::read` / `read_batches`).
  `inspect` is not a metadata-only operation for every format: text inspection
  reads a bounded prefix to infer the schema
  (`backend/crates/stillflow-connector-local-tabular/src/inspect.rs:44`,
  `inference.rs:26`–`:36`), under the existing inference row/byte caps and the
  request deadline, and reports truncation as an inspection finding; Parquet
  inspection uses footer metadata only. The guarantee is therefore "no execution
  read path", not "no sampling", and no new sampling bound is introduced here.
- Stage 3 checks must not leak the existence of foreign resources; responses
  for foreign and absent objects stay identical.

### 8.2 Single-deadline law

One request context is created per request and shared by every stage:

- Sub-stages receive the remaining budget; a sub-stage must not create a fresh
  full-timeout context, reset the deadline, or multiply the budget.
- Expiry is a typed timeout failure (`limitExceeded`), not an internal error,
  and it is reported with the same diagnostic shape.
- Cancellation propagates through the same context.

Today the preview path creates a second context for the engine stage
(`backend/crates/stillflow-api/src/service.rs:2429`, `:2452`); NX-A1 removes
that behavior.

The deadline must satisfy the strictest consumer of the request, not only the
API ceiling. Preview is the strictest today: the engine rejects a preview whose
remaining budget exceeds `PREVIEW_MAX_DEADLINE` (30 s,
`backend/crates/stillflow-engine/src/lib.rs:123`, checked at
`backend/crates/stillflow-engine/src/preview.rs:66`–`:75`), while
`request_context` defaults to `ApiLimits::max_timeout_seconds` (300 s,
`backend/crates/stillflow-api/src/service.rs:3645`–`:3653`).

Frozen timeout law — one behavior per case, with no silent clamping. An
**explicit** value is either accepted as given or rejected; only an **absent**
value is resolved from the operation default, and that default is itself bounded
by the strictest consumer cap:

| `timeoutSeconds` | Node-graph preview | Compile / durable execution |
| --- | --- | --- |
| absent | 30 s (`min(ApiLimits::max_timeout_seconds, PREVIEW_MAX_DEADLINE)`) | 300 s (`ApiLimits::max_timeout_seconds`) |
| `0` | reject `limitExceeded` | reject `limitExceeded` |
| `30` | 30 s | 30 s |
| `31` | reject `limitExceeded` (above the preview cap) | 31 s |
| `300` | reject `limitExceeded` (above the preview cap) | 300 s |
| `301` | reject `limitExceeded` (above the API ceiling) | reject `limitExceeded` |

- The accepted value becomes one absolute deadline, created once and shared by
  the inspect, compile, and preview stages.
- Rejection happens during request validation, before stage 3 or 4, so a
  boundary rejection performs no connector call at all.
- The engine's own caps remain in force as defense in depth; a request that
  reaches the engine must already satisfy them.

Compatibility decisions, stated explicitly and owned by NX-A1:

- An absent `timeoutSeconds` on preview currently produces a 300 s context that
  the engine rejects as `EngineError::BoundExceeded` →
  `invalidRequest`/400 ("engine rejected the request"). Under the frozen law it
  succeeds with a 30 s deadline. This is an intentional change, and no
  previously *successful* request is affected because the absent case never
  succeeded.
- An explicit over-cap preview value (for example `31`) is currently rejected
  inside the engine as `invalidRequest`/400 **after** the source has already
  been inspected. Under the frozen law it is rejected up front as
  `limitExceeded`/413 with zero connector calls. The failure class changes, so
  NX-A1 must record it as an error-surface change together with the R-8
  ordering change.
- `0` and values above the API ceiling keep their current `limitExceeded`
  outcome.

### 8.3 Existing request bounds

The HTTP request body bound already exists and is **not** being introduced:
`max_request_bytes = 2 MiB` (`backend/crates/stillflow-api/src/limits.rs:18`),
enforced by `DefaultBodyLimit` in `backend/crates/stillflow-service/src/process.rs:129`.
NG-C0 §7's 2 MiB serialized-graph bound and this transport bound are not the
same limit and not independent either: the graph travels inside the
`{ meta, body }` request envelope, so on the HTTP path the transport bound is
the tighter of the two and a graph can never consume the full 2 MiB of body.
The transport-neutral compiler entry keeps NG-C0's graph-byte bound for direct
callers. Neither bound may be described as absent, and neither may be widened.

NX-A1 must keep the bound in force on every typed and native entry point,
including the typed-binary routes, and must not add a second, weaker size check
that runs after decoding has already allocated.

`ApiLimits::bounded` remains the only caller-facing knob and may only lower
limits, never raise them.

## 9. Compile resource accounting boundary (NX-B1)

NX-C0 does not set the schema-amplification numbers; it fixes the surface that
NX-B1 must respect:
1. Work that scales with schema fan-out is charged before proportional
   allocation, in the same style as the existing compile-work inequality
   (NG-C0 §7). Rejection happens before the allocation it would bound.
2. Accounting covers at least: input schema fields, intermediate schema fields,
   the largest intermediate schema, emitted rules, and the serialized response
   size of the compile result.
3. NX-B1 must measure the merged baseline first — typical chain, 64-node wide
   schema, deep expression — recording wall time, allocations/peak memory, and
   response bytes with the environment and sample. Unmeasured performance
   claims are not evidence.
4. Accounting may tighten product bounds. It may never widen, replace, or
   pre-empt an engine or storage bound; where the engine is stricter, the engine
   wins.
5. Optimizations are adopted only where measurement shows a gain, and must be
   proven semantics-preserving by the differential corpus (identical canonical
   bytes, schemas, errors, and actual results).
6. A compact schema representation in responses is optional and off by default;
   the default response stays backward compatible, and any reuse of schema
   objects must respect authorization and version boundaries.

### 9.1 Bound summary for this series

Unchanged bounds are those frozen by NG-C0 §7 (P-12) and the engine/storage
law; the table records only what the NX series adds or clarifies, so no number
below may contradict a stricter existing bound.

| Dimension | Frozen value | Owner |
| --- | --- | --- |
| Rows | No new row bound. Preview row/byte limits stay with the existing preview contract (#50); accounting must not replace them. | NX-B1 |
| Bytes — request | 2 MiB HTTP body (existing, section 8.3) and 2 MiB serialized graph for the transport-neutral entry (NG-C0 §7) | NX-A1 |
| Bytes — response | 2 MiB (`ApiLimits::max_response_bytes`), including diagnostics | NX-A1, NX-B1 |
| Bytes — diagnostics | ≤ 64 items, ≤ 1 KiB text per item, aggregate inside the response bound | NX-A1 |
| Memory — compile | `O(nodes + edges + config bytes + expression nodes + schema fields)` with bounded auxiliary memory; schema fan-out charged before proportional allocation | NX-B1 |
| Concurrency | No shared mutable compile state and no process-global registry mutation; per-request isolation only. Existing `max_concurrent_requests` (64) is unchanged and is not a compile budget. | NX-S1, NX-N1 |
| Time | One absolute deadline per request, resolved by the section 8.2 table: an explicit timeout is accepted or rejected with `limitExceeded` and never clamped; an absent timeout uses the operation default (30 s preview / 300 s otherwise); sub-stages consume the remaining budget and never reset it | NX-A1 |

## 10. Version-1 compatibility baseline for the eleven built-in nodes

### 10.1 Identity and mapping baseline

Frozen for all eleven built-ins:

- `configVersion` is exactly `1`; `NodeGraph.version` is exactly `1`.
- The compiled plan contains exactly one plan node per product node, in path
  order, and `nodePlanIds[nodeId] == PlanNodeId::from_uuid(nodeId)`.
- A rule node emits exactly one `ApplyRules` node with exactly one `Rule`;
  neighboring product nodes are not coalesced.
- `nodeSchemas` records the schema after every node, including source and
  output; `outputSchema` equals the output node's schema.
- `PlanNodeId` values are never generated, reordered, or compacted.
- The product rules-per-node bound stays `1`
  (`backend/crates/stillflow-core/src/node_graph.rs:23`). The engine's looser
  internal bound (`MAX_RULES_PER_NODE = 256`,
  `backend/crates/stillflow-engine/src/lib.rs:93`) is an execution bound and
  must not be adopted as the product bound; the product path stays the stricter
  authority (NG-C0 §7).

| Type ID | Config | Lowering | Plan node | Emitted rules |
| --- | --- | --- | --- | --- |
| `stillflow.node.source` | `sourceAssetId` (required), `projection` (optional) | `Scan` | 1 | 0 |
| `stillflow.node.select` | `columns` (required) | `Project` | 1 | 0 |
| `stillflow.node.filter` | `predicate` (required) | `Filter` | 1 | 0 |
| `stillflow.node.rename` | `column`, `to` | `ApplyRules` | 1 | 1 (`Rename`) |
| `stillflow.node.trim` | `column` | `ApplyRules` | 1 | 1 (`Trim`) |
| `stillflow.node.cast` | `column`, `dataType`, `onFailure` | `ApplyRules` | 1 | 1 (`Cast`) |
| `stillflow.node.replace-literal` | `column`, `from`, `to` | `ApplyRules` | 1 | 1 (`ReplaceLiteral`) |
| `stillflow.node.fill-null` | `column`, `value` | `ApplyRules` | 1 | 1 (`FillNull`) |
| `stillflow.node.drop-column` | `column` | `ApplyRules` | 1 | 1 (`DropColumn`) |
| `stillflow.node.derive-column` | `id`, `name`, `dataType`, `nullable`, `expression` | `ApplyRules` | 1 | 1 (`DeriveColumn`) |
| `stillflow.node.output` | `outputLabel` | `Materialize` | 1 | 0 |

### 10.2 Canonical bytes and fingerprint baseline

- The execution identity of a compiled graph is the existing
  `LogicalPlan::canonical_bytes()` and its fingerprint under the existing
  algorithm `stillflow-fnv1a64x4-v1`
  (`backend/crates/stillflow-plan/src/plan.rs:15`, `:229`, `:235`).
- No graph-level canonical form is an execution identity. The product-diff form
  of NG-C0 §5.2 (nodes by `NodeId`, edges by endpoint tuple, keys by UTF-8 order)
  may be used for product diffs and persistence work (D-3) only.
- A normalized graph with the same node IDs, configs, authorized source
  asset/schema, and contract versions must produce byte-identical canonical
  bytes and the same fingerprint, independent of node/edge array order,
  registry insertion order, hash order, clock, locale, and environment.
- Changing `graphId`, graph metadata, or node `metadata` alone must not change
  the plan bytes.

### 10.3 Result baseline

Observationally frozen for a previously valid version-1 plan: the compiled
schema, the preview output, and the materialized result must not change. NX-S1,
NX-N1, NX-A1, and NX-B1 each prove this for their own slice.

### 10.4 Error baseline

The failure classes each node can produce today, which the NX series may
*enrich with location* but must not reclassify:

| Type ID | Node-specific codes | Shared codes |
| --- | --- | --- |
| `stillflow.node.source` | `NG_INVALID_CONFIG`, `NG_SOURCE_BINDING`, `NG_UNKNOWN_COLUMN` | `NG_UNSUPPORTED_GRAPH_VERSION`, `NG_UNKNOWN_NODE_TYPE`, `NG_UNSUPPORTED_CONFIG_VERSION`, `NG_INVALID_PORT`, `NG_INVALID_TOPOLOGY`, `NG_UNSUPPORTED_TARGET`, `NG_LIMIT_*`, `NG_PLAN_INVALID`, `NG_INTERNAL` |
| `stillflow.node.select` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN` | as above |
| `stillflow.node.filter` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.rename` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN` | as above |
| `stillflow.node.trim` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.cast` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.replace-literal` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.fill-null` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.drop-column` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN` | as above |
| `stillflow.node.derive-column` | `NG_INVALID_CONFIG`, `NG_UNKNOWN_COLUMN`, `NG_INCOMPATIBLE_TYPE` | as above |
| `stillflow.node.output` | `NG_INVALID_CONFIG` | as above |

`NG_UNSUPPORTED_TARGET` is shared rather than node-specific because it is
raised by preview-target validation for any node ID that is not an emitted
non-materialize node (`backend/crates/stillflow-plan/src/node_graph_compiler.rs:131`,
`:138`, `:210`). `NG_INVALID_TOPOLOGY` is likewise shared because the topology
rules are graph-level, not per-node.

Classification changes are permitted only for the R-5 divergence cases, and
only in the direction "compiled successfully but could never execute → rejected
at compile time", each listed with its minimal case in the owning PR.

### 10.5 Baseline corpus, capture point, and regeneration

- The corpus records **pre-change** behavior. It is captured at the frozen base
  the first implementation issue branches from — for NX-S1 that is
  `main@5bff563cc4d33ef11f0cff4935aec02e92985166`, or the then-current `main`
  after an authorized rebind — **before** that issue modifies any authorized
  file. It is never captured from a post-change head.
- The capturing step records the base SHA, the exact command, the toolchain, and
  the environment, and commits the resulting fixtures unchanged. The capture is
  therefore reproducible by a reviewer from the same base.
- A candidate implementation is compared against that captured baseline. A
  difference is a compatibility event: the PR either fixes the candidate or
  cites the contract clause that authorizes the change and states the migration
  consequence. Regenerating a fixture from the candidate head is forbidden,
  because it would record refactor output as the reference and make the
  comparison vacuous.
- New surfaces (new catalog fields, new diagnostic fields, additional
  diagnostics or warnings) get their own expectation files, versioned separately
  from the frozen v1 baseline, so the v1 files keep describing old behavior:
  `backend/crates/stillflow-plan/tests/fixtures/nx-v1/` for frozen pre-change
  behavior and `backend/crates/stillflow-plan/tests/fixtures/nx-next/` for new
  surfaces. A v1 fixture may change only under a contract that explicitly
  authorizes a change to version-1 execution identity, naming the reason and the
  migration decision; NX-C0 authorizes none.
- Corpus content, for every built-in node and for representative chains: the
  normalized input graph, the authorized source schema, the expected canonical
  plan bytes digest, the expected fingerprint, the expected per-node schemas,
  and the expected error code for negative cases.
- Corpus location: `backend/crates/stillflow-plan/tests/fixtures/nx-v1/`,
  created by NX-S1 at the pre-change base and extended — never rewritten — by
  later issues.

## 11. Downstream entry criteria

Each issue below may start only after its declared dependencies are accepted and
merged and `main` has been re-fetched at that accepted head. Each must satisfy
its own acceptance criteria in #334 in addition to these boundaries.

### Ordered checklist

Paths listed under an issue that do not exist at the authorized base are created
by that issue; paths that exist are modified in place. A new file named here is
a frozen location, not an authorization to create it before the issue starts.

The dependency order of #334 is restated as an executable checklist. Work may
run in parallel only where the entries write disjoint surfaces.

- [ ] 1. #335 (this contract) accepted and merged; `main` re-fetched at that head.
- [ ] 2. NX-S1 (#336) — shared semantic analysis plus the differential corpus. No later runtime issue starts before this merges.
- [ ] 3. NX-N1 (#337) — per-node definition modules, machine-readable constraints, catalog.
- [ ] 4. NX-A1 (#338) — safe diagnostics, strict decoding, pipeline order, single deadline. Must not start before 2 and 3 merge.
- [ ] 5. NX-B1 (#339) — baseline measurement, then schema/response accounting. Same dependencies as 4, and it writes the same `stillflow-plan` compile surface, so it sequences after NX-A1 rather than running beside it.
- [ ] 6. NX-C1 (#340) — composite-node and declarative-package contract (needs 2 and 3 merged).
- [ ] 7. NX-N2 (#341) — bounded composite nodes and controlled declarative packages (needs 3, 2, 4, 5).
- [ ] 8. NX-V0 (#342) — GraphRevision and configuration-migration contract (needs 6).
- [ ] 9. NX-V1 (#343) — graph save/restore and explicit migration API (needs 8, 7, 4).
- [ ] 10. NX-G1 (#344) — end-to-end, compatibility, and resource acceptance (needs 2, 3, 4, 5, 7, 9).
- [ ] 11. NX-D0 (#345) and NX-P0 (#346) — design-only follow-ups after the gate; neither starts the DAG or verification runtime.

Each implementation issue additionally follows the repository flow for its own
risk level: `agent/issue-NNN-nx-*` branch in its own worktree, no direct commit
to `main`, Draft PR, exact-head CI, and independent review or a compliant
acceptance receipt before Ready/merge.

### NX-S1 (#336)

Paths: `backend/crates/stillflow-plan/src/semantics.rs` (or `semantics/`),
`node_graph_compiler.rs`, `lib.rs`; `backend/crates/stillflow-engine/src/typing.rs`,
`preflight.rs`, `incremental.rs`; tests `stillflow-plan/tests/nx_s1_semantics.rs`
and `stillflow-engine/tests/nx_s1_differential.rs`; fixtures
`stillflow-plan/tests/fixtures/nx-v1/`.

Independently executable test boundary: the differential corpus runs against
both entry points without any connector, API, or service dependency, and passes
with the network and the filesystem untouched.

Must not: add a dependency from `stillflow-core` to `stillflow-plan`; move
physical executor code; unlock a paused capability; change canonical bytes or
the fingerprint for a previously successful fixture; introduce a second
diagnostic vocabulary.

### NX-N1 (#337)

Paths: `backend/crates/stillflow-core/src/node_graph.rs` (or a `node_graph/`
module tree with per-node definition modules), `stillflow-plan` compiler
wiring, `backend/crates/stillflow-api/src/manifest.rs`.

Independently executable test boundary: positive/negative samples derived from
the definition source are checked against the validator for all eleven types,
plus one test-only definition proving that a new node type needs no generic
traversal change and does not enter the production catalog.

Must not: change any of the eleven type IDs, config field names, config
versions, ports, or lowering targets; make the catalog the execution authority;
introduce a second registration path.

### NX-A1 (#338)

Paths: `backend/crates/stillflow-core/src/node_graph.rs` (decoding and
structured errors), `stillflow-plan` diagnostics, `stillflow-api`
(`error.rs`, `envelope.rs`, `service.rs`, `manifest.rs`), `stillflow-service`
(`adapter.rs`, `routes.rs`, `process.rs`).

Independently executable test boundary: a counting connector stub proves zero
`inspect`/`read` calls for decodable graphs that fail stage 4, and error-shape
tests prove node/field location and request-ID preservation.

Must not: change route paths or operation IDs; change `ApiErrorCode` variants;
reclassify existing codes; echo user values; introduce an unversioned schema
cache; or reset the request deadline in a sub-stage.

### NX-B1 (#339)

Paths: `backend/crates/stillflow-plan` (compile work and schema
representation), `backend/crates/stillflow-core` (validation internals),
`backend/crates/stillflow-api` (compile response options), plus performance
evidence under `docs/evidence/`.

Independently executable test boundary: boundary cases at and above each frozen
input/intermediate/output bound are rejected deterministically, and the
before/after differential over the corpus shows identical bytes, schemas,
errors, and results.

Must not: enable a historical O0 experiment; widen an engine bound; publish an
unmeasured percentage; raise a node limit instead of bounding amplification;
change the default response shape.

## 12. Objective acceptance matrix for #335

This documentation delivery is acceptable only when every row is testable from
the repository:

| Acceptance item | Frozen evidence |
| --- | --- |
| NG-C0 is classified item by item | Section 2 (P/R/D/X tables) |
| Every revision has a compatibility decision and an objective test | Section 2.2 |
| Public Rust/JSON/API changes are enumerated with decisions | Section 3 |
| Module responsibilities and dependency direction are frozen, and a separate crate is evaluated without being mandated | Section 4 |
| Shared semantic result, capability boundary, and divergence law are frozen without unlocking a paused operator | Section 5 |
| Machine-readable config constraints, catalog support conditions, and versioning are frozen | Section 6 |
| Catalog constraints are expressed in wire values and carry a round-trippable sample | Sections 6.2 and 6.5 |
| Safe diagnostic fields, canonical intra-stage fault ordering, count, byte caps, and secret law are frozen | Section 7 |
| Pipeline order, the case-by-case timeout table (absent, `0`, `30`, `31`, `300`, `301`), the single-deadline law, and the existing 2 MiB HTTP bound are stated with exactly one behavior per case | Section 8 |
| Resource accounting boundaries are frozen without setting unmeasured numbers | Section 9 |
| All eleven built-in nodes have byte/mapping/result/error baselines, captured at the pre-change base and compared against candidates | Section 10 |
| New surfaces have separately versioned expectations, so the v1 baseline keeps describing old behavior | Section 10.5 |
| NX-S1/NX-N1/NX-A1/NX-B1 have independently executable paths and test boundaries | Section 11 |
| Docs-only scope is preserved | No Rust, dependency, lockfile, persistence, workflow, or client change in the #335 diff |

## 13. Contract deviations and refused compatibility

- NX-C0 uses the existing repository contracts rather than inventing public
  physical types. `LogicalSchema`, `Expr`, `Rule`, `LogicalPlan`, `PlanNodeId`,
  plan canonicalization, and engine/storage bounds remain authoritative.
- The shared semantic module is a *lower-layer* extraction. The engine keeps
  runtime capability enforcement and may keep performance representations only
  with an explicit, differentially tested justification.
- Nullability is frozen as part of the shared analysis result because it is
  observable in the compiled schema and currently derived in more than one
  place.
- The single-primary-diagnostic rule is deliberate: NG-C0 declared a bounded
  diagnostic list, the current compiler emits at most one failure, and NX-C0
  preserves that behavior while making the field set useful.
- Divergence between the compiler and the engine is resolved by contract and
  recorded as minimal cases; no side is chosen silently.
- This contract deliberately does not set schema-amplification numbers, does not
  design composite nodes, declarative packages, graph persistence, or DAG
  execution, and does not authorize any runtime change.

### 13.1 Known risks and stop conditions

Known risks, each with its mitigation:

| Risk | Mitigation |
| --- | --- |
| Unifying the two semantic paths changes which inputs compile, so an input that used to compile and then failed at preview now fails earlier. | §5.3 records each divergence as a minimal case, keeps previously successful fixtures byte-identical, and requires the change to be listed in the NX-S1 PR body. |
| Sharing semantics erodes the engine's performance path (`IncrementalSchema` exists for a reason). | §5.4 permits an engine-internal representation only with a named non-sharable justification and a differential test against the shared analyzer; NX-B1 measures before optimizing. |
| Rejecting unknown graph/node/edge fields breaks a client that currently sends extras. | NG-C0 §2.3 bans "ignore what this runtime does not understand", and NX-C0 extends the config-level rule to the envelope (R-1); the owning PR must report any merged fixture or request that relied on the old behavior, and the change is a tightening, not a reclassification. |
| A new schema-amplification budget rejects graphs that are accepted today. | §9 requires measuring the merged baseline first and publishing the numbers; rejections must be deterministic, documented, and never replace a stricter engine bound. |
| The catalog becomes a second execution authority. | §6.1: one definition source; the catalog describes, the validator decides, and a constraint the validator cannot express is a defect. |
| Diagnostics leak caller values or third-party error text. | §7.3 field whitelist plus secret-sentinel tests in the owning PR. |
| The v1 baseline is captured after the refactor, so it records the new behavior as the reference and cannot prove compatibility. | §10.5 captures the corpus at the frozen pre-change base, compares candidates against it, forbids regenerating a v1 fixture from a candidate head, and puts new surfaces in separately versioned `nx-next/` expectations. |
| The contract text drifts from the merged implementation. | §1 cites the observed state with `path:line`; an implementation issue that finds a citation false must amend this contract before proceeding. |

Stop and return to contract review when any of these occurs:

- a needed public change is not listed in section 3;
- a dependency arrow reverses or cycles, or `stillflow-core` gains a dependency
  on `stillflow-plan`;
- two authorities own the same semantics, or an engine-internal second
  implementation lacks the §5.4 justification;
- deterministic output would depend on unordered state, wall clock, or random
  IDs;
- raw credentials, source values, or third-party error text could enter
  diagnostics, logs, or events;
- a compatibility decision is ambiguous, including a §5.3 divergence that
  neither rule resolves;
- any paused capability would be unlocked without a new contract;
- any bound would be widened, or an engine/storage bound replaced by a product
  bound;
- `main` drift overlaps an authorized surface (graph model, compiler, engine
  typing/preflight, error envelope, pipeline order) and the branch has not been
  re-evaluated or rebound.

## 14. Explicit non-goals
NX-C0 does not authorize: DAG, Join, Union, multiple sources, multi-output
graphs; typed ports or fan-in/fan-out; composite or declarative nodes; graph
persistence, revision history, or configuration migration; draft preview or
verification-node admission; a second expression/rule/plan language, optimizer,
or canonicalizer; any user-supplied executable code; physical executor
extraction or enabling the historical experimental branches; unlocking #93 XR
work, SQL connector #9, or DuckDB executor #10; widening any bound; and any
change to Openship production code. Already published `PlanVersion` records
remain independently executable and historical evidence is not rewritten.
