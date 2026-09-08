# Issue #324: NG-C0 NodeGraph / compiler contract

> Status: Candidate frozen contract for acceptance by #324
> Risk: L3 — public product configuration and compiler boundary
> Parent: #323
> Authorized base: `main@7f74e1b026a26997e72b8a791bdc596613d1d399`
> Suggested implementation branch: `docs/issue-324-ng-c0-nodegraph-contract`

This document freezes the Phase-1 contract for a product-facing configurable
`NodeGraph`. It authorizes the later NG-R1 model/registry delivery and NG-R2
compiler delivery. It does not authorize either implementation in this issue.

The existing `stillflow-plan::LogicalPlan` remains the only execution-facing
intermediate representation. A `NodeGraph` is a product configuration and
persistence input; it is never an executor, a second plan language, or a
durable execution authority.

## 1. Decision and authority boundary

The supported product flow is:

```text
NodeGraph + authorized source schema
        ↓
NodeRegistry / NodeDefinition validation
        ↓
deterministic NG compiler
        ↓
stillflow_plan::LogicalPlan + schema/mapping
        ↓
existing LogicalPlan validation, Preview, PlanVersion, JobRuntime
        ↓
existing ExecutionEngine → Snapshot / Artifact
```

The following ownership rules are normative:

| Concern | Authority | NG-C0 rule |
| --- | --- | --- |
| Product graph identity, node arrangement, and UI metadata | `NodeGraph` | May be persisted and edited; never controls execution directly. |
| Node type/config metadata and validation | `NodeDefinition` / `NodeRegistry` | Declarative, built-in only, deterministic, and transport-neutral. |
| Graph topology and schema checks | NG compiler | Must fail before connector execution or materialization. |
| Execution IR, logical operator meaning, canonical bytes, and plan fingerprint | `stillflow-plan::LogicalPlan` | The single execution authority. No graph-specific canonicalizer or rule language. |
| Durable plan identity and lifecycle | Existing `PlanVersion` APIs | Store the compiled `LogicalPlan` and its existing canonical digest/fingerprint. |
| Preview and formal execution | Existing Preview / `JobRuntime` / `ExecutionEngine` | NG adds no executor, queue, job lifecycle, or publication path. |

`NodeGraph.graph_id` and graph-level metadata are product persistence data and
are excluded from the compiled logical plan. Stable product `node_id` values
are semantic identities: they are preserved through edits and are used as the
deterministic `PlanNodeId` mapping. A graph copy with different node IDs may
therefore have a different plan fingerprint even when its visible layout is
otherwise equal. A graph identity change alone must not change the plan.

## 2. Versioned product contract

### 2.1 `NodeGraph`

Version 1 is the following transport-neutral shape. Field names shown in
camelCase are the wire form; implementation may use Rust newtypes and modules
without changing these semantics.

```text
NodeGraph {
  version: u16,                 // exactly 1
  graphId: UUID,                // product persistence identity; non-semantic
  sourceNodeId: NodeId,
  outputNodeId: NodeId,
  nodes: [NodeConfig],
  edges: [NodeEdge],
  metadata: Map<String, String> // non-semantic, secret-free
}

NodeConfig {
  id: NodeId,                   // UUID; non-nil and unique in this graph
  typeId: String,               // stable registry key
  configVersion: u16,           // definition-owned; exactly 1 for NG-C0 nodes
  config: JSON object,          // validated only by its NodeDefinition
  metadata: Map<String, String> // product-only, non-semantic, secret-free
}

NodeEdge {
  from: { nodeId: NodeId, port: PortId },
  to:   { nodeId: NodeId, port: PortId }
}
```

The graph validator must enforce all of the following before a registry or
compiler allocates unbounded state:

- `version == 1`; an unknown or newer graph version fails closed.
- `graphId`, every node ID, and every endpoint node ID are non-nil UUIDs.
- Node IDs are unique; edge tuples are unique; every edge endpoint exists.
- `sourceNodeId` and `outputNodeId` each name exactly one node of the required
  built-in type. They must not be the same node.
- All nodes are reachable from the source and can reach the output.
- The graph has exactly one source, exactly one output, and one connected
  directed path from source to output. Branches, merges, cycles, disconnected
  nodes, skipped ports, and alternate paths fail closed.
- Phase 1 has `edges == nodes - 1`; every non-source node has one incoming edge
  on its declared input port and every non-output node has one outgoing edge on
  its declared output port.
- Node order in the serialized `nodes` array is not execution order. Topology
  is determined only by validated edges. The compiler walks the unique path.
- A node config is a JSON object. Unknown fields, wrong primitive types, null
  where a field is required, and missing required fields are rejected by the
  definition schema; they are not silently ignored.

`NodeId` and `ColumnId` values are supplied by the product/client or trusted
compiler caller. NG never generates identities, timestamps, credentials, or
source values during validation or compilation.

### 2.2 Ports and source/output identity

Every NG-C0 definition declares a fixed port contract. Port IDs are ASCII
strings, case-sensitive, and definition-owned. The only port IDs in this
contract are `in` and `out`:

| Definition class | Input ports | Output ports | Identity/config authority |
| --- | --- | --- | --- |
| `source` | none | `out` | `sourceAssetId` in the config, checked against the authorized compile context |
| transform | `in` | `out` | Node ID plus typed config |
| `output` | `in` | none | Node ID and non-empty `outputLabel` |

Source identity is not a filesystem path, connection object, credential, or
connector configuration. It is the existing `SourceAsset.id` UUID and must be
authorized by the service/runtime caller. The compiler rejects a graph whose
source asset differs from the authorized source context before inspecting or
reading a connector.

Output identity is the stable output node ID plus its validated `outputLabel`.
The label is lowered only to the existing `Materialize.output_label` field; it
does not select a physical executor or create a new publication destination.

### 2.3 Version and compatibility policy

The compatibility policy is fail-closed:

| Surface | Version 1 policy | Unknown/newer value |
| --- | --- | --- |
| `NodeGraph.version` | Exactly `1` | `NG_UNSUPPORTED_GRAPH_VERSION`; no best-effort decode |
| `NodeConfig.configVersion` | Exactly the version advertised by the resolved definition; all NG-C0 built-ins use `1` | `NG_UNSUPPORTED_CONFIG_VERSION` |
| `typeId` | Must be present in the built-in registry | `NG_UNKNOWN_NODE_TYPE` |
| Logical schema | Existing `LogicalSchema` version `1` | Existing schema error; no downgrade |
| Logical plan | Existing `PLAN_VERSION == 1` | Existing plan validation error |
| Compiler contract | `ng-nodegraph-compiler-v1` | `NG_UNSUPPORTED_COMPILER_VERSION` in any persisted/requested compiler stamp |

There is no runtime migration registry in NG-C0. A future graph or config
version requires an explicit contract and migration decision before it is
accepted. Unknown fields are not treated as a forward-compatible extension.
Compatibility shims, dynamic plugin loading, and “ignore what this runtime does
not understand” behavior are forbidden.

## 3. `NodeDefinition` and `NodeRegistry`

`NodeDefinition` is a declarative capability record for one stable node type.
Its responsibilities are:

1. stable `type_id` and display/config metadata for a catalog;
2. supported `config_version` and a bounded machine-readable config schema;
3. fixed input/output port and arity declaration;
4. typed config validation against the authorized schema/compile context;
5. a closed logical lowering target (`Scan`, `Project`, `Filter`, `ApplyRules`
   with one approved `Rule`, or `Materialize`).

It must not hold a connector, read a source, evaluate an expression, execute a
DataFrame, emit a job, write storage, or contain user-supplied executable code.
The compiler owns graph traversal and plan assembly; a definition may describe
the typed lowering target but may not introduce a second logical semantics.

`NodeRegistry` has these semantics:

- It contains only the fixed NG-C0 built-ins listed in section 4.
- Registration is deterministic and closed: duplicate type/version keys fail
  during construction; runtime/user registration and dynamic loading do not
  exist in Phase 1.
- Catalog output is sorted by `type_id`, then `config_version`, and contains
  only bounded display/config/port/support metadata.
- Definition lookup is exact on `(type_id, config_version)`; a fallback to a
  nearby version is not allowed.
- Registry content and ordering are part of deterministic compiler inputs. A
  `HashMap` iteration order, locale, wall clock, process randomness, or machine
  capabilities must not affect validation, plan construction, or diagnostics.

## 4. Phase-1 built-in catalog and lowering matrix

The following type IDs and config fields are frozen for NG-C0. `ColumnId`
references are always logical IDs, never display names.

| Type ID | Required config | Lowering target | Phase-1 status |
| --- | --- | --- | --- |
| `stillflow.node.source` | `sourceAssetId: UUID`; optional non-empty ordered `projection: [ColumnId]` | `Scan { source_asset_id, projection, predicate: None }` | Supported |
| `stillflow.node.select` | non-empty unique ordered `columns: [ColumnId]` | `Project { columns }` | Supported |
| `stillflow.node.filter` | `predicate: Expr` | `Filter { predicate }` | Supported |
| `stillflow.node.rename` | `column: ColumnId`, non-empty `to: String` | `ApplyRules { [Rule::Rename] }` | Supported |
| `stillflow.node.trim` | `column: ColumnId` | `ApplyRules { [Rule::Trim] }` | Supported for `Utf8` |
| `stillflow.node.cast` | `column`, `dataType`, `onFailure` | `ApplyRules { [Rule::Cast] }` | Supported subject to existing cast law |
| `stillflow.node.replace-literal` | `column`, `from`, `to` | `ApplyRules { [Rule::ReplaceLiteral] }` | Supported subject to existing type law |
| `stillflow.node.fill-null` | `column`, non-null `value` | `ApplyRules { [Rule::FillNull] }` | Supported subject to existing type law |
| `stillflow.node.drop-column` | `column: ColumnId` | `ApplyRules { [Rule::DropColumn] }` | Supported; cannot remove the last field |
| `stillflow.node.derive-column` | `id`, unique `name`, `dataType`, `nullable`, `expression: Expr` | `ApplyRules { [Rule::DeriveColumn] }` | Supported subject to expression/type law |
| `stillflow.node.output` | non-empty `outputLabel: String` | `Materialize { output_label }` | Supported |

Each transform node emits exactly one logical plan node. Each rule node emits
exactly one `ApplyRules` node containing exactly one existing `Rule`; NG-R2
must not coalesce neighboring product nodes because preview mapping and schema
boundaries are product-visible. The source node and output node are also
logical plan nodes, so no synthetic plan IDs are needed.

`Rule::Validate` and `Rule::Deduplicate` exist in the existing logical rule
language but are not NG-C0 executable built-ins. The ordinary E2 execution path
rejects them; the existing internal verification path may admit them for its
separate E4 contract. NG compilation must reject them unless a future contract
explicitly supplies an authorized verification target and execution context.
They must not be advertised as supported by the NG catalog in this phase.

Join, Union, multiple Scan sources, arbitrary DAGs, variadic ports, SQL, raw
Polars/DuckDB expressions, and user/plugin nodes have no registry entry and
must fail closed as unsupported.

## 5. Compiler contract

### 5.1 Inputs and outputs

The transport-neutral compiler consumes:

- a structurally validated `NodeGraph`;
- the exact built-in `NodeRegistry`;
- an authorized source context containing one `SourceAsset.id` and its
  validated version-1 `LogicalSchema`;
- the requested compile target, either execution mode or a product node ID for
  preview mapping. Target selection never creates a plan fragment.

It returns a bounded result equivalent to:

```text
CompiledNodeGraph {
  plan: LogicalPlan,
  outputSchema: LogicalSchema,
  nodePlanIds: ordered map<NodeId, PlanNodeId>,
  nodeSchemas: ordered map<NodeId, LogicalSchema>,
  diagnostics: bounded list<sanitized diagnostic>
}
```

`nodePlanIds` contains one entry for every graph node and is the direct mapping
`PlanNodeId::from_uuid(node.id)`. `nodeSchemas` records the schema after each
node, including the source schema and the final output schema. Diagnostics are
bounded and contain codes, node/column IDs, and type/kind names only; they do
not contain raw config JSON, cell values, credentials, connector paths, or
backtraces.

The compiler does not receive a connector or connection credential, does not
inspect/read data, does not allocate a `PlanVersion`, and does not invoke
`ExecutionEngine`. A service may later wrap it for API and preview behavior,
but that belongs to NG-A1.

### 5.2 Deterministic lowering algorithm

NG-R2 must implement the following observable sequence:

1. Enforce graph, config, expression, schema, and work bounds before
   allocating proportional collections or traversing user-controlled nesting.
2. Resolve every node by exact `(typeId, configVersion)` in the fixed registry
   and validate its config and ports.
3. Validate the unique source-to-output path and bind the source asset to the
   authorized context.
4. Start the working schema at the authorized source schema. If source
   `projection` is absent, use all authorized fields in their existing schema
   order; if present, preserve the requested order after validating IDs and
   uniqueness.
5. Emit one `PlanNode` per graph node in path order. Inputs contain exactly the
   preceding path node ID. The root is the output node's `PlanNodeId`.
6. Propagate the working schema through each node using section 6 and reject
   the first invalid reference, type, name, nullability, or bound.
7. Construct `LogicalPlan::new`, call `LogicalPlan::validate()`, then compute
   the existing `canonical_bytes()` and `fingerprint()`. Failure of any step
   is a compile failure, never a warning-and-continue result.

The compiler output must not depend on input array order for graph nodes/edges,
registry insertion order, a hash-map order, current time, UUID generation,
locale, environment, or connector behavior. A normalized graph with the same
node IDs, configs, authorized source asset/schema, and contract versions must
produce byte-identical logical-plan canonical bytes and the same existing plan
fingerprint.

Graph persistence may use a separate non-security canonical form for product
diffs: nodes are ordered by `NodeId`, edges by `(from.nodeId, from.port,
to.nodeId, to.port)`, and metadata/object keys by UTF-8 key order. That form
is not an execution identity and must not be persisted as a substitute for the
compiled `LogicalPlan` canonical bytes.

### 5.3 Explicit logical target boundary

Only these existing types may appear in compiler output:

- `stillflow_plan::PlanNodeKind::{Scan, Project, Filter, ApplyRules,
  Materialize}`;
- existing `stillflow_plan::Rule` variants explicitly marked Supported in
  section 4;
- existing `stillflow_core::Expr`, `LogicalSchema`, `LogicalType`,
  `ColumnId`, and `ScalarValue` contracts.

The compiler must not add a graph-specific `PlanNodeKind`, a second expression
or rule AST, a physical plan, a DataFrame/SQL object, an optimizer, or a hidden
executor selection policy. The output is valid for the existing linear
preflight; a plan that only a graph-specific path can execute is invalid.

## 6. Schema, port, and column identity propagation

Schema propagation is ordered and identity-based. A display-name lookup is
never substituted for a `ColumnId` lookup.

| Node/rule | Resulting schema rule |
| --- | --- |
| Source | Start from authorized schema. Projection selects existing fields in requested order; IDs, names, types, nullability, and safe metadata are preserved. |
| Select / `Project` | Keep exactly the ordered requested fields. Reject an empty projection, unknown ID, or duplicate ID. |
| Filter | Validate the expression against the current schema and require a Boolean result. Schema is unchanged. |
| Rename | Keep the same `ColumnId`, type, nullability, metadata, and field position; replace only the name. Reject unknown ID, empty name, or duplicate resulting name. |
| Trim | Require an existing `Utf8` field. Schema is unchanged. |
| Cast | Keep ID, name, metadata, and position; replace the type. `SetNull` widens nullability to true; `Error` preserves it. Apply the existing paused-cast and binary restrictions. |
| Replace literal | Require both literals to be valid for the current field type. `to = Null` widens nullability; otherwise schema is unchanged. Preserve existing binary/null restriction. |
| Fill null | Require a non-null value valid for the field type. Set the field nullable flag to false. Preserve ID/name/type/metadata/position. |
| Drop column | Remove the field and reject unknown IDs or removal of the final remaining field. |
| Derive column | Validate expression references and type. Append the supplied new ID/name/type/nullability. Reject an existing ID, duplicate name, invalid declared type, or nullability narrower than inferred. |
| Output / `Materialize` | Schema is unchanged and becomes `outputSchema`. `outputLabel` is validated as bounded data only. |

All schema constructors and existing logical validators remain authoritative.
The compiler must reuse their rules rather than reimplementing a weaker check.
Rename never changes identity. Drop permanently removes an identity from the
working schema. A derived identity is caller-supplied and is never generated.
Expressions reference only current `ColumnId` values; references to a column
removed earlier in the path fail before execution.

`LogicalSchema` version, field order, logical type, nullability, metadata, and
column IDs are part of the compiler result. Metadata remains secret-free and
bounded; arbitrary connector or source-row metadata is not copied into a
NodeGraph config.

## 7. Limits and complexity law

The following are hard NG-C0 bounds. They are checked before work that scales
with the corresponding input. Existing lower-level limits remain in force if
they are stricter.

| Resource | Maximum | Failure |
| --- | ---: | --- |
| Serialized `NodeGraph` bytes | 2 MiB | `NG_LIMIT_GRAPH_BYTES` |
| Nodes, including source/output | 64 | `NG_LIMIT_NODES` |
| Edges | 63 (`nodes - 1`) | `NG_LIMIT_EDGES` |
| Config bytes per node | 64 KiB | `NG_LIMIT_CONFIG_BYTES` |
| Aggregate config bytes | 1 MiB | `NG_LIMIT_CONFIG_BYTES` |
| One string, including names/labels/type/port IDs | 4 KiB UTF-8 | `NG_LIMIT_STRING_BYTES` |
| Aggregate graph/config metadata bytes | 64 KiB | `NG_LIMIT_METADATA_BYTES` |
| JSON/config nesting depth | 64 | `NG_LIMIT_NESTING_DEPTH` |
| Expression nodes and depth | 1,024 / 64 | Existing `MAX_EXPR_NODES` / `MAX_EXPR_DEPTH` failure |
| Logical schema fields and nesting depth | 4,096 / 64 | Existing schema contract failure |
| Rules emitted by one product node | 1 | `NG_LIMIT_RULES_PER_NODE` |
| Total emitted rules | 64 | `NG_LIMIT_RULES` |
| Diagnostics per compile | 64 | `NG_LIMIT_DIAGNOSTICS` |
| Diagnostic text per item | 1 KiB UTF-8 | `NG_LIMIT_DIAGNOSTIC_BYTES` |
| Compile work units | 2,000,000 | `NG_LIMIT_COMPILE_WORK` |

The compile-work budget is computed without overflow before proportional work:

```text
nodes
+ edges
+ serialized_config_bytes
+ 4 * total_expression_nodes
+ 4 * total_schema_fields
+ 16 * metadata_entry_count
<= 2,000,000
```

Graph validation and compilation must be `O(nodes + edges + config bytes +
expression nodes + schema fields)` with bounded auxiliary memory. Recursive
walks over user-controlled graph/config/expression nesting are forbidden when
they could exceed the depth bounds; iterative traversal is preferred. Integer
overflow, allocation failure, and bound exhaustion are typed failures, not
panics or partial plans.

The existing engine bounds remain execution authority, including the current
logical-plan byte budget, rule budget, operator-state budget, batch limits,
cancellation, and deadline law. NG-R2 may reject earlier with a stricter
product bound but may not enlarge those existing limits.

## 8. Error, fail-closed, and secret-safety law

### 8.1 Stable error categories

The transport-neutral layer must expose stable categories/codes. API wording
and HTTP status mapping are NG-A1 concerns, but the code and sanitized fields
below are frozen now:

| Code | Meaning | Safe context |
| --- | --- | --- |
| `NG_UNSUPPORTED_GRAPH_VERSION` | Graph version is unknown/newer | version |
| `NG_UNKNOWN_NODE_TYPE` | Registry has no exact type | type ID, node ID |
| `NG_UNSUPPORTED_CONFIG_VERSION` | Definition/config version mismatch | type ID, config version |
| `NG_INVALID_CONFIG` | Config shape/value is invalid | node ID, field name, kind |
| `NG_INVALID_PORT` | Port is not declared or connected correctly | node ID, port ID |
| `NG_INVALID_TOPOLOGY` | Branch, merge, cycle, disconnected path, or wrong source/output count | bounded node IDs/counts |
| `NG_SOURCE_BINDING` | Graph source is not the authorized source | node ID, asset IDs |
| `NG_UNKNOWN_COLUMN` | Column ID is absent from the working schema | node ID, column ID |
| `NG_INCOMPATIBLE_TYPE` | Expression/literal/cast/type law rejects the config | node ID, type names |
| `NG_UNSUPPORTED_TARGET` | Existing execution path does not authorize the node/rule | node ID, target kind |
| `NG_LIMIT_*` | A frozen resource bound was exceeded | bound name and numeric bound |
| `NG_PLAN_INVALID` | Emitted existing `LogicalPlan` failed validation/canonicalization | no raw plan/config |
| `NG_INTERNAL` | Unexpected compiler invariant failure | stable generic summary only |

Every failure is returned before connector read, physical lowering, plan
persistence, or materialization. Unknown node types, unknown versions,
unsupported topology, unsupported rules, and failed schema checks are errors,
not warnings. A result containing a partial `LogicalPlan` is never returned.

### 8.2 Secret safety

NodeGraph config, metadata, diagnostics, logs, events, manifests, and persisted
graph/plan payloads must remain secret-free:

- Reuse the existing `ensure_no_secret_fields` policy for JSON objects and
  strings, including secret-like keys and markers such as `password=`,
  `token=`, `api_key=`, `secret=`, and `bearer `.
- Source config contains only an authorized `SourceAsset.id` and logical
  projection. It never contains connection config, credential material,
  filesystem contents, raw paths, access tokens, or a `CredentialRef` value.
- Literal strings, derive expressions, names, labels, and metadata are all
  checked by the same secret-safety policy. Rejection does not echo the value.
- Diagnostics/logging may include bounded IDs, type IDs, port IDs, column IDs,
  logical type names, counts, and stable codes. They must never include raw
  JSON, cell values, credentials, full source paths, or third-party error text.
- Plan fingerprints and canonical digests are identifiers/integrity values;
  they are not a permission boundary or a secret-redaction mechanism.

## 9. Preview mapping and durable execution boundary

The mapping returned by compilation is the only node-level preview mapping:

```text
product NodeId ──identity mapping──> PlanNodeId
                         │
                         └── existing preview target validation/execution
```

Preview later compiles first and delegates to the existing logical-plan preview
path. It must not build a graph-specific prefix plan, run a second compiler, or
create Job/Run/Artifact/PlanVersion records for preview. A preview target must
be one of the emitted non-`Materialize` plan nodes and uses the exact
`nodeSchemas[node_id]` boundary from this compilation result.

Formal execution later persists and publishes the returned `LogicalPlan` using
the existing PlanVersion flow. `PlanVersion.canonical_plan_bytes` and its
existing SHA-256 canonical digest remain authoritative; the graph is not needed
by `JobRuntime`, and no process-local registry or NodeGraph state may be
required to execute a saved plan.

## 10. Explicit non-goals and future boundary

NG-C0, NG-R1, and NG-R2 do not authorize:

- Join, Union, multiple source scans, branches, merges, arbitrary DAGs, or
  multi-output graphs;
- frontend Canvas work or a frontend-generated contract in this repository;
- HTTP routes, OpenAPI, API status mapping, or service authorization handlers;
- a new graph database, graph-specific PlanVersion, job, run, event, snapshot,
  artifact, queue, retry, cancellation, or publication lifecycle;
- Polars, DuckDB, SQL, connector objects, physical expressions, or arbitrary
  code/plugin execution in product contracts;
- a new rule/expression language, optimizer, physical plan, executor, or
  canonicalization/fingerprint algorithm;
- `Validate` / `Deduplicate` in normal NG execution; their existing internal
  verification authorization remains a separate E4 boundary;
- #93 XR work, SQL connector #9, or DuckDB executor #10.

True DAG, multiple-source, Join, and Union execution remain future work under
the existing F-ENG1 / #93 boundary. A future contract must define topology,
port cardinality, join/union semantics, schema reconciliation, determinism,
resource law, preview mapping, and durable execution before any such node is
registered.

## 11. Downstream entry criteria

### NG-R1 (#325)

NG-R1 may start only after this contract is accepted/merged and `main` is
re-fetched at that accepted head. Its implementation must:

- add versioned `NodeGraph`, `NodeConfig`, node/edge/port IDs, config parsing,
  structural validation, limits, and secret-safe metadata checks;
- implement exact built-in `NodeDefinition` records and deterministic closed
  `NodeRegistry` content/order;
- expose a transport-neutral catalog view with type ID, config version/schema,
  ports, display metadata, and support status;
- add serde, duplicate/reference/topology, bounds, unknown version/type,
  secret-safety, and registry determinism tests;
- introduce no compiler, HTTP route, execution, connector, PlanVersion, or
  physical engine code.

### NG-R2 (#326)

NG-R2 may start only after NG-R1 is accepted/merged and `main` is re-fetched at
that accepted head. Its implementation must:

- consume only validated graph/registry data plus an authorized source/schema
  context;
- implement the one-node-to-one-plan-node lowering and identity mapping above;
- implement all schema/column/type/nullability rules and fail-closed errors;
- prove canonical-byte/fingerprint determinism independent of map insertion and
  graph array order;
- run existing `LogicalPlan::validate`, canonicalization, and current linear
  engine preflight without changing `lower.rs` semantics;
- add golden and negative tests for supported chains, unknown columns, type
  failures, invalid topology, Join/Union requests, version failures, bounds,
  secret-like config, and preview mapping;
- introduce no API route, second executor, PlanVersion lifecycle, JobRuntime,
  or #93 XR behavior.

## 12. Objective acceptance matrix for #324

This docs-only delivery is accepted only when the following statements are
testable from the document and repository state:

| Acceptance item | Frozen evidence |
| --- | --- |
| NodeGraph vs LogicalPlan authority is unambiguous | Sections 1, 5, and 9 |
| Phase-1 topology and rejection behavior are explicit | Sections 2.1, 2.2, and 8 |
| Every Phase-1 built-in has a target or explicit unsupported status | Section 4 |
| Column/schema propagation is explicit | Section 6 |
| Determinism and preview mapping are testable | Sections 5.2 and 9 |
| Bounds and compile complexity are testable | Section 7 |
| Versioning and unknown type/config behavior are fail-closed | Section 2.3 |
| Secret safety covers config/metadata/errors/logs/events/persistence | Section 8.2 |
| No second executor/canonical algorithm is introduced | Sections 1, 5.3, and 10 |
| NG-R1 and NG-R2 have concrete entry criteria | Section 11 |
| Docs-only scope is preserved | No Rust, TypeScript, workflow, dependency, or lockfile changes |

## 13. Contract deviations and implementation notes

This document deliberately uses the existing repository contracts rather than
inventing new public physical types:

- `LogicalSchema`, `Expr`, `Rule`, `LogicalPlan`, `PlanNodeId`, existing plan
  canonicalization, and existing engine/preflight bounds remain authoritative.
- `NodeGraph` is not a new execution IR. Its canonical product representation
  and metadata are not accepted in place of `LogicalPlan` canonical bytes.
- Verification-only `Validate` / `Deduplicate` support is explicitly excluded
  from the normal NG path until a separate contract authorizes it.
- All numbers in section 7 are product-input bounds; existing lower-level
  engine/storage bounds remain independently enforced.

No new dependency, public Rust implementation, route, executor, or persistence
format is authorized by NG-C0.
