# Issue #345: typed ports and staged DAG execution boundary

> Status: Design-only delivery for NX-D0
> Risk: L1 documentation design; any DAG, Join, or Union runtime is a
> separate L3 contract and implementation
> Parent: #334
> Dependency: NX-G1 / #344, merged by PR #358
> Design base: main@f43d4566c1913b9fc8e0b1eb1cce5ef17a1e9cd8
> Suggested implementation branch: agent/issue-345-nx-d0

This document is the design deliverable for #345. It freezes the vocabulary,
boundary, and staged entry conditions for a future typed-port graph. It does
not change NodeGraph, NodeDefinition, LogicalPlan, GraphRevision, API
schemas, execution behavior, or persistence. The current version-1 linear
graph remains the only supported product graph after this document lands.

The authoritative references are:

- docs/contracts/issue-324-ng-c0-nodegraph-compiler-contract.md for the
  version-1 NodeGraph boundary;
- docs/contracts/issue-335-nx-c0-node-extension-contract.md for shared node
  semantics, diagnostics, compatibility, and resource laws;
- docs/contracts/issue-340-nx-c1-composite-node-contract.md for the
  single-input/single-output composite boundary;
- docs/contracts/issue-342-nx-v0-graph-revision-contract.md for graph
  revision persistence and migration;
- docs/architecture/adr-002-deterministic-runtime-and-physical-executors.md
  for the LogicalPlan/PhysicalPlan and runtime/executor boundary;
- docs/architecture/xr-task-id-reconciliation.md for canonical XR identifiers.

## 1. Current boundary and authority

The verified NX-G1 baseline is main@f43d4566c1913b9fc8e0b1eb1cce5ef17a1e9cd8,
the merge of PR #358. That gate proves the current composite, revision,
restart, PlanVersion, Job, and Snapshot path over real HTTP and real data. It
also records typed ports and DAG execution as follow-up design work, not as
implemented capability.

The current product graph is intentionally narrower than the logical-plan
representation:

| Surface | Current authority | Current rule |
| --- | --- | --- |
| Product graph | stillflow-core::node_graph::NodeGraph | Version 1; one source, one linear path, one output |
| Port wire identity | NodePort { node_id, port } | Definition-owned ASCII port id; current ports are in and out |
| Node declaration | NodeDefinition | Declarative, built-in, deterministic; input/output port lists are fixed |
| Graph validation | NodeGraph::validated_configs | Rejects branches, merges, cycles, disconnected paths, and alternate paths |
| Execution IR | stillflow_plan::LogicalPlan | The only execution authority; it already validates a logical DAG |
| Runtime | stillflow-engine | Owns preflight, resources, cancellation, execution, and publication |
| Editable graph persistence | GraphRevisionStore | Stores graph input/history; it is not a durable execution authority |
| Published execution | Existing PlanVersion / JobRuntime / Snapshot path | Does not require the original graph or node package |

The future typed-port design must extend the product input boundary without
creating a graph-specific rule language, executor, canonicalizer, job
lifecycle, or publication path. stillflow-core continues to own stable domain
vocabulary, stillflow-plan continues to own logical-plan construction, and the
engine continues to own execution laws.

The existing F-ENG1 boundary remains separate. #345 identifies the decisions
that a future Join/Union contract must make; it does not implement those
operators and does not open the XR physical-executor program.

## 2. Typed-port vocabulary

The following vocabulary is normative for future contracts. These are design
records, not Rust or wire types introduced by this Issue.

### 2.1 Port descriptor

Every node definition that participates in a typed graph exposes a bounded,
definition-owned descriptor for each port:

    PortDescriptor {
      portId: PortId,
      direction: Input | Output,
      role: PrimaryData | AuxiliaryData | Control,
      dataCategory: TabularStream | Scalar | ControlSignal,
      cardinality: One | OptionalOne | BoundedMany(maxConnections),
      ordering: None | Ordered | Unordered,
      required: Boolean,
      schemaConstraint: SchemaConstraint,
      identity: PortIdentityPolicy
    }

The descriptor is declarative metadata. It cannot carry a connector, a
credential, an expression, a SQL fragment, a physical type, executable code,
or a resource handle.

The axes have separate meanings:

- direction is the edge endpoint direction and is checked against the
  containing definition's input or output set;
- role distinguishes the primary data path, an explicitly named auxiliary
  data path, and a non-data control path. A role never changes the logical
  meaning of an operator;
- dataCategory is the value family carried over the edge. The first future
  DAG slice admits only TabularStream, backed by the existing bounded
  BatchEnvelope stream. Scalar and ControlSignal are reserved for separately
  contracted paths;
- cardinality bounds the number of graph connections to one port. There is no
  unbounded variadic port;
- ordering is `None` for `One` and `OptionalOne`, and is required to be
  `Ordered` or `Unordered` for `BoundedMany`. It controls connection-slot
  identity, not stream row order;
- required says whether a valid active node must have a connection for the
  port, counted inbound for an input and outbound for an output. `One` is
  always required, `OptionalOne` is always optional, and `BoundedMany(n)`
  has a minimum of one exactly when `required` is true. Port presence is a
  definition property, not a promise that a stream has a row;
- schemaConstraint is a closed, inspectable precondition over the logical
  schema, not an arbitrary predicate;
- identity specifies how the port is named in diagnostics, preview mapping,
  lineage, and multi-output publication. Identity is never inferred from an
  array position.

The closed identity policies are:

- `DefinitionPort`: `(nodeId, portId)` is the stable diagnostic, preview,
  and lineage identity;
- `DefinitionPortAndOutputLabel`: `(graphRevision, nodeId, portId,
  outputLabel)` is used only when a publication record needs a logical output
  label in addition to the port identity. The label is explicit, bounded, and
  never an array ordinal.

Role compatibility is also closed: `PrimaryData` connects only to
`PrimaryData`, `AuxiliaryData` only to `AuxiliaryData`, and `Control` only
to `Control`. A role mismatch is rejected before schema inspection. The first
future DAG slice accepts only `PrimaryData + TabularStream`; the other role and
category combinations remain reserved until separately contracted.

For the existing version-1 graph, the conceptual mapping is:

| Existing port | Direction | Role | Data category | Cardinality | Ordering | Required |
| --- | --- | --- | --- | --- | --- | --- |
| in on a transform/output | Input | PrimaryData | TabularStream | One | None | true |
| out on a source/transform | Output | PrimaryData | TabularStream | One | None | true |

No existing version-1 definition is changed by this design.

### 2.2 Port roles and named semantic slots

Ports are named semantic slots. A definition must not use a caller's edge
array order to decide whether an input is left, right, or a union member. The
definition owns the stable portId and role.

Examples for a later F-ENG1-compatible definition are:

| Operator | Port ids | Meaning |
| --- | --- | --- |
| Branch | in, branch-a, branch-b | One primary input and explicitly named data outputs |
| Join | left, right | Two required primary tabular inputs; left/right are not interchangeable |
| Union | member with bounded ordered slots | At least two required tabular inputs in explicit slot order |
| Multi-output transform | out-data, out-rejected | Distinct output identities; no implicit ordinal meaning |

portId is definition-owned, ASCII, case-sensitive, versioned with the node
definition, and stable across graph edits. Renaming a port is a compatibility
event requiring a new definition/config version or an explicit graph migration.

### 2.3 Arity and repeated connections

The cardinality rules are:

| Cardinality | Valid connection count | Ordering |
| --- | ---: | --- |
| One | exactly 1; `required` must be true | `None` |
| OptionalOne | 0..1; `required` must be false | `None` |
| BoundedMany(n) | 0..n, or 1..n when required | `Ordered` or `Unordered` |

maxConnections is finite and checked before graph planning allocates
connection state. `n` must be at least 1. A repeated connection requires an
explicit future wire slot or an equivalent definition-owned member identity.
The current NodeEdge shape has no slot and therefore cannot represent repeated
connections; version 1 must continue to reject them.

The following combinations fail closed:

- a required port with zero connections;
- `One` with `required=false`, `OptionalOne` with `required=true`, or
  `ordering` not matching the cardinality;
- more connections than the declared maximum;
- duplicate edge plus slot identity;
- an ordered-many port with a missing, duplicate, or non-contiguous slot;
- an unordered-many port whose semantic result depends on arrival order;
- an edge whose source and target data categories differ;
- an edge whose source output role or target input role is incompatible.

The minimum validation matrix is therefore:

| Shape | Result | Connector I/O |
| --- | --- | --- |
| Existing single-source `in`/`out` linear path | Accept in Stage 0 | Allowed only after normal source authorization |
| Missing required input, excess connections, invalid slot, role/category mismatch | Typed rejection | Zero |
| Duplicate output ColumnId/name or empty Join key list | Typed schema/plan rejection | Zero |
| Branch, merge, cycle, disconnected component, or multiple source/output in Stage 0 | Typed rejection | Zero |
| Stage 1–3 shape before its contract gate is accepted | Unsupported capability | Zero |

The graph validator remains iterative and bounded. It must reject cycles and
unreachable components before connector I/O, schema inspection, or execution.

## 3. Schema constraints and field identity

### 3.1 Closed schema-constraint vocabulary

For TabularStream, a future port may declare one of these closed constraints:

    SchemaConstraint =
      AnyLogicalSchema
      RequiredFields([FieldRequirement])
      ExactFields([FieldRequirement])
      SameAsInput(portId)

    FieldRequirement {
      columnId: ColumnId,
      logicalType: Optional<LogicalType>,
      nullable: Optional<Boolean>,
      name: Optional<String>
    }

The list order is semantic only for ExactFields. RequiredFields matches by
ColumnId; it does not match by display name or position. name is an additional
compatibility check, never the identity key. A future contract may add a new
closed constraint, but it may not embed a free-form expression or
backend-specific schema predicate.

Schema constraints are preconditions. The node's logical operator contract
owns the output schema transformation. A catalog entry must not advertise a
constraint the validator does not enforce.

### 3.2 Column identity and conflicts

The following rules apply to any future multi-input graph:

1. ColumnId is the field identity. Names and positions are not identity.
2. The same ColumnId appearing in two merge inputs is not, by itself, an output
   field. It is accepted only when the operator contract explicitly declares a
   shared semantic field and supplies an output mapping that retains exactly
   one field or derives one new, stable ColumnId. Without that mapping, the
   merge fails with a typed schema-conflict error.
3. Different ColumnId values with the same display name are not silently
   suffixed or overwritten. The graph must rename one field explicitly before
   a merge; every emitted LogicalSchema must have unique names.
4. Implicit casts, nullability repair, field reordering, and name-based
   matching are forbidden at a merge boundary unless the owning logical
   operator contract explicitly specifies them.
5. Output fields retain deterministic identity and order. A backend may not
   choose a different field order or identity representation. The default
   merge profile rejects any duplicate ColumnId or display name before
   connector reads; keep/drop/coalesce/rename behavior must be an explicit
   operator-level output mapping.

### 3.3 Join semantics required by F-ENG1

The first Join contract must freeze these behaviors before a Join node can be
registered:

- left and right are required, exactly-one TabularStream inputs;
- join keys are an ordered list of explicit left/right logical expressions;
  no key is inferred from names, positions, or matching ColumnId values;
- a key evaluating to NULL never equals another NULL key. Inner, semi, and
  anti joins use this rule; outer joins preserve unmatched rows according to
  their declared join type;
- incompatible key types fail before connector reads. No backend-specific
  implicit cast or collation is allowed;
- the output schema is left fields in left order followed by right fields in
  right order only when the resulting IDs and names are unique. Otherwise the
  join fails before connector reads. Dropping, renaming, or coalescing a field
  requires an explicit logical output mapping with stable identity;
- the key list is non-empty. An empty list is rejected; a cross join requires
  a separate operator contract and is not represented by this Join shape;
- row order is deterministic and independent of partitioning or hash-map
  iteration. For each left row in left input order, emit all matching right
  rows in right input order; this also defines duplicate-match multiplicity.
  For Left and Full joins, an unmatched left row is emitted at its left-row
  position with nullable right fields. After all left-driven rows, Right and
  Full joins append unmatched right rows in right input order with nullable
  left fields. Semi/anti output emits one row per left input row in left order;
  Right/Full matching rows remain left-major, and only right-only rows are
  appended;
- batch boundaries may change, but row values, field identities, field order,
  NULL behavior, output nullability, and declared logical row order may not
  change.

These rules describe the semantic slots for F-ENG1. They do not authorize
engine execution, a DuckDB path, SQL pushdown, or an executor selection change.

### 3.4 Union semantics required by F-ENG1

The first Union contract must freeze these behaviors before a Union node can be
registered:

- member is a bounded ordered-many TabularStream input with at least two
  members. Member order is explicit and semantic;
- every member has the same ordered ColumnId sequence, exactly equal logical
  types for each corresponding field (including nested parameters and
  timestamp timezone), and exactly equal names. A mismatch fails closed;
  Union does not align fields by name or position;
- output field order and identity equal the first member. Output nullability is
  the deterministic logical union of member nullability; no other type
  widening or implicit cast is introduced;
- output row order is all rows from member 0, then all rows from member 1,
  and so on, preserving each member's source order. Repartitioning and batch
  boundaries are not observable;
- a NULL value remains NULL. Union never deduplicates, sorts, or coalesces
  rows unless a separate logical node explicitly requests that behavior.

These are conservative semantic defaults. Any deliberate deviation must be
recorded in the F-ENG1 frozen contract with executable evidence.

## 4. Multi-source authorization and lineage

A future graph compile context must carry an explicit, bounded set of
authorized source bindings:

    AuthorizedSourceBinding {
      sourcePort: NodePort,
      sourceAssetId: UUID,
      authorizedSchema: LogicalSchema,
      schemaFingerprint: LogicalSchemaFingerprint
    }

The binding rules are:

- `sourcePort` is the complete graph endpoint `(sourceNodeId, portId)`, not
  a bare PortId. There is exactly one binding per declared source output port;
  two source nodes using the same definition-owned `out` port therefore
  remain distinct;
- every source port names an exact authorized sourceAssetId;
- workspace and capability checks are performed by the existing service
  authority before schema resolution;
- credentials, paths, connector objects, and raw source configuration do not
  cross the graph compiler boundary;
- a graph cannot infer a second source from a file path, name, or connector
  discovery result;
- the first multi-source slice rejects two distinct source ports that
  accidentally bind to the same asset. A deliberate self-join requires an
  explicit future alias/reuse contract;
- source schema fingerprints are inputs to compilation and provenance, not
  graph-generated values.

The source bindings are compile-time authorization context. They do not become
part of the stable logical rule language or a second persistence authority.

## 5. Staged topology and runtime gates

The stages below are design stages, not enabled capabilities. Each stage needs
its own accepted contract, executable evidence, and implementation issue.

### Stage 0 — version-1 linear compatibility

This is the current supported profile:

- exactly one source and one output;
- one connected path;
- one incoming and one outgoing edge for transform nodes;
- only the existing in/out ports;
- no branches, merges, cycles, multiple sources, or multiple outputs;
- compile to the existing stillflow_plan::LogicalPlan;
- preview, PlanVersion, JobRuntime, Snapshot, and Artifact paths remain
  unchanged.

Any future typed-port implementation must retain this profile byte-for-byte
and behavior-for-behavior for existing version-1 graphs and published plans.

### Stage 1 — single-source branching

Candidate scope:

- one authorized source;
- finite fan-out through named output ports;
- no multi-source fan-in, Join, Union, or automatic executor selection;
- a compile target identifies one named branch boundary;
- a branch may be selected for preview or a separately contracted execution
  request, but no multi-output publication is implied.

Required entry gates:

- typed-port descriptors, arity, schema constraints, port identity, and
  branch validation are frozen;
- a deterministic branch-to-LogicalPlan mapping is frozen;
- fan-out/backpressure and shared-subgraph ownership are covered by tests;
- the implementation issue is separately authorized and does not change
  version-1 decoding or published-plan execution.

### Stage 2 — multi-source merge

Candidate scope:

- explicit source bindings for at least two source slots;
- only a separately contracted Join or Union shape;
- one merge output until multi-output publication is independently contracted;
- explicit schema conflict, NULL, order, resource, cancellation, and failure
  behavior.

Required entry gates:

- F-ENG1 logical Join/Union contract accepted;
- Stage 1 or an equivalent typed-port validation contract accepted;
- bounded join/union state and spill policy have executable evidence;
- connector and permission behavior is verified from the consuming namespace;
- no SQL/DuckDB physical pushdown is implied by the logical contract.

### Stage 3 — multi-output boundary

Candidate scope:

- more than one named output port;
- output identity is (graph revision, node id, port id) plus the explicit
  logical output label where publication needs one;
- preview targets and lineage are port-qualified;
- each output has a deterministic schema and bounded publication policy.

Required entry gates:

- a publication contract defines all-or-nothing versus independently committed
  outputs;
- PlanVersion, JobRuntime, Snapshot, and Artifact lineage are mapped without
  creating a second lifecycle;
- failure, cancellation, restart, and partial-output recovery behavior are
  executable and fail closed;
- the multi-output implementation is separately reviewed at the risk level
  of its public and persistence changes.

The stages are ordered because fan-out, fan-in, and publication multiply the
resource and failure surface. A later stage cannot be enabled merely because
the graph validator accepts its shape.

## 6. Shared subgraphs and dataflow ownership

A node with more than one outgoing edge is a shared producer, not an implicit
copy operation. A future runtime contract must state:

1. whether the producer evaluates once and fans out one bounded stream, or
   evaluates separately per consumer;
2. how consumer backpressure is combined without allowing an unbounded
   producer queue;
3. how cancellation by one consumer affects the producer and sibling
   consumers;
4. how schema and lineage are preserved on every branch;
5. how a shared producer's memory and operator state are charged exactly once;
6. how a retry avoids duplicate side effects and duplicate publication.

The default design is evaluate-once, bounded fan-out with downstream-driven
backpressure. A future contract may choose another behavior only with an
explicit resource and determinism proof. No shared mutable product-node state
may leak into the existing LogicalPlan or job lifecycle.

## 7. Resource, cancellation, and atomic-publication laws

The current limits are the starting evidence, not permission to multiply them:

- MAX_GRAPH_BYTES = 2 MiB;
- MAX_NODES = 64;
- bounded BatchEnvelope batches, including the existing 64 MiB batch-byte
  ceiling;
- MAX_LIVE_COLUMNAR_PAYLOADS = 3;
- MAX_ENGINE_CONCURRENT_RUNS = 4;
- existing request, expression, metadata, and compile-work ceilings.

A future DAG contract must add measured bounds for edge count, fan-out,
fan-in, concurrent branch buffers, schema snapshots, join state, spill bytes,
and output count. The bound must be checked before allocating the state it
limits. A graph shape that cannot be bounded is rejected before connector I/O.

Backpressure and spill laws:

- every edge carries bounded envelopes, never an unbounded row collection;
- the runtime remains the owner of memory accounting and cancellation;
- no hidden prefetch, retry, or spill may change observable order or values;
- spill is either explicitly contracted with a versioned deterministic format,
  cleanup/recovery rules, and provenance, or the operation fails closed on the
  memory bound;
- batch boundaries are not semantic, but row order and field identity are.

Cancellation and deadline laws:

- one request context flows through all branches and merge inputs;
- checkpoints occur before source reads, before/after merge state growth,
  before spill/output publication, and before the final commit;
- cancellation or deadline before commit publishes no partial Snapshot,
  Artifact, or PlanVersion;
- any permitted deadline overshoot is disclosed using existing runtime laws;
  a branch must not create an independent unbounded deadline.

Publication laws:

- existing storage and runtime authorities remain responsible for atomic
  visibility, verification, recovery, and lineage;
- a NodeGraph or GraphRevision never becomes an execution queue or an
  executor-owned publication record;
- a published PlanVersion remains executable without the original graph,
  package, editor state, or registry process state.

## 8. Logical-plan mapping and preview boundary

The future compiler may map a typed product graph to the existing
stillflow_plan::LogicalPlan, including its existing positional Join and Union
node kinds, only after the corresponding logical contract is accepted.

Mapping rules:

- the graph is validated first; no connector inspect occurs for an invalid
  topology or invalid port contract;
- graph edges translate to explicit logical-plan inputs; source and merge input
  order comes from named ports and declared slots, never from an unordered
  collection;
- a product node may map to one or more logical-plan nodes, but the mapping is
  port-qualified and deterministic;
- LogicalPlan remains the only canonical bytes, digest, fingerprint, and
  execution identity surface;
- no graph-specific AST, canonicalizer, optimizer, physical plan, executor,
  queue, retry, or publication authority is introduced;
- a future preview target is (nodeId, portId) and resolves to an emitted
  logical boundary. Preview remains non-durable and does not create Job, Run,
  Artifact, or PlanVersion records.

GraphRevision stores the editable graph document and migration history.
Changing graph topology or port profile requires an explicit graph/revision
format contract and migration decision; an older version-1 graph is never
silently reinterpreted as a DAG.

## 9. Subsequent delivery slices

This Issue does not invent or dispatch implementation Issue numbers. The
minimum follow-up topology is:

| Slice | Required outcome | Gate |
| --- | --- | --- |
| Typed-port model/validation | Versioned descriptors, port compatibility, arity, slots, schema constraints, and fail-closed diagnostics | This design accepted; separate contract |
| Single-source branch compiler | Deterministic branch topology and one-target preview mapping | Typed-port contract plus resource/backpressure evidence |
| F-ENG1 Join/Union contract | Logical NULL, ordering, schema, key, and lineage semantics | Independent architecture/logic acceptance |
| Multi-source runtime | Explicit source authorization and bounded merge execution | F-ENG1 plus runtime resource/cancellation contract |
| Multi-output contract | Port-qualified preview, PlanVersion, Snapshot, and Artifact publication | Independent persistence/publication acceptance |
| Automatic physical selection | Executor conformance, provenance, and XR gates | XR-C0/XR-R0/XR-R1/XR-G1; outside #345 |

The first three rows may be documented independently, but no row authorizes
production runtime changes by itself. Implementation must use the existing
dependency direction and the lowest risk level matching its changed authority
surface.

## 10. Acceptance matrix

| #345 requirement | This document |
| --- | --- |
| Typed-port role, arity, requiredness, data category, and schema constraints | §2 and §3 |
| Multi-output identity | §2.1, §2.2, and Stage 3 in §5 |
| Single-source branch → multi-source merge → multi-output stages | §5 |
| Multi-source authorization and lineage | §4 |
| Column identity/conflict and schema merge behavior | §3.2 |
| Join/Union NULL and order semantics | §3.3 and §3.4 |
| Shared-subgraph reuse | §6 |
| Backpressure, memory, spill, cancellation, and atomic publication | §7 |
| Alignment with #81 F-ENG1, #93, ADR-002, and XR reconciliation | §1, §3.3, §5, and §8 |
| Exact version-1 compatibility and explicit unsupported boundaries | §1, Stage 0, §8, and §11 |
| Future minimum runtime slices and dependencies | §9 |

## 11. Non-goals and explicit stop conditions

This delivery does not:

- change Rust, TypeScript, API, OpenAPI, schema, serialization, or storage
  code;
- alter NodeGraph version 1, NodePort, NodeDefinition, or the current
  NodeRegistry;
- register Join, Union, multi-source, branch, multi-output, or control ports;
- enable arbitrary DAG execution, SQL pushdown, DuckDB execution, or automatic
  physical-executor selection;
- change LogicalPlan, PlanVersion, Job, Run, Snapshot, Artifact, verification,
  retry, cancellation, or publication semantics;
- create a second Rule/Expr language, executor, queue, or canonicalizer;
- modify Openship production code;
- use Registry claims or locks for this L1 docs-only delivery;
- clear the XR HOLD or imply that #93 has been unblocked.

Stop and return to contract review if a follow-up implementation needs an
unlisted public field, a new persistence format, an executor-owned semantic
decision, an unbounded operation, or a second runtime/publication authority.
