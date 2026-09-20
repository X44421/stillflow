# Issue #363: stateful operators and multi-source execution contract

> Status: Candidate frozen contract for acceptance by #363
> Risk: L1 — documentation and contract only; it freezes L3-class runtime decisions but authorizes no runtime change
> Parent: #361 (Epic: complete the twelve NodeGraph data-processing capabilities)
> Depends on: #362 (merged, PR #382), design NX-D0 #345 (merged)
> Alignment: #345, #93, ADR-002, and the frozen contracts NG-C0 #324, NX-C0 #335, NX-C1 #340, NX-V0 #342, NX-P0 #346
> Base: `main@361d4e54bc30d2cc8b54f51ed96cd64d8597d147` (post-#382)
> Suggested branch: `agent/issue-363-nx-c1-stateful-multi-source-contract`

This document freezes the ports, schema, resource and publication semantics required to implement
sorting, grouping, cross-batch state and multi-source execution, so that #370, #371, #375, #376, #377,
#378 and #379 have one accepted entry gate. It registers no node, changes no execution behaviour, and
authorizes no runtime work by itself: every slice in §11 still needs its own accepted L3 contract,
implementation issue and evidence.

Per decision D1 of #362, this contract also **carries the F-ENG1 logical Join/Union contract** (§7) and
therefore satisfies the #345 Stage 2 gate "F-ENG1 logical Join/Union contract accepted".

---

## 1. Decision and authority boundary

### 1.1 What this contract freezes

1. The typed-port descriptor, role, cardinality, ordering and identity vocabulary (§2), together with
   the complete legal/illegal port and topology matrix.
2. The closed schema-constraint vocabulary and the field-identity/conflict laws for any multi-input
   graph (§3).
3. Multi-source authorization, source identity, the **source version axis**, and lineage (§4).
4. The shared-producer laws, answering the six questions #345 §6 leaves open (§5).
5. The staged unlocking gates for single-source branching, multi-source merge and multi-output (§6).
6. The F-ENG1 logical Join/Union semantics (§7).
7. Stateful execution laws: stable ordering, tie order, NULL placement, grouping and group boundaries,
   and batch-boundary invariance (§8).
8. Resource, spill, cancellation, retry, backpressure, partial-failure and atomic-publication laws (§9).
9. The logical-plan mapping and preview boundary for typed graphs (§10).
10. The per-slice dependency and minimum acceptance scope for every follow-up runtime issue (§11).

### 1.2 What this contract does not authorize

It does not register Join, Union, aggregation, sort, structural-transform or multi-output nodes; does
not change `NodeGraph`, `NodeDefinition`, `NodeGraph` version 1, `PlanNodeKind`, plan serialization,
`GraphRevision.format_version`, storage, API, preview or publication behaviour; does not lift any paused
capability recorded in #362 §4; does not lift the #93 XR HOLD; and does not enable a second executor,
queue, canonicalizer or publication path.

### 1.3 Authority

`stillflow_plan::LogicalPlan` (canonical bytes + fingerprint `stillflow-fnv1a64x4-v1`) remains the only
execution authority; `Rule`/`Expr` remain the only logical languages; `stillflow-engine` owns preflight,
resources, cancellation, execution and publication; `stillflow-storage` owns atomic visibility and
recovery. A typed port descriptor is **declarative metadata**: it can never carry a connector, a
credential, an expression, a SQL fragment, a physical type, executable code or a resource handle. Port
vocabulary, role, cardinality and schema constraints are never a second execution language.

### 1.4 Stage 0 remains the only supported profile

Until a stage in §6 is separately contracted, accepted and implemented, version 1 stays what it is
today: one source, one output, one connected linear path, `edges == nodes - 1`, `in`/`out` ports only,
`NodeEdge` without slots, repeated connections rejected, and no branches, merges, cycles, disconnected
components or multiple sources/outputs. An older version-1 graph is never silently reinterpreted as a
DAG.

---

## 2. Typed ports

### 2.1 Port descriptor

Every definition participating in a typed graph exposes one bounded, definition-owned descriptor per
port:

```text
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
```

Axis rules, unchanged from #345 §2.1:

* `direction` is the edge endpoint direction, checked against the containing definition's input or
  output set.
* `role` distinguishes the primary data path, an explicitly named auxiliary data path and a non-data
  control path; a role never changes an operator's logical meaning. Role compatibility is **closed**:
  `PrimaryData` connects only to `PrimaryData`, `AuxiliaryData` only to `AuxiliaryData`, `Control` only
  to `Control`. A role mismatch is rejected **before schema inspection**.
* `dataCategory` is the value family carried over the edge. The first typed slice admits only
  `TabularStream`, backed by the existing bounded `BatchEnvelope` stream; `Scalar` and `ControlSignal`
  stay reserved for separately contracted paths.
* `cardinality` bounds the number of graph connections to one port. **There is no unbounded variadic
  port.**
* `ordering` is `None` for `One` and `OptionalOne`, and must be `Ordered` or `Unordered` for
  `BoundedMany`. It controls **connection-slot identity, not stream row order**.
* `required` says whether a valid active node must have a connection for that port, counted inbound for
  an input and outbound for an output. `One` is always required, `OptionalOne` always optional, and
  `BoundedMany(n)` has a minimum of one exactly when `required` is true. **Port presence is a
  definition property, not a promise that a stream has a row.**
* `schemaConstraint` is a closed, inspectable precondition over the logical schema (§3.1), never an
  arbitrary predicate.
* `identity` names the port in diagnostics, preview mapping, lineage and multi-output publication, and
  is **never inferred from an array position**.

Closed identity policies:

| Policy | Identity | Use |
| --- | --- | --- |
| `DefinitionPort` | `(nodeId, portId)` | stable diagnostic, preview and lineage identity |
| `DefinitionPortAndOutputLabel` | `(graphRevision, nodeId, portId, outputLabel)` | only when a publication record needs a logical output label; the label is explicit, bounded, and never an array ordinal |

### 2.2 Named semantic slots

Ports are named semantic slots: a definition must not use the caller's edge-array order to decide which
input is left, right or a union member. The definition owns the stable `portId` and role.

| Operator | Port ids | Meaning |
| --- | --- | --- |
| Branch | `in`, `branch-a`, `branch-b` | one primary input and explicitly named data outputs |
| Join | `left`, `right` | two required primary tabular inputs; left/right are not interchangeable |
| Union | `member` with bounded ordered slots | at least two required tabular inputs in explicit slot order |
| Multi-output transform | `out-data`, `out-rejected` | distinct output identities; no implicit ordinal meaning |

`portId` is definition-owned, ASCII, case-sensitive, versioned with the node definition and stable
across graph edits. **Renaming a port is a compatibility event**: it requires a new definition/config
version or an explicit graph migration.

### 2.3 Arity and repeated connections

| Cardinality | Valid connection count | Ordering |
| --- | ---: | --- |
| `One` | exactly 1; `required` must be true | `None` |
| `OptionalOne` | 0..1; `required` must be false | `None` |
| `BoundedMany(n)` | 0..n, or 1..n when `required` | `Ordered` or `Unordered` |

`maxConnections` is finite and checked **before** graph planning allocates connection state; `n >= 1`.
A repeated connection requires an explicit future wire slot or an equivalent definition-owned member
identity. The current `NodeEdge` shape has no slot and therefore **cannot** represent repeated
connections; version 1 continues to reject them.

### 2.4 Legal / illegal matrix (acceptance item 1)

| Shape | Result | Connector I/O |
| --- | --- | --- |
| Existing single-source `in`/`out` linear path | accept (Stage 0) | allowed only after normal source authorization |
| Missing required input, excess connections, invalid slot, role/category mismatch | typed rejection | **zero** |
| Duplicate output `ColumnId`/name, or empty Join key list | typed schema/plan rejection | **zero** |
| Branch, merge, cycle, disconnected component, or multiple source/output in Stage 0 | typed rejection | **zero** |
| A Stage 1–3 shape submitted before its contract gate is accepted | unsupported capability | **zero** |

Fail-closed combinations, each rejected with zero connector I/O: a required port with zero connections;
`One` with `required=false`; `OptionalOne` with `required=true`; `ordering` not matching cardinality;
more connections than the declared maximum; a duplicate edge plus slot identity; an ordered-many port
with a missing, duplicate or non-contiguous slot; an unordered-many port whose semantic result depends
on arrival order; an edge whose source and target data categories differ; an edge whose roles are
incompatible.

The validator stays iterative and bounded, and rejects cycles and unreachable components before
connector I/O, schema inspection, or execution.

---

## 3. Schema constraints and field identity

### 3.1 Closed schema-constraint vocabulary

```text
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
```

* List order is semantic **only** for `ExactFields`.
* `RequiredFields` matches by `ColumnId`; never by display name or position. `name` is an additional
  compatibility check, never the identity key.
* A future contract may add a new closed constraint, but may not embed a free-form expression or a
  backend-specific schema predicate.
* Schema constraints are **preconditions**; the node's logical operator contract owns the output schema
  transformation, and a catalog entry must not advertise a constraint the validator does not enforce.

### 3.2 Column identity and conflict laws

1. `ColumnId` is the field identity. Names and positions are not identity.
2. The same `ColumnId` in two merge inputs is not by itself an output field. It is accepted only when
   the operator contract explicitly declares a shared semantic field **and** supplies an output mapping
   that either retains exactly one field or derives one new stable `ColumnId`. Without that mapping the
   merge fails with a typed schema-conflict error.
3. Different `ColumnId` values sharing a display name are never silently suffixed or overwritten. The
   graph must rename one field explicitly before a merge, and every emitted `LogicalSchema` must have
   unique names.
4. Implicit casts, nullability repair, field reordering and name-based matching are forbidden at a merge
   boundary unless the owning operator contract explicitly specifies them.
5. Output fields retain deterministic identity and order; no backend may choose a different field order
   or identity representation. The default merge profile rejects any duplicate `ColumnId` or display
   name **before connector reads**, and keep/drop/coalesce/rename behaviour must be an explicit
   operator-level output mapping.

---

## 4. Multi-source authorization, identity and lineage

A graph compile context carries an explicit, bounded set of authorized source bindings:

```text
AuthorizedSourceBinding {
  sourcePort: NodePort,
  sourceAssetId: UUID,
  inputVersionDigest: Digest,        // added by this contract, see below
  authorizedSchema: LogicalSchema,
  schemaFingerprint: LogicalSchemaFingerprint
}
```

Binding rules:

* `sourcePort` is the complete graph endpoint `(sourceNodeId, portId)`, not a bare `PortId`. There is
  exactly one binding per declared source output port, so two source nodes using the same
  definition-owned `out` port remain distinct.
* Every source port names an exact authorized `sourceAssetId`.
* Workspace and capability checks are performed by the existing service authority **before** schema
  resolution.
* Credentials, paths, connector objects and raw source configuration never cross the compiler boundary,
  and a graph can never infer a second source from a file path, name or connector discovery result.
* The first multi-source slice **rejects two distinct source ports that bind to the same asset**; a
  deliberate self-join requires an explicit future alias/reuse contract.
* Source schema fingerprints are compile inputs and provenance, never graph-generated values.
* **Source version axis (closing the #345 §4 gap).** Every binding carries an explicit
  `inputVersionDigest` for the authorized input version. A binding without it fails closed. This is the
  same identity that verification and report outputs must record (#346
  `VerificationExecutionContext.authorizedInputVersionDigest`), and its frozen domain belongs to #364
  (decision D4 of #362). A placeholder or all-zero digest is never acceptable as an authorized input
  version.

Source bindings are compile-time authorization context: they do not become part of the stable logical
rule language and are not a second persistence authority.

---

## 5. Shared producers

A node with more than one outgoing edge is a **shared producer**, not an implicit copy. This contract
answers the six questions #345 §6 requires, as law:

1. **Evaluation model.** Evaluate once and fan out one bounded stream. Per-consumer re-evaluation is not
   authorized: it would change memory, cancellation and provenance accounting. Any future deviation
   requires its own resource and determinism proof.
2. **Backpressure.** Downstream-driven and combined across consumers: the slowest consumer bounds the
   producer. An unbounded producer queue is forbidden; exceeding the bounded buffer fails closed with a
   typed limit error.
3. **Cancellation.** One request context and one cancellation token span all consumers. Cancelling any
   consumer cancels the producer and every sibling consumer, and publishes nothing.
4. **Schema and lineage.** Every branch preserves the producer's schema and carries the same lineage
   identity; a branch may not re-derive or rename fields implicitly.
5. **Accounting.** A shared producer's memory and operator state are charged **exactly once**, not once
   per consumer.
6. **Retry.** Re-executing a fragment with identical inputs yields identical outputs (§9.4); a retry may
   not duplicate side effects or publication.

No shared mutable product-node state may leak into `LogicalPlan` or the job lifecycle.

---

## 6. Staged unlocking

The stages are design stages, not enabled capabilities. Each stage needs its own accepted contract,
executable evidence and implementation issue, and a later stage is never enabled merely because the
validator accepts its shape.

| Stage | Scope | Entry gates |
| --- | --- | --- |
| **0 — version-1 linear** | exactly one source and output, one connected path, `in`/`out` only | the current baseline: always preserved, byte-for-byte and behaviour-for-behaviour |
| **1 — single-source branching** | one authorized source, finite fan-out through named output ports, no fan-in/Join/Union, one named branch boundary per compile target | typed-port descriptors, arity, schema constraints, port identity and branch validation frozen; a deterministic branch-to-`LogicalPlan` mapping frozen; fan-out/backpressure and shared-producer ownership covered by tests; separately authorized implementation that does not change v1 decoding |
| **2 — multi-source merge** | explicit bindings for at least two source slots, one contracted Join or Union shape, one merge output | **F-ENG1 accepted** (§7 satisfies the logical half); Stage 1 or an equivalent typed-port validation contract accepted; bounded join/union state and spill policy with executable evidence; connector and permission behaviour verified from the consuming namespace; no SQL/DuckDB pushdown implied |
| **3 — multi-output boundary** | more than one named output port | a publication contract defining all-or-nothing versus independently committed outputs; `PlanVersion`/`JobRuntime`/Snapshot/Artifact lineage mapped without a second lifecycle; failure, cancellation, restart and partial-output recovery fail closed; reviewed at the risk level of its public and persistence changes |

**Scope ruling for this wave.** The twelve capabilities require Stage 1 (`#378` structural transforms
operate on a single input but need the branch/port vocabulary where a target names a port) and Stage 2
(`#375`, `#376`). They do **not** require Stage 3: rejected rows and reports from `#372`–`#374` flow
through the existing E4 verification artifacts and are never a second graph output. Stage 3 therefore
stays gated and unclaimed by this wave.

---

## 7. F-ENG1 logical Join/Union contract (decision D1)

The semantics below are the frozen logical contract that #345 Stage 2 requires. They authorize no
engine execution: `PlanNodeKind::Join` and `PlanNodeKind::Union` remain rejected at preflight until
#375/#376 land their own runtime contract.

### 7.1 Join

* Left and right are **required, exactly-one** `TabularStream` inputs, named `left` and `right` and not
  interchangeable.
* Join keys are an **ordered list of explicit left/right logical expressions**. No key is inferred from
  names, positions or matching `ColumnId` values.
* A key evaluating to NULL **never equals** another NULL key. Inner, semi and anti joins use this rule;
  outer joins preserve unmatched rows according to their declared join type.
* Incompatible key types fail **before connector reads**. No backend-specific implicit cast or collation
  is allowed.
* The output schema is left fields in left order followed by right fields in right order, and only when
  the resulting ids and names are unique. Otherwise the join fails before connector reads. Dropping,
  renaming or coalescing a field requires an explicit logical output mapping with stable identity (§3.2).
* The key list is non-empty. An empty list is rejected. A cross join requires a separate operator
  contract and is not represented by this Join shape.
* Row order is deterministic and independent of partitioning or hash-map iteration: for each left row in
  left input order, emit all matching right rows in right input order (this also defines duplicate-match
  multiplicity). For Left and Full joins, an unmatched left row is emitted at its left-row position with
  nullable right fields. After all left-driven rows, Right and Full joins append unmatched right rows in
  right input order with nullable left fields. Semi/anti output emits one row per left input row in left
  order; Right/Full matching rows remain left-major and only right-only rows are appended.
* Batch boundaries may change; row values, field identities, field order, NULL behaviour, output
  nullability and declared logical row order may not.

### 7.2 Union

* `member` is a bounded ordered-many `TabularStream` input with at least two members. Member order is
  explicit and semantic.
* Every member must have the same ordered `ColumnId` sequence, exactly equal logical types for each
  corresponding field (including nested parameters and timestamp timezone), and exactly equal names. A
  mismatch fails closed. Union does not align fields by name or position.
* Output field order and identity equal the first member. Output nullability is the deterministic
  logical union of member nullability; no other type widening or implicit cast is introduced.
* Output row order is all rows from member 0, then all rows from member 1, and so on, preserving each
  member's source order. Repartitioning and batch boundaries are not observable.
* NULL stays NULL. Union never deduplicates, sorts or coalesces rows unless a separate logical node
  explicitly requests it.

### 7.3 What remains for the runtime contracts

#375 and #376 still owe, at L3 with executable evidence: the concrete bound values for join/union state
and spill, the preflight admission change, the deterministic lowering of a port-qualified graph into the
existing plan kinds, source-binding enforcement in the service path, memory/cancellation wiring, and the
differential corpus proving v1 graphs are unaffected. §11 fixes their minimum acceptance scope.

---

## 8. Stateful execution laws

These laws bind #370 and everything built on it (#371, #372, #373, #374, #377, #379).

### 8.1 Ordering

1. **Stable ordering.** A sort or ordering declaration defines a total order over the declared keys; rows
   that compare equal keep their relative input order (stable sort), so the result is reproducible.
2. **Tie order is declared, never incidental.** Where stability is not the intended semantic, the
   operator must declare an explicit tie-break key; hash-map, partition or arrival order is never a
   tie-breaker.
3. **NULL placement is explicit.** Each ordering key declares where NULLs sort (first or last), and the
   declaration is part of the configuration surface, not an engine default.
4. **Ordered comparison rules stay fail-closed.** Comparing incompatible ordered types is rejected
   (`OrderedComparisonIncompatible`); a sort or join key over mixed types is never coerced implicitly.

### 8.2 Grouping

1. Group keys are an ordered, non-empty, typed tuple of existing `ColumnId`s.
2. Group equality is exact and typed, including NULL, NaN, signed zero, timestamp, Utf8, Binary and
   timezone laws. NULL groups with NULL; a NULL key is a group, not a dropped row.
3. Group boundaries are determined by declared keys and declared order only. A group must never be split
   across batches in a way that changes its result.

### 8.3 Cross-batch state and the invariance law

**Batch-boundary invariance.** Changing the input batch size — including a size that splits a group or a
duplicate-key run across batches — may not change values, field identity, field order, nullability, row
order, group boundaries or reported counts. Stateful operators therefore carry their state across
batches explicitly and must be cancellable and bounded while doing so.

### 8.4 State ownership

Operator state is owned by the engine's existing accounting (`MemoryTracker`/`AllocatorPhase`), not by a
new subsystem. No stateful operator may keep state in a graph revision, a node definition, or process
globals: a published plan must execute with no editor, graph or registry process state.

---

## 9. Resource, spill, cancellation, retry, backpressure and publication laws

### 9.1 Starting bounds (evidence, not permission to multiply)

`MAX_GRAPH_BYTES = 2 MiB`; `MAX_NODES = 64`; bounded `BatchEnvelope` batches with the existing
`MAX_BATCH_BYTES = 64 MiB` ceiling; `MAX_LIVE_COLUMNAR_PAYLOADS = 3`;
`MAX_OPERATOR_STATE_BYTES = 5 MiB` (`MAX_COMPILED_PLAN_BYTES 4 MiB + MAX_FFI_SCRATCH_BYTES 1 MiB`);
`MAX_ENGINE_PEAK_BYTES = 197 MiB` (`3 × 64 MiB + 5 MiB`); `MAX_ENGINE_CONCURRENT_RUNS = 4`;
`ENGINE_DEFAULT_DEADLINE = 15 min`, `ENGINE_MAX_DEADLINE = 30 min`; plus the existing request,
expression, metadata and compile-work ceilings.

### 9.2 Required new bounds

Before a slice in §11 may execute, it must declare **measured** bounds for every axis it introduces —
edge count, fan-out, fan-in, concurrent branch buffers, schema snapshots, join/aggregation state, spill
bytes and output count — and the bound must be checked **before** allocating the state it limits. A
graph shape that cannot be bounded is rejected before connector I/O. The numeric values are fixed by the
implementing issue with measurement evidence, and they may never exceed the engine laws in §9.1 (a
product bound may tighten, never widen or replace an engine bound).

### 9.3 Backpressure and spill

* Every edge carries bounded envelopes, never an unbounded row collection.
* The runtime remains the owner of memory accounting and cancellation; no operator allocates outside it.
* No hidden prefetch, retry or spill may change observable order or values.
* Spill is either explicitly contracted with a versioned deterministic format, cleanup/recovery rules
  and provenance, **or** the operation fails closed on the memory bound. An uncontracted spill path is
  forbidden.

### 9.4 Cancellation, deadline, retry and partial failure

* **One request context** flows through all branches and merge inputs; no branch may create an
  independent or unbounded deadline, and no stage resets the deadline or multiplies a budget.
* **Checkpoints** occur before source reads, before and after merge-state growth, before spill or output
  publication, and before the final commit.
* Cancellation or deadline expiry **before commit publishes nothing**: no partial Snapshot, Artifact or
  PlanVersion.
* Any permitted deadline overshoot during an uninterruptible region is **disclosed** in run metadata;
  it is never silently absorbed.
* **Retry.** Operators contain no hidden retries. Every fragment must be idempotent: re-executing it
  with identical inputs yields identical outputs, so recovery may replay fragments without inventing a
  second publication path. `EngineError.retryable()` remains the sole retryability authority, and any
  bounded shrink-retry adaptation is disclosed as an adaptation event, never relabelled as a retry.
* **Partial failure** never publishes a partially successful result. A merge or stateful stage either
  publishes its complete output or nothing.
* **Temporary resources** created by a stateful operator (spill files, indexes, scratch buffers) are
  released on success, cancellation, failure and restart; recovery must not depend on them, and an
  abandoned temporary resource must never be treated as a published artifact.

### 9.5 Publication

* Existing storage and runtime authorities remain responsible for atomic visibility, verification,
  recovery and lineage; the staging → fsync'd rename → manifest commit → visibility sequence stays
  solely `stillflow-storage`'s.
* A `NodeGraph` or `GraphRevision` never becomes an execution queue or an executor-owned publication
  record.
* A published `PlanVersion` remains executable without the original graph, package, editor state or
  registry process state.

---

## 10. Logical-plan mapping and preview boundary

* A typed product graph may map to the existing `stillflow_plan::LogicalPlan`, including its existing
  positional Join and Union kinds, **only after** the corresponding logical contract is accepted (§7
  covers Join/Union) and only through the implementing issue's runtime contract.
* The graph is validated first: no connector inspect occurs for an invalid topology or invalid port
  contract.
* Graph edges translate to explicit plan inputs; source and merge input order comes from **named ports
  and declared slots**, never from an unordered collection.
* A product node may map to one or more plan nodes, but the mapping is port-qualified and deterministic.
* `LogicalPlan` remains the only canonical-bytes, digest, fingerprint and execution-identity surface. No
  graph-specific AST, canonicalizer, optimizer, physical plan, executor, queue, retry or publication
  authority is introduced.
* A preview target is `(nodeId, portId)` and resolves to an emitted logical boundary. Preview stays
  non-durable and creates no Job, Run, Artifact or PlanVersion record.
* Changing graph topology or the port profile requires an explicit graph/revision format contract and a
  migration decision (§12).

---

## 11. Runtime slices and per-slice boundaries (acceptance item 2)

Each slice is separately authorized at L3 and may start only when its gate column is satisfied.

| Slice | Issue | Gate | Minimum acceptance scope |
| --- | --- | --- | --- |
| Shared ordering + stateful substrate | #370 | this contract accepted | batch-size invariance; NULL/tie/group-boundary stability; over-limit, cancellation and temporary-resource cleanup; declares its measured bounds (§9.2); no second publication path |
| Forward/grouped fill | #371 | #363, #369, #370 | no cross-group fill; group-head policy; batch continuity; memory limit and cancellation |
| Product admission of dedup | #372 | #363, #364 (E4 laws) | rows actually reduced with a verifiable keep policy; NULL keys; cross-batch duplicates; report/ArtifactRef bound to Run and input version |
| Product admission of validation | #373 | #363, #364 | per-row pass/reject verification; diagnostics locate rule/`nodeId`/`columnId`/field path; report bound to input, PlanVersion, Run; no implicit mode switch |
| Report runtime boundary | #374 | #363, #364 | full vs sampled scope with markers, denominator rules, input fingerprint; no fabricated data; materialized output unchanged |
| Bounded multi-source graph + port execution | #375 | this contract (incl. §7 F-ENG1), Stage 1 accepted or equivalent | v1 graphs still compile/preview/run unchanged; illegal topology rejected before connector I/O; per-input identity, permission, schema and **version** explicit; failure publishes nothing; declares measured merge bounds |
| Join / Union nodes | #376 | #375 | empty key never matches; duplicate keys, NULL, unmatched rows and left/right order per §7; no implicit key conversion; schema uniqueness or fail before reads; output blow-up bounded; memory and cancellation |
| Grouped aggregation | #377 | #370 | empty input, NULL group/metric, overflow and precision; batch-size invariance; `count-distinct` and resource limits; stable output identity/type/provenance across preview, PlanVersion, Run, Artifact and export |
| Array explode / unpivot | #378 | #363; the #362 §4 `ListStructPaused` decision | row-count change, nested paths, empty-array/NULL policy, stable source-record association, expansion bound, v1 regression |
| Bounded pivot | #379 | #377, #378 | duplicate-cell aggregation; new column values/order/identity stability; output-column and memory bounds; NULL/empty input and cancellation |

---

## 12. Compatibility

* Version 1 keeps compiling to byte-identical plans; `LogicalPlan::canonical_bytes()` and the fingerprint
  algorithm are untouched. Evidence is the frozen corpus at
  `backend/crates/stillflow-plan/tests/fixtures/nx-v1/`, never a regenerated fixture.
* `NodeGraph.version` and `configVersion` stay `1`; the catalog stays a version-1 surface. Existing
  definitions are unchanged: the version-1 `in`/`out` ports map to `Input/Output`, `PrimaryData`,
  `TabularStream`, `One`, ordering `None`, required `true`.
* Renaming a port, changing a cardinality or adding a required port is a compatibility event requiring a
  new definition/config version or an explicit graph migration.
* `NodeEdge` cannot express repeated connections; version 1 keeps rejecting them.
* A topology or port-profile change requires a `GraphRevision` `format_version` bump plus a deterministic
  migration of exactly the kind #342 freezes (pure, side-effect-free dry-run, append-only apply,
  idempotent at the known format, no downgrade, no auto-migration). Format 1 is never silently extended.
* Published `PlanVersion` records remain independently executable and are never rewritten.

---

## 13. Acceptance matrix for this delivery

| Issue #363 acceptance item | Where satisfied |
| --- | --- |
| Complete legal/illegal port and topology matrix | §2.1–§2.4, including the zero-I/O rejection rules |
| Every follow-up runtime issue has an explicit dependency and minimum acceptance scope | §11 |
| Resource, failure, cancellation, recovery and result-publication semantics are determined | §9 (bounds, spill, cancellation, retry, partial failure, temporary resources, publication) and §5 (shared producers) |
| docs-only; registers no Join/Union and changes no execution behaviour | §1.2, §14; the delivery touches `docs/contracts/` and `docs/issues/` only |

---

## 14. Non-goals and stop conditions

This delivery does **not**: register Join, Union, sort, aggregation, structural-transform or multi-output
nodes; change `NodeGraph`, `NodeEdge`, `NodeDefinition`, `PlanNodeKind`, plan serialization,
`GraphRevision.format_version`, storage, API, preview or publication behaviour; add a second executor,
queue, canonicalizer, optimizer or publication path; widen or replace any engine or storage bound;
un-pause any capability recorded in #362 §4; enable Stage 3; lift the #93 XR HOLD; modify the frozen
contracts #324/#335/#340/#342/#346 or the #345 design; change OpenShip code; or run (or claim to have
run) any Rust build, test, clippy or fmt.

**Stop and return to contract review** if a slice needs an unlisted public field, a new persistence
format, an executor-owned semantic decision, an unbounded operation, a second runtime/publication
authority, a bound that would widen an engine law, or a topology outside §6.
