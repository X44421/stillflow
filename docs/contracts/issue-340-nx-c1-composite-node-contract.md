# Issue #340: NX-C1 composite product nodes and declarative node packages contract

> Status: Candidate frozen contract for acceptance by #340
> Risk: L1 documentation delivery. Every public contract change it names is
> implemented at L3 by the owning issue (NX-N2 #341).
> Parent: #334
> Predecessors: #324 NG-C0, #335 NX-C0 (§4/§5/§6/§9/§10 remain normative)
> Authorized base: `main@7a5174f0ccfd72573e19c058f90986fc122850b6`
> Suggested implementation branch: `agent/issue-340-nx-c1`

This document revises NG-C0 §4's one-product-node-to-one-plan-node rule for
audited composite product nodes only, freezes the deterministic expansion
mapping, defines the declarative node package format, and freezes the first
composite sample. It authorizes no runtime change by itself; NX-N2 (#341)
implements the slice at L3 under its own risk level.

The invariants of #334 and NX-C0 remain in force: Rust is authoritative; the
existing `LogicalPlan`/`Rule`/`Expr` remain the only logical execution
language; every composite expansion emits only existing plan node kinds and
existing `Rule` variants; preview, `PlanVersion`, `JobRuntime`, and the
snapshot/artifact publication path are reused; already published version-1
plans keep their canonical bytes, digest, column identity, and execution
behavior.

### Document location

The canonical normative text is this file under `docs/contracts/`; an
8-line pointer stub is kept at `docs/issues/issue-340-nx-c1-composite-node-contract.md`.

## 1. Decision, authority, and current-state evidence

The authority table of NG-C0 §1 is unchanged. Composite nodes do not create
a second executor, a second AST, a graph-specific plan-node kind, or a
second registry. A composite product node is *syntax*: it expands, at
compile time, into a bounded linear sequence of plan nodes that the atomic
compiler could have produced from existing atomic nodes. The execution
authority remains `LogicalPlan::canonical_bytes()` and its fingerprint.

Evidence at the authorized base (falsifiable from the repository):

| Observed fact | Evidence |
| --- | --- |
| The compiler emits exactly one plan node per product node and `nodePlanIds[nodeId] == PlanNodeId::from_uuid(nodeId)`. | `backend/crates/stillflow-plan/src/node_graph_compiler.rs` (`compile`, `node_plan_ids` construction); NX-C0 §10.1 |
| `MAX_NODES` (64) bounds product nodes; `MAX_EDGES = 63`; the work estimate counts configs, expressions, schema fields, and metadata. | `backend/crates/stillflow-core/src/node_graph/mod.rs:13-24`; `check_shape_work`/`check_compile_work` |
| The registry is a closed, sorted, duplicate-rejecting definition list. | `backend/crates/stillflow-core/src/node_graph/registry.rs` (NX-N1) |
| Config constraints and support conditions come from one definition source per node type. | `backend/crates/stillflow-core/src/node_graph/definitions/` (NX-N1) |
| Preview targets map to emitted non-materialize plan nodes and preview maps through `nodePlanIds`. | `preview_plan_node_id`, `backend/crates/stillflow-plan/src/node_graph_compiler.rs` |
| There is no graph-level canonical form and no graph identity beyond `graphId`. | NX-C0 §10.2 |

## 2. Delta ledger against NG-C0 and NX-C0

### 2.1 Revised (with a compatibility decision and an objective test)

| ID | Clause | Revision | Owner | Compatibility decision | Objective test |
| --- | --- | --- | --- | --- | --- |
| R-12 | NG-C0 §4 "one product node maps to one logical node / one rule" | A registered **composite** product node may expand into a bounded linear sequence of existing logical operators. Atomic nodes keep the one-to-one rule unchanged. | NX-N2 | Strictly additive: every graph that compiled before this series compiles to byte-identical plans; only graphs that use a registered composite type can expand. | The NX-S1 v1 corpus stays byte-identical; a composite graph's expansion is stable under permutation, restart, and repeated compilation. |
| R-13 | NG-C0 §5.2/§9 (plan-node identity) | Internal plan nodes get deterministic derived `PlanNodeId`s (§4.2) instead of `PlanNodeId::from_uuid(nodeId)`. Product-node mapping entries stay identity-mapped. | NX-N2 | Additive for composite graphs only; atomic graphs are unchanged. `nodePlanIds` keeps one entry per product node; internal nodes are not product-mappable preview targets. | For a fixed graph, package, and authorization, every internal id is a pure function of the product node id and its expansion ordinal; collision with any product id fails closed. |
| R-14 | NG-C0 §6 (per-node schema propagation) | Composite nodes carry a frozen expansion schema contract: the sequence's schema effects compose through the existing shared rule effects (NX-S1). | NX-N2 | Behavior-preserving: expansion replays the shared `rule_effect`/`project_effect` of the atomic nodes it stands for. | A composite node and its hand-written equivalent atomic chain produce identical value, NULL, column order, schema, and error results. |
| R-15 | NG-C0 §3 (closed registration) | A second, audited registration channel exists for **declarative packages** (§6): explicit deployment, content digests, exact versions, closed operator set. No runtime or user-supplied registration. | NX-N2 | Additive; the built-in registry remains closed and untouched. | Unknown package versions, unknown operators, conflicting packages, and over-expansion fail closed with frozen codes; a deployed package cannot mutate built-ins. |

### 2.2 Preserved without change

P-1 The authority table (NG-C0 §1); P-2 the version-1 wire shape of atomic
node graphs (NX-C0 §2.1 P-2..P-6); P-3 the frozen resource numbers and the
work inequality (NG-C0 §7, NX-C0 §9); P-4 the `NG_*` code vocabulary
(§8.1 P-13) — composite failures reuse existing codes, NX-A1's location
fields apply unchanged; P-5 the secret-safety policy; P-6 preview semantics
(§9); P-7 the non-goals (#93 XR HOLD, #9, #10); P-8 the shared semantic
analysis and capability gate (NX-C0 §5); P-9 the single-source, linear,
single-output product path — this series does not open DAG/Join/Union.

### 2.3 Deferred

D-1 Typed ports, fan-in/fan-out, Join/Union (NX-D0 #345). D-2 Draft preview
and verification-node admission (NX-P0 #346). D-3 `GraphRevision`
persistence (NX-V0 #342 / NX-V1 #343).

### 2.4 Compatibility explicitly refused

X-1 Executable callbacks, SQL text, Polars/DuckDB expressions, downloaded
content, or any user code inside a package. X-2 Recursive or cyclic
expansion: a package may reference only atomic operators, never another
composite. X-3 A second AST, plan-node kind, optimizer, or canonicalizer.
X-4 Unpausing any paused capability through expansion. X-5 Widening any
bound, or lifting `MAX_NODES` instead of accounting for the expanded form.
X-6 Runtime package mutation, hot reload, or fallback decoding of unknown
package versions. X-7 Modifying the canonical-byte or fingerprint
algorithms, or persisting a graph-level canonical form.

## 3. Composite product nodes

### 3.1 What a composite node is

A composite product node is a registered `NodeDefinition` whose expansion
replaces its single plan node with an ordered sequence of two or more
existing plan nodes. Its definition declares, in one source (NX-N1 §6.1):

- the type id under the `stillflow.composite.` namespace (§6.2);
- its typed config and machine-readable constraints (NX-C0 §6.2, unchanged);
- the frozen expansion: an ordered list of steps, each step being exactly
  one existing product operator (one of the eleven atomic type ids) with
  its config derived from the composite config by the frozen parameter
  binding (§5);
- input/output port ownership: the composite's `in`/`out` ports belong to
  the first/last internal plan node respectively.

### 3.2 Expansion and plan assembly

The expansion is a pure compile-time function `expand(definition, config)`
→ ordered steps. The compiler then walks the sequence exactly as it walks
atomic nodes today: each step lowers through the same `compile_transform`
logic to the same plan node kind and the same shared rule effects, with the
step's derived config. The composed `nodeSchemas` records the schema after
each internal plan node under the internal ids (§4.2) and after the last
one under the product node id.

The expansion happens **before** all work accounting: `check_shape_work`,
`check_compile_work`, and the serialized-graph bound of NG-C0 §7 are
evaluated against the expanded form, so no bound is circumvented by
expansion. `MAX_NODES` continues to bound the product graph; the expanded
form is bounded by §8's expansion budget.

### 4.2 Internal plan-node identity (R-13)

For product node `p` and step ordinal `n` (0-based), the internal
`PlanNodeId` is

```text
internal_id(p, n) = Uuid::from_u128(bits)
bits = (p_uuid_u128 & !0x0000_0000_0000_FFFF)      // keep the high 112 bits
     | (0x8000 | n)                                 // low 16 bits: marker + ordinal
```

Frozen properties, each mechanically testable:

1. **Determinism**: `bits` depends only on `p_uuid_u128` and `n` — no hash
   iteration, clock, registry order, or environment. Repeated compilation,
   restart, and array permutation produce identical ids.
2. **Injectivity in `n`**: at most 32 767 steps (0x7FFF); the ordinal is
   bounded far below that by §8, so distinct steps of one node get distinct
   ids.
3. **Cross-node collision resistance**: two product nodes `p ≠ q` collide
   only if their uuids differ solely in the low 16 bits. This is not
   assumed impossible — it is **detected**: if any derived internal id
   equals any product node's `PlanNodeId` or another derived id, compilation
   fails closed with `NG_INVALID_CONFIG` and the product node id of the
   offending node. The detection is a set intersection over at most
   `MAX_NODES + expanded` ids.
4. **Non-mappability**: internal ids are never values of `nodePlanIds`;
   `preview_plan_node_id(target)` rejects an internal id with
   `NG_UNSUPPORTED_TARGET` exactly as it rejects any non-emitted target.

### 4.3 Preview, errors, and diagnostics

- The product node previews its **output boundary**: the preview target is
  the composite's product node id, mapping to the last internal plan node.
  Internal steps are not preview targets.
- A failure inside step `n` reports the product node id (never the internal
  id) plus the step-scoped `fieldPath` `steps[n].<field>` (NX-C0 §7.1
  shape). The primary diagnostic remains exactly one per failed compile
  (NX-C0 §7.2), ordered by the same canonical rules with the composite
  node's product id.
- The engine never sees expansion metadata: the compiled plan is an
  ordinary `LogicalPlan`; `nodeSchemas` in the API view lists product nodes
  with their final (post-expansion) schema.

## 5. Controlled parameter binding

A step config is derived from the composite config only through the frozen
binding forms:

| Form | Meaning | Example |
| --- | --- | --- |
| `literal` | A constant value embedded in the package | `{"column": <composite column>}` targets |
| `param` | A named, typed parameter from the composite config | `trim.column := param("column")` |
| `identity` | The same `ColumnId` the composite config carries | the shared `column` parameter |

Binding rules, each failing closed with `NG_INVALID_CONFIG`:

1. Every parameter referenced by any step must be declared in the
   composite definition's config schema (NX-C0 §6.2 constraints apply).
2. A step's `column`-kind parameter must resolve to the composite's
   declared `column` parameter — internal steps cannot address other
   columns, so the composite's schema contract is single-column in, single-
   column out (the first composite sample is single-column; a multi-column
   package requires its own contract).
3. Unknown parameter names, missing bindings, and type-incompatible
   literals are rejected at config validation, before the source schema is
   resolved.
4. Parameter values never enter diagnostics or logs (NX-C0 §7.3).

## 6. Declarative node packages

### 6.1 Package format

A package is a JSON document with exactly these fields (unknown fields
rejected, mirroring R-1):

```json
{
  "packageFormat": 1,
  "namespace": "ops.clean",
  "name": "trim-clean",
  "version": "1.0.0",
  "contentDigest": "sha256-<64 hex of the canonical content>",
  "dependsOn": [],
  "operators": ["stillflow.node.trim", "stillflow.node.replace-literal"],
  "typeId": "stillflow.composite.trim-clean",
  "configVersion": 1,
  "definition": {
    "displayName": "Trim clean",
    "description": "Trim one UTF-8 column, then map empty strings to null.",
    "configSchema": { "fields": ["…frozen NX-N1 field records…"], "additionalProperties": false },
    "expansion": ["…ordered steps with frozen bindings…"]
  }
}
```

`contentDigest` is the SHA-256 of the canonical JSON encoding (UTF-8,
sorted object keys, no whitespace) of every field except `contentDigest`
itself. The digest binds the entire semantics: two packages with the same
namespace/name/version but different content are distinguishable and
colliding — deployment of the second after the first is a frozen conflict
failure (`NG_INVALID_CONFIG` at compile: "package content changed for
<namespace>/<name>@<version>").

### 6.2 Namespace, versioning, and deployment

- `namespace` is `^[a-z][a-z0-9-]{0,63}$`, lower-case, no dot. The type id
  namespace `stillflow.composite.` is reserved for composites and never
  collides with the eleven atomic `stillflow.node.` ids.
- `version` is strict semver `MAJOR.MINOR.PATCH`. **Config/semantic
  versioning**: the package's `configVersion` (wire-facing, exactly `1`
  today) may only change when the config schema breaks wire compatibility;
  the semver MAJOR bumps when the expansion semantics change in any
  observable way. PATCH/MINOR must be semantics-preserving; the content
  digest still differs, so a changed-content same-version deployment is
  refused (§6.1).
- Deployment is explicit and audited: packages are added to the compiled-in
  deployment manifest by the operator (NX-N2 introduces the manifest as an
  in-crate static list; no runtime upload path exists or is authorized).
- Published plans never depend on packages: a published `PlanVersion`
  stores only the compiled `LogicalPlan`; disabling or removing a package
  after publication does not affect the plan's execution (NG-C0 §9 P-15).

### 6.3 The closed operator set

`operators` may name only existing atomic product type ids whose lowering
targets are `ApplyRules`, `Project`, or `Filter` — for this contract:
`stillflow.node.select`, `stillflow.node.filter`, `stillflow.node.rename`,
`stillflow.node.trim`, `stillflow.node.cast`,
`stillflow.node.replace-literal`, `stillflow.node.fill-null`,
`stillflow.node.drop-column`, `stillflow.node.derive-column`. `source` and
`output` are not admissible (single-source/single-output ownership stays
with the graph). The set is closed over the eleven built-ins; a future
operator requires its own contract.

### 6.4 Dependency policy

`dependsOn` is frozen to the empty list. Package-to-package dependencies
require their own contract (they imply expansion composition, refused
here). A non-empty `dependsOn` fails closed at deployment.

## 7. The first composite sample (frozen for NX-N2)

`namespace: "ops.clean"`, `name: "trim-clean"`, `version: "1.0.0"`,
`typeId: "stillflow.composite.trim-clean"`, `configVersion: 1`.

- Config: one required `column` (`valueKind: columnId`), no other fields.
- Support condition: `RequiresType(Utf8)` on `in` (mirrors trim), plus the
  universal `RequiresExecutableType`.
- Expansion (two steps, ordinals 0 and 1):
  1. `stillflow.node.trim` with `column := identity`;
  2. `stillflow.node.replace-literal` with `column := identity`,
     `from: ""` (Utf8 empty string), `to: null`.
- Schema contract: input field `c: Utf8 (nullable k)` → after step 1
  `Utf8 (k)` (trim is schema-preserving) → after step 2 `Utf8 (true)`
  (replace-literal `to: null` widens nullability through the existing
  shared rule effect). Column id, order, and metadata are preserved; no
  field is added or removed.
- Value semantics: each value is trimmed (existing `Trim`), then empty
  strings become null (existing `ReplaceLiteral`). Non-empty trimmed values
  pass through unchanged; a non-UTF-8 input column is rejected at compile
  with `NG_INCOMPATIBLE_TYPE` (same code as atomic trim, §6.3 of NX-C0).
- Equivalence law: the composite graph
  `source → trim-clean(column) → output` and the hand-written atomic graph
  `source → trim(column) → replace-literal(column, "", null) → output`
  produce byte-identical canonical plans up to the internal-id derivation
  (R-13), identical `nodeSchemas`, identical preview values, and identical
  materialized results including NULL handling.

## 8. Resource bounds for this series

Unchanged bounds: NG-C0 §7 and NX-C0 §9 (work inequality, request/response
bytes, diagnostics, deadline law). The additions below count the **expanded
form**; no engine or storage bound may be widened (X-5).

| Dimension | Frozen value | Rationale |
| --- | --- | --- |
| Expansion steps per composite node | ≤ 4 | The sample needs 2; 4 leaves bounded headroom. A step count above 4 fails at deployment. |
| Rules per composite node | ≤ 4, and ≤ `MAX_RULES_PER_NODE` (1) per internal `ApplyRules` node | Preserves the stricter product authority. |
| Expanded graph size | product nodes ≤ `MAX_NODES` (64); internal nodes ≤ 4 × 64 = 256; expanded `edges == expanded nodes − 1` | The expanded form is a linear chain. |
| Expansion depth | 1 (no composite-in-composite, X-2) | No recursion. |
| Compile work | The existing `MAX_COMPILE_WORK` (2 000 000) inequality over the expanded form | Expansion cannot amortize work. |
| Package document | ≤ 16 KiB canonical content; ≤ 8 operators; `dependsOn` empty | Bounded deployment artifacts. |
| Plan-node fan-in/out | unchanged (linear chain, one in / one out) | P-9. |

Boundary tests (NX-N2): a 5-step package fails at deployment; a composite
whose expanded form crosses `MAX_COMPILE_WORK` fails with
`NG_LIMIT_COMPILE_WORK`; a graph that is valid with atomic nodes keeps
compiling after packages are deployed (deployment is additive only).

## 9. Compatibility matrix

| Graph kind | Compiler before #341 | Compiler after #341 | Published plans |
| --- | --- | --- | --- |
| Atomic-only v1 graph | compiles (frozen baseline) | byte-identical plan | unchanged, executable without packages |
| Graph referencing `stillflow.composite.*` before deployment | `NG_UNKNOWN_NODE_TYPE` | `NG_UNKNOWN_NODE_TYPE` (fail closed; no fallback, X-6) | n/a |
| Same graph after explicit deployment | n/a | expands per this contract; deterministic ids; preview on the output boundary | published form is a standard plan, executable with the package removed |
| Package with unknown format/version/operator | n/a | deployment refused | n/a |
| Package content changed under same version | n/a | deployment/compile refused (`NG_INVALID_CONFIG`) | published plans unaffected |

## 10. Downstream entry criteria (NX-N2 #341)

NX-N2 may start only after this contract is accepted and merged and its
dependencies (#335, #336, #337) are merged. Paths frozen for NX-N2:
`backend/crates/stillflow-core/src/node_graph/` (composite definition
records and package deployment manifest), `stillflow-plan/src/`
(`node_graph_compiler.rs` expansion and identity, plus differential tests),
`stillflow-api`/`stillflow-service` (catalog surface for deployed
composites), and tests under `backend/crates/*/tests/`. Must-not list:
everything in §2.4, plus: no second registry path, no package execution
state, no preview of internal steps, no GraphRevision work (D-3).

## 11. Objective acceptance matrix for #340

| Acceptance item | Frozen evidence |
| --- | --- |
| One-to-many mapping with a deterministic ID algorithm and conflict counterexamples | §3.2, §4.2 (properties 1-4, including the collision-detection failure) |
| Determinism across reorder, restart, repeated compilation | §4.2 property 1; §9 matrix |
| Existing graphs and the new version/compatibility matrix | §2.1 R-12, §9 |
| Packages compose only existing semantics; no second AST/executor | §6.3 closed operator set; §2.4 X-1/X-2/X-3 |
| Exact numbers with boundary tests for expansion, rules, depth, bytes, and budget | §8 |
| No runtime code | This PR touches only `docs/issues/` and `docs/contracts/` |

## 12. Explicit non-goals

Composite-in-composite, multi-column packages, package dependencies, DAG or
multi-source graphs, typed ports, draft preview, verification nodes,
`GraphRevision` persistence, runtime package upload, unpausing any
capability, widening any bound, changing Openship code, and any change to
published `PlanVersion` records.
