# Issue #346: draft upstream preview and verification-node boundary

> Status: Design-only delivery for NX-P0
> Risk: L1 documentation design; preview or verification execution changes
> require separate L3 contracts and implementation tasks
> Parent: #334
> Dependency: NX-G1 / #344, merged by PR #358
> Design base: `main@4583b8e654172ff4aef8d8ec7b29ee93e5bcd40d`
> Suggested implementation branch: `agent/issue-346-nx-p0`

This document defines the boundary for previewing a valid upstream portion of
an incomplete or invalid draft graph and for admitting `Validate` /
`Deduplicate` nodes to the existing E4 verification path. It does not change
the NodeGraph wire format, compiler, `LogicalPlan`, Preview, Job, Run,
Artifact, storage, or API behavior.

The current version-1 graph and preview behavior remain in force until a later
runtime contract explicitly accepts the candidate in section 6. This is a
future admission boundary, not a new executable capability.

## 1. References and current-state evidence

The following documents remain authoritative:

- [`docs/contracts/issue-324-ng-c0-nodegraph-compiler-contract.md`](issue-324-ng-c0-nodegraph-compiler-contract.md): product graph, compiler, preview mapping, and durable execution authority;
- [`docs/contracts/issue-335-nx-c0-node-extension-contract.md`](issue-335-nx-c0-node-extension-contract.md): shared semantics, diagnostics, bounds, and secret safety;
- [`docs/contracts/issue-340-nx-c1-composite-node-contract.md`](issue-340-nx-c1-composite-node-contract.md): composite package identity and expansion;
- [`docs/contracts/issue-342-nx-v0-graph-revision-contract.md`](issue-342-nx-v0-graph-revision-contract.md): immutable graph revisions and source/package binding metadata;
- [`docs/issues/issue-345-nx-d0-typed-ports-dag-execution-boundary.md`](../issues/issue-345-nx-d0-typed-ports-dag-execution-boundary.md): typed ports, staged DAG candidates, and multi-source identity;
- [`docs/issues/issue-050-node-preview-contract.md`](../issues/issue-050-node-preview-contract.md): existing E3 preview target, full-plan validation, limits, and read-only behavior;
- [`docs/issues/issue-054-validation-rejected-rows-contract.md`](../issues/issue-054-validation-rejected-rows-contract.md): E4 Validate/Deduplicate semantics and verification artifacts;
- [`docs/issues/issue-314-svc-a2-verification-report-refs-contract.md`](../issues/issue-314-svc-a2-verification-report-refs-contract.md): verification report ArtifactRef boundary; and
- [`docs/architecture/adr-002-deterministic-runtime-and-physical-executors.md`](../architecture/adr-002-deterministic-runtime-and-physical-executors.md): runtime, executor, resource, cancellation, and publication ownership.

NX-G1 was rechecked before this design work. PR #358 is merged at
`f43d4566c1913b9fc8e0b1eb1cce5ef17a1e9cd8`, its accepted head is
`ac3439b6a5096212a37567931febca331efb0d3b`, and its five required CI checks
completed successfully. Its acceptance evidence covers real HTTP values,
composite/atomic equivalence, GraphRevision CAS and migration, restart
durability, PlanVersion publication, Job, and committed Snapshot. The current
`main` additionally contains the merged NX-D0 design at the design base above.

## 2. Authority and vocabulary

The authority chain is unchanged:

```text
GraphRevision / NodeGraph
        -> NodeDefinition / NodeRegistry
        -> shared graph and semantic validation
        -> existing LogicalPlan authority
        -> existing Preview or E4 verification path
        -> existing storage publication path, when authorized
```

The terms below are design vocabulary only; they are not new wire enums,
database states, or API fields in this issue.

### 2.1 Validation modes

| Mode | Meaning | Required result |
| --- | --- | --- |
| `ReleaseValidation` | The graph is intended to produce a publishable plan | Every node, edge, package, source binding, schema, and semantic dependency is valid; any error rejects |
| `DraftPreview` | A user wants to inspect a target before the whole draft is publishable | The target dependency closure is valid; safe non-dependent defects may be returned as isolated diagnostics but are never executed |
| `VerificationExecution` | A full valid plan explicitly enters E4 verification | The request carries an E4 context and uses only E4 bundle/artifact rules; ordinary materialization is not upgraded |

`DraftPreview` is not a weaker release check. It asks whether one target can
be evaluated safely with the authorized input. `ReleaseValidation` asks whether
the entire graph can become a durable published plan. A draft preview never
establishes the latter.

### 2.2 Target dependency closure

For target `t`, `Closure(t)` is the bounded backward dependency closure of `t`,
including the target and its input ports, every upstream node and edge needed
to produce it, every source binding and authorized schema used by that closure,
every package/definition version/composite expansion/parameter binding used by
it, and the typed-port checks needed to prove one unambiguous input and target
output.

The closure uses explicit `NodePort` identities and validated edges, never
array order, display names, or a guessed source. The validator is iterative and
bounded under NX-C0 and must stop before connector I/O if the closure cannot be
determined deterministically.

`ReleaseValidation` validates the entire graph. `DraftPreview` may isolate a
defect outside the closure only when it cannot alter the closure, its input
binding, its target identity, or validation safety.

### 2.3 Blocking and isolated diagnostics

- `blocking`: the requested mode cannot safely continue;
- `isolated`: a non-closure defect is reported without inspecting, reading, or
  executing that defective region.

`isolated` is not success and never permits publication. A future preview
response containing one must carry an explicit draft-only/non-publishable
status. Diagnostics stay bounded, ordered, and sanitized under NX-C0; they may
not expose unauthorized assets, paths, credentials, package bodies, or cells.

## 3. Validation order and dependency rules

The future admission path must preserve this observable order:

1. decode the graph envelope and enforce body, depth, count, byte, and secret
   limits;
2. validate graph/revision version, identity, duplicate tuples, endpoints,
   port direction/role/category/cardinality, and bounded slots;
3. establish the target and compute `Closure(t)` without consulting a
   connector;
4. resolve the closed node/package catalog and exact package version/content
   digest and expansion bounds;
5. validate closure semantics, including shared typing, schema effects,
   ColumnId identity, and target output mapping;
6. bind closure sources to authorized schema identities and check Schema drift;
7. apply the mode-specific capability check; and
8. only after all earlier checks pass, enter the existing read-only Preview or
   the separately authorized E4 verification path.

No step may perform connector `inspect`, `read_batches`, stream polling,
storage writes, or publication before its checks pass. Internal check fusion is
allowed only if these fail-closed and zero-I/O properties remain observable.

### 3.1 Always-blocking safety checks

These are blocking even outside `Closure(t)` because they make the request
unsafe or ambiguous:

- unknown/newer graph or revision format;
- malformed or duplicate node/edge/source identity, invalid endpoint, or an
  edge that cannot be decoded as a `NodePort`;
- request/body/depth/diagnostic/compile-work limits exceeded;
- secret-safety rejection or authorization failure that would reveal protected
  resource existence;
- missing/ambiguous target, a non-output target, or a target not reachable from
  an authorized source; and
- a graph-level identity or port conflict that makes target mapping
  non-deterministic.

All produce zero connector I/O and no partial plan.

### 3.2 Closure checks

Any invalidity in `Closure(t)` blocks all three modes: upstream bad config,
unknown package/version, required cycle, incompatible port/schema edge, missing
source binding, source Schema drift, or indeterminate target output identity.

The runtime must not preview the last known-good prefix when the target closure
is invalid. There is no partial LogicalPlan authority in that case.

### 3.3 Non-closure checks

For `DraftPreview`, an error may be isolated only when the offending component
is outside `Closure(t)`, no edge/binding/package/schema dependency crosses into
the closure, target mapping is deterministic, and the diagnostic is safe to
emit. The same error blocks `ReleaseValidation`. It also blocks
`VerificationExecution`, which requires a complete publishable logical input
and auditable full-plan identity.

## 4. Draft-preview decision matrix

The following matrix is normative for the future `DraftPreview` contract. “0”
means zero connector inspect/read/stream I/O.

| Condition | ReleaseValidation | DraftPreview outside `Closure(t)` | DraftPreview in `Closure(t)` | I/O |
| --- | --- | --- | --- | ---: |
| Unknown graph/revision version | Reject | Reject | Reject | 0 |
| Malformed/duplicate node, edge, or port identity | Reject | Reject if target identity is ambiguous; otherwise isolate | Reject | 0 |
| Missing endpoint, role/category mismatch, invalid cardinality/slot | Reject | Isolate only for a disconnected component with a known boundary | Reject | 0 |
| Cycle | Reject | Isolate only for a disconnected cycle with no crossing dependency | Reject | 0 |
| Disconnected node/component | Reject | Isolate if it is not a source, target, or closure dependency | Reject | 0 |
| Unknown package/version/content digest | Reject | Isolate if it is outside the closure and no unknown port/schema crosses into it | Reject | 0 |
| Invalid downstream config/semantic rule | Reject | Isolate if independent of the target closure | Reject | 0 |
| Invalid upstream config/semantic rule | Reject | Reject | Reject | 0 |
| Source asset/Schema drift | Reject | Isolate only for a source outside the closure | Reject | 0 |
| Duplicate ColumnId/name at a merge | Reject | Isolate only for a non-closure merge with no boundary effect | Reject | 0 |
| Missing/ambiguous/Materialize target | Reject | Reject | Reject | 0 |
| Unsupported DAG/Join/Union before its gate | Reject | Isolate only for a non-closure component with a known boundary | Reject | 0 |
| `Validate`/`Deduplicate` in ordinary Preview | Current E3 reject | Current E3 reject until separate admission contract | Current E3 reject | 0 |
| Envelope or safety resource bound exceeded | Reject | Reject | Reject | 0 |
| Non-closure expansion exceeds a future local bound | Reject | Isolate if safe envelope bounds remain intact | Reject if closure-dependent | 0 |

An isolated downstream defect is never silently dropped: it is returned with a
bounded diagnostic and draft-only status. A later release validation treats the
same defect as a blocker.

For the current version-1 single-source linear graph, existing E3 behavior is
unchanged: full-plan preflight rejects invalid downstream plans before
connector inspection, and `Validate`/`Deduplicate` remain unsupported in
ordinary materialization and preview. The broader matrix is staged design for
future graph contracts.

## 5. Binding, identity, and cache invalidation

Every future preview result is bound to an identity equivalent to:

```text
PreviewBinding {
  graphRevisionId,
  graphDigest,
  targetPort: NodePort,
  sourceBindings: [(sourcePort, sourceAssetId, sourceVersionDigest,
                    authorizedSchemaFingerprint)],
  packageBindings: [(namespace, name, version, contentDigest)],
  compilerVersion,
  previewContractVersion,
  resourceProfile,
}
```

This is not a new API DTO in #346. `graphDigest` covers the complete immutable
GraphRevision, not just a target prefix. `targetPort` is a product identity,
never an expansion ordinal or array position. Source identity includes the
authorized asset/version and exact logical Schema fingerprint; paths,
credentials, and raw content are excluded. Package identity includes exact
namespace/name/version/content digest. Compiler, semantic-contract,
preview-contract, and resource-profile changes invalidate old results.

The conservative rule is to discard a result whenever any complete binding
component changes. A closure-digest optimization is allowed only after the
complete graph binding and diagnostics key are rechecked; a cache hit never
bypasses authorization, Schema-drift checks, or release validation.

Diagnostics are ordered by phase, dependency depth/definition port order, and
stable node/field identity. They contain only approved codes, IDs, field paths,
logical type/kind names, and bounded expected/actual type metadata. They never
contain package bodies, credentials, paths, cell values, or third-party errors.

## 6. Candidate revision to NG-C0 §9

NG-C0 §9 currently requires Preview to delegate to the existing logical-plan
Preview path and forbids a graph-specific prefix plan. That rule remains active
before this candidate is accepted.

The precise future revision is:

> `DraftPreview` may create one bounded, in-memory, preview-only evaluation
> view of `Closure(t)` and lower it through the existing logical semantics and
> Preview runner. The view may use the existing `LogicalPlan` representation
> internally, but is explicitly non-durable: it is not publishable, receives no
> PlanVersion identity, is not returned as a plan fragment, and cannot enter
> Job, Run, Snapshot, or Artifact publication. It must be produced by the same
> NodeGraph compiler/shared semantic authority and may not add a graph-specific
> rule language, plan-node kind, canonicalizer, executor, queue, or publication
> path.

This narrows the prohibition to a second durable execution authority while
allowing one bounded ephemeral evaluation view for a safe draft target. It does
not allow an invalid target closure, a partial release plan, or a silent
downstream skip.

The future runtime contract must freeze the diagnostic response, product
node/port mapping, ColumnId/field order/nullability/row order, reuse of E3
limits/cancellation, prevention of publication calls, and the client-visible
difference between draft preview success and publishability.

Until then, E3 full-plan preflight, pre-I/O Join/Union and verification-rule
rejection, read-only Preview, and the single existing logical-plan Preview path
remain unchanged. This candidate authorizes no direct code change.

## 7. Validate/Deduplicate verification boundary

`Validate` and `Deduplicate` do not become ordinary product operators merely
because a draft can name them. Their execution context is separate from
ordinary Preview and materialization.

### 7.1 Verification target and context

The future verification contract carries a target/context equivalent to:

```text
VerificationTarget {
  graphRevisionId,
  graphDigest,
  targetNodeId,
  targetPort,
  ruleOrdinal,
  fullPlanFingerprint,
  canonicalPlanDigest,
}

VerificationExecutionContext {
  verificationContractVersion,
  sourceBindings,
  authorizedInputVersionDigest,
  reportOutputPolicy,
  rejectedRowsPolicy,
  cancellationDeadline,
  memoryAndArtifactLimits,
}
```

These names are design vocabulary only. The future implementation must bind
the context to E4 and must recompute/verify plan and Schema digests; it must
not trust client-supplied values.

Rules:

- target identity includes explicit product node, port, and rule ordinal;
- verification requires a complete valid logical plan and E4 full-plan identity;
  a draft-only closure is insufficient;
- source version and authorized Schema identity are required;
- target/package/version/content mismatch fails before connector reads; and
- ordinary `materialize` never infers verification from a rule's presence.

### 7.2 Validate mapping

The future mapping reuses E4 exactly:

| Predicate | Warning | Error |
| --- | --- | --- |
| `true` | Keep row; no finding | Keep row; no finding |
| `false` | Keep row; one warning finding | Remove row; one error finding and permitted rejected-row payload |
| `NULL` | Same as `false` | Same as `false` |

Rules run in declared order. A terminal Error removes the row and later rules
cannot re-admit it. Findings, report ColumnIds, source row ordinals,
node/rule identity, and sanitization remain E4 authority. An ordinary Preview
result cannot masquerade as a ValidationReport.

### 7.3 Deduplicate mapping

The future mapping reuses E4 exactly:

- keys are an ordered, non-empty, typed tuple of existing ColumnIds;
- equality is exact and typed, including E4 NULL, NaN, signed-zero, timestamp,
  Utf8, Binary, and timezone laws;
- keep-first uses ascending logical Scan output ordinal;
- canonical key byte limits are enforced before SQLite insertion;
- SQLite index and operator state remain within E4 memory/row/byte/page limits;
  and
- duplicate findings and rejected rows use only E4 verification outputs.

Ordinary materialization continues to reject these rules; it never silently
switches to verification, creates a VerificationBundle, or changes Snapshot
behavior.

### 7.4 Output and status mapping

The explicit verification output is:

```text
accepted snapshot
  + validation report
  + optional rejected rows
  + deduplication report
  -> one atomically committed VerificationBundle
```

Under E4 and SVC-A2, success commits the complete bundle and report refs
atomically. Failure, cancellation, deadline, bound violation, or staging
mismatch publishes no partial Snapshot, report, ArtifactRef, or membership.
Reports remain readable only through the existing bundle/member/section
boundary. No route, storage table, artifact type, executor, or runtime call is
added by this design.

## 8. Two independent future runtime slices

### 8.1 Preview slice: target-closure evaluation

This later slice owns the `DraftPreview` mode, the matrix, closure diagnostics,
binding/invalidation, and one ephemeral evaluation view; it delegates row
production, limits, cancellation, rebatching, and read-only guarantees to E3.
It does not own release plans, PlanVersions, Jobs, Runs, artifacts, storage,
verification reports, or a second compiler/executor. DAG/Join/Union execution
requires the separate F-ENG1/#93 gate.

### 8.2 Verification slice: explicit E4 execution

This later slice admits a full valid logical plan with authorized verification
rules, binds the E4 target/context, reuses E4 row routing/reports/dedup index/
cancellation/memory/publication laws, and produces exactly the E4
VerificationBundle/ArtifactRef outcome. It does not produce ordinary Preview,
perform draft-closure execution, fall back from ordinary materialization, or
relax E4 storage/verification boundaries.

The preview slice may display verification as an unexecuted dependency; it may
not execute or publish E4 artifacts. The verification slice may have a
read-only preflight stage; it may not create a second Preview executor or
publication path.

## 9. Acceptance matrix for this design delivery

| Acceptance item | Evidence in this document |
| --- | --- |
| NX-G1 dependency rechecked | §1 binds PR #358 accepted head, merge commit, current base, and five-check CI |
| Upstream/downstream distinction | §§2–4 define closure, diagnostic classes, order, and matrix |
| Invalid downstream cannot imply release | §§2.1, 3.3, and 4 require release rejection and draft-only isolated output |
| Unsafe/invalid target closure fails before I/O | §§3.1–3.2 and §4 require zero I/O |
| Disconnected/cycle/package/Schema cases covered | §4 explicitly covers each |
| Preview identity and cache invalidation | §5 binds graph, target, source, package, compiler, contract, and resource identities |
| Bounded, sanitized diagnostics | §§2.3 and 5 |
| Preview has no durable side effects | §§6 and 8 retain E3 read-only behavior |
| Final publication requires full valid plan | §§2.1, 3.3, 6, and 7.4 |
| E4 Validate/Deduplicate semantics retained | §§7.2–7.4 |
| Ordinary materialization does not switch modes | §7.3 |
| NG-C0 §9 candidate is precise and gated | §6 keeps the current rule active until a later contract |
| Preview and verification are independent | §8 |
| Docs-only scope | Intended changes are this contract and its `docs/issues/` pointer only; no Rust/API/storage/dependency/workflow/OpenShip changes |

## 10. Explicit non-goals and stop conditions

This issue does not implement or register typed ports, DAG, Join, Union,
multi-source, multi-output, preview runtime, verification runtime, caches,
routes, storage, or API wire changes. It does not alter NodeGraph,
NodeDefinition, NodeRegistry, LogicalPlan, GraphRevision, Preview, Job, Run,
Event, Snapshot, Artifact, E3, E4, #93 XR HOLD, SQL/DuckDB boundaries, or
OpenShip code. It creates no Registry claim and no second Rule/Expr language,
canonicalizer, fingerprint authority, executor, queue, retry, or publication
path.

Return to contract review if a later slice needs a public DTO/route, persisted
identity, changed E3/E4 semantics, new storage/ArtifactRef law, new executor or
queue, broadened package capability, or a resource/cancellation law not
covered by the cited contracts.
