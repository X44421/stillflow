# ADR-002: Deterministic runtime and physical executors

> Status: Proposed
> Date: 2026-08-24
> Decision owners: Stillflow maintainers
> Charter: [#97](https://github.com/X44421/stillflow/issues/97) (umbrella #93, ledger #81)
> Factual input: [`execution-backend-coupling-inventory.md`](../issues/execution-backend-coupling-inventory.md) (merged PR #95), cited below as `XR-D0 §n`
> Supersedes: only the precisely quoted ADR-001 statements in the
> [Supersession map](#9-supersession-map). [ADR-001](adr-001-logical-physical-and-storage-boundaries.md)
> otherwise remains Accepted historical authority.

Citation discipline for this document:

- Statements about code that exists at `main@636cd7db443bed45e7adcf1596785670cfc3ff1c`
  cite `path:symbol` (repo-root-relative, backend crates under `backend/crates/`)
  or `XR-D0 §n` for facts established by the accepted inventory.
- Statements that introduce a new rule of this ADR are labeled **[Decision]**.
  Where this ADR resolves an interpretation question left open by XR-D0, the
  resolution is labeled **[Decision — interpretation]**.
- Items that cannot be settled today are stated in
  [Open questions](#open-questions) and nowhere else. No unknown is inferred away.

## Context

Today the engine has exactly one physical path. Every validated plan executes
through one cascade: `engine.rs:consume_envelope` calls
`ffi.rs:record_batch_to_dataframe`, then `lower.rs:transform`, then
`ffi.rs:dataframe_to_record_batch` (XR-D0 §2.2). Polars 0.46 is confined to
private modules of `stillflow-engine` and `stillflow-connector-local-tabular`;
no public signature in any crate names a `polars` type, and no DuckDB
dependency or identifier exists anywhere in backend code (XR-D0 §1.3, §2.1).
Raw Arrow is different: `batch.rs:BatchEnvelope.payload` and
`batch.rs:BatchEnvelope.into_payload` make the internal `RecordBatch`
reachable through public functions of stable `stillflow-core` (XR-D0 §1.3).

Executor-neutral seams already exist inside the engine:
`preflight.rs:CompiledStep` is built before any engine contact,
`typing.rs:type_check_expr` contains no physical types,
`remainder.rs:CanonicalRebatcher` builds canonical output from pure arrow-rs
builders, and `predict.rs:largest_feasible_k` sizes chunks from logical types
and observed Arrow offsets (XR-D0 §7). What separates these seams from a real
executor boundary is that nothing names them, versions them, or gates what may
sit behind them.

The evidence base is thin where it matters for plurality. XR-D0 §6.4 records
nine gaps: no second executor or oracle exists; NaN payload and signed-zero
survival through the FFI bridge and Parquet are untested; timezone retention
is evidenced for `"UTC"` only (`t48_timestamp_timezone_retention`); Trim's
accepted codepoint set is unpinned; null-comparison truth tables are unpinned;
deadline overshoot during a synchronous transform is unmeasured; concurrent-run
memory attribution is untested; the materialize path persists no plan
fingerprint (`domain/snapshot.rs:DatasetSnapshot` carries `schema_fingerprint`
only); and Validate/Deduplicate have contract prose with no executable
reference.

This ADR freezes the contract for changing that shape without ever creating a
second definition of what a cleaning rule means.

Relationships held fixed by this ADR **[Decision]**: SQL Connector #9 remains
Post-MVP and DuckDB preview SQL #10 remains Phase 1D exactly as
[`data-ingestion-architecture.md`](../data-ingestion-architecture.md)
§17–§18 state; neither is accelerated or blocked here. The active E4 contract
(`issue-054-validation-rejected-rows-contract.md`) stays the sole authority for
Validate/Deduplicate semantics and its E4-S2 hold stands; ADR-002 adds only
conformance obligations for whoever eventually executes them. Issues #80/#91
are out of scope and untouched.

## 1. Vocabulary and ownership

Each term below has exactly one meaning in this ADR and in all XR delivery
tasks **[Decision]**.

| Term | Definition |
| --- | --- |
| LogicalPlan | The public, stable plan DAG owned by `stillflow-plan` (`plan.rs:LogicalPlan`, `PLAN_VERSION = 1`). Its meaning never depends on an executor. |
| Canonical logical semantics | The single definition of every `Rule` and `Expr`: validation, typing, nullability, and law text from `stillflow-plan` plus frozen contracts (issue-046 §§10–11). There is exactly one per operator, forever. |
| PhysicalPlan | A private, versioned, engine-internal compiled form of one validated plan — the successor of today's opaque `preflight.rs:PreparedPlan` / `preflight.rs:CompiledStep`. Never public API, never persisted unless a versioned format contract says otherwise. |
| Fragment | A maximal ordered subsequence of `CompiledStep`s that one executor executes end-to-end over one execution chunk. The unit of eligibility, selection, fallback, and provenance. |
| Capability | A statically declared property of an executor or connector, expressed as versioned declaration data in the style of `capabilities.rs:Capability` / `ConnectorCapabilities`. Declared, never probed from the environment at planning time. |
| Executor | A private component that executes fragments: bounded Arrow batches in, canonical Arrow batches plus typed errors and provenance events out. Owns physical operator choice inside its fragments; owns nothing else. |
| Runtime | The engine orchestrator that owns preflight, fragment planning, chunk sizing, memory accounting, cancellation checkpoints, publication calls, and provenance — embodied today by `engine.rs:ExecutionEngine`. |
| Effect / worker | A typed boundary for non-deterministic side effects (AI interpretation, document workers, remote calls). Never part of the deterministic cleaning path. |
| Run | One invocation of `ExecutionEngine::materialize` or `ExecutionEngine::preview`. |
| Chunk | An execution chunk as defined by issue-046 §14.2: a row prefix of one connector envelope sized by `predict.rs:largest_feasible_k`. |

Ownership map **[Decision]** (current owners cited; all remain unchanged):

| Concern | Owner | Anchor |
| --- | --- | --- |
| Rule meaning, plan validity, fingerprints | `stillflow-plan` + core logical types | `plan.rs:LogicalPlan.validate`, `plan.rs:LogicalPlan.fingerprint` |
| Physical planning, fragmenting, selection | Runtime (private) | `preflight.rs:preflight` |
| Fragment execution | The registered executor selected for the fragment | `lower.rs:transform` today |
| Identity (IDs, timestamps, lineage) | Caller, injected via `ExecutionIdentities`; executors and runtime are forbidden from generating them (issue-046 §15; tests `t21`, `t22`) | `lib.rs:ExecutionIdentities` |
| Verification | `stillflow-storage` verification flow over `core/src/verification.rs` contracts (`VERIFICATION_CONTRACT_VERSION = 1`) | `store.rs:SnapshotStore.verify_snapshot` |
| Storage formats | `stillflow-storage` | `store.rs:SnapshotStore` |
| Publication | `stillflow-storage` commit path — the only visibility point | `store.rs:SnapshotWriter.commit` |

## 2. Executor boundary

**Allowed inputs** **[Decision]**: bounded envelope payloads within
`MAX_BATCH_ROWS = 65_536` / `MAX_BATCH_BYTES = 64 MiB`
(`batch.rs:BatchEnvelopeFactory.try_build`), a private fragment descriptor,
the working `LogicalSchema`, the run's cancellation/deadline handles
(`request/mod.rs:RequestContext`), and an explicit memory budget. Nothing else.

**Allowed outputs** **[Decision]**: canonical Arrow batches handed to the
runtime's rebatcher (`remainder.rs:CanonicalRebatcher`), typed errors
normalized into the `error.rs:EngineError` taxonomy, and per-fragment
provenance events. No executor writes storage, emits events, or touches the
network.

**Arrow is the interchange protocol, not an executor** **[Decision]**. The
arrow-rs crates define bounded columnar data at boundaries; execution authority
belongs only to registered executors. Rebatching, chunk sizing, and schema
propagation (`remainder.rs`, `predict.rs`, `preflight.rs:propagate_schema`)
are runtime-owned plumbing and must stay free of any executor's types.

**No physical type or SQL string in stable public contracts** **[Decision]**,
reaffirming AGENTS rules 1, 2, 6, and 8 with current-code evidence: Polars
types appear in no public signature and DuckDB does not exist in backend code
(XR-D0 §1.3). Any future public surface that names a physical engine type, a
DuckDB connection, or carries a SQL fragment fails architecture review.

**RecordBatch reachability** **[Decision — interpretation]**: XR-D0 §1.3
records, without verdict, that `BatchEnvelope`'s raw `RecordBatch` is publicly
reachable while AGENTS rule 4 calls it an internal payload pending its own
delivery node. This ADR decides: the payload is internal *by intent*; the
existing accessors (`batch.rs:BatchEnvelope.payload`, `.into_payload`,
`BatchEnvelopeFactory.arrow_schema`, `logical_schema_to_arrow`) are the
transitional Phase-0 surface that rule 4 already schedules for replacement;
no new boundary introduced under this ADR may add raw-payload accessors, and
closing the existing ones belongs to the BatchEnvelope delivery node — not to
XR-R0 and not to this document.

**Executors hold no authority over rule meaning, identity, verification, or
publication** **[Decision]**. Concretely: an executor cannot introduce a second
interpretation of any `Rule` (single-semantics law, §1); it receives identities
and never mints them; it cannot invoke `verify_snapshot`, recovery, or bundle
writers, which today have no callers outside `stillflow-storage` (XR-D0 §2.3
absences); and it becomes visible to readers only through
`store.rs:SnapshotWriter.commit`.

## 3. Fragment planning and capability matching

**Deterministic capability declarations** **[Decision]**: each executor
registers a static capability declaration set — identifier, executor contract
version, supported operators/rules/expressions, resource ceilings, declared
equivalence levels (§5), and a priority rank. Declarations are data, fixed at
registration; planning never probes filesystems, environments, or hardware.

**Eligibility** **[Decision]**: an executor is eligible for a fragment if and
only if (a) its declaration covers every operator, rule, and expression kind in
the fragment; (b) every required resource ceiling it declares is ≤ the runtime
ceiling for the position; and (c) its declared equivalence evidence (§5) meets
or exceeds the level the fragment requires. Failing any clause renders the
executor ineligible; eligibility failures surface as typed pre-stream errors in
the pattern of `preflight.rs:reject_phase_kinds`.

**Selection** **[Decision]**: when more than one eligible executor exists, the
runtime selects by a total order: lower declared priority rank wins; ties break
by lexicographic executor identifier. Selection is a pure function of (logical
plan fingerprint, capability declarations, registry contents). Identical
inputs yield identical choices on every machine, every day, regardless of load
or locale.

**Initial extraction state** **[Decision]**: exactly one executor exists after
XR-R0 — `PolarsExecutor`, wrapping the extracted current cascade
(`ffi.rs:record_batch_to_dataframe` → `lower.rs:transform` →
`ffi.rs:dataframe_to_record_batch`). It is bound explicitly, not chosen by the
selection function. Automatic matching among multiple executors remains
unimplemented until XR-G1 passes (§10).

**Fallback semantics** **[Decision]**: the only permitted fallback is
planner-directed fragmentation — splitting a plan into more fragments for the
*same* executor when a single fragment would violate a bound. Switching to a
different executor because the first was ineligible is forbidden: ineligibility
is a typed error (`UnsupportedCapability` family, retryable false, per the
issue-046 §16.1 mapping), never a silent substitution.

**No silent semantic downgrade** **[Decision]**: support at a lower equivalence
level than the fragment requires equals non-support. An executor that "mostly"
matches a truth table is ineligible; there is no partial credit, no degraded
mode, no warning-and-continue.

## 4. Runtime laws

These laws bind every executor equally. Ceilings are hard: exceeding one fails
the run with a typed error (issue-046 §14 preamble).

**Cancellation and deadline checkpoint ownership** **[Decision]**: the runtime
owns the token (`request/mod.rs:RequestContext`) and keeps the issue-046 §15
checkpoint list (before inspect, before read, per stream poll, before lowering
each envelope, before append, before commit). Executors must observe
cooperative checkpoints at fragment boundaries and declare their maximum
uninterruptible region. Because a synchronous region cannot be interrupted
mid-flight (XR-D0 §5: checks occur between cascades), every run whose deadline
expired or cancel fired during an uninterruptible region must disclose the
overshoot in its result metadata rather than swallow it. Overshoot magnitude
measurement is owned by XR-D1 (§10, Open questions).

**Memory, concurrency, and size bounds** **[Decision]**: the issue-046 §14.1
law generalizes verbatim — live columnar payloads ≤ `MAX_LIVE_COLUMNAR_PAYLOADS = 3`,
each ≤ `MAX_BATCH_BYTES`, operator state ≤ `MAX_OPERATOR_STATE_BYTES = 5 MiB`,
peak ≤ `MAX_ENGINE_PEAK_BYTES = 197 MiB` — with the phase accounting of
`memory.rs:MemoryTracker` / `memory.rs:AllocatorPhase` retained and extended so
each executor's working set is attributable to a named phase. Executors declare
maximum worker parallelism as a constant; unbounded thread pools, prefetch
queues, collects, or full-source materializations are forbidden. The run gate
stays runtime-owned (`MAX_ENGINE_CONCURRENT_RUNS = 4`,
`engine.rs:ExecutionEngine`).

**Retry and idempotency** **[Decision]**: executors contain no hidden retries.
The only sanctioned intra-chunk adaptation is the bounded shrink-retry pattern
of `preview.rs:lower_chunk` (`n /= 2` halving with cooperative yields), which
must be disclosed as adaptation events in provenance, never re-labeled as
retry. `error.rs:EngineError.retryable()` remains the sole retryability
authority. Every fragment must be idempotent: re-executing a fragment with
identical inputs yields identical outputs, so crash recovery may replay
fragments without inventing new publication paths.

**Error normalization** **[Decision]**: executor failures cross into the
runtime only as `EngineError` values carrying category/retryable/sanitized
summary per the issue-046 §16 tables. Raw third-party error strings, cell
values, labels, credentials, and backtraces are forbidden in Display, Debug,
and summaries, exactly as issue-046 §16 requires today.

**Recovery, atomic visibility, cleanup** **[Decision]**: staging → fsync'd
rename → manifest commit → visibility remains solely `stillflow-storage`'s
(`store.rs:SnapshotWriter.commit`); failure drops the writer without commit and
storage's abort semantics clean up (XR-D0 §5). No executor or runtime code may
create a second publication or cleanup path.

**Provenance** **[Decision]**: every run records — in manifest/event metadata
governed by §6 versioning — planner contract version, executor identifier and
version, the capability-declaration snapshot used, fragment boundaries,
fallback events, and adaptation events. Current gap, stated plainly: the
materialize path computes no plan fingerprint and persists none
(XR-D0 §2.3, §5). Closing that gap is XR-S1 scope; XR-R0 must not change any
persisted artifact.

## 5. Semantic equivalence levels

Equivalence claims are level-specific and evidence-gated. Claiming "equivalent"
without naming a level and citing its evidence fails architecture review
**[Decision]**.

| Level | Name | Meaning | Evidence required before claiming |
| --- | --- | --- | --- |
| L0 | Plan portability | The target executor accepts the plan: validation, typing, and lowering succeed without unsupported errors. | Structural pass of the XR-R1 conformance suite section for accepted shapes; no behavioral claim implied. |
| L1 | Logical-result equivalence | For fixed inputs, plan, `batch_size`, and injected identities: identical ordered logical rows and identical output `LogicalSchema`, including null-comparison truth tables, cast-failure sets, and Trim codepoint sets, invariant to input partitioning (the issue-046 §13 determinism law, generalized beyond one executor). | Differential matrix against the reference outputs covering the XR-D1 corpus: pinned null truth tables, cast failure sets, NaN/signed-zero survival, Trim codepoints, timestamp/timezone retention. Today none of these fixtures exist (XR-D0 §6.4 gaps 2–5); L1 is therefore unclaimable until XR-D1 lands. |
| L2 | Canonical-artifact equivalence | L1 plus identical canonical rebatched output: same envelope boundaries, sequences, schema metadata, and fingerprint inputs under fixed `batch_size` — the generalization of `t02_two_input_partitionings_yield_equal_rows_and_stats` and `t03_fixed_batch_size_yields_equal_output_envelope_boundaries`. | XR-R1 differential runs comparing canonical envelopes byte-for-byte at the envelope level across partitionings, plus manifest statistics equality. |
| L3 | Byte identity | Identical persisted Parquet partition bytes and digests for identical inputs, identities, and pinned encoder/storage versions. | Golden fixtures proving digest equality across repeated runs and across both executors' full stacks, including NaN bit-pattern and `-0.0` survival through conversion and `write_envelope_parquet` (gap 2). Requires pinning encoder versions in the claim. |

Claims expire: when either side's executor or storage encoder version changes,
all recorded claims lapse until re-evidenced **[Decision]**. Levels are
monotone prerequisites: L2 requires L1 evidence, L3 requires L2.

## 6. Compatibility and versioning

**Versioned surfaces** **[Decision]**: `PLAN_VERSION = 1` and
`PLAN_FINGERPRINT_ALGORITHM = "stillflow-fnv1a64x4-v1"` exist
(`plan/src/plan.rs`). This ADR adds four private u16 counters, monotonic,
bumped on any breaking change: `PHYSICAL_PLAN_VERSION` (PhysicalPlan form),
`EXECUTOR_CONTRACT_VERSION` (fragment ABI + capability declaration schema),
`CAPABILITY_DECLARATION_VERSION`, and `PROVENANCE_RECORD_VERSION`. They live in
the engine's private surface because PhysicalPlan is private.

**Unknown or newer versions fail closed** **[Decision]**: a persisted
provenance record, registration, or fragment descriptor bearing a version
greater than the runtime's own, or one the runtime does not know, is rejected
with a typed error mapped to `InvalidConfiguration`, retryable false. Best-
effort interpretation, silent field skipping, and "assume compatible" are all
forbidden.

**No persistence of unstable internal plans** **[Decision]**: PhysicalPlan
bytes, compiled steps, and capability declarations are never written to SQLite,
Parquet, events, or API payloads. Persisted execution references use the
logical plan fingerprint plus contract versions only. Persisting any internal
form first requires its own versioned format contract.

**Migration and rollback** **[Decision]**: executor upgrades are additive —
the previous `EXECUTOR_CONTRACT_VERSION` stays registrable while supported;
rollback is re-registering the prior version and re-running XR-R1 evidence.
Persisted snapshots stay readable across executor changes because they depend
only on storage format versions (`DATASET_SNAPSHOT_VERSION`,
`VERIFICATION_CONTRACT_VERSION`), never on executor internals. Breaking a
published contract follows the AGENTS breaking-change gate, unchanged.

## 7. Cross-executor danger matrix

For each hazard: current pinned state (cited), then the gate requirement for
any second executor **[Decision]**. Until a row's requirement is met, no
executor other than the reference may claim above L0 for plans touching that
row.

| Hazard | Current pinned state | Gate requirement |
| --- | --- | --- |
| NULL / three-valued logic | Filter keeps Boolean `true` only (issue-046 §10.3); And/Or/comparisons delegate to Polars (`lower.rs:lower_expr`); compound truth tables with nulls asserted by no test (gap 5). | Explicit truth-table fixtures for every comparison and logical operator, executed identically on both sides; differences are eligibility failures, not caveats. |
| Casts and overflow | Policy mapping strict/lenient per `CastFailurePolicy` (`lower.rs` cast arms); entire arithmetic surface paused (`typing.rs:reject_paused_expr`); checked overflow semantics exist as contract text only (issue-046 §11.5, no implementation). | Unpausing arithmetic requires a checked-semantics decision frozen first, then identical failure sets proven per level; cast failure sets compared cell-for-cell. |
| NaN and signed zero | Literals must be finite (`expression.rs:FiniteF64`); survival of NaN payloads and `-0.0` through the FFI bridge and Parquet publish/read is untested (gap 2). | Bit-level survival fixtures across both directions before any float-bearing L2/L3 claim. |
| Unicode, trim, collation | Trim lowers to Polars default whitespace strip (`lower.rs` trim arm); accepted codepoint set unpinned (gap 4); string ordering unreachable in v1 plans (`typing.rs:ordered_pair` restricts ordered comparison to numerics/date/timestamps). | Codepoint-enumerating fixtures for Trim; collation order pinned as codepoint order per executor; any relaxation of string ordering re-runs the full matrix. |
| Timestamp units, timezones, DST | Second unit paused (`types.rs:polars_data_type` rejects it); mixed timezones incomparable by the least-upper-bound rule; retention evidenced for `"UTC"` only (gap 3). | Retention fixtures across units and a zone set including fixed offsets and DST-bearing zones; identical instant semantics proven, not assumed. |
| Ordering, repartitioning, batch boundaries | Operators must not sort/shuffle/sample/hash-aggregate; scan arrival order is snapshot order (issue-046 §10); within-engine partition invariance tested by `t02`/`t03`; order preservation itself rests on Polars behavior (XR-D0 §4, class P). | A second executor must prove arrival-order preservation under its own internal parallelism, plus pass the t02/t03-style partition-invariance matrix at L2. |
| Validation, deduplication, error mapping | `Rule::Validate`/`Rule::Deduplicate` are typed-rejected today (`preflight.rs`, `lower.rs` defense arms); executable semantics belong to the active E4 contract (#54) and its correction path (#91); error normalization is the issue-046 §16 mapping. | Whoever executes them under E4 must additionally pass XR-R1 sections for dedup key canonicalization — computed above the executor boundary from canonical key bytes — and identical rejection routing; no conflict with, and no change to, the E4-S2 hold. |

## 8. AI and remote effects

**Typed, provenance-bearing effect boundary** **[Decision]**: AI inference,
document workers, and remote calls attach only through a dedicated effect/
worker contract whose requests carry artifact references and digests, model or
worker identity and version, and request IDs, and whose responses carry their
own lineage. Effects observe deadlines and row/byte caps like every other
operation; unbounded payloads are forbidden everywhere.

**AI is not a deterministic bulk-cleaning executor** **[Decision]**, freezing
[`data-ingestion-architecture.md`](../data-ingestion-architecture.md) §2
and AGENTS rule 9 as law: no AI-backed component may register as an executor;
AI consumes metadata, previews, profiles, and committed results; it never sits
on the cleaning path and never replaces a deterministic fragment output.

**Raw credentials and bulk exposure remain forbidden** **[Decision]**:
credential material crosses boundaries only as references (`CredentialRef`),
per AGENTS rule 10 and architecture doc §14; effect workers receive resolved,
least-privilege handles from the credential layer, never secret values in
domain objects, logs, events, or payloads.

## 9. Supersession map

ADR-001 stays Accepted as historical authority. Exactly one of its normative
statements is superseded, effective when XR-R0 merges:

> "**Polars is the sole canonical executor for cleaning and transformation
> rules.**" — ADR-001, *Execution plane*.

Superseded reading **[Decision]**: the word *sole* no longer forbids additional
physical executors. It is replaced by the registry law of this ADR: Polars is
the initial and reference executor; further executors may be admitted only
through the §5 evidence levels and §10 gates; and no executor may redefine
canonical logical semantics (§1). Behavior is unchanged until a second
executor passes those gates.

Not superseded, and reaffirmed: ADR-001's rejected alternative "*Use DuckDB
for cleaning too:* creates two rule languages and semantic drift"; its
invariant "A logical plan is deterministic and execution-engine independent";
its BatchEnvelope authorization; its persistence-plane and dependency-direction
decisions; and its requirement that a DuckDB operation never define a second
interpretation of a cleaning rule.

AGENTS.md statements requiring a later governance-alignment task (quoted
verbatim; **not edited in this PR**) **[Decision]**:

- Frozen engineering rule 5 (AGENTS.md line 42): "**Polars is the one canonical
  cleaning and transformation executor.**" — conflicts with multi-executor
  registry law once a second executor is admitted; must be rewritten to
  delegate to this ADR.
- Frozen engineering rule 4 (AGENTS.md lines 39–41): "…raw `RecordBatch`
  values remain an internal payload. This contract is introduced in its own
  delivery node." — consistent as written, but the governance task should
  cross-reference the reachability interpretation fixed in §2 so the envelope
  delivery node closes it deliberately.

The dependency-direction diagram needs no alignment: executors live inside
`stillflow-engine` private modules and add no crate arrows. The
governance-alignment task itself is to be chartered by the #93 umbrella; this
PR neither creates nor dispatches it.

## 10. Delivery gates

Ordering: XR-R0 first; XR-D1 and XR-R1 follow; XR-S1 next; XR-A1 independent;
XR-G1 last **[Decision]**. Scope note: no charter for XR-A1, XR-S1, or XR-D1
exists anywhere on `main` (the repository tree contains no XR task definitions
beyond the merged inventory); the scopes below are therefore assigned by this
ADR as decisions, and each task's charter PR must match or supersede by
explicit reference.

### XR-R0 — zero-observable-change extraction

- Entry: this ADR Approved; base rebuilt from latest `main`; coupling inventory
  merged (done, PR #95); no conflict with active locks.
- Outcome: inside `stillflow-engine` only, introduce the private fragment
  descriptor and executor seam and move the existing cascade behind a single
  explicitly-bound `PolarsExecutor`. No new crate, no dependency change, no
  public symbol.
- Zero observable change means: all existing tests pass unmodified (mechanical
  import moves aside); golden `t15`, partition invariance `t02`/`t03`, identity
  tests `t21`/`t22`, FFI failure release `t36`/`t39` behave identically; memory-law
  assertions unchanged; identical `PreviewResult` fields and manifests on fixed
  fixtures; identical error categories.
- Stop conditions: any public-surface or Cargo change; any behavioral delta;
  any attempt to add selection logic, a second executor, or provenance fields
  (those belong to XR-S1).

### XR-D1 — danger-matrix differential corpus

- Entry: XR-R0 merged.
- Outcome: turn XR-D0 §6.4 gaps 2–9 into executable, deterministic fixtures and
  recorded observations: null truth tables, NaN/`-0.0` survival, Trim codepoint
  set, non-UTC/DST timezone retention, deadline/cancel overshoot magnitude
  measurement, concurrent-run memory attribution, and the arithmetic
  checked-semantics decision inputs. Observed Polars behavior is labeled
  observed-vs-contract, never silently promoted to law.
- Stop conditions: any production-code change; any behavior change (this task
  pins and documents, it does not fix); ambient-random or wall-clock-dependent
  fixtures.

### XR-R1 — executor conformance harness

- Entry: XR-R0 merged; XR-D1 corpus merged (or co-delivered and green).
- Outcome: test-only harness that drives a candidate executor over the corpus
  and emits per-level (L0–L3) evidence reports with the executor version and
  fixture list, so equivalence claims become citable artifacts. No production
  path changes.
- Stop conditions: harness requiring production API changes; flaky or
  environment-dependent results; evidence emitted without version pinning.

### XR-S1 — selection, fallback, and provenance

- Entry: XR-R1 harness available; XR-D1 corpus green; this ADR Approved.
- Outcome: implement the §3 eligibility/selection/fallback machinery and the §4
  provenance record behind `PROVENANCE_RECORD_VERSION`, closing the
  materialize-path plan-fingerprint gap (XR-D0 §5) as the first gated
  observable change. Still exactly one registered executor.
- Stop conditions: enabling automatic selection among multiple executors
  (XR-G1 scope); any silent-downgrade path; provenance lacking version fields;
  persisting internal plan forms.

### XR-A1 — typed effect/worker boundary

- Entry: this ADR Approved; contract note per the AGENTS risk gates.
- Outcome: freeze and then implement the §8 typed effect/worker contract with
  provenance-bearing requests/responses, deadlines, and bounds; route AI and
  remote effects exclusively through it.
- Stop conditions: any effect path touching bulk cleaning; secret values
  crossing the boundary; bypassing publication ownership; unbounded payload
  sizes.

### XR-G1 — automatic-selection gate

- Entry: at least two executors registered, each holding unexpired XR-R1
  evidence at its claimed levels; XR-S1 shipped; XR-D1 corpus green at those
  levels.
- All of the following must hold before automatic selection may be enabled
  **[Decision]**: a property test proves the §3 selection function is pure and
  total (identical inputs → identical choice); no executor can be reached whose
  claimed level is below the fragment requirement (fail closed verified);
  fallback occurs only among level-proven executors and is always
  provenance-visible; a configuration kill-switch forces explicit selection;
  every run emits complete §4 provenance.
- Stop conditions: any evidence expiry; any downgrade path; any environmental
  input (load, locale, clock, hostname) influencing selection.

## Open questions

Stated, owned, and blocked on evidence — none is resolved by assumption in
this ADR:

1. Deadline/cancel overshoot magnitude inside a synchronous transform is
   unmeasured (XR-D0 §6.4 gap 6). Owned by XR-D1; §4 requires disclosure
   regardless of magnitude.
2. Memory attribution under genuinely concurrent runs (gap 7). Owned by XR-D1;
   until measured, per-executor phase attribution extends, and does not relax,
   the issue-046 §14.1 law.
3. Actual values of the unpinned semantic surfaces — null truth tables, Trim
   codepoints, non-UTC/DST retention, NaN/-0.0 round-trips (gaps 2–5). Owned
   by XR-D1; §7 treats them as unpinned today.
4. Final checked-arithmetic semantics for unpausing (contract text exists in
   issue-046 §11.5 with no implementation, XR-D0 §4). Decision deferred to the
   task that unpauses, gated by §7 row 2.
5. Whether the unused scan-predicate channel (`domain/read.rs:ReadRequest.filter`;
   the engine passes `filter: None` on every read today, XR-D0 §2.3) is ever
   enabled per-executor. Deferred to a future contract; this ADR requires only
   that enabling it be capability-gated and equivalence-evidenced.
6. Whether any second executor will ever be admitted. None exists (gap 1);
   nothing in this ADR prescribes one, and the gates fire only if one arrives.

## Consequences

### Benefits

- Cleaning semantics gain a second implementation path without ever gaining a
  second meaning; portability stops being folklore and becomes evidence.
- Fail-closed versioning makes executor and format evolution auditable and
  reversible.
- Provenance turns "which engine produced this snapshot, and why" into a
  persisted fact instead of archaeology.
- AI and remote effects get a contained, bounded doorway instead of ad-hoc
  integration pressure on the engine.
- The existing seams (`preflight.rs:CompiledStep`, `remainder.rs:CanonicalRebatcher`,
  `predict.rs`, `typing.rs`) acquire a named contract instead of implicit
  status.

### Costs

- XR-R0 adds an indirection layer inside the engine that pays for itself only
  if plurality ever arrives.
- Four new version counters and expiry rules demand discipline from every
  touched PR.
- The conformance corpus (XR-D1/XR-R1) is permanent maintenance surface.
- Equivalence claims now expire on version bumps, forcing re-evidence runs.
- AGENTS.md rule 5 wording lags this ADR until the governance-alignment task
  lands; until then this document and the frozen rule coexist under the
  supersession reading of §9.

## Rejected alternatives

- **Keep the single-executor shape informal:** the seams stay private folklore;
  the first second engine forks rule semantics precisely because nothing froze
  the meaning boundary.
- **Publish `PhysicalPlan` or an executor trait as public API:** freezes
  internals into compatibility debt and violates the AGENTS risk gates for
  public traits.
- **Persist internal compiled plans:** unstable formats in durable storage;
  violates §6 unless a versioned format contract precedes it.
- **Best-effort interpretation of newer versions:** converts compatibility
  surprises into silent corruption; fail closed instead.
- **Automatic executor selection from the start:** equivalence claims without
  evidence; deterministic-looking choices that depend on registry mutation
  order.
- **Silent fallback or partial-credit matching:** hidden semantic downgrades;
  the hardest class of bug to observe.
- **Treat Arrow (or SQL-over-Arrow) as an executor:** conflates interchange
  with execution and smuggles SQL strings toward public contracts.
- **Admit AI as a bulk-cleaning executor:** contradicts architecture doc §2 and
  AGENTS rule 9; nondeterminism on the cleaning path destroys reproducibility.
- **Rewrite ADR-001 or AGENTS.md here:** out of charter for #97; handled by the
  named governance-alignment task instead.

## Verification

Mechanically checkable enforcement for this ADR's own acceptance:

- Single-file diff, docs-only: `git diff --stat` against the base shows exactly
  one new file; `git diff --check` is clean; the diff contains no
  unfinished-work marker of any kind (the repository's forbidden list applies
  verbatim to this file).
- Dependency arrows: unchanged from AGENTS "Dependency direction";
  `tests.rs:t19_engine_crate_does_not_depend_on_adapter_crates` and
  `t20_engine_depends_on_core_plan_connectors_storage` continue to pass
  untouched.
- Links: relative links resolve within `docs/architecture/` and `docs/issues/`;
  issue numbers (#97, #93, #81, #95, #54, #91, #80, #9, #10) match their
  subjects as stated in the charter and architecture doc.
- Future gates inherit mechanical checks: XR-R0 by the unchanged-test battery
  listed above; XR-D1/XR-R1 by deterministic fixtures; XR-S1 by provenance
  field-presence assertions; XR-G1 by the selection-function property test and
  fail-closed version unit tests.
