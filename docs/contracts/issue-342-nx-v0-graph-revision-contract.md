# Issue #342: NX-V0 GraphRevision persistence and configuration-migration contract

> Status: Candidate frozen contract for acceptance by #342
> Risk: L1 documentation delivery. The persistence/API implementation is L3
> by the owning issue (NX-V1 #343).
> Parent: #334
> Predecessors: #324 NG-C0, #335 NX-C0, #340 NX-C1
> Authorized base: `main@f401c2a` (merge of PR #351)
> Suggested implementation branch: `agent/issue-342-nx-v0`

This document freezes the boundaries for saving and restoring editable
NodeGraph revisions, the optimistic-concurrency and immutability rules, the
explicit configuration-migration model, and the database upgrade, backup,
and rollback story. It authorizes no runtime change by itself; NX-V1 (#343)
implements the slice at L3.

The invariants of #334, NX-C0, and NX-C1 remain in force: the published
`LogicalPlan` is the only execution authority; a published plan never needs
the original graph, package, or revision to execute (NG-C0 §9, P-15); no
second `Job`/`Run`/publication path is created; preview stays side-effect-
free; Openship owns interactive and temporary draft state.

### Document location

The canonical normative text is this file under `docs/contracts/`; an
8-line pointer stub is kept at `docs/issues/issue-342-nx-v0-graph-revision-contract.md`.

## 1. Decision, authority, and current-state evidence

| Concern | Authority | NX-V0 rule |
| --- | --- | --- |
| Editable graph identity, arrangement, metadata | `NodeGraph` (StillFlow) | Persisted as immutable `GraphRevision` records; StillFlow owns durable graph state. |
| Interactive editor state (undo, unsaved buffers, canvas layout) | Openship | Never persisted server-side; cross-repo DTO handoff only (§8). |
| Execution semantics, canonical bytes, fingerprint | `LogicalPlan` | Revisions never alter plans; the association is bookkeeping only. |
| Durable plan identity and lifecycle | Existing `PlanVersion` APIs (`cp_plan_versions`, states draft/published/superseded/archived) | Reused; revisions reference, never replace, this lifecycle. |
| Control-plane storage | SQLite STRICT tables under `cp_*`, numbered `PRAGMA user_version` migrations | One additive migration (§6); no in-place rewrite of history. |

Evidence at the authorized base (falsifiable from the repository):

| Observed fact | Evidence |
| --- | --- |
| `cp_plan_versions` stores only the compiled plan: `logical_plan_json`, `canonical_plan_bytes`, digest, fingerprint, state, `parent_version_id`, `UNIQUE (plan_id, version_number)`. | `backend/crates/stillflow-storage/src/store.rs:1425-1443` |
| There is no table storing the original `NodeGraph` wire document or its metadata; a compile request is the only place a graph exists. | absence of any `cp_graph*` DDL in `backend/crates/stillflow-storage/src/store.rs` |
| Control-plane migrations are numbered `PRAGMA user_version` blocks; the accepted schema version is 12. | `backend/crates/stillflow-storage/src/store.rs:1010-1248`; `backend/crates/stillflow-storage/src/manifest.rs:12` (`STORAGE_SCHEMA_VERSION: u16 = 12`) |
| Backup/restore is manifest-bound and version-gated: `storage_schema_version` must equal `STORAGE_SCHEMA_VERSION` on restore. | `backend/crates/stillflow-storage/src/backup.rs:58-96, 204, 518` |
| Jobs reference `plan_version_id`; publication states are enforced by CHECK constraints. | `backend/crates/stillflow-storage/src/store.rs:1445-1466` |
| The NodeGraph wire document carries `version: 1` and validates fail-closed (unknown fields rejected, exact version match). | NG-C0 §2.1/§2.3; NX-C0 R-1 (deny_unknown_fields) |
| Preview compiles first and never creates `Job`/`Run`/`Artifact` records. | NG-C0 §9; `preview_node_graph` in `backend/crates/stillflow-api/src/service.rs` |

## 2. Ownership boundary (frozen)

1. **StillFlow owns**: durable, versioned `GraphRevision` records; the
   revision history; compile associations; migration records; their APIs.
   A graph saved by a client survives process restarts and is restorable
   byte-for-byte.
2. **Openship owns**: interactive and temporary draft state — unsaved
   buffers, undo/redo stacks, canvas layout, comment threads, local
   validation hints. These never cross the StillFlow API as durable state;
   the DTO handoff is the revision fetch/save payloads themselves (§8).
3. No second `Job`, `Run`, publication, or artifact path is created.
   Revision save is a control-plane write only.

## 3. The GraphRevision record (frozen shape)

NX-V1 introduces one additive table `cp_graph_revisions` (STRICT, §6):

| Column | Meaning |
| --- | --- |
| `id` | Revision UUID (primary key). |
| `workspace_id` | Owner workspace; FK to `cp_workspaces`; all scope checks reuse the existing plan/asset permission surface. |
| `graph_id` | Caller-supplied stable graph UUID. Identity of the *editable graph*, not of any compiled plan. `UNIQUE (workspace_id, graph_id, revision_number)`. |
| `revision_number` | Monotonic per `(workspace_id, graph_id)`, starting at 1. History is append-only. |
| `parent_revision_id` | Nullable FK to the previous revision; `NULL` for the first. |
| `format_version` | The revision envelope's graph format version (currently `1`; see §5). |
| `graph_json` | The complete version-1 `NodeGraph` wire document, byte-preserved as received (canonicalized JSON encoding: sorted keys, no whitespace) so restoration is exact. |
| `graph_digest` | SHA-256 of `graph_json` (64 hex). Used for idempotency and conflict detection. |
| `source_binding_json` | The `connectionId`/`assetId` the graph was last successfully compiled against, plus the asset's authorized-schema identity (schema digest) at that compile — drift detection input (§4.4). |
| `package_digests_json` | The deployed composite packages referenced by the graph: `namespace/name@version` plus content digest (NX-C1 §6). Empty for atomic-only graphs. |
| `compiler_version` | `NODE_GRAPH_COMPILER_VERSION` of the compile this revision was last submitted with (informational; not an execution authority). |
| `plan_version_id` | Nullable FK to `cp_plan_versions(id)` — the PlanVersion produced from this revision, written in the same transaction as PlanVersion creation (§4.3). `NULL` for never-compiled revisions. |
| `migration_json` | NULL for originals; for migrated revisions: `{fromFormat, toFormat, migrationId, appliedAt}` (§5). |
| `created_at_utc`, `created_by` | Audit columns; `created_by` is the existing actor/principal identity, never a client-supplied label. |

No `state` column: revisions are immutable facts. "The current revision" is
`MAX(revision_number)` per graph. There is no update or delete statement in
this contract (§7).

## 4. Save, concurrency, and compile association

### 4.1 Save semantics

`save(workspaceId, graphId, expectedRevision?, graph, metadata…)`:

1. **First save** (no revision exists): creates revision 1.
2. **CAS save**: the request names `expectedRevision` (revision number and,
   optionally, the expected `graph_digest`). If the current revision matches
   the expectation, a new revision (`n+1`) is appended.
3. **Idempotent save**: if the incoming content's `graph_digest` equals the
   current revision's digest, the call is a no-op returning the current
   revision — repeated saves of unchanged content do not grow history.
4. **Conflict**: if `expectedRevision` names an older revision than the
   current one, the save fails with the existing `Conflict` API code and
   the response names the current revision number and digest. Two editors
   based on the same revision cannot silently lose an update; the loser
   re-fetches and re-applies. There is no merge.

### 4.2 Permissions

Revisions live under the plan/asset permission surface: same-workspace
reads/writes use the existing capability checks; cross-workspace access is
rejected exactly like foreign plan reads today — indistinguishable from
absent (`NotFound` semantics, NX-A1 §8.1 stage-3 rule).

### 4.3 Compile association integrity

- Compiling from a revision (NX-V1's "compile saved graph" path) may link
  the produced `PlanVersion` by writing `plan_version_id` in the same
  transaction that creates the PlanVersion record. The link is
  **append-only bookkeeping**: a revision may be re-linked to a newer
  PlanVersion of the same graph lineage, and the previous association
  remains visible in the PlanVersion record itself (`parent_version_id`
  chain) — the revision's history rows are never rewritten; the link lives
  on the newest revision row.
- Publication does not require the revision, the graph, or the package:
  once `cp_plan_versions` holds the published plan, everything needed for
  execution lives there (NG-G1's durable-execution proof). Deleting a
  revision or disabling a package cannot break a published plan.
- The association records the `compiler_version` and package digests that
  were current at compile time so a later open can detect drift (§4.4).

### 4.4 Schema drift

On open (fetch), the stored authorized-schema identity from
`source_binding_json` is compared with the asset's current schema identity
if the asset still exists. Drift is **reported, not repaired**: the open
response carries a `sourceSchemaDrift` marker; the stored revision is never
rewritten; preview/compile continues to use the current authorized schema
through the existing pipeline (NX-A1 §8.1). If the asset or connection was
deleted, the marker reports the binding as unresolvable; the graph content
is still restorable.

## 5. Explicit configuration migration

Migrations are pure, deterministic, side-effect-free functions over the
revision's graph document:

1. **Format versioning**: the revision envelope carries `format_version`
   (independent of the graph's wire `version`, which stays `1` while the
   graph shape is v1). A future format change bumps `format_version` and
   ships an explicit migration `v(n) → v(n+1)`.
2. **Preview (dry-run)**: `migrate(revision, toFormat, dryRun: true)`
   returns the deterministic diff — per-node changed fields under the
   NX-C0 §7.1 diagnostic shape (`nodeId`, `fieldPath`, `expected`,
   `actual`) plus the would-be `graph_digest` — and performs **no write**
   of any kind.
3. **Apply**: `migrate(..., dryRun: false)` validates the migrated document
   against the target format's full validation (the same fail-closed decode
   as any graph) and, on success, appends a **new revision** with
   `format_version = toFormat`, `migration_json` set, and the original
   revision as `parent_revision_id`. History is never rewritten in place.
4. **Idempotency**: applying a migration to a revision whose
   `format_version` already equals `toFormat` returns the existing
   revision unchanged (no duplicate). A failed apply (target validation
   rejects) leaves no partial revision, no plan association, and no
   side effect.
5. **Failure interruption**: because apply is a single append transaction,
   interruption cannot produce a partial state; the original revision is
   intact and the apply is retryable.
6. **Future-version rejection**: opening or compiling a revision whose
   `format_version` is greater than this binary knows fails closed with a
   frozen `invalidRequest`-class error ("graph revision format is newer
   than this service"); no best-effort decode (NX-C0 X-6 analog).
7. **Downgrade**: not supported. A migrated revision is never auto-reverted;
   clients that need the old shape read the parent revision.

## 6. Database upgrade, backup, and rollback

1. **Migration**: one additive numbered migration
   (`PRAGMA user_version` 12 → 13) executing
   `CREATE TABLE cp_graph_revisions (...) STRICT` plus its indexes. No
   existing table is altered, no row is rewritten, no backfill runs: the
   migration is trivially resumable and idempotent under the existing
   transactional bootstrap.
2. **Old binaries**: the existing `user_version` gate makes an older binary
   refuse a newer database (current behavior, unchanged).
3. **Backup/restore**: `STORAGE_SCHEMA_VERSION` becomes 13; the existing
   manifest gate (`backup.rs:96`) then refuses a pre-13 archive restored
   into a post-13 binary until re-exported — the documented,
   already-operational discipline. Revisions are ordinary rows in the
   backup archive; restore is all-or-nothing as today.
4. **Rollback**: the sanctioned path is restore-from-backup (existing
   mechanism). Dropping revisions in place is not a supported rollback.
5. **Old graphs**: graphs compiled before this feature have no revisions
   and none are invented. Where a historical PlanVersion's graph is
   unknown, the association is simply absent (nullable by design); no
   "unrecoverable metadata" marker row is fabricated. Published plans are
   unaffected either way.

## 7. Retention and reference integrity

- Revisions are never deleted by this contract. Workspace deletion follows
  the existing FK cascade behavior; asset or connection deletion does not
  delete revisions — the read path reports unresolvable bindings (§4.4).
- Retention policies (existing) may archive *plans*; revisions are not
  inputs to retention and stay intact.
- `plan_version_id` references are validated by FK; a PlanVersion is never
  hard-deleted while referenced (existing lifecycle uses state transitions,
  not deletes).

## 8. Cross-repo DTO handoff (no Openship changes)

The API surface NX-V1 adds (save/fetch/history/migrate) is
transport-neutral JSON under the existing envelope and error contract
(NX-C0 §3, NX-A1 §7). Everything Openship needs to render an editor — the
graph document, revision numbers for CAS, drift markers, migration diffs —
is carried by those payloads. StillFlow does not persist canvas position,
zoom, comments, or any UI-only state; if Openship needs them, they live in
Openship-owned storage and are keyed by `graphId` + revision number.

## 9. Objective acceptance matrix for #342

| Acceptance item | Frozen evidence |
| --- | --- |
| Version compatibility matrix, permissions/CAS conflicts, migration diffs, duplicate migration, failure interruption | §4.1, §4.4, §5 (each mechanically testable) |
| Published plans do not require the original graph/package | §4.3, §6.5 |
| Old database upgrade, backup/restore, rollback, retained references | §6, §7 |
| No SQL/Rust/client code in this delivery | This PR touches only `docs/issues/` and `docs/contracts/` |

## 10. Downstream entry criteria (NX-V1 #343)

NX-V1 may start only after this contract is accepted and merged and
dependencies (#335, #340 → #341 NX-N2, #338) are merged. Frozen paths:
`backend/crates/stillflow-storage/src/` (one migration, revision store,
backup manifest version), `backend/crates/stillflow-core/src/` (revision
contracts), `backend/crates/stillflow-api/src/` + `stillflow-service/src/`
(revision/migration endpoints under the existing envelope), and tests.
Must-not: any second Job/Run/publication path, any rewrite of history, any
auto-migration, any Openship change, any execution-semantics change.

## 11. Explicit non-goals

Multi-workspace or shared graphs; branching revision trees (linear history
only, `parent_revision_id` is bookkeeping); merge tooling; server-side
draft autosave; runtime package deployment changes; DAG/multi-source
graphs; deleting or rewriting published plans; migrating published plans;
schema inference changes; any change to Openship production code.
