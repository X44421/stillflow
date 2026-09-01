# Stillflow — Agent Instructions

These rules apply to automated review and implementation in this repository.
The accepted architecture is in
[`docs/data-ingestion-architecture.md`](docs/data-ingestion-architecture.md), and
the delivery workflow is in
[`docs/development/ai-development-workflow.md`](docs/development/ai-development-workflow.md).

## Sources of truth

1. The latest `main` branch is the only implementation base.
2. Historical branches are read-only references. Do not merge or cherry-pick them.
3. A frozen Implementation Contract may authorize a deliberate exception to these
   rules. Record the exception in the PR body.

## Dependency direction

```text
stillflow-api -> stillflow-engine
stillflow-engine -> stillflow-plan, stillflow-connectors, stillflow-storage
stillflow-plan -> stillflow-core
stillflow-connectors -> stillflow-core
stillflow-storage -> stillflow-core
stillflow-core -> no workspace crate
```

`stillflow-storage` is an accepted target boundary and may not exist yet. No
dependency may point from a lower layer back to a higher layer.

## Frozen engineering rules

1. `stillflow-core` contains stable domain contracts only. It must not depend on
   Polars, DuckDB, SQLx, Axum, or an adapter crate.
2. `stillflow-plan` contains deterministic logical schemas, expressions, rules,
   logical plans, validation, and canonicalization. It must not contain physical
   engine objects.
3. Apache Arrow 59 is the bounded execution interchange protocol. Prefer the
   focused `arrow-array` and `arrow-schema` crates over the `arrow` meta crate.
4. Public batches cross execution boundaries in a versioned `BatchEnvelope`; raw
   `RecordBatch` values remain an internal payload. This contract is introduced
   in its own delivery node.
5. Polars is the one canonical cleaning and transformation executor.
6. DuckDB owns bounded preview SQL, federation, joins, and temporary
   materialization. It must not define a second cleaning-rule language.
7. SQLite stores control-plane metadata. Immutable Parquet partitions store
   materialized tabular snapshots. Neither format is an in-memory domain model.
8. Connectors expose capabilities and bounded streams. They must not expose
   Polars `DataFrame` or DuckDB connection objects.
9. AI may interpret metadata and results; it must never become the bulk-data
   execution path.
10. Secrets must not appear in domain objects, logs, events, fixtures, snapshots,
    or serialized API payloads. Persist `CredentialRef` values only.
11. Do not add `clone`, `Arc`, or `Box` merely to suppress ownership errors.
    Explain material ownership choices in the Implementation Contract.
12. Do not change frontend layout, components, CSS, or design tokens unless the
    issue explicitly requests a UI change.

## Contract and risk gates

Use the lowest risk level that matches the changed authority surface. Do not
apply the L3 ceremony to routine work.

| Level | Typical work | Required flow |
| --- | --- | --- |
| `L0` | typo, comments, docs wording, CI naming, metadata-only cleanup | branch -> PR -> relevant CI -> merge |
| `L1` | test infrastructure, internal refactor, CI behavior, isolated private change | Issue -> PR -> relevant CI -> normal PR review -> merge |
| `L2` | private runtime behavior, performance path, bounded implementation touching a shared writer surface | Issue -> scoped CLAIM/lock -> Draft PR -> exact-head CI -> independent PR Review -> merge |
| `L3` | public contracts, persistence/schema, execution semantics, secrets, cross-crate architecture | frozen contract -> scoped CLAIM/lock -> Draft PR -> exact-head CI -> independent PR Review -> guarded merge |

Only L2/L3 work may use `coordination/task-registry`. L0/L1 work must not
create Registry rows merely to mirror GitHub state.

Treat work as L3 when it changes a public trait, core domain type, logical
schema, expression/rule AST, plan serialization, Arrow stream or envelope,
cancellation/backpressure semantics, secret handling, persistence format, or
three or more crates with shared authority.

Breaking a merged public contract is allowed only when all of these are true:

- an open issue states the migration and non-goals;
- a frozen contract names every public breaking change;
- the PR links both documents and reports downstream compile fixes;
- compatibility shims are either implemented or explicitly rejected.

Issue #23 is the authorization to break the contracts merged in #5 for stable
`LogicalSchema`, typed `Expr`, serializable rules, and plan DAGs. `BatchEnvelope`
is intentionally deferred to a later contract.

## Branch and PR conventions

- L1-L3 branches use `agent/issue-NNN-short-description`; L0 may use a short
  descriptive branch without creating a task Issue.
- Prefer one implementation boundary per PR. A docs-only governance PR may group
  tightly coupled policy changes when its body lists the affected Issues.
- Keep commits atomic and make a stacked PR's base branch explicit.
- L3 PRs must link the frozen contract under `docs/issues/`.
- L2/L3 independent acceptance is recorded as a GitHub **PR Review** bound to the
  reviewed commit. Do not create a separate acceptance Issue for a single PR.
  Separate acceptance Issues are reserved for cross-PR, release, or migration
  gates.
- The accepted SHA lives in the PR review/PR state. Do not duplicate it into
  Issue comments, Registry rows, #81, and checklist documents solely as a mirror.
- Do not rewrite or delete another contributor's changes to make a branch clean.

## Required verification

Run the checks relevant to touched files and report any unavailable tool as an
environment limitation, never as a pass:

```bash
cd backend && cargo fmt --all -- --check
cd backend && cargo clippy --workspace --all-targets -- -D warnings
cd backend && cargo test --workspace
npm run typecheck
npm run build
```

For contract or architecture-only changes, also verify links, issue numbers,
dependency arrows, and that every acceptance criterion is objectively testable.

## Coordination authority and completion

Do not maintain duplicate copies of facts GitHub already owns.

Authoritative surfaces:

- **GitHub Issue** — task scope/lifecycle when the risk level requires an Issue.
- **Pull Request** — implementation head, CI, reviews, accepted commit, Ready and
  merge state.
- **`coordination/task-registry`** — active L2/L3 writer/lock claims only.
- **Epic #81** — roadmap/dependency planning only; it is not a live head/CI/lock
  dashboard.
- **Repository checklists** — completion definitions and historical planning,
  not per-task live status mirrors.

### Registry and locks

For L2/L3 work, register only the active claim needed to prevent conflicting
writers. Use the narrowest stable surface lock, for example
`storage:control-plane`, `storage:export`, or `engine:verification`.
Crate-wide locks require L3 or an explicit reason.

Registry mutations use the taskctl compare-and-swap path directly. A separate
coordination PR is not required for claim, heartbeat, head updates, or release.
A CAS conflict is still a hard STOP: re-read state and re-evaluate; never
blind-retry.

### Exact-head review

For L2/L3, the independent PR Review is the exact-head acceptance binding.
Before merge, verify:

1. the PR head still equals the commit approved by the independent review;
2. required CI for that exact head is green;
3. no new commit was added after approval.

Do not copy the same SHA through multiple ledgers as an additional safety gate.

### Main drift and rebind

A changed `main` SHA does not automatically invalidate an open branch. Rebind
only when main drift:

- overlaps the task's authorized paths;
- changes a declared shared dependency or frozen contract surface;
- creates a merge conflict; or
- otherwise changes a semantic assumption used by the task.

Unrelated documentation, another isolated crate, or other non-overlapping drift
does not require a ceremonial rebind/reaccept cycle.

### Completion

A task is complete when its technical acceptance is satisfied and the
authoritative surfaces are correct. Routine completion must not create
coordination work solely to mirror GitHub.

At completion:

- update/close the canonical Issue when one exists;
- use the PR's native state for head, CI, review, and merge facts;
- release any active Registry claim/locks;
- update #81 only when roadmap topology/dependencies/milestone state changed;
- update repository checklist documents only when their completion definition
  or historical planning content changed.

Do **not** require PR-body rewrites, board refreshes, checklist refreshes,
Registry history entries, duplicate merge-SHA receipts, or post-merge CI merely
to restate facts already recorded by GitHub.

Post-merge CI is required only when branch protection/release policy requires
it or the merge commit contains additional code/resolution not present in the
accepted head.

Terminal classes:

- `TASK_COMPLETE` — technical acceptance complete; any active claim released.
- `TASK_BLOCKED` — current authorization/prerequisites cannot complete the task.

The legacy `TASK_COMPLETE_COORDINATION_PENDING` class and
`No coordination update = no DONE` rule are retired.

## Completion report

Every implementation handoff must list:

```text
Modified files
New dependencies
Public API changes
unwrap / expect usage
TODO items
Test results
Contract deviations
Remaining risks
Branch name
Commit SHA
```

Stop and return to contract review if implementation needs an unlisted public
contract change, a new execution engine responsibility, an unbounded operation,
or access to raw credentials.
