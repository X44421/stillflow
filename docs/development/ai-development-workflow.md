# Contract-first development workflow

> Status: Accepted
> Last updated: 2026-09-01

This workflow separates architectural decisions from implementation so parallel
or automated work cannot silently invent incompatible public contracts.

## 1. Roles

The names describe responsibilities, not particular tools.

| Role | Responsibility | May write runtime code? |
| --- | --- | --- |
| Contract owner | Turns an issue into testable scope, invariants, and non-goals | No, while freezing the contract |
| Implementer | Changes only what the frozen contract authorizes | Yes |
| Architecture reviewer | Checks dependency direction, semantics, and deviations | Review fixes only |
| CI | Repeats mechanical verification on a reproducible toolchain | No |

One person or agent may perform multiple roles sequentially, but the contract
must be frozen before high-risk implementation begins.

## 2. Source and branch policy

1. Fetch and identify the latest accepted `main` commit.
2. Treat all unmerged historical branches as read-only research material.
3. Reconstruct useful ideas in new commits; do not merge or cherry-pick history.
4. L1-L3 use one canonical Issue and `agent/issue-NNN-short-description`.
   L0 may use a short descriptive branch without creating a task Issue.
5. A docs-only governance branch may group tightly coupled policy changes when
   it names the affected Issues and changes no product runtime.
6. For stacked delivery, the second PR targets the first PR's branch until the
   base merges. Rebase/rebuild only when semantic/path overlap requires it.
7. E3-C0 (Issue #50) was explicitly independent of PR #49. It is approved
   at SHA `d2809de294bb16ae8fe11f425a4f910ec2ed43cc`, merged in PR #51 as
   `main@da3d03b`, and its contract branch was deleted. PR #49 merged as
   `main@85502cb`; the E3 runtime gate is now open under Issue #52.

This policy makes authorship, accepted state, and rollback boundaries observable.

## 3. Risk routing

Use the lowest level that matches the changed authority surface.

### L0 — trivial

Examples: typo, comments, wording, metadata-only cleanup, CI job naming.

Flow: branch -> PR -> relevant CI -> merge.

No task Issue or Registry row is required.

### L1 — low

Examples: test infrastructure, internal refactor, CI behavior, isolated private
change.

Flow: Issue -> PR -> relevant CI -> normal PR review -> merge.

No Registry row is required.

### L2 — medium

Examples: bounded private runtime behavior, performance path, or implementation
touching a shared writer surface without changing a frozen public contract.

Flow: Issue -> scoped CLAIM/lock -> Draft PR -> exact-head CI -> independent
GitHub PR Review -> merge.

### L3 — high

Examples: public traits/domain models, schemas, expression/rule ASTs, plan
serialization, cancellation/backpressure semantics, persistence formats,
secrets, or cross-crate authority changes.

Flow: frozen Implementation Contract -> scoped CLAIM/lock -> Draft PR ->
exact-head CI -> independent GitHub PR Review -> guarded merge.

Only L2/L3 belong in `coordination/task-registry`.

## 4. Contract format

An L3 contract under `docs/issues/` must contain:

- objective and risk classification;
- authorized public changes and compatibility decision;
- exact in-scope files or crates;
- explicit non-goals;
- invariants and mathematical laws;
- bounds for rows, bytes, memory, concurrency, and time;
- error, cancellation, and security semantics;
- ordered implementation checklist;
- objective acceptance tests;
- dependency and lockfile changes;
- known risks and stop conditions.

Use normative words consistently: **must** is required, **must not** is forbidden,
and **may** is optional. Avoid acceptance criteria such as “works correctly.”

## 5. Implementation loop

1. Restate the risk level, scope/non-goals, and Issue/contract when the level
   requires them. Record the branch base for L2/L3, but do not treat unrelated
   future main drift as an automatic rebind.
2. Inspect the actual code; do not assume an architectural document is already
   implemented.
3. Make the smallest coherent change that establishes one invariant.
4. Add tests with the invariant, not in a later cleanup commit.
5. Run the narrowest useful check first. Once an exact-head superset gate has passed, do not rerun contained subsets merely to accumulate more PASS labels.
6. Compare the final diff with the contract line by line.
7. Record every deviation; unapproved deviations stop the delivery.
8. Publish an atomic commit and a draft PR with an explicit base branch.

## 6. Mathematical and algorithmic review

Architecture review must reason about laws and resource bounds, not only types.

### Logical types

For supported widening operations, test the least-upper-bound operator `join`:

```text
join(a, b) = join(b, a)                       commutativity
join(join(a, b), c) = join(a, join(b, c))    associativity
join(a, a) = a                                idempotence
```

An unsupported pair must produce a typed error; it must not depend on input order
or silently fall back to text.

### Plans

- DAG validation is `O(V + E)`.
- Canonicalization orders all unordered collections explicitly.
- Node input order is preserved where semantics are positional, such as joins.
- Serialization must not generate IDs, read clocks, or inspect process state.
- Equivalent plans produce identical canonical bytes and fingerprints.

### Data movement

- Streaming memory is `O(batch_size + bounded_operator_state)`.
- Preview has explicit row and byte ceilings and reports truncation.
- Projection and safe predicates are pushed toward a source when supported.
- A fallback that materializes a full source is forbidden in an interactive path.

## 7. Verification matrix

| Change | Required evidence |
| --- | --- |
| Docs/contracts | Links, issue numbers, status, dependency arrows, testable criteria |
| Core/plan | format, Clippy, unit tests, serialization fixtures, invariant tests |
| Connector | capability tests, malicious-path tests, bounded preview/read tests |
| Engine | semantic parity, batch-size invariance, cancellation, memory-bound tests |
| Storage | migrations, transaction tests, atomic publish/recovery tests |
| API | schema tests, status/error mapping, deadline/cancellation tests |
| Frontend touched | typecheck, build, affected interaction tests |

Standard repository checks:

```bash
cd backend && cargo fmt --all -- --check
cd backend && cargo clippy --workspace --all-targets -- -D warnings
cd backend && cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above
npm run typecheck
npm run build
```

When a local environment lacks a tool, mark that check **not run** and require the
corresponding GitHub check before merge. An unavailable tool is never a passing
result.

Evidence is layered by information gain, not by the number of commands run:

- focused tests are development/reproduction feedback;
- affected-crate tests are optional pre-handoff evidence when the full gate has
  not already superseded them;
- exact-head PR CI is the canonical full-workspace regression proof;
- independent acceptance consumes that CI result and adds contract/adversarial
  evidence instead of blindly rerunning the same workspace suite;
- `--all-features` is required only when the changed surface, a frozen
  contract, or a dedicated integration/release/nightly gate requires it.
  Private experimental or measurement-only features are not universal
  per-task boilerplate.

The repository test runner uses normal Rust parallelism. Tests that share
mutable process state must isolate that state themselves; a parallel-only
failure is fixed at the exact fixture, not by globally forcing
`--test-threads=1`.

Routine workspace checks may explicitly skip a named physical-scale test when
that exact test is preserved in a dedicated slow/release workflow. The current
case is `total_output_cap_is_accepted_at_eight_gib_and_enforced_above`, which exercises the real 8 GiB export boundary and is
run by `.github/workflows/slow-boundaries.yml`. This is evidence routing, not
test deletion or semantic weakening.

## 8. Pull request protocol

The PR body should report only information not already obvious from GitHub:

- link the canonical Issue when L1-L3 requires one;
- link the frozen contract for L3;
- enumerate public/dependency changes and deviations;
- report unavailable or scope-specific checks;
- list remaining risks.

GitHub itself is authoritative for PR head, CI status, review state, Ready, and
merge state. Do not duplicate those facts across Issue comments, Registry rows,
Epic #81, and repository checklists solely as bookkeeping.

For L2/L3, independent acceptance is a GitHub **PR Review** bound to the
reviewed commit. Do not create a separate acceptance Issue for one PR. Before
merge, verify the current head still equals the approved review commit and the
required exact-head CI is green.

Main drift requires rebind only when it overlaps the task's authorized paths,
declared shared dependency/contract surfaces, creates a merge conflict, or
changes a semantic assumption. A new main SHA by itself is not a rebind reason.

Completion uses native authority:

- Issue = scope/lifecycle;
- PR = head/CI/review/merge;
- Registry = active L2/L3 writer locks only;
- Epic #81 = roadmap/dependencies only;
- repository checklist = completion definition/history only.

Release active Registry claims at completion. Do not create extra board,
checklist, PR-body, CI, or merge-SHA synchronization work merely to copy GitHub
facts.

## 9. Current delivery sequence

The accepted sequence is:

1. **PR0 — governance and contracts**: Issues #15 and #16, accepted architecture,
   ADR, repository rules, local-tabular contract, and Issue #23 plan contract.
2. **PR1 — logical contracts**: stable `LogicalSchema`, typed `Expr`/`Rule` AST,
   validated plan DAG, deterministic serialization and fingerprinting.
3. **PR2 — batch boundary**: versioned `BatchEnvelope`, Arrow 59 adapters, batch
   invariance, cancellation and backpressure.
4. **PR3 — storage**: SQLite metadata, immutable Parquet partitions, atomic
   snapshots, recovery and garbage-collection rules.
5. **PR4 — local tabular**: CSV, TSV, JSON, NDJSON, and Parquet connector under
   the frozen Issue #6 contract. Workbook #7 and object-store #8 follow the same
   connector boundary.
6. **Engine E1 — execution contract**: Issue #46 docs-only freeze, approved at
   `32f1c53` and merged in PR #47. Live engine memory is connector envelope +
   complete Polars working set + canonical remainder + 5 MiB state (peak 197 MiB).
7. **Engine E2 — streaming executor**: Issue #48. Implemented and merged in
   PR #49; approved head `55f663fc46d23186a0ad1d7c711fced1f984a990`, merged
   as `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`. Connector envelope →
   execution chunker → Polars → canonical rebatcher → atomic Snapshot.
   E2 branch and PR are closed; do not reopen or expand E2 operators.
8. **Engine E3–E5**: node-level Preview, Validate/Rejected Rows/Deduplicate, then
   job runtime and Axum. DuckDB (#10) and SQL Connector (#9, Post-MVP) stay
   outside this sequence until their own contracts.

   - E3-C0 (Issue #50): docs-only Preview contract on
     `agent/issue-050-node-preview-contract`. It freezes `PreviewRequest` /
     `PreviewResult`, `target_node_id` cutoff, 1,000/10,000 rows,
     8 MiB/50 MiB bytes, a 100,000-row / 64 MiB raw input scan bound,
     30 s deadline, the shared E2 `MAX_ENGINE_CONCURRENT_RUNS` gate, the
     earliest-prefix truncation/scan/exhaustion flags, read-only/no-Snapshot
     rules, the allocated-capacity response memory law, and the P01–P15
     acceptance matrix. **Approved SHA
     `d2809de294bb16ae8fe11f425a4f910ec2ed43cc`; merged in PR #51 as
     `main@da3d03b`.** E3 runtime is now authorized by Issue #52 from
     `main@85502cb`; implement only `stillflow-engine` Preview runtime.

Do not pull work forward merely because a downstream type is convenient. A
temporary placeholder must remain private and must not become a public contract.

## 10. Stop conditions

Stop and return to contract review if any of these occurs:

- implementation needs a public change absent from the contract;
- a dependency arrow reverses or cycles;
- two engines would own the same cleaning semantics;
- an interactive path becomes unbounded;
- deterministic output depends on unordered state, current time, or random IDs;
- raw credentials or sensitive source values could enter logs/events;
- a required compatibility decision is ambiguous;
- main drift overlaps an authorized/shared semantic surface and the branch has
  not been re-evaluated or rebound.

## 11. Completion report

Use this exact handoff shape:

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
