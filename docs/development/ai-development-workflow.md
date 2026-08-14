# Contract-first development workflow

> Status: Accepted
> Last updated: 2026-08-14

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
4. Create `agent/issue-NNN-short-description` from the accepted base.
5. Use one issue per implementation branch. A docs-only governance branch may
   group tightly coupled contracts if it names each issue and changes no runtime
   behavior.
6. For stacked delivery, the second PR targets the first PR's branch until the
   base merges. Rebase/rebuild it from `main` afterward.

This policy makes authorship, accepted state, and rollback boundaries observable.

## 3. Risk routing

### Low risk

Examples: comments, private refactors in one crate, test-fixture additions.

Flow: issue -> implementation -> CI.

### Medium risk

Examples: additive private module, new adapter behind an existing interface,
bounded performance change.

Flow: contract note -> implementation -> review -> CI.

### High risk

Examples: public traits or domain models, schemas, expression/rule ASTs, plan
serialization, streams, cancellation/backpressure, persistence, secrets, or
three or more crates.

Flow: issue -> frozen Implementation Contract -> implementation -> architecture
review -> fixes -> CI -> final contract check.

## 4. Contract format

A high-risk contract under `docs/issues/` must contain:

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

1. Restate the base SHA, issue, contract, scope, and non-goals.
2. Inspect the actual code; do not assume an architectural document is already
   implemented.
3. Make the smallest coherent change that establishes one invariant.
4. Add tests with the invariant, not in a later cleanup commit.
5. Run the narrowest check first, then workspace-wide checks.
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
cd backend && cargo test --workspace
npm run typecheck
npm run build
```

When a local environment lacks a tool, mark that check **not run** and require the
corresponding GitHub check before merge. An unavailable tool is never a passing
result.

## 8. Pull request protocol

The PR body must:

- link the issue and frozen contract;
- name the exact base branch;
- enumerate public and dependency changes;
- report checks individually;
- list `unwrap`/`expect`, TODOs, deviations, and remaining risks;
- say whether historical branches were consulted;
- remain draft until required CI and architecture review pass.

Reviewers verify the diff, not the branch narrative. A later implementation PR
must not silently relax a contract merged by an earlier docs PR.

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
6. **Engine E1 — execution contract**: Issue #46 docs-only freeze of deterministic
   single-source Polars execution. PR #47 remains unapproved until revision R3
   passes a fourth architecture review. Do not start the executor in the same PR.
   E2, once approved, must chunk before Polars; live engine memory is connector
   envelope + complete Polars working set + canonical remainder + 5 MiB state
   (peak 197 MiB). It must not transform a whole connector envelope and split
   afterwards.
7. **Engine E2 — streaming executor**: after E1 approval, rebuild from latest
   `main`. Connector envelope → execution chunker → Polars → canonical
   rebatcher → atomic Snapshot. No Dependabot mix-in. No Join/Union/DuckDB/SQLx/API.
8. **Engine E3–E5**: node-level Preview, Validate/Rejected Rows/Deduplicate, then
   job runtime and Axum. DuckDB (#10) and SQL Connector (#9, Post-MVP) stay
   outside this sequence until their own contracts.

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
- the observed repository base differs from the documented base.

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
