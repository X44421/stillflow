# AI Development Workflow

> Status: Active
> Last updated: 2026-07-28

This document defines how Sol (contract + review) and Composer (implementation) collaborate without sharing long chat history or editing the same worktree concurrently.

## Control flow

```text
GitHub Issue
    ↓
Sol → Implementation Contract (medium/high risk)
    ↓
Composer → independent branch
    ↓
Draft PR + CI
    ↓
Sol reviews diff only
    ↓
Composer fixes BLOCKER / IMPORTANT
    ↓
CI green + Sol final confirmation
    ↓
Merge to main
```

`main` is the only confirmed source of truth.

## Repository artifacts

| File | Audience | Purpose |
| --- | --- | --- |
| [`AGENTS.md`](../../AGENTS.md) | Sol, Composer | Permanent engineering constraints |
| [`.cursor/rules/stillflow-engineering.mdc`](../../.cursor/rules/stillflow-engineering.mdc) | Composer | Auto-loaded implementation rules |
| `docs/development/ai-development-workflow.md` | Humans + agents | This workflow |
| `docs/issues/issue-NNN-implementation-contract.md` | Sol → Composer handoff | Scoped contract for medium/high risk issues |
| [`.github/pull_request_template.md`](../../.github/pull_request_template.md) | PR author | Required PR sections |

Low-risk work (single crate, frozen interfaces, no new deps) does **not** need a contract file.

## Risk labels

Apply GitHub labels:

- `risk:low`
- `risk:medium`
- `risk:high`

### Routing

| Level | When | Flow |
| --- | --- | --- |
| Low | Single crate, no new deps, interfaces frozen | Composer → CI → merge |
| Medium | 2–3 crates or new adapter | Composer → Sol review → Composer fix → merge |
| High | Public trait, streams, secrets, concurrency, lifecycle | Sol contract → Composer → Sol review → fix → Sol final → merge |

### Auto-upgrade to high

Any issue that:

- Modifies `SourceConnector` or core domain types
- Spans three or more crates
- Adds a core dependency
- Touches Arrow streams, cancellation, timeouts, or backpressure
- Touches secrets, PII, or authorization
- Touches Dataset / Snapshot / Checkpoint lifecycle
- Changes the Polars / DuckDB responsibility boundary

## Phase 1 — Sol generates contract

Prompt:

```text
为 Issue #N 生成 Implementation Contract。
只分析，不修改代码。
基于 main 最新代码和已冻结接口。
```

Contract must include:

- Risk level and rationale
- Goals and non-goals
- Allowed files / crates
- Frozen types (must not change)
- Error, cancellation, and resource semantics
- Test matrix
- Allowed new dependencies
- Stop conditions

Output: `docs/issues/issue-NNN-implementation-contract.md`

## Phase 2 — Composer implements

Branch naming:

```text
agent/issue-NNN-short-description
```

Prompt:

```text
Issue body
+
Implementation Contract
+
Current branch name
```

Composer completion report:

```text
Modified files
New dependencies
Public API changes
unwrap / expect
TODO
Test results
Contract deviations
Remaining risks
Branch name
Commit SHA
```

## Phase 3 — Draft PR

Use the PR template. Minimum:

```markdown
Closes #N

## Contract
docs/issues/issue-NNN-implementation-contract.md

## Scope
## Dependency changes
## Public API changes
## Error and cancellation semantics
## Tests
## Contract deviations
## Remaining risks
```

Composer pushes the branch. Sol opens or reviews the PR on GitHub.

## Phase 4 — Sol reviews diff

Prompt:

```text
按照 Issue #N 的 Implementation Contract 审查这个 PR。
只输出 BLOCKER、IMPORTANT、FOLLOW-UP。
```

Review focus:

- Crate dependency direction
- Third-party type leakage across boundaries
- Error handling and resource cleanup
- Async cancellation and backpressure
- Unnecessary dependencies
- `Arc` / `Box` / `clone` abuse
- Boundary and failure-path tests
- Unauthorized file changes
- Frontend UI changes

## Phase 5 — Composer fixes

- Fix **BLOCKER** and **IMPORTANT** only.
- **FOLLOW-UP** → new GitHub Issue; do not expand current PR.
- Maximum two fix rounds. If the same issue fails twice, return to Sol for contract or design revision.

## CI gate

Current required checks (`.github/workflows/ci.yml`):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm run build
```

Planned (separate issue — do not block workflow rollout):

```bash
cargo audit
cargo deny check
cargo tree --duplicates
```

## Issue roadmap (Phase 1 ingestion)

| Issue | Risk | Notes |
| --- | --- | --- |
| #5 Arrow connector contracts | High | Freezes trait, stream, errors — **merged before adapters** |
| #6 Polars local files | Medium | Must not modify frozen #5 types |
| #7 Calamine Excel | Medium | Sheet/region semantics |
| #8 object_store | High | Secrets, range reads, streams |
| #9 SQLx | High | Credentials, cursors, type mapping |
| #10 DuckDB | High | Polars/DuckDB boundary |
| #11 End-to-end API | High | Full object lifecycle |

### Issue #6 stop condition

> If implementation requires changing `SourceConnector`, `ReadRequest`, `BatchStream`, or core error types — stop and return to Sol.

## Principles

- **Issue** = task entry
- **Implementation Contract** = inter-model protocol
- **PR diff** = review object
- **CI** = completion gate
- **main** = confirmed fact
