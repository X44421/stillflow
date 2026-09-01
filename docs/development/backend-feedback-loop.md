# Backend development feedback loop (DX-F0)

Issue: [#85](https://github.com/X44421/stillflow/issues/85) · Test-gate policy update:
[#204](https://github.com/X44421/stillflow/issues/204) · Parent roadmap:
[#81](https://github.com/X44421/stillflow/issues/81)

This guide describes the layered local feedback loop for backend (Rust)
development:

```
edit/save → compiler diagnostics → focused serial test → full local gate
         → independently identifiable GitHub checks
```

Scope note: DX-F0 is developer-experience infrastructure only. It is **not**
the product Live Event Stream tracked by E5-E1 under the #81 roadmap, and it
is **not** production observability (tracing/metrics/OpenTelemetry) tracked by
OPS-O1. Those require their own delivery nodes after their write surfaces
stabilize.

## 1. Installation

The loop is driven by [Bacon](https://dystroy.org/bacon/) watching the
repository; all jobs are declared in the version-controlled root
[`bacon.toml`](../../bacon.toml).

```sh
cargo install bacon --locked   # one-time, per machine
bacon --version                # any recent bacon 3.x works
```

VS Code setup:

1. Install the **rust-analyzer** extension (`rust-lang.rust-analyzer`).
   It uses `rust-toolchain.toml`, so the editor picks up Rust 1.85.0
   automatically.
2. Open the repository root (not `backend/`) so `rust-analyzer` and file
   watchers resolve the workspace the same way Bacon does.
3. Run Bacon in the integrated terminal: `bacon` (default job = fast check).
   Split the terminal (`Ctrl+Shift+5`) to keep tests or logs beside it.
4. Optional: keep a second terminal on `bacon clippy` while working on a
   lint-sensitive change. Do not run two jobs writing the same `target/`
   directory simultaneously with different toolchains; prefer one watcher at
   a time.

## 2. Latency tiers — which job when

| Tier | Job | Command | Expected latency | Use for |
| --- | --- | --- | --- | --- |
| 0 | `check` (default) | `bacon` / `cargo check --manifest-path backend/Cargo.toml --workspace --all-targets` | seconds after first build | immediate compiler diagnostics on save |
| 0 | `check-msrv` | `bacon check-msrv` | like `check` + toolchain switch | confirming MSRV (Rust 1.85.0) compatibility before push |
| 1 | `test-engine` | `bacon test-engine` | fast-minutes | focused `stillflow-engine` library test feedback |
| 2 | `fmt` | `bacon fmt` | seconds | formatting gate before commit |
| 2 | `clippy` | `bacon clippy` | ~minutes, cached | lint gate before commit |
| 3 | `test-workspace` | `bacon test-workspace` | slowest | full local gate before opening/updating a PR |

Rules of thumb: iterate on tier 0–1, then run only the broader gate that adds
new evidence for the change. Do not mechanically run focused, crate, workspace,
and CI suites back-to-back when a later superset already proves the same
behavior. Tier 3 mirrors the canonical MSRV semantic test leg; CI also adds
stable compatibility and newer-Clippy drift checks.

## 3. Exact commands

All jobs address the workspace through `--manifest-path backend/Cargo.toml`
so the root-level `bacon.toml` works from the repository root.

```sh
# Tier 0 — compiler diagnostics (default job, non-mutating)
bacon
cargo check --manifest-path backend/Cargo.toml --workspace --all-targets

# Tier 0 — MSRV check on Rust 1.85.0 regardless of active default
bacon check-msrv

# Tier 1 — focused Engine library tests, strictly serial
bacon test-engine
cargo test --manifest-path backend/Cargo.toml -p stillflow-engine --lib -- --test-threads=1

# Tier 2 — local lint/format gates
bacon fmt
cargo fmt --manifest-path backend/Cargo.toml --all -- --check
bacon clippy
cargo clippy --manifest-path backend/Cargo.toml --workspace --all-targets -- -D warnings

# Tier 3 — full local MSRV-style semantic gate
bacon test-workspace
cargo test --manifest-path backend/Cargo.toml --workspace -- --test-threads=1
```

Every job is read-only by contract: no auto-fix flags, no lockfile updates,
no retries, no hidden warnings, no wall-clock/random inputs. If you need an
apply-mode format, run `cargo fmt` manually and review the diff.

## 4. Evidence layering and CI policy

Use the smallest gate that produces new information:

1. **Focused tests** are for edit-time feedback and reproduction. Once a
   broader exact-head suite containing the same tests has passed, rerunning the
   focused subset is not an additional acceptance proof.
2. **Affected-crate tests** are useful before handoff when the change is
   localized and the full workspace gate has not yet run.
3. **Exact-head PR CI** is the canonical repository-wide regression evidence.
   On Rust 1.85.0 it runs fmt, Clippy, and the full serial workspace test suite.
4. **Stable compatibility** is intentionally narrower: compile/check the full
   workspace and run stable Clippy to catch language/toolchain and lint drift.
   Stable does not duplicate rustfmt or the full semantic test suite on every
   PR.
5. **Independent acceptance** should consume exact-head CI evidence and add
   adversarial, contract, race, fail-closed, or missing-edge checks. It should
   not rerun the same full workspace suite without a specific reason.
6. **Private and measurement-only features** are not automatically mandatory
   for unrelated tasks. Use `--all-features` only when the task changes those
   surfaces or in a dedicated integration/release/nightly gate.
7. **Large physical boundary tests** that depend on filesystem-scale behavior
   should live in a dedicated slow/release lane once such a lane exists. Keep
   routine tests focused on the boundary algorithm itself. Issue #204 changes
   policy only; it does not move or weaken any existing Rust test.

The current PR CI backend matrix is therefore:

- Rust 1.85.0: fmt + Clippy + full serial workspace tests;
- stable: workspace compatibility check + Clippy;
- frontend: unchanged.

This policy reduces duplicated evidence; it does not reduce contract coverage.

## 5. Serial-test constraints (E3/E4)

Current Engine and storage fixtures rely on **global-state discipline**:
shared environment handles, fixed fixture paths, ordered acceptance windows.
Therefore:

- Always pass `--test-threads=1` to `cargo test`; both Bacon jobs already do.
- Do not introduce `cargo-nextest` or any process-level parallel runner in
  this phase — its isolation model is unproven against these fixtures.
- A test that passes alone but fails under default parallelism usually means
  leaked global state, not a flaky test; fix the fixture ownership, do not
  add retries.
- If a new test needs isolation, scope its state explicitly instead of
  relying on thread ordering.

## 6. Classifying failures

Before touching anything, classify the failure into exactly one bucket:

| Bucket | Symptom | First response |
| --- | --- | --- |
| Compiler | `error[E...]` from `cargo check` in your edited crate | yours; fix the code, tier 0 reruns automatically |
| Contract test | failing assertion in a test that pins an accepted public contract | stop; contracts are frozen — return to contract review instead of "fixing" the test ([workflow](ai-development-workflow.md)) |
| Pre-existing baseline | same failure on the exact base commit without your changes | verify with A/B: `git stash && cargo test ...` (or a scratch worktree of base), then report it in the PR; never suppress or delete it |
| Environment | missing toolchain component, sandbox/network denial, out-of-disk, `rustup` target absent | fix the environment; report unavailable tools as limitations, never as passes |
| CI-only | green on tier 3 locally, red in GitHub Actions | compare job log vs local command; typical causes are cache-cold builds, newer stable toolchain than local, or runner-specific paths |

Escalation rule: anything that looks like a contract violation, a needed
public-contract change, or a baseline regression goes back to issue/contract
review — do not patch around it inside a DX task.

## 7. Record base/head before every push

The coordination registry verifies exact head bindings, so record:

```sh
git fetch origin main --quiet
git rev-parse origin/main        # base your branch must rebase onto (record)
git rev-parse HEAD               # the head you are about to push (record)
git log --oneline origin/main..HEAD   # what the push adds
```

Paste both SHAs into the PR/task report before pushing. Re-fetch immediately
before the push itself; if `origin/main` moved past your recorded base,
rebase only when authorized by the dispatch protocol — never silently.

## 8. Troubleshooting watchers and stale diagnostics

- **Bacon not rerunning on save**: confirm the editor saves files inside the
  repository Bacon watches (launch Bacon at the repo root). On WSL2, files
  under `/mnt/*` often break inotify events — clone the repo into the Linux
  filesystem instead.
- **"Too many open files"/watch limit hit** (Linux): raise the inotify watch
  limit (`fs.inotify.max_user_watches`) or narrow what you keep open in the
  editor.
- **Stale diagnostics after switching branches/toolchains**: restart Bacon;
  if output still contradicts a clean build, clear the affected package
  cache with `cargo clean -p <crate>` (run from `backend/`) rather than
  wiping the whole `target/`.
- **Diagnostics disagree with CI**: check the toolchain first
  (`rustc --version`, `rustup show`); `rust-toolchain.toml` pins 1.85.0 for
  local tools while CI also runs stable compatibility-check and Clippy legs —
  reproduce the relevant matrix leg with `bacon check-msrv` or the stable
  toolchain explicitly.
- **Two watchers fighting over `target/`**: kill the extra Bacon; lockfile or
  artifact contention between concurrent cargo invocations produces
  misleading errors that look like code problems.
