# XR task-ID reconciliation

> Status: Accepted (governance record)
> Date: 2026-09-05
> Charter: [#287](https://github.com/X44421/stillflow/issues/287) (GOV-XR1), under umbrella
> [#93](https://github.com/X44421/stillflow/issues/93), ledger [#81](https://github.com/X44421/stillflow/issues/81)
> Authority resolved: the conflict between #93's delivery topology and
> [ADR-002 §10](adr-002-deterministic-runtime-and-physical-executors.md) as merged in PR #98.

This note is the single canonical mapping for every XR-series task ID. Where any
older document disagrees with this registry, this registry wins; historical
text is preserved and mapped through §4 (alias map), never silently rewritten.

## 1. Inventory of definitions found

Sources examined: #93 (umbrella), #81 (ledger, F-RUN1 sequence),
`docs/development/backend-completion-execution-checklist.md` (diagram + node
list), `docs/issues/execution-backend-coupling-inventory.md` (XR-D0
deliverable), ADR-002 as merged in PR #98, and the GitHub issue/PR search
below. Issues/PRs whose titles mention XR: #94, #95 (XR-D0), #97, #98
(XR-C0), #287 (this task). No issue or PR body charters XR-A1, XR-S1, or
XR-D1 implementation; no XR implementation code exists on `main`.

| ID | #93 / #81 / checklist say | ADR-002 §10 (PR #98) says | Status of the underlying work |
| --- | --- | --- | --- |
| XR-D0 | execution coupling inventory | (not assigned; cited as factual input) | **Accepted fact** — PR #95 merged, issue #94 closed |
| XR-C0 | runtime abstraction contract / ADR-002 | (the charter of ADR-002 itself) | **Accepted fact** — PR #98 merged, issue #97 closed (ADR-002 header still reads "Proposed"; see §6) |
| XR-R0 | behavior-preserving PolarsExecutor extraction | zero-observable-change extraction | **Agree in substance** — no conflict; not started |
| XR-R1 | backend conformance harness | executor conformance harness | **Agree in substance** — no conflict; not started |
| XR-A1 | **Arrow-native pilot** | **typed effect/worker boundary** | **CONFLICT** — never implemented, never chartered |
| XR-S1 | **SQL pushdown** | **selection, fallback, and provenance** | **CONFLICT** — never implemented, never chartered |
| XR-D1 | **DuckDB physical executor integration** | **danger-matrix differential corpus** | **CONFLICT** — never implemented, never chartered |
| XR-G1 | multi-executor gate | automatic-selection gate | **Agree in substance** — no conflict; not started |

ADR-002 §10's own scope note claimed that "no charter for XR-A1, XR-S1, or
XR-D1 exists anywhere on `main`". That was written against the repository
tree only and is factually stale with respect to the accepted program
documents: #93 (umbrella), #81 (ledger), and the checklist all define the
three IDs, and they agree with each other.

## 2. Decision

**The #93 / #81 definitions are canonical for XR-A1, XR-S1, and XR-D1.**

Rationale:

1. Three accepted program documents (#93, #81, checklist) agree with one
   another; only ADR-002 §10 differs.
2. ADR-002 §10 self-describes its scopes as assignments "by this ADR as
   decisions", explicitly deferring to each task's future charter PR to
   "match or supersede by explicit reference". #93 chartered the program that
   produced ADR-002; its topology is the senior authority.
3. None of the three conflicting scopes was ever implemented, chartered, or
   referenced by any other artifact, so renumbering ADR-002's internal trio
   is zero-cost today and permanently removes the ambiguity.

ADR-002's three §10 nodes therefore receive new unique IDs (§4). Every
occurrence inside ADR-002 has been renamed in the same change that accepts
this note, and §10 now defers XR-A1/XR-S1/XR-D1 to #93 explicitly.

## 3. Canonical topology

Ordering follows #93 §Sequencing with ADR-002 §10's refined entry/exit
glasses. Every node below is HOLD until its entry conditions are met and a
separate charter issue/PR is dispatched; nothing here authorizes
implementation.

| Order | ID | Node | Charter text | Entry | Outcome (one line) | Exit gate | Status |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | XR-D0 | execution coupling inventory | #93 / #95 | — | inventory doc | accepted against exact SHA | **Done** (PR #95) |
| 0 | XR-C0 | runtime abstraction contract / ADR-002 | #93 / #97 | XR-D0 | ADR-002 | architecture review | **Done** (PR #98); see §6 |
| 1 | XR-R0 | behavior-preserving PolarsExecutor extraction | #93 + ADR-002 §10 | ADR-002 Approved; no lock conflict | private executor seam, zero observable change | unchanged test battery (ADR-002 §10) | HOLD |
| 2 | XR-B1 | danger-matrix differential corpus *(alias: ADR-002 draft "XR-D1")* | ADR-002 §10 (renamed) | XR-R0 merged | executable fixtures for XR-D0 §6.4 gaps 2–9 | deterministic fixtures, no behavior change | HOLD |
| 2 | XR-R1 | backend conformance harness | #93 + ADR-002 §10 | XR-R0 merged; XR-B1 corpus green (or co-delivered) | per-level (L0–L3) evidence reports | version-pinned, reproducible evidence | HOLD |
| 3 | XR-A1 | Arrow-native pilot | #93 (§Delivery topology) | XR-R1 harness available | narrow deterministic second-executor subset proves abstraction | conformance evidence at declared level | HOLD |
| 3 | XR-S1 | SQL pushdown | #93 (§Delivery topology) | XR-R1 harness; XR-C0 contract | safe Scan/Projection/Filter/Limit fragments | explicit dialect behavior; unproven fragments stay canonical | HOLD |
| 3 | XR-D1 | DuckDB physical executor integration | #93 (§Delivery topology) | XR-R1 harness; XR-C0 contract | federation/joins/preview SQL under shared contracts | no second cleaning-rule language | HOLD |
| 4 | XR-P1 | selection, fallback, and provenance *(alias: ADR-002 draft "XR-S1")* | ADR-002 §10 (renamed) | XR-R1 harness; XR-B1 corpus green; ADR-002 Approved | §3 selection machinery + §4 provenance record | first gated observable change; single executor still | HOLD |
| 4 | XR-E1 | typed effect/worker boundary *(alias: ADR-002 draft "XR-A1")* | ADR-002 §10 (renamed) | ADR-002 Approved; AGENTS risk-gate contract note | §8 effect/worker contract implemented | no effect path touches bulk cleaning | HOLD |
| 5 | XR-G1 | multi-executor gate | #93 + ADR-002 §10 | ≥2 executors with unexpired XR-R1 evidence; XR-P1 shipped | enable automatic selection | purity property test, fail-closed checks, kill switch | HOLD |

Notes:

- XR-B1 keeps ADR-002 §10's full node text (entry/outcome/stop conditions)
  verbatim except for the renamed identifier; same for XR-P1 and XR-E1.
- XR-A1/XR-S1/XR-D1 keep #93's scope text; ADR-002 §10's machinery (§3
  selection law, §5 equivalence levels, §7 danger matrix) binds them as it
  binds every executor node — the danger-matrix corpus they must pass is
  XR-B1.
- GPU, remote, and distributed executors remain deferred until after XR-G1
  (#93 §Sequencing), unchanged.

## 4. Alias / supersession map

Historical references to ADR-002 as merged in PR #98 must be translated as
follows. The pre-rename text remains readable in PR #98's diff; nothing was
rewritten there.

| Historical ID (ADR-002 as merged in PR #98) | Canonical ID | First canonical reference |
| --- | --- | --- |
| XR-D1 (danger-matrix differential corpus) | **XR-B1** | this note + ADR-002 §10 |
| XR-S1 (selection, fallback, and provenance) | **XR-P1** | this note + ADR-002 §10 |
| XR-A1 (typed effect/worker boundary) | **XR-E1** | this note + ADR-002 §10 |

Namespace disambiguation (no conflict, recorded to prevent future confusion):

- the XR series = executor-runtime program (this registry, umbrella #93, ADR-002).
- `X-R1` = export runtime (ADR-004 §§2–8, issue #184, closed) — **not** an
  XR-node; `XR-R1` (conformance harness) and `X-R1` (export runtime) are
  unrelated tasks in different namespaces.
- `X-A1` = export-program task in the ADR-004 namespace — unrelated to
  `XR-A1` (Arrow-native pilot).

No other XR identifier has ever been used in this repository's docs, issues,
or PRs (mechanically checked via the §7 grep and GitHub
search on 2026-09-05); the complete set is {XR-C0, XR-D0, XR-R0, XR-R1,
XR-A1, XR-S1, XR-D1, XR-G1} plus the three new IDs {XR-B1, XR-P1, XR-E1}.

## 5. What this decision does not do

- It does not approve ADR-002's status header question away (§6) and does not
  dispatch, charter, or unblock any implementation node. Every node in §3
  remains HOLD behind its explicit entry gate.
- It does not reinterpret any cleaning semantics, select backends, or touch
  code, dependencies, or public API.
- It does not rewrite #81, the checklist, or the XR-D0 inventory; all three
  already match the canonical mapping.

## 6. Observations recorded for follow-up (no action in this task)

1. ADR-002's header still reads `Status: Proposed` even though its charter
   issue #97 is closed and PR #98 merged. Whether ADR-002 is formally
   "Accepted" is an ADR-governance decision outside this task's charter; XR-R0's
   entry condition ("this ADR Approved") remains unsatisfied either way until
   that status is settled explicitly.
2. ADR-002 §9 names a future AGENTS.md governance-alignment task (frozen
   rules 4 and 5). That task is still unchartered; it is **not** given an
   XR-series ID here because it is not an executor-runtime delivery node. If the
   #93 umbrella wants it in the topology, it must be added by a future
   charter with a fresh unique ID.

## 7. Mechanical checks

- `git diff --check` clean for the docs change introducing this note and the
  ADR-002 rename.
- `grep -rohE "XR-[A-Z0-9]+" docs/ | sort -u` after the change yields exactly
  {XR-A1, XR-B1, XR-C0, XR-D0, XR-D1, XR-E1, XR-G1, XR-P1, XR-R0, XR-R1}.
- Within ADR-002, the IDs `XR-D1`, `XR-S1`, and `XR-A1` occur only in the
  header task-ID registry pointer and the explicit #93-deferral scope note in
  §10 (both intentionally denote the umbrella's canonical scopes).
