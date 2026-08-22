# Frontend Target-Boundary Decision (F0-D0 Slice S0)

| Field | Value |
| --- | --- |
| Canonical tracker | [#79](https://github.com/X44421/stillflow/issues/79) — slice S0 of the F0-D0 plan |
| Plan of record | [frontend-boundary-deprecation-inventory.md](frontend-boundary-deprecation-inventory.md) §7 S0 (merged via PR #83) |
| Base | `main@7c5f163aa2b60a32955e088edc66910a5ce70d1d` |
| Type | Docs-only decision record. No source, dependency, CI, or backend change. |
| Risk | `risk:low` |

## Decision

**RETAIN** the repository-root Vite/React application as the Phase-1 product UI shell. No relocation, no rewrite, and no new frontend architecture contract is opened by this decision.

## Justification (evidence-keyed to the F0-D0 inventory)

1. **It confirms the accepted positioning instead of amending it.** `docs/data-ingestion-architecture.md:422` already keeps the frontend at the repository root during Phase 1. Recording retention as the explicit S0 outcome produces zero contract churn.
2. **Retention costs the backend nothing.** The inventory proved zero frontend→backend coupling at the audited bases (§4.5: no fetch/XHR/WebSocket/API client/proxy anywhere in `src/`; only the jsDelivr CDN fetch inside `@duckdb/duckdb-wasm`).
3. **No alternative surface exists to switch to.** The browser-consumable backend API is not built: E3 node-preview runtime is still unmerged (PRs #53/#71), E4-S2 verification materialization is in progress (#80). A rewrite now would be speculative work against an uncontracted API and would collide with the UI-freeze discipline of AGENTS.md rule 12.
4. **The retained shell is in a verified healthy state.** After slice S3 (PR #84): all dead units removed with grep-zero proofs, strict typecheck and production build green, single self-contained artifact `dist/index.html` at 475,438 B, CI frontend job green on the merge commit.

## Consequences for the remaining slices

- **S4 (execution-path cutover) stays fenced until all three triggers hold:**
  - **T1** — a backend preview/pipeline endpoint under an accepted contract is deployed and reachable from a browser client;
  - **T2** — inventory item U1 is resolved: one executed browser session (manual smoke or scripted trace) against `vite preview` or the deployed Pages build, capturing console output during "Run All";
  - **T3** — the secrets posture for that call path is designed per AGENTS.md rule 10 (`CredentialRef` only; no raw credentials in client code or payloads).
- **S5** (CI/build gate update) executes only if a T1-triggered cutover changes entry points or gates; otherwise the current gates remain exactly as they are.
- **S6/S7** stay ordered after S4 per the inventory §7.
- Dependabot npm weekly updates continue unchanged on the root manifest.
- Inventory UNKNOWN items U2 (Pages traffic signal) and U3 (jsDelivr/CSP posture) remain open and non-blocking; U3 is mooted entirely once T1 fires.

## Explicit non-decisions

This record deliberately does not choose a desktop/embed shell, a component library, a visual system, or any API shape — each remains owned by its own future issue (see #79 out-of-scope list).

## Rollback

Docs-only. Revert this file through a normal PR. A future boundary change supersedes this decision by a new decision note referencing this file and stating the new triggers.

Refs #79
