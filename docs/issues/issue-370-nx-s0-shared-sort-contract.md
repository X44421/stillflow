# Issue #370: shared sort and the stateful execution base

Canonical contract: [`docs/contracts/issue-370-nx-s0-shared-sort-contract.md`](../contracts/issue-370-nx-s0-shared-sort-contract.md)

This pointer is kept under `docs/issues/` for the Issue contract index. The
normative node, plan and execution semantics are the linked contract; this Issue
remains docs-only and does not authorize runtime, executor, storage, API,
schema, or OpenShip changes beyond what the linked contract names.

It records the slice-1 decision for #370 against the stateful execution laws
already frozen in
[`docs/contracts/issue-363-nx-c1-stateful-multi-source-execution-contract.md`](../contracts/issue-363-nx-c1-stateful-multi-source-execution-contract.md)
§8 (ordering, grouping, cross-batch state, state ownership) and §9 (bounds,
spill, cancellation, publication), and freezes the stable-ordering primitive the
later stateful operators build on.
