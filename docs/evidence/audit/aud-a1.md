# AUD-A1 Audit / Lineage API

## Delivery record

- Issue: #263
- Exact implementation base: `5d187e2a12d12c1bf00ca7e1db227babc42d0412`
- Scope: immutable, redaction-safe audit storage and transport-neutral API surface for audit events, lineage, and bounded export.
- Out of scope: HTTP handlers, frontend work, retention scheduling, general garbage collection, raw dataset or credential values, and a second event stream.

## Contract and storage

The implementation freezes the AUD-C0 event shape as a versioned envelope separate from `cp_events`. Events carry actor kind/reference, action, reason, request/correlation/trace identifiers, object identity, optional before/after references, lineage edges, source event identity, idempotency key, event digest, and retention state. Payload validation rejects credential-like or raw cell data and bounds text, JSON, lineage, and page sizes.

Storage schema version 9 adds `audit_events` with workspace-scoped sequence numbers, append-only event identity, unique idempotency replay, digest identity, retention state, and indexes for workspace, time, actor, object, trace, and correlation filters. Reopening an existing event with the same idempotency key is accepted only when the canonical event digest matches. Expiration changes visibility state while retaining identity and digest.

## API surface

- `audit.events.list` — `GET /v1/audit/events`
- `audit.lineage.read` — `GET /v1/audit/lineage`
- `audit.export` — `GET /v1/audit/export`

The API exposes `audit:read` and `audit:export` capabilities. Queries require workspace authorization, use deterministic bounded pagination, and bind cursors to workspace, filters, sort order, and API version. Unknown or cross-workspace objects are not disclosed. Lineage covers Dataset → PlanVersion → Run → Artifact plus Snapshot/Export edges. Export uses a deterministic event-view digest and the same bounded query contract.

## Verification

- `cargo check -p stillflow-storage -p stillflow-api` — passed.
- `cargo test -p stillflow-api --test aud_a1_api -- --nocapture` — 3 passed, 0 failed.
- `cargo test -p stillflow-storage migration_is_idempotent_and_future_versions_fail_closed -- --nocapture` — 1 passed, 0 failed.
- `cargo test -p stillflow-storage future_storage_version_is_rejected_before_identity_access -- --nocapture` — 1 passed, 0 failed.
- The focused API tests cover idempotent append and filter-bound cursors, workspace/retention/redaction isolation, RBAC, lineage, and deterministic export.

## Acceptance boundary

This receipt is for the AUD-A1 implementation only. Merge, post-main CI, Issue closure, Registry completion, and the next route node remain separate governance steps.
