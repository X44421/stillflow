# AUT-A1 — Automation API

Issue: #272 — `[AUT-A1] Automation API`
Entry head: `3ea78cf3968dbbffc64c0fe75ee507f77ceddd86`

## Implemented boundary

The transport-neutral API projects the durable AUT-J1 `aut_schedules` state
and adds a v12 `aut_executions` handoff/history table. Create, update, pause,
resume, and delete use durable compare-and-set revision tokens. List and
history endpoints use workspace-bound, version-bound, bounded cursors.

Manual triggering requires an explicit idempotency key, persists the E5 Job
identity before submission, and reuses the existing `ApiService::submit_job`
path. A repeated request returns the same E5 Job; it never creates a second
executor or queue. Stored templates contain only bounded typed Job references
and policies; secret-like material is rejected and audit payloads contain
identities/state/digests only.

The seven AUT-A1 capabilities are `automation:read`, `automation:create`,
`automation:update`, `automation:pause`, `automation:resume`,
`automation:delete`, and `automation:trigger`. Cross-workspace reads fail
closed. Audit records use the existing AUD-C0 append-only store.

## Verification matrix

| Acceptance criterion | Exact check | Result |
| --- | --- | --- |
| v12 migration and history table | `cargo test -p stillflow-storage store::tests::migration_is_idempotent_and_future_versions_fail_closed` | passed |
| CAS update and idempotent execution replay | `automation_api::tests::automation_update_and_execution_replay_are_bounded_and_cas_guarded` | passed |
| CRUD, pause/resume, next-run non-mutation, history, cross-workspace scope | `service::automation::tests::automation_crud_cas_next_run_history_and_workspace_scope_are_bounded` | passed |
| Server authorization fail-closed | `service::automation::tests::server_automation_mutation_requires_explicit_capability` | passed |
| Manual E5 trigger, replay, history projection | `service::automation::tests::manual_trigger_reuses_e5_idempotency_and_projects_history` | passed |
| Full storage regression | `cargo test -p stillflow-storage` | 128 passed |
| Full API regression | `cargo test -p stillflow-api` | 18 unit/integration suites passed |
| Lint | `cargo clippy -p stillflow-storage -p stillflow-api --all-targets -- -D warnings` | passed |
| Manifest/OpenAPI consistency | `manifest::tests::every_manifest_route_references_known_schemas` and `openapi_representation_is_derived_from_manifest` | passed |

## Non-goals

No HTTP listener, UI, credential provider, event subscription, second Job/Run
authority, or scheduler execution logic was added. Scheduler/runtime behavior
remains AUT-J1 and all work is submitted through E5.
