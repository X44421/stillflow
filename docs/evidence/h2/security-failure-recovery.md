# H2 Security / Failure / Recovery Matrix

## Receipt and decision

- Issue: #278
- Entry main: `a267e453667f00da09d05c7999b2f9d875ad178c`
- Scope: review-only matrix over the completed backend route
- Decision: PASS with explicitly bounded adapter scope and one non-production test-flake residual

The matrix below points to committed contract evidence and executable tests already present in the repository. A row is not treated as implemented merely because a future adapter could implement it.

## Matrix

| ID | Risk surface | Evidence | Result / boundary |
| --- | --- | --- | --- |
| H2-01 | Secrets and credentials | `docs/evidence/security/sec-c0.md`, `sec-s1.md`, `sec-a1.md`; `identity::tests::credential_records_never_persist_plaintext_secret_sentinel`; `service::tests::connection_registration_rejects_embedded_secret_without_mutation` | PASS: opaque provider references, plaintext rejection, redacted views, and fail-closed credential operations are explicit. |
| H2-02 | Authentication, RBAC, tenant/object isolation | `docs/evidence/security/sec-a1.md`, `docs/evidence/audit/aud-a1.md`; `service::tests::server_rbac_is_workspace_scoped_and_cache_invalidates`; `identity::tests::cross_workspace_owner_lookup_fails_closed`; `audit_filters_are_workspace_scoped_and_expiry_is_explicit` | PASS: workspace and object authorization precede disclosure/mutation; audit reads are workspace-scoped. |
| H2-03 | Filesystem, managed roots, symlinks, overwrite | `docs/evidence/operations/ops-o3.md`, `ops-o4.md`; `backup::tests::manifest_rejects_unbounded_or_external_paths`; `export::tests::destination_root_rejects_symlinks_non_directories_and_non_canonical_paths`; `store::tests::symlinked_partition_fails_closed` | PASS: path boundaries, symlink rejection, and create-new publication are fail-closed. |
| H2-04 | Corruption, schema versions, migration | `docs/evidence/operations/ops-o3.md`; `store::tests::migration_is_idempotent_and_future_versions_fail_closed`; `control_plane::tests::corrupt_typed_queued_job_becomes_one_terminal_failure`; `export::tests::corrupt_journal_rows_fail_closed_without_wrong_deletion` | PASS: unknown/future versions and corrupt rows do not silently publish or mutate the wrong object. |
| H2-05 | Bounds and pagination | `docs/evidence/h1/golden-e2e.md`, `ops-o1.md`, `ops-o2.md`; `control_plane::tests::exact_queue_cap_has_zero_mutation_at_257`; `thousand_plus_event_replay_is_paginated_and_bounded`; `retention::tests::policy_rejects_unbounded_values` | PASS: queue, event replay, retention, download, and maintenance bounds are explicit and tested. |
| H2-06 | Cancellation, deadlines, partial output | H1 evidence; `e5_g1_runtime_e2e::out_of_bound_deadline_fails_closed_without_partial_outputs`; `export::tests::cancellation_at_checkpoints_leaves_no_artifact`; `export::tests::deterministic_retry_after_cancellation_or_failure` | PASS: cancellation/deadline paths are typed, bounded, and do not expose partial committed output. |
| H2-07 | Restart, recovery, backup/restore, retention/GC | `docs/evidence/h2/h2-b01.md`, `docs/evidence/operations/ops-o2.md`, `ops-o3.md`; `control_plane::tests::restart_reconciliation_is_atomic_and_idempotent`; `store::tests::recovery_removes_precommit_files_and_preserves_committed_snapshot`; `backup::tests::backup_and_restore_refuse_overwrite_and_tampering` | PASS: recovery and retention remain owned by storage/control-plane authorities; H2-B01 is test-only. |
| H2-08 | Health, auditability, daemon lifecycle | `docs/evidence/operations/ops-o1.md`, `ops-o4.md`, `docs/evidence/audit/aud-c0.md`; `ops_o1_observability::health_reports_liveness_and_dependency_readiness`; OPS-O4 deployment unit tests | PASS: liveness/readiness, redacted telemetry, deterministic lifecycle, bounded recovery, and explicit shutdown are covered. |
| H2-09 | Cross-platform packaging boundary | `docs/evidence/operations/ops-o4.md` platform matrix | BOUNDED: Windows/macOS/Linux adapter ownership is documented, but OS process spawning, installers, IPC listeners, and HTTP transport are intentionally outside the transport-neutral backend contract. No release claim is made for an unimplemented adapter. |

## Residuals and controls

1. The dedup activity-gate test `open_critical_section_excludes_recovery_via_activity_guard` has a known runner-level timing sensitivity tracked by H2-B01 Issue #274. Current main CI run `33918311723` passed 6/6 after one failed attempt was rerun; the rerun changed no source and the failure did not implicate OPS-O4. This is recorded as test infrastructure residual, not a production behavior exception.
2. OS service managers, installers, IPC, and HTTP listeners remain adapter work. The backend exposes only the versioned, health, authorization, and lifecycle contracts needed by those adapters.

## Gate evidence

- OPS-O4 merge: PR #277, merge `a267e453667f00da09d05c7999b2f9d875ad178c`.
- Current main CI: run `33918311723`, 6/6 passed.
- Registry before H2 claim: revision 375, no active locks.
- Roadmap transition: #81 comment `5546443334`.
