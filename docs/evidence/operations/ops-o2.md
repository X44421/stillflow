# OPS-O2 Retention / GC Evidence

## Scope

OPS-O2 adds a storage-owned, bounded retention plane for Dataset, Snapshot,
Artifact, Event, and Run objects. Retention is an explicit maintenance
operation. It does not start a scheduler, expose an HTTP endpoint, add a
frontend, or replace the existing Export adapter.

## Contract

- Storage schema version 10 adds the strict `retention_tombstones` ledger.
- `RetentionPolicy` has independent per-kind durations, policy version 1,
  and the existing bounded maintenance candidate limit.
- `retention_plan` is read-only and returns deterministic candidates.
- `collect_retention` supports dry-run and write modes.
- Write mode records a tombstone before physical collection, rechecks
  Dataset/Snapshot/Artifact/Run references, and retains blocked objects for a
  later pass.
- A successful write pass records one AUD-A1 system receipt per affected
  workspace in the same SQLite transaction as the ledger/state changes.
- Snapshot/Export legacy GC is invoked through a shared inner helper while one
  store-wide maintenance gate is held; the Export GC implementation remains
  in its existing adapter.
- Missing managed files are treated as already collected. Symlinked or
  malformed managed directories fail closed.

## Verification

The storage crate tests cover policy bounds, schema migration to version 10
and fail-closed future-version handling, dry-run planning, archived Dataset
collection, active Dataset preservation, audit receipt creation, and safe
idempotent repetition. Full workspace formatting, tests, and clippy remain
the release acceptance gates for the branch.
