# OPS-O3 Backup / Restore

## Scope

OPS-O3 provides a production-gate backup and restore primitive for one
StillFlow managed storage root.

- `SnapshotStore::backup` acquires the existing maintenance gate, checkpoints
  SQLite WAL state, and publishes a new private backup directory by staged
  atomic rename.
- The backup contains the checkpointed `metadata.sqlite3` and immutable
  `partitions/<uuid>/<file>` entries. The versioned `backup.json` manifest
  records storage schema version, sorted relative paths, byte counts, and
  SHA-256 digests.
- `SnapshotStore::restore` validates the manifest, exact backup tree shape,
  every file digest and size, SQLite integrity, foreign keys, and schema
  version before publishing a new managed-root directory.
- Symlinks, traversal paths, duplicate or unexpected entries, unsupported
  versions, corrupted data, and destination overwrite are rejected.
- Restored roots retain the normal storage lock, migration, and fail-closed
  integrity behavior when reopened.

## Safety boundary

Backups are create-new operations. Existing backup and restore destinations are
never overwritten or deleted. Backup does not run beside readers, publishers,
export publishers, or other maintenance. Staging directories, temporary
directories, lock files, SQLite WAL/SHM residue, and external Export
destinations are intentionally excluded. Credential rows remain provider-owned
references; secret material is not introduced into the backup format.

## Non-goals

This node does not add remote storage, encryption or key management,
incremental backups, retention or GC policy, scheduler/automation, desktop
daemon, HTTP routes, frontend, or general system-wide retention.

## Verification

All checks were run from the isolated OPS-O3 worktree at the implementation
head.

| Check | Result |
| --- | --- |
| `cargo test -p stillflow-storage backup --no-fail-fast` | 3 passed |
| `cargo test -p stillflow-storage --all-features --no-fail-fast -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above` | passed; existing 8 GiB resource test skipped |
| `cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above` | passed; existing 8 GiB resource test skipped |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | passed |
| `git diff --check` | passed |
