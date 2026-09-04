# SEC-S1 Identity / Credential persistence evidence

## Scope

SEC-S1 adds the storage and provider seam for workspace-scoped identity and
credential references. It does not add authorization middleware, HTTP routes,
or transport authentication; those remain SEC-A1 scope.

## Durable contract

- Storage schema version 8 adds `sec_members`, `sec_roles`,
  `sec_role_capabilities`, `sec_member_roles`, `sec_service_accounts`, and
  `sec_credentials` to the existing managed SQLite root.
- Every identity lookup and write includes the workspace boundary. Composite
  foreign keys and owner triggers reject cross-workspace credential ownership.
- Unknown storage versions and unknown persisted identity or credential states
  fail closed. Startup changes interrupted `rotating` records to the explicit
  `recovery_required` state; recovery is never implicit reactivation.
- Durable credential rows contain only provider kind and a validated
  `cred://...` reference. `SecretMaterial` is non-serializable, redacted in
  `Debug`, and zeroed on drop.
- `CredentialProvider` is provider-neutral. Environment, OS-keychain, and
  external-provider adapters are seams; material is resolved or mutated only
  at those provider boundaries.

## Lifecycle evidence

The focused storage tests cover:

1. membership, roles, capabilities, service accounts, and restart persistence;
2. cross-workspace owner isolation and fail-closed lookup;
3. plaintext-secret sentinel exclusion from durable rows and views;
4. rotation, revocation, expiry, interrupted-rotation recovery, and restart;
5. environment, keychain, and external provider seams with redacted debug;
6. future storage-version rejection before identity access.

## Verification

From the SEC-S1 isolated worktree:

```text
cargo test -p stillflow-storage identity --all-features --no-fail-fast
cargo test -p stillflow-storage --all-features --no-fail-fast -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above
cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

All commands passed. The existing 8 GiB resource-boundary test was filtered in
the bounded workspace/storage runs because it is an intentionally large
resource test; no test source or limit was changed.

## Explicit non-goals

- no plaintext secret persistence, serialization, event, log, or error surface;
- no password/token/private-key provider payload columns;
- no HTTP/API authentication or authorization middleware;
- no role-policy evaluation or request enforcement (SEC-A1);
- no provider-specific SDK or OS-keychain implementation beyond testable seams.
