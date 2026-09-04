# SEC-A1 Workspace / Member / RBAC API evidence

## Scope

SEC-A1 adds a transport-neutral authorization gate and workspace-scoped
identity-management API on top of SEC-S1 persistence. Authorization is
centralized at the API boundary and is applied before object lookup or runtime
dispatch.

## Durable and authorization contract

- Workspace reads and writes, member lifecycle, role lifecycle, role
  capabilities, member-role assignment, service-account lifecycle, and
  credential-reference lifecycle are exposed as typed API operations.
- Every request is workspace-scoped. The gate verifies that the workspace is
  active, the member or service account is active, and the principal's role
  capabilities belong to that same workspace.
- Server mode requires a principal and fails closed for missing, inactive,
  cross-workspace, unknown, or malformed identity state. Local trusted mode
  preserves the existing in-process bootstrap boundary; workspace creation is
  intentionally unavailable in server mode until a separate global bootstrap
  contract exists.
- Capabilities are parsed from a fixed allowlist. Unknown capabilities are
  rejected and never grant access. `workspace:admin` is the explicit full
  capability, not an implicit fallback.
- Authorization decisions are cached only as sanitized capability names and
  are invalidated after membership, role, assignment, and service-account
  mutations. Credential, connector-test, sensitive export, and artifact
  download operations require distinct capabilities.
- Service accounts are persisted and have an explicit lifecycle, but receive
  no implicit privileges because SEC-S1 contains no service-account role
  assignment table. This is fail-closed by design; a future assignment
  contract must be added before service-account authorization is broadened.
- Authorization failures occur before object-existence lookup, preventing
  unauthorized cross-workspace existence leakage.

## Verification evidence

From the SEC-A1 isolated worktree at implementation head:

```text
cargo test -p stillflow-api --all-features --no-fail-fast
cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

All commands passed. The API run covered 15 unit tests, 5 event-stream tests,
and 19 runtime E2E tests. The workspace run covered the full workspace,
including 119 storage tests and 223 engine tests; only the intentionally large
8 GiB resource-boundary test was filtered, with no source or limit change.

The focused API tests additionally cover:

1. server-mode member, role, capability, service-account, and credential
   lifecycle operations;
2. cross-workspace principal and object isolation;
3. cache invalidation after role-capability changes;
4. fail-closed unknown capabilities and missing principals;
5. separate permissions for credential operations, connector tests, export,
   and artifact download;
6. pre-lookup authorization ordering and stable manifest/OpenAPI schema
   references.

Independent acceptance was run from a detached exact-head worktree before
merge. The implementation parent was the live SEC-S1 main head
`e750b8dc1470f72b30ba01245879d9af855d85d5`; the branch head and CI receipts
are recorded in the completion entry for Issue #254.

## Explicit non-goals

- no HTTP server, transport authentication, frontend, audit/operations,
  automation, or second execution engine;
- no service-account role-assignment schema beyond the fail-closed lifecycle;
- no plaintext credential material, provider payload, or secret-bearing error
  surface;
- no object-specific policy engine outside the centralized capability gate.
