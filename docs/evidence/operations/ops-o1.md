# OPS-O1 Observability / Health / Metrics

## Scope

OPS-O1 adds a provider-neutral observability seam and bounded health/metrics
surfaces across the API, storage, queue, job, run, engine, and connector
boundaries.

- Health has explicit liveness and dependency readiness views.
- Metrics use a fixed typed vocabulary for components, operations, outcomes,
  metric names, and metric kinds.
- Labels are bounded to component, operation, outcome, and connector. Request
  IDs, workspace IDs, credentials, paths, raw cell contents, and payload values
  are never labels.
- Structured logs and spans carry bounded correlation IDs and apply telemetry
  redaction to credential, token, password, private, path, cell, raw, and value
  fields.
- In-memory aggregation is bounded; exporter failures are observational and do
  not affect the request path.
- The API exposes liveness, readiness, health, and metrics routes. Queue depth
  is read from the control plane with workspace scoping.
- The telemetry types are suitable for a later OpenTelemetry adapter, but this
  change does not bundle an SDK or select a vendor.

## Invariants

Health and telemetry remain observational: they do not change job state,
authorization decisions, credential material, connector behavior, or storage
publication semantics. Readiness reports unavailable optional dependencies as
degraded while liveness remains healthy. Metrics and logs are bounded and
provider-neutral.

## Verification

All commands were run from the OPS-O1 isolated worktree at the implementation
head.

| Check | Result |
| --- | --- |
| `cargo test -p stillflow-core observability --no-fail-fast` | 5 passed |
| `cargo test -p stillflow-api --test ops_o1_observability --no-fail-fast` | 2 passed |
| `cargo test -p stillflow-api --all-features --no-fail-fast` | passed: API unit, event-stream, E2E, and OPS-O1 tests |
| `cargo test --workspace -- --skip total_output_cap_is_accepted_at_eight_gib_and_enforced_above` | passed; existing 8 GiB resource test intentionally skipped |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --workspace --all-targets -- -D warnings` | passed |
| `git diff --check` | passed |

## Non-goals

This node does not add an HTTP server, frontend, dashboards, a telemetry
provider, OpenTelemetry SDK wiring, audit export, automation, job scheduling,
or the later OPS-O2, OPS-O3, and OPS-O4 work.
