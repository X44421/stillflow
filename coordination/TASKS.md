# StillFlow coordination registry

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. Registry locks represent only
> currently active L2/L3 writer surfaces.

- Registry revision: `278`
- Updated: `2026-09-01T12:16:00Z`
- Source main snapshot: `a70b0ac57f65f448f4abd6d6112f6238fd14f92f`

## Task records

| ID | Risk | Status | Owner | Issue | Branch | Active locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-J1` | L3 | **done** | `agent-e5-j1-220` | #220 | `agent/issue-220-e5-j1-bounded-job-runtime` | — |
| `SIMP-R1` | L2 | **done** | `agent-simp-r1-221` | #221 | `agent/issue-221-simp-r1-quality-verification-debt` | — |
| `TS-151-PROD` | L2 | **done** | `agent-ts-151-prod` | #151 | `agent/issue-151-ts-151-temporal-decoding` | — |
| `Q-D1-C0` | L3 | **done** | `agent-q-d1-c0-222` | #222 | `agent/issue-222-q-d1-drift-contract` | — |
| `E5-A1` | L3 | **running** | `agent-e5-a1-228` | #228 | `agent/issue-228-e5-a1-versioned-api` | api:bootstrap, api:objects, storage:control-plane-api, storage:artifact-read |
| `E5-E1` | L3 | **running** | `agent-e5-e1-229` | #229 | `agent/issue-229-e5-e1-event-stream` | api:event-stream |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `api:bootstrap` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `api:event-stream` | `E5-E1` | `agent-e5-e1-229` | `2026-09-01T13:46:00Z` |
| `api:objects` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `storage:artifact-read` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `storage:control-plane-api` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
