# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `275`
- Updated: `2026-09-01T07:59:30Z`
- Source main snapshot: `e401f49a5978c5efebaa5bb717ee630e8fdccf20`
- Legacy/inactive rows retained in JSON for migration compatibility: `0`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-J1` | L3 | **running** | `agent-e5-j1-220` | #220 | `agent/issue-220-e5-j1-bounded-job-runtime` | engine:job-runtime, storage:control-plane |
| `SIMP-R1` | L2 | **running** | `agent-simp-r1-221` | #221 | `agent/issue-221-simp-r1-quality-verification-debt` | engine:quality, engine:verification, storage:bundle |
| `TS-151-PROD` | L2 | **running** | `agent-ts-151-prod` | #151 | `agent/issue-151-ts-151-temporal-decoding` | connector:local-tabular:temporal |
| `Q-D1-C0` | L3 | **running** | `agent-q-d1-c0-222` | #222 | `agent/issue-222-q-d1-drift-contract` | docs:quality-drift-contract |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `connector:local-tabular:temporal` | `TS-151-PROD` | `agent-ts-151-prod` | `2026-09-01T09:29:30Z` |
| `docs:quality-drift-contract` | `Q-D1-C0` | `agent-q-d1-c0-222` | `2026-09-01T09:29:30Z` |
| `engine:job-runtime` | `E5-J1` | `agent-e5-j1-220` | `2026-09-01T08:41:30Z` |
| `engine:quality` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |
| `engine:verification` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |
| `storage:bundle` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |
| `storage:control-plane` | `E5-J1` | `agent-e5-j1-220` | `2026-09-01T08:41:30Z` |

## L2/L3 protocol

```bash
export STILLFLOW_AGENT_ID=wsl-agent-01
python3 coordination/taskctl.py heartbeat TASK_ID
python3 coordination/taskctl.py rebind-check TASK_ID
python3 coordination/taskctl.py release TASK_ID
```

L0/L1 work does not register here. A CAS conflict means STOP and re-read;
never blind-retry.
