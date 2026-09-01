# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `274`
- Updated: `2026-09-01T07:11:30Z`
- Source main snapshot: `e401f49a5978c5efebaa5bb717ee630e8fdccf20`
- Legacy/inactive rows retained in JSON for migration compatibility: `0`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-J1` | L3 | **running** | `agent-e5-j1-220` | #220 | `agent/issue-220-e5-j1-bounded-job-runtime` | engine:job-runtime, storage:control-plane |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `engine:job-runtime` | `E5-J1` | `agent-e5-j1-220` | `2026-09-01T08:41:30Z` |
| `storage:control-plane` | `E5-J1` | `agent-e5-j1-220` | `2026-09-01T08:41:30Z` |

## L2/L3 protocol

```bash
export STILLFLOW_AGENT_ID=wsl-agent-01
python3 coordination/taskctl.py heartbeat E5-J1
python3 coordination/taskctl.py rebind-check E5-J1
python3 coordination/taskctl.py release E5-J1
```

L0/L1 work does not register here. A CAS conflict means STOP and re-read;
never blind-retry.
