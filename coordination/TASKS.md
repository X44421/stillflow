# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `272`
- Updated: `2026-09-01T06:59:39Z`
- Source main snapshot: `05bebde339e0b8ea66ad29a3dacb95003ce63ebc`
- Legacy/inactive rows retained in JSON for migration compatibility: `0`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-S1` | L3 | **running** | `agent-e5-s1-198` | #198 | `agent/issue-198-e5-s1-control-plane-persistence` | branch:agent/issue-198-e5-s1-control-plane-persistence, storage:stillflow-storage, core:stillflow-core |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `branch:agent/issue-198-e5-s1-control-plane-persistence` | `E5-S1` | `agent-e5-s1-198` | `2026-09-01T07:15:00Z` |
| `core:stillflow-core` | `E5-S1` | `agent-e5-s1-198` | `2026-09-01T07:15:00Z` |
| `storage:stillflow-storage` | `E5-S1` | `agent-e5-s1-198` | `2026-09-01T07:15:00Z` |

## L2/L3 protocol

```bash
export STILLFLOW_AGENT_ID=wsl-agent-01
python3 coordination/taskctl.py register TASK_ID --risk L2 --issue N \
  --branch agent/issue-N-short --base FULL_MAIN_SHA \
  --lock storage:export --path 'backend/crates/stillflow-storage/src/export.rs'
python3 coordination/taskctl.py claim TASK_ID
python3 coordination/taskctl.py heartbeat TASK_ID
python3 coordination/taskctl.py rebind-check TASK_ID
# PR/CI/review state stays in GitHub; release removes the active claim.
python3 coordination/taskctl.py release TASK_ID
```

L0/L1 work does not register here. A CAS conflict means STOP and re-read;
never blind-retry. Legacy commands remain available only for rows created
under the pre-GOV-R1 workflow.
