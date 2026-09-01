# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `273`
- Updated: `2026-09-01T07:05:30Z`
- Source main snapshot: `e401f49a5978c5efebaa5bb717ee630e8fdccf20`
- Legacy/inactive rows retained in JSON for migration compatibility: `0`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| — | — | — | — | — | — | — |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| — | — | — | — |

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
never blind-retry.
