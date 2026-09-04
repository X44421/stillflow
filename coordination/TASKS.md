# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `333`
- Updated: `2026-09-04T08:39:37Z`
- Source main snapshot: `ca18a092552a0037e33ef4945c8c2d99ae487b38`
- Legacy/inactive rows retained in JSON for migration compatibility: `13`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-A1` | L3 | **queued** | `—` | #228 | `agent/issue-228-e5-a1-versioned-api` | api:bootstrap, api:objects, storage:control-plane-api, storage:artifact-read |
| `E5-E1` | L3 | **queued** | `—` | #229 | `agent/issue-229-e5-e1-event-stream` | api:event-stream |

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
never blind-retry. Legacy commands remain available only for rows created
under the pre-GOV-R1 workflow.
