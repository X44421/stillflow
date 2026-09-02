# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `282`
- Updated: `2026-09-02T08:36:08Z`
- Source main snapshot: `a70b0ac57f65f448f4abd6d6112f6238fd14f92f`
- Legacy/inactive rows retained in JSON for migration compatibility: `4`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `E5-A1` | L3 | **running** | `agent-e5-a1-228` | #228 | `agent/issue-228-e5-a1-versioned-api` | api:bootstrap, api:objects, storage:control-plane-api, storage:artifact-read |
| `E5-E1` | L3 | **running** | `agent-e5-e1-229` | #229 | `agent/issue-229-e5-e1-event-stream` | api:event-stream |
| `E5-J2-C0` | L3 | **queued** | `—` | #233 | `agent/issue-233-e5-j2-c0-job-operation-contract` | contract:job-operation |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `api:bootstrap` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `api:event-stream` | `E5-E1` | `agent-e5-e1-229` | `2026-09-01T13:46:00Z` |
| `api:objects` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `storage:artifact-read` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |
| `storage:control-plane-api` | `E5-A1` | `agent-e5-a1-228` | `2026-09-01T13:46:00Z` |

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
