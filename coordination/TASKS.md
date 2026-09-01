# StillFlow active coordination claims

> Machine source: `coordination/registry.json`. GitHub Issues/PRs own task
> lifecycle, head, CI, review, and merge state. This registry owns only
> active L2/L3 writer/lock claims.

- Registry revision: `276`
- Updated: `2026-09-01T11:59:48Z`
- Source main snapshot: `a70b0ac57f65f448f4abd6d6112f6238fd14f92f`
- Legacy/inactive rows retained in JSON for migration compatibility: `1`

## Registered / active claims

| ID | Risk | Status | Owner | Issue | Branch | Locks |
| --- | --- | --- | --- | --- | --- | --- |
| `SIMP-R1` | L2 | **running** | `agent-simp-r1-221` | #221 | `agent/issue-221-simp-r1-quality-verification-debt` | engine:quality, engine:verification, storage:bundle |
| `TS-151-PROD` | L2 | **running** | `agent-ts-151-prod` | #151 | `agent/issue-151-ts-151-temporal-decoding` | connector:local-tabular:temporal |
| `Q-D1-C0` | L3 | **running** | `agent-q-d1-c0-222` | #222 | `agent/issue-222-q-d1-drift-contract` | docs:quality-drift-contract |

## Active locks

| Lock | Task | Owner | Lease expires |
| --- | --- | --- | --- |
| `connector:local-tabular:temporal` | `TS-151-PROD` | `agent-ts-151-prod` | `2026-09-01T09:29:30Z` |
| `docs:quality-drift-contract` | `Q-D1-C0` | `agent-q-d1-c0-222` | `2026-09-01T09:29:30Z` |
| `engine:quality` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |
| `engine:verification` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |
| `storage:bundle` | `SIMP-R1` | `agent-simp-r1-221` | `2026-09-01T09:29:30Z` |

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
