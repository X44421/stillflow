# Phase 2/3 execution dependency freeze

Issue: #248 — P23-C0
Baseline: `main@1530e0714c53d3d290703bcd4596eab2f162698e`
Predecessor: Phase 1 deterministic backend MVP closed through H1 #244 and P1-CLOSE #246
Scope: contract/roadmap freeze only; no runtime implementation.

## Frozen dependency graph

```text
P23-C0
  └─ SEC-C0
       ├─ SEC-S1 ── SEC-A1
       ├─ AUD-C0 ── AUD-A1
       ├─ OPS-O1 ── OPS-O3 ─────────────┐
       │                                 ├─ OPS-O4
       ├─ AUD-C0 ── OPS-O2 ──────────────┘
       └─ AUT-C0 ── AUT-J1 ── AUT-A1 ────┘

SEC + AUD + AUT + major OPS ── H2 ── H3 ── Release closeout
```

The ordering rules are:

- `SEC-C0` is the only next mainline node and must be completed before identity, audit, observability, retention/GC, automation, or packaging implementation is dispatched.
- `SEC-S1` depends on `SEC-C0`; `SEC-A1` depends on both.
- `AUD-C0` depends on `SEC-C0` and may overlap with `SEC-S1` only after separate claims and non-overlapping surfaces are confirmed. `AUD-A1` follows `AUD-C0` and should wait for `SEC-A1` authorization foundations.
- `OPS-O1` may start after `SEC-C0`. `OPS-O3` is ordered before `OPS-O2` and `OPS-O4`; `OPS-O2` depends on the audit event contract (`AUD-C0`).
- `AUT-C0` follows the SEC/AUD contract foundations; `AUT-J1` follows `AUT-C0`, and `AUT-A1` follows `AUT-J1`.
- `OPS-O4` is late: it depends on SEC, `OPS-O1`, and `OPS-O3`, and must not become a second transport or execution authority.
- `H2` is a gate after SEC, AUD, AUT, and the major OPS nodes. `H3` is the final release gate after H2.

No future node is pre-claimed or dispatched by this freeze. When parallelism is
allowed, each L2/L3 node still gets one canonical Issue, one scoped Registry
claim/lock, one isolated branch, exact-head CI, and independent acceptance.

## Mode boundary

| Mode | Boundary | Authority |
| --- | --- | --- |
| Desktop local | Local daemon or IPC/port boundary; managed-root and local credential trust boundary; explicit version/health handshake; local process lifecycle and recovery | Rust backend remains the only executor, validator, identity/digest, Artifact, and persistence authority |
| Workspace server | Remote transport adapter over the versioned API; authenticated Workspace/Member/Role/Capability boundary; object-level authorization and tenant isolation | The same Rust backend contracts and JobRuntime; transport never becomes domain authority |
| Web / Desktop / CLI clients | Client presentation, request construction, bounded reads, and protocol compatibility only | No client-side executor, canonical cleaning algorithm, digest authority, or hidden queue |

The freeze does not select an HTTP daemon, IPC mechanism, credential vendor, or
deployment topology. Those are implementation decisions for the scoped nodes;
the semantic and trust boundaries above are fixed.

## Completion definitions

### Phase 1 — frozen

The deterministic backend MVP is closed: real Source → Dataset/Snapshot → Plan
→ Preview → Job/Run → Verification/Profile/Quality → Export/Manifest lineage,
boundedness, recovery, and digest evidence are recorded by H1. No new Phase 1
feature is admitted through the Phase 2/3 work.

### Phase 2 — client-ready backend

Phase 2 is complete only when the versioned backend supports Workspace/member/
permission/credential foundations, complete Dataset/Session/Plan/Run/Event/
Artifact APIs and stream/read bounds, audit/lineage, automations, profile
history/drift, and protocol-equivalent Web remote and Desktop local modes. All
objects remain workspace-scoped and all client operations remain bounded and
typed.

### Phase 3 — production backend

Phase 3 is complete only when security hardening, health/readiness, metrics and
tracing, structured redacted logs, migrations, backup/restore, retention/GC,
cross-platform recovery, packaging, upgrade/rollback, and release evidence are
accepted through H2/H3. Production packaging cannot introduce a second runtime
or bypass the frozen authorization/credential boundaries.

## Canonical task-ID map

| Task ID | Kind | Frozen prerequisite / position |
| --- | --- | --- |
| `P23-C0` | contract/docs gate | current node; complete before SEC dispatch |
| `SEC-C0` | contract | next mainline after P23-C0 |
| `SEC-S1` | implementation | SEC-C0 |
| `SEC-A1` | implementation | SEC-C0 + SEC-S1 |
| `AUD-C0` | contract | SEC-C0; may overlap SEC-S1 |
| `AUD-A1` | implementation | AUD-C0; preferably SEC-A1 |
| `OPS-O1` | implementation | SEC-C0; early OPS lane |
| `OPS-O3` | implementation | SEC-C0 + OPS-O1 sequencing |
| `OPS-O2` | implementation | AUD-C0; after O3 planning boundary |
| `AUT-C0` | contract | SEC/AUD foundations |
| `AUT-J1` | implementation | AUT-C0 |
| `AUT-A1` | implementation | AUT-J1 |
| `OPS-O4` | implementation | SEC + OPS-O1 + OPS-O3 + late client boundary |
| `H2` | gate | SEC/AUD/AUT/major OPS complete |
| `H3` | release gate | H2 complete |

`H1`, `P1-CLOSE`, and `P23-C0` are the completed/current Phase 1 transition
nodes. H1 blockers are conditional only and are not assigned IDs until a real
H1 failure exists. `O0` remains an optional measured side line; AI and future
database/distributed/document/connector tracks are deferred and are not Phase
2/3 prerequisites.

## Dispatch boundary

The next and only dispatch after this freeze is `SEC-C0`. Do not create or
claim `SEC-S1`, `AUD-C0`, `OPS-O1`, or any later node until the current SEC-C0
contract has its own canonical Issue and exact acceptance. This document adds
no Rust, TypeScript, frontend, API, queue, persistence, or CI behavior.
