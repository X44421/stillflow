# OPS-O4 Service packaging / Desktop daemon

## Receipt

- Issue: #276
- Scope: transport-neutral service packaging and Desktop-daemon contract
- Implementation: `backend/crates/stillflow-api/src/deployment.rs`
- Existing authorities reused: API version/handshake, route manifest, health views, authorization, scheduler, JobRuntime, storage backup/retention

## Contract

`ServiceConfig` is bounded and local-safe by default: Desktop-local transport, loopback bind, remote access disabled, a non-root managed root, a bounded shutdown grace period, and bounded recovery attempts. Remote binding requires the Web/remote transport and an explicit non-loopback host.

`TransportContract` is generated from the existing `BOOTSTRAP_MANIFEST` and `SUPPORTED_API_VERSIONS`. Desktop-local and Web/remote adapters therefore have different trust/binding policies but the same API version, route manifest, and supported-version set.

Credential configuration accepts only `credential://...` references. Plaintext values, whitespace/control characters, and `=`-style secret assignments are rejected both through construction and deserialization.

## Lifecycle and recovery

`DaemonLifecycle` is a deterministic wrapper state machine:

`Stopped -> Starting -> Ready -> Draining -> Stopped`

Failure from startup, ready, draining, or recovery enters `Failed`; recovery is bounded and explicit:

`Failed -> Recovering -> Ready`

Invalid transitions are rejected. Graceful shutdown is represented by `Draining` and delegates actual cancellation to the existing scheduler and JobRuntime. Recovery exhaustion is terminal until the wrapper is stopped and its recovery budget is reset.

`health_status()` maps `Ready` to healthy, `Stopped` to unavailable, and transitional/failed states to degraded for reuse by the existing liveness/readiness surface.

## Upgrade / rollback

`UpgradePlan` requires supported API versions, rejects no-op upgrades, and requires its rollback target to equal the upgrade source. `RollbackPlan` is derived from the same explicit target and rejects no-op or unsupported rollback versions. No process replacement or schema migration is performed by this contract.

## Platform matrix

| Platform | Packaging owner | Local boundary | Remote boundary |
| --- | --- | --- | --- |
| Windows | Desktop shell / OS service adapter | Desktop-local, loopback or local IPC selected by adapter | Web/remote only with explicit non-loopback bind and authorization |
| macOS | Desktop shell / launchd adapter | Desktop-local, loopback or local IPC selected by adapter | Web/remote only with explicit non-loopback bind and authorization |
| Linux | Desktop shell / systemd adapter | Desktop-local, loopback or local IPC selected by adapter | Web/remote only with explicit non-loopback bind and authorization |

The crate contains no OS-specific process spawning, installer, HTTP listener, or second scheduler. Those remain adapter concerns around the transport-neutral API and existing runtime authorities.

## Tests

The module unit tests cover local-safe defaults, remote-binding opt-in, local transport rejection, credential reference serialization/deserialization, managed-root traversal/root rejection, protocol equivalence, graceful shutdown, bounded recovery, invalid transitions, and upgrade validation.
