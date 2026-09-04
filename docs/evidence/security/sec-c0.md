# SEC-C0 — Security and tenant contract

Issue: #250 — `[SEC-C0] Security & tenant contract`
Predecessor: P23-C0, merged at `e8557402ae0cd9d8ec9eb0a15501bb85c04e43fe`
Scope: contract and evidence only; no runtime, schema, transport, or UI implementation.

## Contract purpose

This document freezes the security and tenancy vocabulary that all Phase 2/3
work must consume. It applies equally to the versioned Workspace API and to
Desktop local mode, while recognizing that the two modes have different trust
boundaries. The Rust backend and its `JobRuntime` remain the only domain,
execution, validation, artifact, identity/digest, and persistence authority.

The contract is intentionally provider- and transport-neutral. It does not
select an HTTP framework, IPC mechanism, identity vendor, credential store,
database, deployment topology, or packaging format.

## Support matrix

| Capability | Desktop local mode | Workspace server mode | Contract rule |
| --- | --- | --- | --- |
| Primary trust boundary | User-controlled local machine, managed root, and local process/IPC boundary | Authenticated remote transport and server-side tenant boundary | The boundary is explicit in every request context; clients do not redefine it |
| Workspace scope | One or more locally available workspaces, selected explicitly | Every request carries or resolves exactly one authorized workspace | No object may be read or mutated outside the resolved workspace |
| Identity source | Local authenticated user/session or explicitly trusted local principal | Authenticated member or service account | Authentication is separate from authorization |
| Multi-user support | Not implied; local mode is single-user by default | Supported through members, roles, capabilities, and object-level checks | Any local multi-user feature needs a later, explicit contract and threat review |
| Credential storage | OS/local provider abstraction; plaintext values never enter ordinary records | Deployment-selected provider abstraction; server-side secret material is provider-owned | Only opaque references and lifecycle metadata cross the domain boundary |
| Authorization enforcement | Backend enforces the same capability/object checks; local trust is not an authorization bypass | Backend enforces tenant, capability, and object checks after authentication | Transport adapters may add context, never grant domain access |
| Process boundary | Explicit version/health handshake, managed-root restriction, lifecycle and recovery policy | Versioned API boundary with authenticated transport | A client cannot become an executor or persistence authority |
| Audit/telemetry | Redacted local events subject to the same event contract | Redacted server events with actor and workspace context | Secret values, bearer tokens, and raw credential payloads are never emitted |

### Local-mode default

Desktop local mode means one user operating a local backend under an explicit
managed root. A local process may establish a trusted transport connection only
after the version/health handshake and local trust checks succeed. “Local” does
not mean “all objects are visible”: workspace scope, object ownership, and
capability checks still apply. A future local multi-user or shared-machine mode
must define a new threat model and cannot be inferred from this default.

### Workspace-server default

Workspace server mode supports multiple members and service accounts. The
server resolves the authenticated principal, workspace, role/capability set,
and target object before performing any domain operation. A remote transport
adapter may reject malformed or unauthenticated requests, but it may not make a
separate authorization decision that diverges from the backend contract.

## Security and tenant vocabulary

| Concept | Contract meaning | Required invariants |
| --- | --- | --- |
| `Workspace` | Tenant/security boundary for durable StillFlow objects and events | Has a stable opaque identifier; is never inferred from an arbitrary object ID; all durable objects have one workspace owner |
| `Member` | Human or organization-linked principal that belongs to a workspace | Membership is explicit, revocable, and workspace-scoped; membership in one workspace grants nothing in another |
| `Role` | Named, reviewable bundle of capabilities assigned within a workspace | Roles are not global authority; assignment and removal are auditable; an unknown role fails closed |
| `Capability` | Atomic permission such as read, create, execute, export, administer, or manage credentials | Checks use stable capability identifiers and target-object context; capability names are not UI-only labels |
| `ServiceAccount` | Non-human principal for bounded automation or integration access | Has an owning workspace, explicit capabilities, credential lifecycle, expiry/revocation state, and auditable actor identity |
| `Principal` | Authenticated subject making a request, including member, service account, or explicitly trusted local principal | Every authorization decision names the principal type and workspace context; anonymous access is not an implicit principal |
| `Object` | Any workspace-scoped Dataset, Snapshot, Plan, Run, Job, Event, Artifact, profile, or future durable record | Object-level checks occur after lookup authorization is established and before disclosure or mutation |
| `SecretReference` | Opaque stable reference to provider-owned secret material | It contains no plaintext secret, token, private key, or reversible encoding of one |

### Relationship rules

1. A member belongs to a workspace through an explicit membership record.
2. A role is assigned to a member or service account within one workspace.
3. A role grants capabilities; a capability is evaluated against an operation
   and the target object (or workspace when the target is the workspace).
4. A service account cannot inherit access from a human member and cannot be
   used outside its owning workspace unless a later cross-workspace contract
   explicitly authorizes that relation.
5. An object reference is not proof of visibility. Object IDs are opaque and
   must not encode tenant identity or allow cross-tenant probing.

## Authentication contract

Authentication establishes a principal and an authenticated request context;
it does not establish permission to perform the requested operation.

The request context must be able to carry, without exposing credential
material:

- principal identifier and principal type;
- authenticated-at and expiry information where supplied by the provider;
- resolved workspace identifier, if the request names a workspace;
- authentication method/provider identifier and assurance result;
- correlation/request identifier for redacted audit and diagnostics.

An unauthenticated or invalid/expired credential is handled as `401` when the
endpoint's contract requires authentication. The response must not reveal
whether a candidate workspace, member, service account, or object exists.

Provider adapters may use sessions, signed tokens, OS identity, or another
mechanism selected by a later implementation task. The domain contract consumes
the normalized request context, not provider-specific token claims.

## Authorization and minimum privilege

Authorization is the ordered evaluation of:

1. request authentication and principal validity;
2. workspace selection and membership/service-account ownership;
3. operation capability;
4. target-object visibility and object-level relation;
5. state-specific constraints such as revoked credentials, archived objects,
   or non-executable Plans;
6. mutation-specific safeguards, including export, credential management, and
   service-account administration.

The effective permission is the intersection of all applicable constraints,
never the union of “any matching” client claims. Missing, unknown, expired, or
revoked inputs fail closed. Administrative capability is not implied by read,
execute, or export capability; credential management is a separate sensitive
capability.

| Operation class | Minimum contract requirement | Object-level requirement |
| --- | --- | --- |
| Workspace/member/role administration | Workspace administration capability | Target workspace must equal the authorized workspace |
| Dataset/Snapshot/Plan read | Corresponding read capability | Target object must be visible in the authorized workspace |
| Job/Run execute or cancel | Execute or run-control capability | Run/Job and referenced Plan/inputs must belong to the same authorized workspace |
| Artifact/Export read | Artifact/export capability | Every returned artifact/export is workspace-scoped and policy-checked |
| Service-account or credential mutation | Credential administration capability plus lifecycle safeguards | Target principal/provider reference must belong to the authorized workspace |
| Audit/event read | Audit-read capability | Events are filtered to the authorized workspace and redacted by policy |

No client-side check is authoritative. A client may hide unavailable actions,
but the backend repeats all checks for every request, including requests made
through Desktop local IPC.

## Lookup, forbidden resources, and error semantics

The API must not create an existence oracle through status codes, timing-sensitive
messages, list counts, error wording, or different response shapes.

| Situation | Required result | Disclosure rule |
| --- | --- | --- |
| No valid authentication when required | `401 Unauthorized` | Generic response; do not disclose candidate principal, workspace, or object existence |
| Authenticated principal lacks membership/capability or object access | `403 Forbidden` when the caller is already entitled to know the protected boundary | Use a stable generic reason; do not return object metadata or secret references |
| Resource is absent, or existence must be hidden from this caller | Not-found semantics (`404` at a transport boundary) | Same externally observable shape for absent and hidden resources |
| Malformed identifier/request | Typed client error; transport mapping is implementation-specific | Do not echo secrets or untrusted sensitive claims |
| Revoked/expired service credential | `401` for authentication failure; `403` only after an authenticated principal is established but not permitted | No indication of whether a similarly named account exists |
| Cross-workspace object reference | Not-found semantics by default | Never confirm the other workspace or object owner |

The exact transport mapping and wording remain an implementation decision, but
the semantic distinction above is frozen. Collection endpoints must apply the
same concealment rule to filters, pagination totals, and sort behavior.

## Credential provider and lifecycle

The credential-provider abstraction separates domain records from secret
material. A provider owns storage, retrieval, rotation primitives, and secure
destruction according to its implementation contract. StillFlow stores only an
opaque `SecretReference` plus non-sensitive lifecycle metadata.

| Lifecycle state | Meaning | Allowed transitions |
| --- | --- | --- |
| `pending` | Reference created but not usable for authenticated work | `active` after provider validation; `revoked`/`expired` only for a recorded terminal decision |
| `active` | Provider reference is eligible for the authorized operation | `rotating`, `revoked`, `expired`, or `recovery_required` |
| `rotating` | Replacement is being prepared; old reference remains governed by explicit overlap policy | `active` after atomic cutover, or `recovery_required`/`revoked` on failure |
| `revoked` | Reference is permanently denied | Terminal for that reference; replacement requires a new reference |
| `expired` | Provider or contract expiry has elapsed | Re-authentication/replacement only; never silently reactivate |
| `recovery_required` | Provider or local recovery action is required before use | `active` only after an explicit recovery procedure; otherwise `revoked` |

Rotation must not expose old or new secret values to ordinary API responses,
logs, artifacts, task records, or error payloads. Revocation must be
idempotent from the caller's perspective. Recovery must require an explicit
authorized action and must produce an auditable, redacted lifecycle event.

Unresolved implementation decisions are intentionally left open: provider
selection, provider-specific overlap duration, hardware/OS vault choice,
backup encryption format, and the exact recovery UX. Those decisions may not
weaken the opaque-reference, fail-closed, rotation, revocation, or recovery
invariants above.

## Secret references and redaction

The following are forbidden in ordinary StillFlow records, API responses,
artifacts, audit events, logs, metrics labels, panic/error text, and test
fixtures checked into the repository:

- plaintext passwords, API keys, bearer tokens, private keys, seed phrases, or
  provider payloads;
- reversible encodings, partial values, or fingerprints that permit secret
  recovery or useful guessing;
- authorization headers, signed-token bodies, or provider response dumps.

Allowed metadata is limited to an opaque reference identifier, provider kind,
non-sensitive creation/update timestamps, lifecycle state, expiry class,
rotation/revocation event identifiers, and redaction-safe diagnostic codes.
The backend may resolve provider-owned material internally for an authorized
operation, but the material must not enter durable domain objects or returned
payloads.

## Client and transport boundary

Web, Desktop, and CLI clients may construct requests, display typed results,
perform bounded reads, and maintain protocol compatibility. They may not:

- decide tenant membership, capability, or object visibility;
- execute jobs, perform canonical cleaning, compute authoritative identity or
  digests, publish Artifacts, or persist security state;
- cache credentials in ordinary client records or silently retry after a
  revocation/authorization failure;
- turn a local trust signal into a server-mode authorization grant.

Desktop local IPC/port behavior, server transport behavior, and protocol
version negotiation are separate implementation surfaces. Both must route to
the same Rust backend authorization and credential abstractions.

## Downstream dependency contract

| Downstream node | SEC-C0 input it must consume |
| --- | --- |
| `SEC-S1` | Identity/session and normalized request-context shape; provider-neutral authentication boundary |
| `SEC-A1` | Workspace/member/role/capability relations and object-level authorization outcomes |
| `AUD-C0` / `AUD-A1` | Actor/principal/workspace fields, redaction rules, and lifecycle event vocabulary |
| `OPS-O1` / `OPS-O3` / `OPS-O2` / `OPS-O4` | Server/local trust boundary, typed error concealment, and single Rust execution authority |
| `AUT-C0` / `AUT-J1` / `AUT-A1` | Service-account constraints, capability scoping, expiry/revocation, and bounded recovery |
| `H2` | Evidence that all Phase 2/3 surfaces preserve tenant isolation and secret redaction |
| `H3` | Release evidence for upgrade/rollback, recovery, packaging, and deployment-specific credential providers |

Each downstream task must record any deliberate refinement as a new decision
with compatibility impact and acceptance evidence. It may not silently replace
the frozen semantics.

## Non-goals and explicit unresolved decisions

This task does not implement or select:

- authentication endpoints, tokens, sessions, middleware, or an identity
  provider;
- database tables, migrations, indexes, tenant sharding, or distributed
  deployment;
- HTTP/IPC protocol details, status middleware, UI flows, or client storage;
- an OS keychain, cloud secret manager, encryption/HSM design, or backup format;
- policy authoring UI, delegated administration, cross-workspace sharing, or
  anonymous/public resources;
- runtime job/queue behavior, audit storage, metrics, tracing, packaging, or
  release automation.

These remain implementation decisions for the ordered downstream nodes. Any
implementation that cannot preserve the invariants here must stop and return
to a contract decision rather than introducing an implicit exception.

## Acceptance checklist

- [x] Single-user local and workspace-server support matrix is explicit.
- [x] `Workspace`, `Member`, `Role`, `Capability`, `ServiceAccount`,
      `Principal`, `Object`, and `SecretReference` are defined.
- [x] Authentication is separated from authorization and object-level checks.
- [x] Minimum-privilege and fail-closed rules are explicit.
- [x] Credential provider abstraction, rotation, revocation, and recovery are
      defined without plaintext storage.
- [x] Desktop local trust boundary and server boundary are explicit.
- [x] Forbidden/existence-leak behavior and `401`/`403`/not-found semantics are
      recorded.
- [x] Secret references and redaction constraints are recorded.
- [x] Downstream consumers, non-goals, and unresolved implementation decisions
      are listed.
- [x] This commit is documentation-only under `docs/evidence/security/**`.
