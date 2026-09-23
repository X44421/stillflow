# Issue #410: Polars-native recipe component library contract (PL-C0)

> Status: candidate contract for review; this document alone authorizes no code change
> Risk: L3 decisions, docs-only delivery
> Base: `main@1c1f76622aa9f01c66d1b347c85adbc35399ff23` (Polars 0.55.2)
> Parent: [#361](https://github.com/X44421/stillflow/issues/361)
> Related: [#363](https://github.com/X44421/stillflow/issues/363), [#399](https://github.com/X44421/stillflow/issues/399), [#408](https://github.com/X44421/stillflow/issues/408), [open PR #397](https://github.com/X44421/stillflow/pull/397), [#10](https://github.com/X44421/stillflow/issues/10), [OpenShip #36](https://github.com/X44421/openship/issues/36)

## 1. Decision and authority

StillFlow will present reusable *components* on the canvas. A component is an immutable, versioned, bounded recipe of approved Polars-backed logical operations. Users compose operations inside a recipe editor and configure published component instances on the canvas. This avoids registering one permanent canvas node type for every Polars operation or every useful combination. The v1 canvas remains `Source -> component(s) -> Output` on one connected linear path.

A recipe is declarative input to the existing authority chain: validated `NodeGraph` and exact definitions -> `stillflow-plan::LogicalPlan` -> governed Preview/Verification or `PlanVersion` -> Run/Artifact. Canonical `LogicalPlan` bytes and its fingerprint remain the execution authority. A recipe is not a second expression language, plan, runtime, SQL escape hatch, Rust closure, Polars object serialization, UDF, plugin, or arbitrary Polars method string.

Crate ownership stays fixed: `stillflow-core` owns Polars-free domain contracts; `stillflow-plan` owns logical schemas, closed `Expr`/`Rule`/plans, validation and deterministic compilation; `stillflow-engine` owns Polars lowering, bounded resources, cancellation, and execution; `stillflow-storage` owns immutable metadata and atomic visibility; `stillflow-api` owns workspace authorization and service integration. Polars `Expr`, `LazyFrame`, `DataFrame`, closures, and physical execution state never cross a public core/plan/storage/API boundary. DuckDB's accepted federation, join, and preview SQL responsibilities are not reassigned by a catalog entry.

This PL-C0 delivery changes only this contract and its `docs/issues/` pointer. It does not add an AST, node registration, Cargo feature, database migration, route, UI, or execution behavior. Follow-up L3 implementation slices require their own issues, scoped claims, accepted contract binding, exact-head evidence, and review under `AGENTS.md`.

## 2. Existing baseline and compatibility boundary

At the stated base, the production registry has 13 built-in definitions: source, select, filter, rename, trim, cast, replace-literal, fill-null, drop-column, derive-column, normalize-text, sort, and output. The existing static Composite package channel is deliberately small: at most eight declared operators and four expanded steps, with a single `column` binding and only identity/literal substitutions. It is not a user-published library. Existing graphs, the deployed `trim-clean` Composite, saved plans, runs, and artifacts retain their current meaning and resolver path.

`NodeGraph v1` already has a bounded `NodeConfig { id, typeId, configVersion, config, metadata }`; the `config` value is a JSON object validated against the selected definition. Its top-level decoder rejects unknown fields. The first recipe implementation SHALL use one new reserved recipe `typeId` with `configVersion=1` and put the exact component reference and public parameter values *inside* `config`. It SHALL NOT add a `componentRef` sibling to `config` or reinterpret an existing node type. Therefore a recipe instance need not change the v1 graph envelope or GraphRevision format. Older code will reject the new type as unknown; existing v1 graphs continue to compile through the legacy path. A future topology or envelope change needs a separately contracted graph version and explicit migration.

Current `NodeRegistry::deployed()` is static. It can register one generic `stillflow.recipe@1` structural definition with fixed `in`/`out` ports, but must not register one definition per published component/version. The API authenticates the workspace before component or source resolution. A storage/service resolver loads exact immutable definitions by tuple and digest for graph save, compile, and preview without mutating a process-global registry. `stillflow-plan` receives already resolved declarative definitions and performs no authentication, database access, or other I/O. Generic graph validation checks the envelope and topology; the resolver and pure compiler then check the exact definition, parameters, recipe, and operator semantics. The compiler may reuse Composite's deterministic expansion and product-node-to-final-plan-node mapping principle, while giving recipes their own format, bounds, and persistence. The resolved recipe must lower to the same logical semantics as an equivalent accepted built-in operation.

## 3. Library, instance, and public data contract

The v1 library has a read-only system scope and a workspace-owned scope. A workspace draft can be edited; publish creates a new immutable version. Cross-workspace or organization-wide sharing is deferred because current graph, source, and authorization authority is workspace-scoped. Display names and a UI's `latest` suggestion are never execution identity. Published versions cannot be edited in place.

The definition is a closed, bounded data structure with these required semantic fields:

```text
RecipeDefinition {
  recipeFormatVersion: 1,
  libraryId, componentId, componentVersion, contentDigest,
  displayName, description,
  input:  one required in/PrimaryData/TabularStream port,
  output: one out/PrimaryData/TabularStream port,
  parameterSchema: closed typed schema,
  steps: ordered [RecipeStep]
}
RecipeStep { stepId, opId, opVersion, arguments: closed typed bindings }
```

The two ports reuse the accepted closed port/schema-constraint vocabulary from #363. Port identifiers and `stepId`s are definition-owned, bounded ASCII, unique within their respective scopes, and stable within a release. The v1 profile allows no nested component, branch, multi-input, multiple output, recursive reference, arbitrary expression text, executable body, or user supplied Polars path. Each step transforms the current logical table/schema in order; `stepId` is a diagnostic identity, not an addressable intermediate output. Step arguments may bind only declared public parameters, logical `ColumnId`s in the current schema, bounded typed literals, or the existing closed `stillflow-core::Expr` grammar where the operation admits it. A derive step appends one caller-identified column, following the existing `DeriveColumn` law. The instance fixes that column's `ColumnId` once in `outputColumnIds[stepId]`; the map must have exactly the deriving steps and no other entries. These IDs remain stable on reload and are checked for collision against source and preceding output columns, including across multiple component instances. Forward references and undeclared parameters fail closed. Defaults, coercion, nullability, and failure policy are explicit; no runtime inference may change a saved recipe's meaning.

An instance uses the existing graph wire shape:

```text
NodeConfig {
  id: NodeId,
  typeId: "stillflow.recipe",
  configVersion: 1,
  config: {
    componentRef: { libraryId, componentId, componentVersion, contentDigest },
    parameters: { ... public parameter values ... },
    outputColumnIds: { ... derive stepId -> fixed ColumnId ... }
  },
  metadata: bounded non-executable metadata
}
```

`configVersion` describes this reserved node envelope. It is distinct from the immutable component release and recipe AST version. The component release/digest binds its parameter schema. An instance may change its public parameter values, but cannot change recipe steps, ports, or its assigned output-column identities except through an explicit graph edit/upgrade. The resolver checks the exact reference, digest, scope, parameter schema, ports, operator support, output-column map, and size limits before compiling. A graph never saves an unpinned `latest` reference.

## 4. Curated operator catalog

The catalog is an allowlist tied to the pinned Polars Rust 0.55.2 build and its *enabled* Cargo features, not a copy or count of methods in Polars documentation. Each stable, versioned operation record contains:

| Field | Required meaning |
| --- | --- |
| `opId`, `opVersion`, status | Permanent StillFlow semantic ID/version; `Supported` or `Deferred` |
| `context`, `apiBinding` | `Expr` or `LazyFrame` and exact Rust API/method family |
| argument schema | Closed type, bounds, defaults, parameter binding, and validation |
| input/output law | Logical types, nullability, `ColumnId`, schema and error effects |
| row/order/state effects | Preserve/filter/expand/reduce; preserve/reorder; stateless/bounded/global |
| `executionScope` | `ChunkLocal`, `RunGlobal`, or `MultiInput` |
| feature and mode gates | Required compiled Cargo features and Streaming/Materialize support |
| resource/cancel and mapping | Existing finite budget/cancellation law and logical `Expr`/`Rule`/`PlanNodeKind` target |
| evidence | Compile and semantic corpus identifiers for the exact pinned build |

An operation becomes `Supported` only after its API compiles under the repository's explicit feature set and its value, null, schema, `ColumnId`, row order, error, cancellation, and resource laws pass the accepted corpus. An API's existence, a catalog label, or a user's JSON cannot enable a missing feature or cross a stage gate. Polars upgrades must revalidate catalog entries; an observable semantic change requires a new `opVersion`, not an in-place reinterpretation. Catalog search may show `Deferred` operations with their reason, but publishing or compiling a recipe that uses them fails closed.

The initial executable candidate set is single-input, linear, row-local and batch-partition invariant: projection/select, filter, rename/drop, cast, fill-null, trim, replace-literal, and constrained scalar derivations expressible by existing `Expr`/`Rule`. Each candidate is individually admitted only after the above evidence exists. Source scanning and output publication remain structural boundaries, not freely reorderable recipe steps. Existing `normalize-text` uses StillFlow-owned Unicode/text logic inside a Polars execution callback; it cannot be labeled Polars-native without an individually proven native equivalent. Sort is global; group/aggregate, deduplication, and pivot need cross-batch state; join/union need multi-input topology. They remain `Deferred` until their relevant #363/#408/#10 gates are implemented and tested.

The product target is broad Polars coverage through staged catalog admission. An operation that already maps to an accepted closed logical `Expr`, `Rule`, or `PlanNodeKind` can be admitted through a catalog/evidence slice. An operation with no such logical representation first requires a separate L3 AST/semantics contract and implementation; a catalog record alone cannot create execution semantics. Neither path automatically generates a new canvas node type. Feature expansion and global execution remain explicit delivery work.

## 5. Compilation and physical execution

After API workspace authorization, the storage/service resolver loads each exact immutable definition, recomputes the digest, validates the instance against the published schema, and supplies bounded declarative steps to the pure graph compiler. The compiler maps them into the accepted logical `Expr`/`Rule`/`PlanNodeKind` vocabulary. It does not authenticate, fetch data, or accept a serialized Polars `LazyFrame`. Internal plan IDs derive deterministically from product node ID and step position, collision-check, and preserve product-node to final-plan-node preview/schema mapping. Derive output `ColumnId`s come from the instance's fixed `outputColumnIds` map, are unique in the current schema, and are preserved by subsequent steps unless an explicit schema operation changes them. Identical graph bytes, exact definition digests, authorized source schema, and compiler/catalog versions produce identical canonical plan bytes and fingerprint. Successful formal compilation persists the ordinary `PlanVersion`; a saved plan and its run do not require library lookup at execution time.

Preview and Run keep their existing bounded source, Arrow interchange, cancellation, rebatching, and artifact paths. Both use the shared Polars lowering appropriate to their accepted scope. No recipe path may add a `collect()` for every graph step or bypass resource prediction. #408 governs one-LazyFrame-per-chunk work and later Preview/Materialize mode decisions. Open PR #397 was based on Polars 0.46 and is still open; its exact head must be reviewed and adapted to main's Polars 0.55.2 before reuse. It is a chunk plan, not a whole-run global plan. Failure attribution is part of semantic parity: a later strict cast or parse failure must name the actual failing step/column, not the first checkpoint in a combined plan.

Current Run must not advertise global sort as supported merely because Preview has a whole-input sort path; the Run chunk lowering currently treats Sort as a no-op. PL-C0 leaves legacy Sort behavior unchanged because it is docs-only, but a follow-up must resolve that existing correctness gap before claiming Materialize Sort support: either implement bounded whole-run ordering or reject Sort during preflight before source I/O, with a deliberate compatibility decision and regression tests. No new recipe may include Sort until that gate is accepted. Grouping, aggregation, deduplication, and pivot cannot be approximated per chunk. Join/Union are currently rejected by engine preflight. Their unlock requires separately accepted cross-batch or multi-input semantics, finite state/spill, authorization, cancellation, and both Preview/Run parity. Unsupported recipe scope fails before source or connector I/O.

## 6. Scope examples and stage gates

An eligible first component is `trim-empty-to-null`: one UTF-8 column parameter, ordered native trim then empty-string-to-null expression, and one tabular output. Its output column becomes nullable. The existing deployed `trim-clean` is a comparison target only; publication requires differential value, null, schema, order, error, and bound evidence on the pinned Polars build. The illustration does not assert that a particular catalog operation is already supported.

A future `orders-with-customer` component could have named `left` and `right` tabular inputs and a left join. Stage 0 rejects it before connector I/O: the current graph admits one source, one output, and only `in`/`out` on a connected linear path. #363's logical Join/Union contract alone does not enable its state, topology, source authorization, or execution. #408 does not silently move DuckDB join/federation ownership to Polars. Similarly, whole-run sort or group cannot be marked supported until both Preview and Materialize satisfy the relevant global laws.

## 7. Version, digest, and provenance

| Axis | Meaning |
| --- | --- |
| Graph `version` / GraphRevision `formatVersion` | Graph wire interpretation; remains v1 for the instance shape in §3 |
| `recipeFormatVersion` | Closed serialized recipe AST |
| `libraryId` + `componentId` + `componentVersion` | Immutable published library identity; strict SemVer release |
| `NodeConfig.configVersion` | Reserved recipe-instance envelope version |
| `opId` + `opVersion` / catalog version | Stable semantic operation and catalog contract |
| Polars/compiler/features | Recorded implementation provenance, never a silent semantic rewrite |
| `contentDigest` | Integrity identity over canonical published definition bytes |

The component digest is lowercase `sha256-` plus 64 hexadecimal characters of `SHA-256(UTF8("stillflow.recipe.definition.v1") || 0x00 || canonicalBytes)`. `canonicalBytes` uses `recipe-cjson-v1`: parse the closed typed definition while rejecting duplicate object keys and unknown fields *before* conversion to generic JSON values; serialize to UTF-8 JSON with object keys sorted by UTF-8 bytes, no insignificant whitespace, and all array order preserved. Strings keep their Unicode scalar sequence without NFC folding and use the pinned workspace `serde_json` JSON escaping; integers are signed/unsigned 64-bit base-10 without leading zeroes. Float literals use the existing finite `FiniteF64` law (reject NaN/infinity; normalize `-0` to `0`) and the serializer's shortest round-trip decimal. The server alone computes the authoritative digest; implementation must freeze byte-for-byte golden vectors for Unicode/escaping, integer bounds, finite floats including `-0`, nested objects, and reordered steps so a dependency update cannot silently alter it. The digest covers **all** definition fields, including display name/description, identity/release, ports, parameter schema/defaults, recipe format, ordered steps, and op versions; only `contentDigest` is excluded. Identical bytes published to the same identity/version are idempotent; the same identity/version with different bytes conflict. Publish never overwrites a released definition. A digest is integrity identity, not authorization.

The instance reference's exact tuple lives inside graph `config`, so the existing canonical graph JSON hash binds that tuple. The immutable component version and digest also bind its parameter schema; v1 needs no independently mutable parameter-schema version. An optional GraphRevision package-digest summary may be derived for audit, but cannot replace the instance's exact reference. Explicit upgrade compares old/new digest, port/schema and parameter changes and a bounded sample result before appending a graph revision. A picker may display the newest release but cannot silently upgrade saved graphs.

## 8. Persistence and legacy migration

The first implementation adds append-only workspace component-version storage and a read-only system catalog; it preserves v1 graph rows and historical revisions. It does not require a NodeGraph v2 or an automatic rewrite. Current main's SQLite schema has advanced through migration 13; a storage implementation must allocate the next migration against freshly checked main, rather than copying #342's older snapshot. Graph revisions keep their existing compare-and-swap, idempotency, canonical hash, and append-only behavior. Missing or mismatched component content rejects a new compile/preview with no partial plan or revision. Existing `PlanVersion`s and runs remain executable from their stored logical plan without a library lookup.

Legacy built-ins and the deployed `stillflow.composite.trim-clean@1.0.0` remain resolvable for saved graphs. An explicit legacy-to-recipe upgrade is dry-runnable, deterministic, repeatable, shows a semantic/parameter/schema diff, and appends only if the graph tip is still current. `trim-clean` conversion needs exact old package digest and differential parity; a name match is insufficient. Unknown Composite packages remain legacy or are rejected for migration. `normalize-text` remains a legacy non-native path until independently re-expressed and proven. Failed migration creates no revision and leaves old plans/runs/artifacts intact. Old nodes may leave the *new-node picker* only after replacement, upgrade, and rollback paths have accepted evidence; removing their execution support is a separate compatibility decision.

## 9. API and OpenShip boundary

The later backend API needs: paginated/searchable operator catalog with support/gate diagnostics; authorized system/workspace component list and exact version read; workspace draft save/validate; immutable publish with version conflict; exact component resolution during graph save/compile/preview; and explicit dry-run upgrade plus normal GraphRevision append. These are contract needs, not routes created by PL-C0. Authentication and workspace scope are checked before component/source resolution. A recipe contains no connector identity, credential, file path, source row, or secret reference. Preview retains its current row/time/byte/cancellation bounds and creates no plan/run/artifact unless an existing governed endpoint explicitly does so.

The UI implementation belongs in the OpenShip repository, after the StillFlow wire contract and API exist. It should turn the existing searchable node menu into a component picker, provide a bounded operation search inside the recipe editor, distinguish editable drafts from immutable releases, render public parameters in the instance inspector, and require an explicit version-upgrade diff. It must not implement recipe execution, source authorization, or Polars semantics in browser code. Existing OpenShip #36 covers older node categories and needs an explicit library follow-up/dependency; PL-C0 does not mutate that repository.

## 10. Bounds, errors, cancellation, and security

The existing graph limits remain: 2 MiB graph; 64 nodes; 63 edges; 64 KiB per node config; 1 MiB total config; 4 KiB string; 64 KiB metadata; JSON nesting 64; `Expr` 1,024 nodes/depth 64; compiler work 2,000,000 units. The new first-release definition cap is **64 KiB canonical bytes**, **16 recipe steps per component**, and **256 expanded recipe steps per graph**, with **1 MiB total resolved definition bytes per operation**. Check these ceilings before proportional allocation/expansion in every graph save, compile, and preview entry; they are not the legacy Composite four-step cap. A follow-up may raise them only with measured compile, memory, and error-bound evidence. Catalog and component lists use bounded pagination. Existing Preview, engine, and publication resource/cancellation limits apply unchanged. In particular, `stillflow-engine/src/lib.rs` defines `PREVIEW_MAX_ROW_LIMIT=10,000`, `PREVIEW_MAX_BYTE_LIMIT=50 MiB`, `PREVIEW_MAX_DEADLINE=30 s`, and `PREVIEW_MAX_SOURCE_ROWS_SCANNED=100,000`; recipes cannot override them or suppress cancellation.

Failure must be typed, bounded, and attributed to component, instance, step, and safe field path where applicable. Minimum new classes: unknown operator/version, disabled feature, unsupported recipe format/scope/streaming mode, component missing/digest mismatch/version conflict, invalid port/parameter/type, and recipe limit. Reuse existing `NG_*`, logical-plan, Preview, and engine classes where they already own the error. Invalid definitions, unsupported targets, unauthorized scope, and missing content fail before connector I/O and never produce a partial graph revision, plan, preview, or artifact. Strict execution failures must preserve the actual failing step/column even when operations share one LazyFrame. Diagnostics, logs, events, and preview metadata must not echo parameter literal values, source paths, credentials, secret-like keys, or raw third-party errors. Apply existing `ensure_no_secret_fields` and bounded validators to definitions, arguments, parameters, and metadata.

## 11. Delivery slices and ownership

1. **PL-C0 (#410):** accept this docs-only contract and its issue pointer after independent consistency/link review. No writer claim is taken for runtime surfaces by this document.
2. **PL-C1, core/catalog:** new closed recipe definition and exact-ref validator, digest, support catalog, reserved v1 node type, and deterministic bounds. L3 issue/claim/contract binding. No Polars type in core.
3. **PL-R1, compiler:** service-side authorized exact resolution feeding a pure deterministic expansion to existing logical semantics, with product-node schema/preview mapping and exhaustive negative tests. L3 issue/claim; recheck the implementation base and overlapping work.
4. **PE/#408:** independently review open #397 exact head, decide reuse/adaptation on Polars 0.55.2, then prove combined chunk lowering and accurate failure attribution. Global Preview/Materialize and Streaming decisions are later PE slices.
5. **PL-S1/PL-A1, storage and API:** immutable workspace versions, draft/publish/read, authorization, graph validation, and explicit migration; use additive SQLite migration and existing revision/plan authority. Split claims/PRs by shared writer surface.
6. **OpenShip library UI:** implement in its designated repository/workspace after backend DTOs and errors are accepted. Then verify save, reload, publish, exact reference, compile, preview, Run, and explicit upgrade end-to-end.
7. **Capability expansion:** global sort/group/aggregation/dedup/pivot, branches, multi-input Join/Union, and new Polars features each pass their respective #363/#408/#10 contracts and semantic/performance corpus before catalog admission.

No slice may change the legacy execution path merely to simplify a new picker. The `wt-polarlib` feature-expansion list from #399 remains an input to separate feature decisions, not an enabled feature set.

## 12. Objective acceptance for PL-C0

| Check | Evidence required |
| --- | --- |
| Product shape | Definition, instance, operation, graph, and plan authority are distinct in §§1, 3, 7 |
| Existing behavior | Current built-ins, Composite, graph v1, PlanVersion, runs, and artifacts have explicit compatibility rules in §§2, 8 |
| Native operator admission | Every supported catalog entry must have compiled-feature and semantic evidence under §4 |
| Execution scope | One-input example compiles only when catalog-supported; global/multi-input example rejects at Stage 0 under §§5–6 |
| Determinism | Exact reference, canonical digest, compile mapping, version conflict/idempotency and explicit upgrade are specified in §§3, 5, 7–8 |
| Bounded and safe | Finite recipe/graph/Preview limits; typed errors; cancellation and secret handling in §10 |
| Ownership | #363/#408/#10 runtime gates and OpenShip UI dependency are stated in §§5–6, 9, 11 |
| Docs-only scope | Only this canonical contract and its `docs/issues/` pointer change in #410 |

Before acceptance, verify internal links, issue/PR state, dependency arrows, and that every follow-up criterion above can be turned into a pass/fail test. A later implementation PR must name its exact changed files/crates, public API and storage changes, ownership/allocations, `unwrap`/`expect` and TODO inventory, tests, deviations, risks, base/head, and the frozen contract. PL-C0 is a reviewable design gate, not a claim that the library already exists.
