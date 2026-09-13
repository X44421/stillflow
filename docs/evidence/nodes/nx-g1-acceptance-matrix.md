# NX-G1 acceptance matrix and evidence (#344)

Gate for the NX series (#334). Baseline: `main@a301ef1` (merge of PR #354).
Toolchain: rustc 1.85.0, cargo workspace tests. Every row maps a contract
acceptance item to a runnable test in this repository; nothing below is
claimed without an executed test.

## End-to-end pipeline (real HTTP, real restart, real data)

`t_nx_g1_node_pipeline_values_revisions_and_durability`
(`backend/crates/stillflow-service/tests/http_entry_e2e.rs`) drives, over
real TCP with a three-row CSV fixture (`1, " alpha "`, `2, "beta"`,
`3, ""`):

1. connection register → asset discover → inspect (column ids discovered);
2. **compile** the declarative composite graph
   (`ops-clean/trim-clean@1.0.0`: trim, then empty-string→null) and the
   hand-written atomic equivalent;
3. **node preview** at the composite's output boundary with direct Arrow
   value checks: `label = ["alpha", "beta", NULL]` (trimmed, untouched,
   empty→null), field names/order `id, label, ignored`, and the per-field
   `stillflow.column.id` metadata carrying the discovered column identity —
   identical between the composite expansion and the atomic chain;
4. **graph revision save** → idempotent same-content save (same revision,
   `idempotent: true`) → changed content → revision 2 → **stale-editor CAS
   conflict** (409 naming "current revision 2") → **migration** dry-run
   (writes nothing) and apply (idempotent at the known format);
5. PlanVersion create/publish from the composite-compiled plan;
6. **process restart**; fetch of revision 1 returns the graph
   **byte-identical** (digest-equal) with metadata intact;
7. **Job** submission against the durable PlanVersion → `succeeded` →
   one committed **Snapshot** output.

## Contract acceptance → test mapping

| Contract item | Test | Result |
| --- | --- | --- |
| NX-C0 §10: v1 canonical bytes/fingerprint/result baseline for all eleven nodes | `stillflow-plan/tests/nx_s1_semantics.rs` (`v1_baseline_is_reproduced` over 52 fixtures captured at the pre-change base) | pass |
| NX-C0 §5/NX-S1: shared semantics — same schema or equivalent rejection at both entry points, all eleven nodes | `stillflow-engine/tests/nx_s1_differential.rs` (5 tests: expressions, rules, projections, full compile loop, frozen classification delta) | pass |
| NX-C0 §6/NX-N1: catalog ↔ validator mechanical round trip; test-only extension; stable ordering; duplicate rejection | `stillflow-core/src/node_graph/definitions/mod.rs` `consistency_tests` (4 tests) | pass |
| NX-C0 §7/NX-A1: zero connector calls before pure-graph validation; safe diagnostics; request-id echo; strict decoding; timeout law | `stillflow-api/tests/nx_a1_pipeline.rs` (3 tests, counting connector), `stillflow-service/tests/nx_a1_strict_decode.rs` (4 tests) | pass |
| NX-C0 §9/NX-B1: schema-snapshot accounting rejects the measured amplification; under-budget shapes byte-identical | `stillflow-plan/tests/nx_b1_resources.rs` (2 tests + measurement harness; evidence: `nx-b1-compile-resources.md`) | pass |
| NX-C1 §6–§8/NX-N2: package deployment rejections; composite vs atomic equivalence (rules, schemas, nullability); determinism; internal ids; fail-closed without deployment | `stillflow-api/tests/nx_n2_composite.rs` (5 tests incl. the digest law), `stillflow-plan/tests/nx_n2_composite.rs` (4 tests) | pass |
| NX-V0 §4–§7/NX-V1: save law (first/CAS/idempotent/conflict), history immutability, PlanVersion link, migrated-append chaining, workspace scoping, reopen durability | `stillflow-storage/src/graph_revisions.rs` tests (4 tests) + the HTTP e2e above | pass |
| NX-G1: atomic vs composite real-result difference matrix | the e2e above (identical preview values, NULLs, and field-level schema identity between the two graphs) | pass |
| NX-G1: published plans independent of packages | the e2e publishes the composite-compiled plan, restarts, and executes it; the plan contains only `Scan`/`ApplyRules`/`Materialize` (no package reference), matching the N2 structural assertion | pass |
| NX-G1: resource evidence on the merged baseline | `docs/evidence/nodes/nx-b1-compile-resources.md` (64n×4096f: ~28.9 MB serialized snapshots / ~31.1 MB peak / ~200 ms → now rejected up front by the frozen budget) | recorded |

## Measured boundary findings recorded during the series

- The config decode path cannot reach the 64-level expression bound (each
  `IsNull` costs two JSON levels); the deep-expression corpus uses depth 30.
- A 4096-column single source projection exceeds the 64 KiB config bound;
  the wide measurement shape uses an unprojected source.
- The local-tabular connector rejects pushed **subset** projections with a
  schema-drift error on this baseline (the e2e uses the full projection);
  a subset-projection read path is a connector work item, not part of this
  series' contracts.

## Uncovered scenarios and follow-up boundaries (recorded, not claimed)

- Subset source projections against local-tabular (connector work).
- Graph-revision migrations beyond the identity format-1 migration (the
  machinery ships; no second format exists yet, per NX-V0 §5).
- Draft upstream preview and verification-node admission (NX-P0 #346 owns
  the design); typed ports and DAG execution (NX-D0 #345 owns the design;
  the XR HOLD and #9/#10 remain closed).
- Multi-source, Join/Union, and any paused capability remain out of scope
  for the entire series (X-4/X-5 and the epic non-goals).
