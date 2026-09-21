# Issue #370: shared sort and the stateful execution base (slice 1)

> Status: Candidate frozen contract for acceptance by #370
> Risk: L3 — new logical plan operator, new node config surface, execution semantics
> Parent: #361 (Epic: complete the twelve NodeGraph data-processing capabilities)
> Depends on: #363 §8 and §9 (merged) — the stateful execution laws these slices must satisfy; #368 (merged) for the established additive-extension pattern
> Base: `main@3f0b958d3e76183e7c535d2f4f548eb752efc0b6`
> Suggested branch: `agent/issue-370-nx-s0-sort-stateful`

This document freezes **slice 1** of #370: the stable-ordering primitive that `FillNull` groups (#369),
`Deduplicate` (#371) and the aggregation operator (#374) all need before they can define their
cross-batch state.

It authorizes a new `sort` node, a new `PlanNodeKind::Sort`, and exactly one execution responsibility:
buffer a bounded input, stable-sort it once, emit it. It authorizes **no** grouping, aggregation,
deduplication, spill, or second executor.

Every law below is binding. The stateful laws of
[#363 §8–§9](../contracts/issue-363-nx-c1-stateful-multi-source-execution-contract.md) already bind
this work; where this contract is silent, #363 governs.

---

## 1. New product node: `stillflow.node.sort`

Config version 1. `additionalProperties: false`.

| Field | Kind | Required | Constraint |
| --- | --- | --- | --- |
| `keys` | list of sort-key objects | yes | 1..=8 entries |

Each sort-key object has exactly these fields, all required:

| Field | Kind | Constraint |
| --- | --- | --- |
| `column` | `ColumnId` | must resolve in the working schema |
| `direction` | enum | `ascending` \| `descending` |
| `nulls` | enum | `first` \| `last` |

* A repeated `column` across keys is rejected: a key list is a tuple of distinct columns, so the
  declared order is the whole ordering intent.
* The `nulls` field is **required and explicit**. Per #363 §8.1.3 there is no engine default for NULL
  placement; a configuration that omits it is rejected rather than defaulted.
* Ports: one `in`, one `out`. `lowering_target` is `Sort` (new). `NodeRole::Transform`.

## 2. New logical plan operator

`PlanNodeKind::Sort { keys: Vec<SortKey> }`, with

```text
SortKey { column: ColumnId, direction: SortDirection, nulls: NullPlacement }
SortDirection { Ascending, Descending }
NullPlacement { First, Last }
```

`SortDirection` and `NullPlacement` are closed vocabularies. Like `CastFailurePolicy` and
`TextOperation`, the product layer and the plan layer each declare their own copy and the node-graph
compiler converts explicitly; no crate dependency is inverted.

The sort is a **first-class `PlanNodeKind`, not a `Rule`.** Ordering is a property of the relation, not
of a row, so it cannot be expressed as a per-row rule and must remain visible to the plan for
optimization, diagnostics and fingerprinting. `Rule` gains no `Sort` variant, and a sort node lowers to
`PlanNodeKind::Sort` directly — never into a rule chain.

## 3. Schema law

* `Sort` is **schema-preserving**: its output schema equals its input schema exactly — same fields,
  same ids, same names, same data types, same nullability, same order.
* It is neither a field-producing nor a field-consuming operator. It declares no
  `minimum_field_count`, and it must not perturb `DeriveColumn` identity or the
  `DerivedNullabilityNarrower` law.
* Ordering is an observable, contracted property of the **result**, not of the schema. Two sorts
  over the same schema with different keys share a schema and differ in plan fingerprint.

## 4. Ordering laws

1. **Stable ordering.** Rows that compare equal under every key keep their relative input order
   (#363 §8.1.1).
2. **Tie order is declared.** The declared tie-break is *input order*, which is the connector's
   declared stream order — a deterministic sequence, never hash-map, partition, thread or arrival
   order (#363 §8.1.2).
3. **NULL placement is per key and explicit.** `First` and `Last` are the only options and each is
   honoured for both directions. `Ascending` + `First` and `Descending` + `First` both place NULLs
   ahead of every non-NULL value; the direction governs non-NULL values only.
4. **Ordered types only, no implicit coercion.** A key column must be one of: `Int8`, `Int16`,
   `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64`, `Date32`,
   `Timestamp { .. }`. `Utf8`, `Boolean`, `Binary`, `Null`, list and struct columns are rejected. A
   mixed-type key list is never coerced — each key is checked against its own column type
   (#363 §8.1.4). `Timestamp` keys are additionally subject to the existing
   `TimestampSecondPaused` capability gate.
5. **Total order over the compared domain.** Floating `NaN` compares and orders deterministically and
   equal to itself for ordering purposes, so the order is total and reproducible.

## 5. Execution model and bounds

1. **One materialization, one sort.** Sort buffers its entire input, performs exactly one stable sort,
   and emits. It never emits a row before consuming its whole input, because any such row could be
   displaced by a later one.
2. **Fail closed, never spill.** If the buffered input would exceed `MAX_OPERATOR_STATE_BYTES`, the run
   fails with `BoundExceeded` and publishes nothing (#363 §9.3: an uncontracted spill path is
   forbidden; this slice contracts none). No partial output, no truncated result, no silent drop.
3. **State is engine-accounted.** The sort buffer is charged through the existing
   `MemoryTracker`/`AllocatorPhase` accounting before the retained allocation is made; no operator
   allocates outside that accounting (#363 §8.4, §9.3).
4. **Cancellation.** The request context is checked at every batch-admission boundary; on cancellation
   or deadline expiry the buffer is dropped and nothing is emitted (#363 §9.4).
5. **Temporary resources.** This slice creates no spill files and no scratch resource that outlives
   the run; the buffer is released on success, cancellation and failure.
6. **Declared bound.** This slice introduces exactly one operator bound,
   `MAX_SORT_INPUT_BYTES = MAX_OPERATOR_STATE_BYTES`, checked before each batch is admitted. It
   tightens no engine bound and widens none. The key-count bound of §1 is a schema law, not a
   resource ceiling.
7. **No second engine.** Sort executes inside the existing engine on the existing `BatchEnvelope`
   stream and publishes through the existing path. It introduces no executor, no scheduler, and no
   publication path.

## 6. Batch-boundary invariance

Per #363 §8.3, changing the request `batch_size` may not change values, field identity, schema,
nullability, row order, or counts. This is the acceptance test that matters most: the same input read
at several batch sizes must produce byte-identical output batches in the same order.

## 7. Compatibility

* **Additive.** No existing `Rule`, `PlanNodeKind`, node config, catalog entry or wire tag changes
  meaning. Plans and graphs that compile today keep compiling and keep serializing to byte-identical
  canonical bytes; the frozen version-1 corpus is unaffected.
* **Catalog agreement** applies: the `sort` catalog entry must describe exactly what the validator
  admits, and the positive/negative sample test of the node-definition harness must cover it.
* **No pause is lifted and no bound is widened.** `CheckedArithmeticPaused`, `ListStructPaused`,
  `TimestampSecondPaused`, `DateToUtf8CastPaused` and `BinaryCastUnauthorized` are untouched.

## 8. Required tests

1. **Batch-boundary invariance**: one input, several `batch_size` values → identical rows and identical
   row order.
2. **Stability**: rows equal under all keys keep input order, including a tie run that straddles a
   batch boundary.
3. **NULL placement**: all four combinations of direction × placement, with NULLs in the data.
4. **Multi-key**: a second key breaks first-key ties, and key order is significant.
5. **Rejected configurations**: zero keys, more than eight keys, a repeated column, a missing
   `nulls`, an unknown direction, an unknown `nulls`, a `Utf8` key column, a `Boolean` key column,
   and an unknown column.
6. **Schema preservation**: output schema equals input schema, including nullability, so a downstream
   `DeriveColumn` declaration still passes the narrowness law.
7. **Bound fail-closed**: an input whose buffered state exceeds `MAX_SORT_INPUT_BYTES` fails with
   `BoundExceeded` and emits nothing.
8. **Cancellation**: an already-cancelled context fails before emitting any batch.
9. **Regression**: the frozen version-1 corpus still compiles to byte-identical plans.

## 9. Non-goals and stop conditions

This slice does **not** authorize: grouping or group keys (#363 §8.2, deferred to the operator that
needs it), aggregation, deduplication, window functions, forward-fill across groups, spill of any
kind, a second executor or scheduler, a new publication path, `SortKey` expressions (only plain
column keys), per-key collation or locale, `Utf8`/`Boolean`/`Binary` ordering, widening any existing
bound, or any OpenShip change.

**Stop and return to contract review** if the implementation needs a dynamic sort key expression, an
unbounded buffer, a spill format, a second execution engine, a change to an existing plan or rule
wire tag, or a semantics that contradicts #363 §8–§9.
