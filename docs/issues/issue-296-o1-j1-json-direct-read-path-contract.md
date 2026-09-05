# O1-J1 — JSON direct projected read path: productionization contract

Issue #296 (`[O1-J1] Productionize the JSON direct projected read path`).
Status: **frozen** before implementation; changes to this contract require a
new commit that states what changed and why, before the affected code lands.

Evidence lineage: E24-JSON-A2 (#158) introduced the direct projected writer as
a private, capability-only experiment; O0-J1 (#283 / PR #294) revalidated it on
post-H3 main with digest parity, identical logical I/O, identical Polars parse
passes and ~36–41% ingest-time reduction on the wide-table main scenario, and
judged the former #151 temporal blocker obsolete for this path (§8 there). The
implementation parity obligations are the module docs of
`direct_projected.rs` (error surface, duplicate keys, temporal normalization,
canonicalization fallbacks, bounded-memory shape); they are inherited verbatim
and NOT restated here.

## 1. Routing conditions (frozen)

1. The switch is a per-connection configuration key, not a cargo feature:
   `jsonDirectProjectedWriter: boolean` in the local tabular connection
   config, default **`false`**.
2. The default `false` preserves today's production byte-for-byte: the generic
   `parse_projected_object` DOM path handles every row. The default IS the
   rollback point.
3. When `true`, every row of every JSON/NDJSON batch read on that connection
   is assembled by `direct_projected::ProjectedRowAssembler` instead of the
   generic DOM reconstruction. No other routing dimension exists: not per
   asset, not per row, not per projection width. Determinism beats micro-tuning.
4. Scope: the batch read path only (`read_batches` → `RawBatchStream`,
   `ReaderKind::Json`). CSV, TSV, Parquet, preview, inspect and inference are
   untouched.

## 2. Support range (frozen)

The direct path is full-range for JSON reads: every row that the generic path
accepts or rejects is accepted or rejected identically when the knob is on.
Edge inputs (nested duplicate keys, raw JSON control bytes inside captured
subtrees, integer literals wider than serde_json's integer parse,
timestamp-containing fields, streaming-invalid List/Struct subtrees) are
handled by the internal canonicalization fallbacks documented in
`direct_projected.rs`, which reproduce generic `Value` semantics exactly.
There is deliberately NO input predicate that routes around the direct path;
the support range is "the same rows the generic path handles".

## 3. Fallback triggers (frozen)

Fallback is whole-read granularity, decided once before the first row streams;
there is never a mid-stream switch, so the observable error ordering cannot
change mode mid-read:

1. knob absent or `false` → generic path for the whole read;
2. assembler construction failure (Internal-class; practically unreachable —
   per-field key encoding) → generic path for the whole read.

Row-level failures on the direct path are NOT fallbacks: they are the audited
parity surfaces (same category, message, earliest failing row as generic) and
stream what the generic path would have streamed.

## 4. Error mapping (frozen)

Unchanged from the audited pair. Both paths raise identical
`ErrorCategory`/message/earliest-row for every input class listed in the
differential oracle (`tests/direct_projected_writer.rs`). The oracle becomes a
RUNTIME differential with this task: every case runs once with the knob off and
once with the knob on and asserts identical observable outcomes, so the dual
compile-mode CI run is no longer needed to prove parity.

## 5. Memory shape (frozen)

Unchanged from the #294-audited bounded-memory shape of `direct_projected.rs`:
per-batch scratch only, one row-sized owned buffer per row (the same re-encode
target the generic path allocates), canonicalization copies bounded by the
captured subtree and dropped with the row, nothing held across batches. The
measurement note re-observes RSS/VmHWM fields for both arms.

## 6. Compile-time surface (frozen)

The private cargo feature `json-direct-projected-writer` is REMOVED. Both
paths are always compiled; `serde_json/raw_value` becomes an unconditional
dependency of the connector crate (purely additive API availability, no
behavior change). Rationale: a runtime fallback requires the generic path to
exist in the same binary as the direct path, which the compile-time
mutual exclusion made impossible. Config compatibility: absent key = today's
behavior; older binaries reject the new key as invalid configuration
(`deny_unknown_fields` retained).

## 7. Default-enablement flip (explicitly out of scope)

Flipping the default to `true` is NOT part of this task. It requires the O1-R1
combined-regression evidence and a separate decision commit. Until then the
direct path is reachable only through explicit connection configuration.

## 8. Acceptance bindings

1. The runtime differential oracle passes at the final PR head (every case:
   knob-off outcome == knob-on outcome == the absolute expectations).
2. Wide-table main scenario (the #294 primary cell): ingest time with the knob
   on is >= 30% below the knob-off arm at the SAME head, SAME machine, BOTH
   arms inside one flock-serialized measurement session; recorded in an
   evidence note under `docs/evidence/performance/` with the exact measured
   head and raw records.
3. Config tests: default-off parse, knob-on parse, unknown-key rejection
   unchanged.
4. Acceptance binds to the final PR head; any post-measurement commit that
   touches measurement behavior re-runs the measurement.
