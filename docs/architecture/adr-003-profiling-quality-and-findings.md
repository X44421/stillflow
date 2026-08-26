# ADR-003: Profiling quality and findings contract

> Status: Proposed
> Date: 2026-08-25
> Decision owners: Stillflow maintainers
> Charter: [#103](https://github.com/X44421/stillflow/issues/103) (Q-C0), under
> [#81](https://github.com/X44421/stillflow/issues/81) Track Q
> Factual input: [`profiling-quality-domain-inventory.md`](../issues/profiling-quality-domain-inventory.md)
> (issue #65, merged docs-only), cited below as `Q0-D0 §n`
> Precedent: [ADR-002](adr-002-deterministic-runtime-and-physical-executors.md)
> structure and citation discipline; [ADR-001](adr-001-logical-physical-and-storage-boundaries.md)
> remains Accepted historical authority.

Citation discipline for this document:

- Statements about code that exists at
  `main@04966586192f8750a02790da988db71a28d82074` cite `path:symbol`
  (repo-root-relative, backend crates under `backend/crates/`); facts already
  established by the accepted inventory cite `Q0-D0 §n`.
- Statements that introduce a new rule of this ADR are labeled **[Decision]**.
  Where this ADR resolves an interpretation question left open by Q0-D0, the
  resolution is labeled **[Decision — interpretation]**.
- Items that cannot be settled today are stated in
  [Open questions](#open-questions) and nowhere else. No unknown is inferred away.

This is a contract freeze only. It defines no runtime behavior, adds no code,
creates no storage table, and changes no public API. Implementation requires
later dispatches ([Delivery gates](#11-out-of-scope-and-delivery-gates)).

## Context

The repository has no profiler. Q0-D0 records the complete factual boundary at
its base: profiling-shaped objects (`ProfileRequest`, `ProfilePolicy`,
`DatasetProfile`, `ColumnProfile`, profile/quality artifacts, a versioned
quality formula, typed findings with evidence) are all `missing`
(Q0-D0 §2.7, §8). What exists today is inspection-only: `InspectionFinding` in
`metadata.rs:InspectionFinding` produced by connector inspect paths, structural
workbook analysis (`workbook.rs:WorkbookInspection`), head/prefix preview
sampling with `Reservoir`/`Random` declared but rejected by every connector
(`preview.rs:SamplingStrategy`; Q0-D0 §2.3), and an opaque,
caller-injected `snapshot.rs:DatasetSnapshot.quality_score: Option<u8>`
persisted as `snapshots.quality_score INTEGER CHECK (quality_score BETWEEN 0
AND 100)` (`store.rs`, Q0-D0 §3). The only quality computation anywhere is an
ad hoc frontend DuckDB-WASM formula that Q0-D0 classifies as mock, not
backend-supported (Q0-D0 §7).

Meanwhile the bounded-execution machinery a profiler must reuse already exists:
`batch.rs:MAX_BATCH_ROWS = 65_536` / `MAX_BATCH_BYTES = 64 MiB` envelopes,
`read.rs:ReadRequest` batch sizes `1..=65_536`,
`request/mod.rs:RequestContext` cancellation/deadline, engine chunking and
memory laws (`memory.rs:MemoryTracker`,
`lib.rs:MAX_ENGINE_CONCURRENT_RUNS = 4`), and atomic snapshot publication
(`store.rs:SnapshotWriter.commit`) (Q0-D0 §5, §6).

Without a frozen contract, the first profiling implementation would invent
metric semantics, unbounded cardinality tracking, an ad hoc score, and
unversioned findings — precisely the failure mode the checklist already
forbids (Q0-D0 §9, row "Forbidden semantics"). This ADR freezes those meanings
in advance so that later implementation dispatches inherit one definition of
each metric, finding, score, and digest.

Relationships held fixed by this ADR **[Decision]**: the E4 contract
([`issue-054-validation-rejected-rows-contract.md`](../issues/issue-054-validation-rejected-rows-contract.md))
and its correction path #91 stay the sole authority for Validate/Deduplicate
execution semantics; this ADR takes no E4/#91 action. ADR-002's executor and
runtime boundaries apply unchanged to any future profiler; nothing here
touches executors, fragments, or provenance of the cleaning path. The opaque
`snapshots.quality_score` column keeps its current meaning; binding computed
scores to snapshots is later wiring, not this document.

## 1. Vocabulary and ownership

Each term below has exactly one meaning in this ADR and in all Q delivery
tasks **[Decision]**.

| Term | Definition |
| --- | --- |
| Scan scope | The exact prefix of the profiled dataset admitted into metric computation: the first `scanned_rows` rows / `scanned_bytes` of envelope payload, in arrival order, within §3 bounds. Metrics are defined over the scan scope only. |
| ProfileRequest | A caller-owned request to profile one referenced dataset: target reference, resolved column selection, and knob values, each validated against the active ProfilePolicy (§3). |
| ProfilePolicy | An operator-owned set of ceilings and defaults (§3). It bounds every ProfileRequest; a request outside policy fails validation before execution. |
| DatasetProfile | The deterministic result of one scan scope under one ProfileRequest: dataset counters plus per-column statistics per §5–§6, including absent-metric flags. |
| Metric | An exact integer count or an exact reduced rational derived from such counts over the scan scope (§4–§5). No float-valued metric exists in canonical output. |
| Absent metric | A metric that the contract declares unavailable when its bounded state overflowed or its evidence is missing (§5, §8). Absence is explicit and flagged; it is never rendered as zero or as a perfect value. |
| Finding | A typed observation produced by a versioned detector: category, severity, origin, sanitized message, evidence references, provenance (§7). |
| FindingEvidence | A reproducible reference backing a finding: metric citations, digests, and scan-scope positions — never raw bulk values (§7). |
| QualityScore | A versioned integer score in `0..=100` computed from exact profile metrics by the formula of its `QualityScoreVersion` (§8). |
| Profile artifact / Quality artifact | A canonical JSON body (`profile_report.v1` / `quality_report.v1`) carrying a DatasetProfile or a QualityReport, identified by its canonical SHA-256 digest (§9). |
| Detector | A deterministic, versioned function from a DatasetProfile (and, after E4 lands, E4 reports) to findings. Detector definitions are Q-R2 scope; this ADR freezes only their output shape. |

Ownership map **[Decision]** (current owners cited; none change in this ADR):

| Concern | Owner today | Anchor |
| --- | --- | --- |
| Inspection findings (unchanged meaning) | `stillflow-core` | `metadata.rs:InspectionFinding`, `FindingSeverity` |
| Preview sampling enum (unchanged; profiling does not use it, §4) | `stillflow-core` | `preview.rs:SamplingStrategy` |
| Opaque persisted quality metadata (unchanged until later wiring) | `stillflow-core` + `stillflow-storage` | `snapshot.rs:DatasetSnapshot.quality_score`, `store.rs` `snapshots` DDL |
| Future stable Profile/Finding/Quality domain values | `stillflow-core` (non-binding candidate, Q0-D0 §10) | no type exists yet |
| Bounded profiler execution, detectors, artifact writers | later Q-R1/Q-R2 dispatches | not owned by any crate yet |

This ADR creates no crate, no dependency arrow, and no public symbol. The
accepted dependency direction (`AGENTS.md`) is unchanged; ownership candidates
remain exactly as recorded in Q0-D0 §10 until implementation contracts land.

## 2. Contract boundary: what this freeze is not

**No runtime in this contract** **[Decision]**: this ADR defines semantics,
bounds, shapes, formulas, and digests. It does not implement accumulators,
sampling, detectors, scoring, serialization, persistence tables, API routes, or
any Rust behavior. Any commit that pairs this contract with implementation
code exceeds Q-C0 scope and fails acceptance
(`q-c0:no-runtime`).

**Existing surfaces keep their meanings** **[Decision]**:

- `InspectionFinding` remains the connector inspection vocabulary. Q0 typed
  findings are a distinct object; no renaming, merging, or migration of
  `InspectionFinding` is authorized here.
- `SamplingStrategy::Head/Reservoir/Random` remains the *preview* sampling
  vocabulary. Profiling v1 uses no sampling parameter at all (§4); connecting
  `SamplingStrategy` to profiling would require a new versioned decision.
- `DatasetSnapshot.quality_score` stays opaque caller-injected metadata
  validated to `0..=100`. This ADR neither computes nor consumes it. The
  frontend ad hoc formula (Q0-D0 §7.2) is non-authoritative and must never be
  promoted to backend semantics by implication.

**Determinism law** **[Decision]**: identical inputs — identical scanned data
bytes, identical resolved ProfileRequest, identical ProfilePolicy — yield
byte-identical DatasetProfile content, identical findings, identical scores,
and identical canonical artifact bodies and digests. No wall-clock, hostname,
locale, load, thread count, hash-map iteration order, or environmental value
may influence any frozen output. Where this ADR resolves an ordering question,
the resolution is total (§5–§6). Caller identity is not an input: the
caller-supplied run identifier (§7.4) travels as envelope metadata outside
every canonical body (§9), so it never participates in digested content.

## 3. ProfileRequest and ProfilePolicy bounds

### 3.1 Policy ceilings

The ProfilePolicy carries exactly these ceilings and defaults. They are
contract constants: values are frozen here, and changing any of them changes
`PROFILING_CONTRACT_VERSION` (§10) **[Decision]**.

| Constant | Value | Meaning |
| --- | --- | --- |
| `PROFILE_MAX_ROWS` | 1_048_576 | Hard ceiling on rows admitted to the scan scope. |
| `PROFILE_MAX_SCAN_BYTES` | 536_870_912 (512 MiB) | Hard ceiling on envelope payload bytes admitted to the scan scope. |
| `PROFILE_MAX_COLUMNS` | 256 | Hard ceiling on columns profiled in one request. |
| `PROFILE_MAX_TOP_K` | 100 | Hard ceiling on top-values K (§6). |
| `PROFILE_MAX_HISTOGRAM_BUCKETS` | 64 | Hard ceiling on numeric distribution buckets (§6). |
| `PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN` | 100_000 | Per-column cap on distinct-value state entries (§5). |
| `PROFILE_MAX_FULL_ROW_DISTINCT_ENTRIES` | 100_000 | Cap on full-row distinct state entries (§5). |
| `PROFILE_MAX_RETAINED_VALUE_BYTES` | 256 | Maximum encoded byte length of a value eligible for verbatim top-value retention (§6). |
| `PROFILE_DEFAULT_TOP_K` | 20 | Default K. |
| `PROFILE_DEFAULT_HISTOGRAM_BUCKETS` | 32 | Default bucket count. |

Scan-scope admission rule **[Decision]**: envelopes are consumed in arrival
order; a row is admitted if, and only if, at admission time
`scanned_rows < PROFILE_MAX_ROWS` and `scanned_bytes ≤ PROFILE_MAX_SCAN_BYTES`.
Admission stops at whichever bound binds first. Row and byte truncation is
disclosed, never an error (§5). Every other ceiling violation is a typed
pre-execution validation error (§3.2).

Deadline law **[Decision]**: a profile run requires a `RequestContext`
deadline (`request/mod.rs:RequestContext`). A request without a deadline is a
typed validation error before execution. Deadline/cancel observance mechanics
reuse the existing context machinery and are wired by Q-R1; this contract
fixes only that profiles are cancellable and deadline-bounded like every other
operation.

Concurrency law **[Decision]**: profile runs pass through the existing engine
run gate (`engine.rs:ExecutionEngine`, `MAX_ENGINE_CONCURRENT_RUNS = 4`). No
second concurrency system may be introduced for profiling (see also §11).

### 3.2 Request shape and validation

A ProfileRequest carries exactly **[Decision]**:

1. a target reference (asset or snapshot identity — the reference form is
   fixed by the owning implementation contract, not by this ADR);
2. a column selection: explicit ordered list of column names, or "all
   columns"; resolved against the target schema before execution;
3. `top_k` with default `PROFILE_DEFAULT_TOP_K`;
4. `histogram_buckets` with default `PROFILE_DEFAULT_HISTOGRAM_BUCKETS`.

Validation rules, each a typed pre-execution error, fail-closed **[Decision]**:

- resolved column count > `PROFILE_MAX_COLUMNS`;
- any selected column absent from the target schema;
- duplicate columns in the explicit selection;
- `top_k > PROFILE_MAX_TOP_K` or `top_k == 0`;
- `histogram_buckets > PROFILE_MAX_HISTOGRAM_BUCKETS` or
  `histogram_buckets == 0`;
- missing deadline (§3.1);
- any field whose meaning this contract does not define (unknown-field
  rejection, mirroring ADR-002 §6 fail-closed versioning).

Column-resolution order is the target schema order for "all columns" and the
given order for explicit lists; both are part of the resolved request and thus
of the determinism inputs.

## 4. Metric semantics: exact vs sampled

**Exact over the scan scope** **[Decision]**: every v1 metric is an exact
function of the scan scope. There is no estimation, extrapolation, or
approximation anywhere in v1: no approximate distinct-count algorithms (no
HyperLogLog, no sampling sketches), no sampled histograms, no percentage
estimates. When the scan stopped early, metrics remain exact over the smaller
scan scope and the result discloses `truncated = true` with the final
`scanned_rows` / `scanned_bytes` (§5).

**Counts and rationals only** **[Decision]**: a metric is either an exact
unsigned integer count or an exact rational recorded as a reduced pair
`(numerator, denominator)` with `denominator ≥ 1` and
`gcd(numerator, denominator) = 1` (the rational `0` is `(0, 1)`). Canonical
artifact bodies contain no floating-point numbers; float domain *data* values
(min/max, histogram edges) appear only in the bit-exact form of §9. Percentage
rendering is presentation and out of contract.

**No sampling parameter exists in v1** **[Decision]**: ProfileRequest (§3.2)
has no sampler, seed, fraction, or strategy field, and profiling does not read
`preview.rs:SamplingStrategy`. If a later versioned decision admits sampled
profiling, it must (a) define the sampler as data — algorithm identifier,
version, and seed carried in result provenance; (b) label every sampled metric
`sampled` in the result schema; and (c) forbid mixing sampled metrics into
exact aggregates or into any QualityScore component. Until such a decision is
merged, sampled profiling metrics fail architecture review.

## 5. Row, null, unique, duplicate, and column statistics

All definitions are over the scan scope; every counter is an exact unsigned
integer **[Decision]**.

Dataset-level counters:

| Metric | Definition |
| --- | --- |
| `row_count_scanned` | Rows admitted to the scan scope. |
| `column_count_profiled` | Columns resolved into the profile (§3.2). |
| `scanned_bytes` | Envelope payload bytes admitted. |
| `truncated` | True iff admission stopped on a §3.1 bound with more source rows available. |
| `distinct_row_count` | Cardinality of the set of canonical full-row keys (below). Available iff the full-row distinct state stayed within `PROFILE_MAX_FULL_ROW_DISTINCT_ENTRIES`; otherwise absent with flag `full_row_distinct_overflow = true`. |
| `duplicate_row_count` | `row_count_scanned − distinct_row_count`. Present iff `distinct_row_count` is present. |

Canonical full-row key **[Decision]**: the lossless concatenation, in resolved
schema order, of per-value encodings — each value framed by its byte length
prefix, with a null sentinel encoding distinct from every non-null encoding,
including the empty string. Keys are compared exactly; grouping by hash digest
is forbidden (collision risk breaks exactness).

Per-column statistics, computed for every profiled column according to its
type family:

| Metric families | Applies to | Contents |
| --- | --- | --- |
| Presence | all types | `null_count` (Arrow validity nulls), `non_null_count = row_count_scanned − null_count` |
| Distinctness | all types | `unique_count`: exact distinct non-null value count over the scan scope; present iff per-column distinct state ≤ `PROFILE_MAX_DISTINCT_ENTRIES_PER_COLUMN`, else absent with flag `distinct_overflow = true` |
| Emptiness | Utf8 | `empty_count`: values that are non-null and zero-length. Empty ≠ null **[Decision]**. |
| Order extremes | integers, floats, Date/Timestamp | `min_value`, `max_value` over non-null values; absent iff `non_null_count = 0` |
| Numeric summary | integers, floats | `sum` and `mean` per §6 accumulation rules |
| Boolean split | Boolean | `true_count`, `false_count`, `null_count` |
| Text/binary lengths | Utf8, Binary | per §6 length policy |
| Distribution | integers, floats | per §6 histogram |

Type-family coverage is closed **[Decision]**: integer types (8/16/32/64-bit,
signed and unsigned) use i128-safe accumulators; `Float32` is upcast to
`Float64` exactly (lossless conversion) before float rules apply;
Date/Timestamp get presence + order extremes only in v1; any other logical
type yields presence metrics only, with per-column status
`skipped_unsupported_type` recorded explicitly. No column is silently dropped:
every resolved column appears in the result with either its metrics or its
explicit skip status.

Distinct-state accounting is part of determinism **[Decision]**: overflow
flags depend only on (scan scope, resolved request, policy). For identical
inputs, the set of columns reporting `distinct_overflow` is identical on
every machine. Overflowed columns report absent metrics with flags — they do
not degrade to estimates, and they do not fail the run.

Relation to E4 **[Decision]**: §5 duplicate metrics are observational
statistics over the scan scope. They drop nothing, mutate nothing, produce no
rejection routing, and never substitute for E4 `Rule::Deduplicate` execution,
its reports, or its correction path (#91). Conversely, E4 owes nothing to
these metrics. Row-group-level duplicate evidence beyond counts arrives only
when E4 artifacts exist (§7).

## 6. Numeric distribution, top values, and Utf8/Binary length policy

### 6.1 Numeric distribution

Inputs: the column's non-null finite values, `N = histogram_buckets`
(§3.2), observed `min` and `max` over non-null values.

Non-finite handling **[Decision]**: `NaN`, `+∞`, `-∞` are excluded from min,
max, sum, mean, and histogram, counted separately as `non_finite_count`.
Negative zero is normalized to positive zero before bucketing (bit-level
survival of `-0.0` remains ADR-002 §7 territory for executors; profiling
defines its own normalization here).

Integer histogram (all integer types) — exact arithmetic **[Decision]**:
compute `span = max − min` in i128;
if `span = 0`, all values go to bucket 0; otherwise
`bucket_index(v) = min(N − 1, ((v − min) · N) div span)` in i128, where `div`
is truncating integer division. The formula is total: `v = min` maps to 0 and
`v = max` maps to `N − 1`.

Float histogram (Float64, including upcast Float32) **[Decision]**: edges are
computed once at finalize: `width = (max − min) / N` in IEEE-754
round-to-nearest-even f64 arithmetic;
`bucket_index(v) = min(N − 1, floor((v − min) / width))` with `floor` yielding
an integer; `v = max` maps to `N − 1` via the `min` clause; `width = 0`
(degenerate single-point range) sends all values to bucket 0. Bucket counts
are exact integers; the frozen edge inputs (`min`, `max`, `width`) are
recorded in the profile in the §9 bit-exact form, so bucket membership is
recomputable and checkable from the artifact alone.

Infinite-width branch **[Decision]**: when `max − min` overflows the f64
range, `width = +∞` and implementations must special-case before evaluating
the general formula: `bucket_index(v) = N − 1` when `v = max`, and
`bucket_index(v) = 0` otherwise (every finite `v − min` divided by +∞ yields
+0.0, whose floor is 0). The general formula
`min(N − 1, floor((v − min) / width))` is invoked only when `width` is
finite, so a NaN intermediate can never arise and the mapping is total and
implementation-independent. All comparisons in this subsection operate on
finite floats only.

Float sums and means **[Decision]**: naive sequential f64 accumulation is
forbidden because it depends on arrival partitioning and violates the §2
determinism law across batch boundaries. Float `sum`/`mean` must be computed
by an order-independent exact method (error-free transformations over the
multiset of values); the required property is: identical multisets of inputs
produce bit-identical results regardless of how rows were partitioned into
batches — the same partition-invariance standard as ADR-002 L2. Integer `sum`
uses i128; if an i128 sum overflows, `sum` and `mean` become absent with flag
`sum_overflow = true` (no wraparound, no saturation).

### 6.2 Top values

Top-K applies to Utf8 and Binary columns only **[Decision]**: integer,
float, Boolean, and temporal columns carry no top-values metric in v1 — their
distribution is fully described by §6.1 plus min/max, and a numeric encoding
convention would add contract surface without information. Candidates are the
column's distinct non-null values whose encoded length is ≤
`PROFILE_MAX_RETAINED_VALUE_BYTES` (UTF-8 bytes for Utf8, raw bytes for
Binary). Ordering: count descending; ties broken by value ascending in
unsigned lexicographic byte order of the encoded value. The order is total;
equal (count, value) pairs cannot occur within one column because values are
distinct. Output: exactly `min(K, distinct_candidate_values)` pairs
`(value, count)` in that order. Computed from the same bounded distinct state
as `unique_count`; if that state overflowed, `top_values` is absent for the
column with the same `distinct_overflow` flag. Values longer than the
retention cap participate in `unique_count` and length statistics but are
never retained verbatim.

### 6.3 Utf8/Binary length policy

Definitions **[Decision]**:

- Utf8 length := number of Unicode scalar values (code points).
- Binary length := number of bytes.
- Arrow `Utf8` arrays contain valid UTF-8 by construction; an "invalid UTF-8"
  metric for Utf8 arrays is forbidden as vacuous. Binary arrays receive byte
  lengths only; no character semantics attach to Binary.

Per-column length statistics: `sum_of_lengths` (u128 accumulation, exact),
`min_length`, `max_length`, `avg_length` as the exact reduced rational
`sum_of_lengths / non_null_count` (absent iff `non_null_count = 0`),
`long_value_count` = values exceeding `PROFILE_MAX_RETAINED_VALUE_BYTES`.

Length histogram, fixed and not configurable **[Decision]**: upper bounds
`0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096` plus a final open
bucket `[4096, ∞)`; bucket of a length `L` is the first bound with
`L ≤ bound`, else the open bucket. Counts are exact integers.

## 7. Findings: categories, evidence, and provenance

### 7.1 FindingCategory

`FindingCategory` v1 is exhaustive and closed **[Decision]**:

| Category | Boundary |
| --- | --- |
| `Schema` | Structure of the profiled schema: type surprises, unexpected nullability, column-count or naming anomalies observable from the DatasetProfile. |
| `Text` | Text-quality observations from §5–§6 text metrics (emptiness, length anomalies, top-value concentration). |
| `Duplicate` | Duplicate-metric observations from §5 (duplicate ratio, near-total duplication). Key-level group identity belongs to E4, not here. |
| `Privacy` | Reserved for detectors chartered by Q-R2; no v1 detector fires it. |
| `Distribution` | Histogram/distribution-shape observations from §6. |
| `Leakage` | Reserved for detectors chartered by Q-R2 (train/production drift-style leakage); no v1 detector fires it. |

Unknown category values fail closed (typed error), never coerce
**[Decision]**. Severity reuses `metadata.rs:FindingSeverity`
(`Info | Warning | Error`) unchanged.

### 7.2 Finding record

Every finding carries exactly **[Decision]**:

- `finding_id`: stable slug, unique within its report;
- `category`: §7.1;
- `severity`: `Info | Warning | Error`;
- `detector_id` and `DETECTOR_CONTRACT_VERSION`: identity and version of the
  producing detector;
- `origin`: `Deterministic` or `AiProposal`;
- `message`: sanitized human text — no cell values above the
  `PROFILE_MAX_RETAINED_VALUE_BYTES` cap, no credential material (AGENTS rule
  10 applies verbatim to messages, evidence, and logs);
- `evidence_refs`: one or more FindingEvidence items from the same report;
- `provenance`: §7.4 block.

Origin law **[Decision]**: AI systems may propose findings only through the
ADR-002 §8 effect boundary, always labeled `origin = AiProposal` and carrying
model/effect identity in provenance. An AiProposal finding is never presented
as deterministic evidence and is excluded from QualityScore components (§8)
and from any gating decision. Removing the label, or emitting AI output
through a `Deterministic` detector, fails architecture review.

### 7.3 FindingEvidence

Evidence is reproducible-from-artifact **[Decision]**: given a report, every
evidence item must be recomputable from metrics and digests inside that same
report, without re-scanning data. v1 evidence kinds are closed:

| Kind | Contents |
| --- | --- |
| `MetricEvidence` | `metric_path` (dotted path into the report), plus optional `numerator`/`denominator` when citing a rational. |
| `ValueDigestEvidence` | `column_ref`, sorted list of SHA-256 digests (lowercase hex) of §5-canonical value encodings, and the observed `count`. Verbatim values never enter evidence **[Decision]**. |
| `RowRangeEvidence` | Half-open position range `[start, end)` within the scan scope (positions, not source-row IDs). |
| `HistogramEvidence` | `column_ref` plus one or more bucket indices with their counts. |

Interpretation rulings **[Decision — interpretation]**: Q0-D0 §8 marks
row-level `FindingEvidence` as additionally `blocked by E4`. Resolved: v1
duplicate/schema/text/distribution findings cite §5–§6 metrics and §7.3
evidence only; any evidence kind that requires row-level Validate/Deduplicate
outcomes is defined by the E4 line when it lands, and until then cannot be
emitted by any Q0 detector. Digest-only evidence is deliberate: raw bulk
values in evidence would create a secret-leakage surface (AGENTS rule 10) for
zero reproducibility gain.

### 7.4 Provenance

Every DatasetProfile, QualityReport, and finding carries a provenance block
containing exactly **[Decision]**: caller-supplied run identifier; target
reference; resolved-request canonical digest and policy version (§3); scanner
contract version (`PROFILING_CONTRACT_VERSION`); plan fingerprint
(`plan.rs:PLAN_FINGERPRINT_ALGORITHM`) when the profiled dataset derives from
a plan; for AiProposal findings, model/effect identity per ADR-002 §8. Wall-
clock timestamps are envelope metadata, never provenance-block members and
never inside canonical artifact bodies (§9). The provenance block itself is
likewise envelope metadata: it is stored beside the artifact but excluded
from every canonical body and digest input (§9), so two executions identical
under the §2 law produce identical canonical bodies and digests even when
their run identifiers differ.

Detector definitions — which conditions fire which findings at which
severities — are Q-R2 scope. This section freezes the output shape, category
boundaries, origin law, evidence discipline, and provenance fields only.

## 8. QualityScore: versioned formula and missing-evidence semantics

### 8.1 Version and formula

`QUALITY_SCORE_VERSION = 1` (u16) **[Decision]**. Score domain: integer
`0..=100`. Inputs are exact metrics of the DatasetProfile in the same report:
`S = row_count_scanned`, `C = column_count_profiled` counting only columns
that contribute presence metrics (i.e., excluding `skipped_unsupported_type`
columns), `T_null = Σ null_count` over contributing columns, `D =
duplicate_row_count` when present, `truncated`.

Formula v1, exact-rational until one final rounding **[Decision]**:

```text
p_null  = 40 · T_null / (S · C)          (rational; term omitted iff S = 0)
p_dup   = 30 · D / S                     (rational; present iff D present)
p_trunc = 10                             (present iff truncated = true)
score_raw = 100 − (Σ present penalties)
score     = clamp(0, 100, round-half-to-even(score_raw))
```

Weights (40 null / 30 duplicate / 10 truncation) are contract constants of
version 1; changing any weight, component, or rounding rule produces
`QUALITY_SCORE_VERSION = 2` (§10).

Normative test vectors **[Decision]** (any implementation must reproduce both):

| Vector | Inputs | Computation | Result |
| --- | --- | --- | --- |
| V1 | S=1000, C=10, T_null=500, D=100, not truncated | p_null = 40·500/10000 = 2; p_dup = 30·100/1000 = 3; p_trunc = 0 | score_raw = 95 → **95** |
| V2 | S=200, C=8, T_null=50, D=15, not truncated | p_null = 40·50/1600 = 1.25; p_dup = 30·15/200 = 2.25; p_trunc = 0 | score_raw = 96.5 → half-to-even → **96** |

### 8.2 Missing-evidence semantics

Absence is explicit, never optimistic **[Decision]**:

| Condition | Result |
| --- | --- |
| `S = 0` | Score absent; reason `no_rows`. An empty scan never yields 100. |
| `D` absent (§5 full-row overflow) | Duplicate penalty omitted; `missing_components = ["duplicate"]`; report completeness `false`. |
| All contributing-penalty evidence missing | Score absent with the reason list; absence is never rendered as a numeric value. |
| `truncated = true` | Not missing evidence: the truncation penalty applies (it is itself exact information). |

Reports record `completeness` and `missing_components`; consumers must treat
incomplete scores as advisory. A score computed under version v is stored,
read, and displayed only together with v; a reader encountering an unknown or
newer `QUALITY_SCORE_VERSION` fails closed (ADR-002 §6 pattern) **[Decision]**.

Wiring note **[Decision]**: the existing opaque
`snapshots.quality_score` pipeline (caller injection → `SnapshotDraft` →
SQLite column) keeps its current meaning. Computing scores per this section
and binding them to snapshots or API responses is later wiring owned by E5/Q-A1
dispatches; this ADR authorizes no change to `DatasetSnapshot`, storage DDL,
or any serialized payload. The frontend ad hoc formula (Q0-D0 §7.2) is and
remains non-authoritative.

## 9. Profile/Quality artifact canonical digest

Artifact bodies **[Decision]**: a `profile_report.v1` body contains exactly
one DatasetProfile (§5–§6) and nothing else; a `quality_report.v1` body
contains the QualityReport — findings (§7), score with
`QUALITY_SCORE_VERSION`, `completeness`, `missing_components` (§8) — plus the
referenced `profile_report.v1` canonical digest. Both carry
`artifact_type`, `artifact_body_version = 1`, and
`PROFILING_CONTRACT_VERSION`. Provenance (§7.4) — including the
caller-supplied run identifier — is envelope metadata persisted beside the
body and is excluded from every canonical body and digest input, exactly like
wall-clock timestamps.

Canonical form **[Decision]**: UTF-8 JSON with lexicographically sorted object
keys (Unicode code point order), no insignificant whitespace. Scalar encodings
are pinned exactly:

- Integers: plain decimal digits with an optional single leading `-`; no `+`
  sign, no leading zeros (`0` itself allowed).
- Rationals: exactly `{"numerator": <integer>, "denominator": <positive
  integer>}`, always in lowest terms (gcd = 1); the sign belongs to the
  numerator; zero is `{"numerator": 0, "denominator": 1}`.
- Enums: their exact string names.
- Floats — every float-domain value (`min`/`max`/`sum`/`mean`/`width`/
  histogram edge inputs): `{"$float": "<uppercase hexadecimal>"}` where the
  string is exactly 16 uppercase hex digits with no `0x` prefix, encoding the
  big-endian IEEE-754 binary64 bit pattern (so `-0.0` would render as
  `"8000000000000000"`). Profiling records `min`/`max`/`sum`/`mean` after the
  §6.1 `-0.0 → +0.0` normalization, so `-0.0` cannot occur in v1 artifacts;
  the 16-digit rule remains total for any float-domain value.
- Temporal extrema: Date values encode as `{"$date_days": <plain integer days
  since 1970-01-01>}`; Timestamp values encode as `{"$epoch_ms": <integer>}`
  or `{"$epoch_us": <integer>}` matching the column's logical unit. String or
  locale-dependent formatting is never used.
- JSON strings: escape only `"`, `\`, and code points U+0000–U+001F; use the
  short escapes `\b \t \n \f \r` where they apply, otherwise `\u00xx` with
  lowercase hex digits; every other character is emitted directly as UTF-8,
  never as surrogate escapes.
- `metric_path`: a dot-separated sequence of exact canonical-body member
  names (case-sensitive); v1 defines no indexed, bracketed, or wildcard
  segments.

Canonical bodies contain no other floats, no map-order-dependent sequences,
and no wall-clock fields.

Digest **[Decision]**: `canonical_digest = lowercase_hex(SHA-256(canonical
UTF-8 bytes))`. Envelope metadata — timestamps, storage locations, host or
environment info, and the §7.4 provenance block including the run identifier —
lives outside the canonical body and is excluded from the digest. Identical
inputs therefore produce byte-identical bodies and identical digests (§2 law);
golden-fixture equality tests are the Q-R1/Q-R2 acceptance mechanism. Golden
fixtures pin exact canonical bytes and digests for fixed §2 input triples
under one documented run identifier; inter-producer convergence follows from
the pinned rules above, not from fixtures.

Ownership note **[Decision]**: writers, storage tables, retention, and read
APIs for these artifacts do not exist and are not created here; artifact
ownership is blocked by E5 (Q0-D0 §6.2, §8) and lands through E5/Q-R1/Q-R2
delivery nodes. This section freezes only the body semantics and digest so
that later writers converge on one canonical form instead of three.

## 10. Compatibility and versioning

**Versioned surfaces** **[Decision]**: this ADR introduces
`PROFILING_CONTRACT_VERSION = 1` (governs §3–§7 and §9 shapes, bounds, and
canonicalization) and `QUALITY_SCORE_VERSION = 1` (governs §8). Both are u16,
monotonic, bumped on any breaking change to what they govern.

**Unknown or newer versions fail closed** **[Decision]**: a reader or
consumer that encounters a profile, finding, score, or artifact bearing a
version greater than its own, or one it does not know, rejects it with a typed
error. Best-effort interpretation and silent field skipping are forbidden.

**No persistence created here** **[Decision]**: this ADR writes no table, no
DDL, no serializer. Persisted profiling formats, when they arrive, require
their own versioned format contracts consistent with §9–§10.

**Breaking a merged contract** follows the AGENTS breaking-change gate
unchanged: open issue stating migration and non-goals, frozen contract naming
every breaking change, linked PR, and shims implemented or explicitly
rejected.

## 11. Out of scope and delivery gates

Explicitly out of scope for Q-C0 and untouched by this file **[Decision]**:
any Rust code or workspace crate change (including
`stillflow-connector-local-tabular`); any profiler/metrics implementation; E4
or #91 action; API endpoints, persistence tables, or serialization beyond the
contract text of §§3–§9; changes to ADR-001, ADR-002, or `AGENTS.md`; UI work
(AGENTS rule 12); retention/lifecycle decisions (blocked by E5, Q-D1).

Implementation requires later dispatches; this ADR neither creates nor
dispatches them **[Decision]**:

| Gate | Entry | Outcome expected there |
| --- | --- | --- |
| Q-R1 | this ADR Accepted | Bounded streaming profiler implementing §§3–§6: accumulators, cancellation/deadline wiring, artifact writer with §9 golden fixtures, batch/partition-invariance, empty/all-null/wide/long-string/high-cardinality tests (per [#81](https://github.com/X44421/stillflow/issues/81) Track Q). |
| Q-R2 | Q-R1 merged | Versioned detectors and typed findings implementing §7; `QualityReportArtifact` writer implementing §8–§9; AI-proposal routing behind the ADR-002 §8 effect boundary. |
| Q-D1 | Q-R1/Q-R2 merged; E5 artifact ownership | Baseline history, drift contracts, retention (out of scope here). |
| Q-A1 | E5 Job/Run/API ownership | Submit/status/cancel/read API reusing E5 primitives — no second task system. |
| E4 line | separate E4 dispatches | Row-level evidence kinds and key-level duplicate identity, extending §7 without reopening it. |

Stop conditions inherited by every gate above: any metric defined outside
§4–§6; any unbounded state; any sampled metric without the §4 labeling regime;
any AI-authored deterministic evidence; any second job/run system.

## Open questions

Stated, owned, and blocked on evidence — none is resolved by assumption in
this ADR:

1. Whether reservoir/random sampling ever enters profiling, and under which
   seed discipline. Forbidden in v1 (§4); any future decision needs a new
   versioned contract with sampler provenance.
2. Whether approximate distinct algorithms are ever admitted. Rejected in v1
   (§4); revisiting requires a new versioned decision with accuracy and
   memory evidence.
3. Retention and lifecycle of Profile/Quality artifacts. Blocked by E5
   artifact ownership; owned by Q-D1 (Q0-D0 §9).
4. Binding computed QualityScores to snapshots/API payloads. Owned by E5/Q-A1
   wiring (§8.2).
5. Concrete Privacy and Leakage detector definitions. Categories are reserved
   (§7.1); detectors are chartered by Q-R2, and PII-safe test obligations sit
   with Q-G1 per #81 Track Q.
6. Whether profiling ever reads `SamplingStrategy` or gains a shared sampler
   vocabulary with preview. Deferred; §4 forbids implicit reuse.

## Consequences

### Benefits

- The first profiler inherits exact, bounded, deterministic semantics instead
  of inventing them; "what does unique_count mean" has one answer forever.
- Overflow-aware absence replaces silent degradation: consumers can distinguish
  "measured", "absent because too large", and "not applicable".
- The score becomes auditable: versioned weights, exact rationals, normative
  test vectors, and explicit incompleteness replace opaque caller-injected
  numbers and the frontend's ad hoc formula.
- Findings become traceable to versioned detectors and reproducible evidence,
  with AI participation contained behind a labeled, non-authoritative origin.
- Artifacts converge on one canonical digest before any writer exists, making
  future storage/API work a consumer problem rather than a format debate.

### Costs

- Exactness costs memory discipline: distinct-state caps mean high-cardinality
  columns legitimately report absent metrics rather than full counts.
- Two rounding regimes (integer-exact and f64-edge histograms) must both be
  implemented and tested; the float rule pins IEEE-754 behavior deliberately.
- Producers now owe canonical-form fidelity: any serializer drift breaks
  digests and is immediately visible.
- Weights (40/30/10) encode a judgment that will eventually be wrong somewhere;
  the version mechanism, not silent edits, is the only sanctioned response.

## Rejected alternatives

- **Sampled-first profiling:** cheap, but every number becomes an estimate
  with a hidden confidence story; contradicts the deterministic-results law
  the repository runs on. Sampling may return only through the §4 regime.
- **Approximate distinct counts everywhere (HLL):** bounded but non-exact;
  merges two semantic classes into one field. Rejected for v1; the overflow-
  and-absent design keeps exactness with bounded state.
- **Unbounded exact cardinality:** exact at any cost — violates the checklist's
  forbidden-semantics row (Q0-D0 §9) and the engine's bounded-memory laws.
- **Float percentages as metrics:** partition- and platform-visible rounding
  drift; exact rationals keep canonical bytes stable.
- **Hash-digest row/value grouping:** collision-tolerant, hence not exact;
  exact comparison over capped state is the contract.
- **Opaque score v1 ("just compute something"):** repeats the current
  caller-injected opacity and the frontend mock; a score without versioned
  formula and missing-evidence semantics is unauditable.
- **Free-form finding codes:** `InspectionFinding`-style strings scale badly
  once evidence, provenance, and AI origin matter; closed categories plus
  versioned detectors keep failures typable.
- **Embedding raw values in evidence:** reproduces leak risks AGENTS rule 10
  forbids; digests reproduce checks without copying data.
- **Defining artifact persistence/API here:** blocked by E5 ownership
  (Q0-D0 §6.2); freezing the digest now prevents three future formats without
  touching storage.
- **Editing ADR-001/ADR-002 or AGENTS.md in this PR:** out of charter for
  #103; nothing in this document conflicts with their accepted statements.

## Verification

Mechanically checkable enforcement for this ADR's own acceptance:

- Single-file diff, docs-only: `git diff --stat` against base
  `04966586192f8750a02790da988db71a28d82074` shows exactly one new file
  (`docs/architecture/adr-003-profiling-quality-and-findings.md`);
  `git diff --check` clean; no unfinished-work markers.
- No runtime: the diff contains no Rust, no crate manifests, no migrations;
  this document defines behavior only in prose, tables, and one normative
  arithmetic block (§8.1).
- Bounds completeness: every operational ceiling and retention constant
  appears exactly once, in §3.1's table (the fixed length-bucket edges are
  pinned in §6.3, the score weights in §8.1); no other section introduces a
  tunable limit.
- Formula totality: §6.1 rules cover `span = 0` / `width = 0`; §8 vectors V1
  and V2 recompute by hand to 95 and 96.
- Coverage: each charter bullet of
  [#103](https://github.com/X44421/stillflow/issues/103) maps to exactly one
  section (§4, §3, §5, §6, §7, §8, §9), and the no-runtime obligation is
  §2/§11.
- Links: relative links resolve within `docs/architecture/` and `docs/issues/`
  (ADR-001, ADR-002, inventory, issue-054 contract); issue numbers (#103, #81,
  #65, #91, and issue-054's #54) match their subjects.
- Determinism review: every **[Decision]** above is a function of
  (input data, resolved request, policy) only; the words "typically",
  "usually", "best effort" appear in none of them.
