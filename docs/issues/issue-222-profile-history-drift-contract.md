# Issue #222 Implementation Contract: profile history and drift (Q-D1-C0)

> Status: Proposed; docs-only contract freeze for independent exact-head acceptance
> Revision: Q-D1-C0-R1
> Risk: L3
> Issue: #222
> Epic / current execution board: #81
> Entry base: `main@e401f49a5978c5efebaa5bb717ee630e8fdccf20`
> Branch: `agent/issue-222-q-d1-c0-drift-contract`
> Worktree: `/home/owl/stillflow-q-d1-c0-impl`
> Last updated: 2026-09-01

This document freezes the Q-D1 contract for profile history, baseline
selection, deterministic drift comparison, retention references, and bounded
drift-report reads. It composes with [ADR-003](../architecture/adr-003-profiling-quality-and-findings.md)
and E5-C0; it does not revise either contract.

No runtime, SQL migration, API endpoint, queue, worker, connector, or AI
feature is authorized by this document. Later implementation must preserve
the exact metric and canonical-artifact semantics already frozen by ADR-003.

## 1. Authority and boundary

### 1.1 Authority order

For profile history and drift, authority is ordered as follows:

1. this document for Q-D1 lifecycle, comparison, and report semantics;
2. ADR-003 for ProfileRequest, ProfilePolicy, DatasetProfile metrics,
   `FindingEvidence`, versioning, and canonical JSON/digest rules;
3. E5-C0 for Workspace, Dataset, Run, Artifact, caller-injected identity,
   transaction, and bounded-read ownership;
4. the current implementation only where it does not contradict the two
   contracts above.

An implementation must reject an ambiguous or unknown version. It must not
silently reinterpret a profile, choose a different baseline, or downgrade an
unavailable metric to zero.

### 1.2 In scope

- Profile history identity and ownership under Workspace and Dataset.
- Committed profile-artifact references and deterministic history ordering.
- Explicit and latest-eligible baseline selection.
- Compatibility checks for profile and drift contract versions.
- Tombstones, retained references, and safe lifecycle transitions.
- Deterministic schema and numeric-distribution comparisons.
- Insufficient-data and unsupported-type outcomes.
- `drift_report.v1` body, findings, evidence, canonical digest, and bounds.
- Idempotent comparison submission and restart behavior.
- The implementation handoff to Q-D1 runtime and Q-A1 API work.

### 1.3 Explicit non-goals

This task does not authorize:

- changes to ADR-003 metric formulas, profile bounds, or QualityScore;
- Rust, Cargo, SQL, migration, storage-schema, API, SSE, UI, or connector
  changes;
- a second Job, Run, Event, queue, scheduler, or concurrency system;
- retention workers, physical garbage collection, or an HTTP lifecycle API;
- E4 row-level evidence or duplicate-execution semantics;
- AI-generated findings, AI thresholds, or promotion of AI output to evidence;
- changes to E5-J1, E5-A1, E5-E1, Golden E2E, TS-151-PROD, or #151.

## 2. Versioned contract and terminology

`PROFILE_HISTORY_DRIFT_CONTRACT_VERSION = 1` is a u16 and governs this
document's history, comparison, report, and lifecycle rules. A breaking
change increments it. `PROFILING_CONTRACT_VERSION` remains the ADR-003 value
governing profile contents and canonical profile artifacts.

The following terms are normative:

| Term | Definition |
| --- | --- |
| Profile artifact | A committed `profile_report.v1` artifact whose body and digest obey ADR-003 §§5–6 and §9. |
| Profile history entry | A Dataset-owned durable reference to one committed profile artifact, its Run, resolved request/policy identity, schema identity, and lifecycle state. It does not own or copy artifact payload bytes. |
| Profile sequence | A monotonically increasing u64 assigned per Dataset when a history entry becomes visible. It is the ordering key; wall-clock time is not an ordering key. |
| Active entry | A history entry eligible for reads and, subject to §5, possible baseline selection. |
| Tombstoned entry | A retained identity/reference that is excluded from new baseline selection and comparison requests but remains available as audit metadata. |
| Baseline | The older profile selected for one comparison. |
| Candidate | The newer profile selected for one comparison. |
| Comparison key | The canonical identity of a comparison request, including both profile digests and every semantic policy input. |
| Drift finding | One deterministic, typed observation in a `drift_report.v1`. |
| Missing metric | A metric unavailable for a declared reason; it is never represented as zero. |

## 3. Ownership, identity, and history

### 3.1 Ownership graph

The ownership graph is:

```text
Workspace
└── Dataset
    └── ProfileHistoryEntry ──references──> ProfileArtifact ──owned by──> Run
                                             └── produced from the Dataset
```

The profile artifact remains owned by its producing E5 Run. The Dataset owns
the history entry because history is a Dataset-scoped view of committed
profiles. A history reference does not transfer Artifact ownership and does
not make a second payload copy authoritative.

Every entry must satisfy all of these invariants:

- `workspace_id` and `dataset_id` identify one isolation scope;
- the referenced Artifact is committed, immutable, and owned by the recorded
  Run;
- the Run's input Dataset is the same Dataset named by the entry;
- the artifact type is exactly `profile_report.v1`;
- the entry records the exact canonical profile digest, profile contract
  version, resolved-request digest, policy version, schema digest, and scan
  scope reported by the artifact;
- an entry cannot be visible before its Artifact commit is durable;
- a history entry cannot point at staging, failed, or tombstoned payload.

### 3.2 History identity and idempotency

The command boundary supplies `profile_history_id`, `created_at`, and the
Dataset-local `profile_sequence`. Persistence must not replace these with
random values or a local wall clock.

The idempotency identity for recording a profile is:

```text
(workspace_id, dataset_id, profile_artifact_digest, producing_run_id)
```

Replaying that identity returns the existing history entry and does not create
a second active entry. A different producing Run may record the same canonical
profile digest as a separate entry because its provenance is different; its
own `profile_history_id` and sequence make the history record distinct. The
canonical profile bytes and digest never include the entry ID, sequence, Run
ID, or wall-clock metadata.

Within one Dataset, active entries are ordered by descending
`profile_sequence`, then descending `profile_history_id` using ASCII byte
order. The order is total and independent of insertion timing, host, locale,
or map iteration.

### 3.3 Recorded provenance

Each entry records, outside the canonical profile body:

- Workspace, Dataset, ProfileHistoryEntry, Run, and Artifact identities;
- profile artifact digest and artifact type;
- `PROFILING_CONTRACT_VERSION` and this contract version;
- resolved ProfileRequest digest, ProfilePolicy version, selected columns,
  histogram/top-K configuration, and schema digest;
- `row_count_scanned`, `scanned_bytes`, `truncated`, and per-column
  availability flags;
- the source input reference and PlanVersion digest when applicable.

Run IDs, storage locations, timestamps, host/build information, and other
envelope metadata are not copied into the profile body or its digest. Raw
cell values and credentials are never stored in a history entry or drift
report.

## 4. Baseline selection and compatibility

### 4.1 Selection modes

A comparison request must choose exactly one baseline mode:

```text
Explicit(profile_history_id)
LatestEligible
```

`Explicit` names the baseline entry directly. `LatestEligible` chooses the
active eligible entry with the greatest Dataset-local profile sequence strictly
less than the candidate sequence, then the greatest profile-history ID under
the ordering of §3.2. It never chooses by `created_at` or another wall-clock
field. An explicit baseline with a sequence greater than or equal to the
candidate sequence is rejected as `INVALID_COMPARISON`.

The candidate is always explicit by `profile_history_id`; a request cannot
implicitly compare against a moving "current" candidate. Baseline and
candidate must be different history entries. A self-comparison is rejected as
`INVALID_COMPARISON`.

### 4.2 Eligibility

An entry is eligible as a baseline only when all of the following hold:

- it is Active and its committed profile artifact is readable;
- it belongs to the same Workspace and Dataset as the candidate;
- its profile and policy versions are known and compatible under §4.3;
- its profile is not truncated and has at least one scanned row;
- its profile body passes ADR-003 canonical digest verification.

Unsupported columns, per-column distinct overflow, and absent optional metrics
do not by themselves make a profile ineligible. They produce the explicit
per-metric outcomes in §7. A profile with no rows or a truncated scan cannot
be an implicit baseline; an explicit request naming one returns the typed
insufficient-data result rather than silently selecting an older profile.

If no entry satisfies `LatestEligible`, the comparison outcome is
`NO_BASELINE`; no drift finding is emitted and no report is published.

### 4.3 Compatibility

The baseline and candidate are comparison-compatible only when all of these
values match:

- `PROFILING_CONTRACT_VERSION`;
- the canonicalization rules and `profile_report.v1` artifact body version;
- ProfilePolicy version;
- `top_k` and `histogram_buckets` resolved in the ProfileRequest;
- the Q-D1 `threshold_policy_version` selected by the request.

The Dataset schema is not required to be identical: schema additions,
removals, type changes, and nullability changes are the subject of this
contract. An unknown, missing, or newer version fails closed with
`INCOMPATIBLE_VERSION`. No fallback to the nearest known version is allowed.

QualityScore and `QUALITY_SCORE_VERSION` are not inputs to Q-D1 drift
compatibility and are not compared by this contract. Drift consumes
`profile_report.v1` metrics only.

## 5. Lifecycle, tombstones, and references

### 5.1 State machine

Profile history entries have this closed lifecycle:

```text
ABSENT -> ACTIVE -> TOMBSTONED
```

The `ACTIVE -> TOMBSTONED` transition is idempotent. There is no transition
back to Active, no in-place replacement of an artifact, and no visible
partially written state. A duplicate create of an existing idempotency identity
returns the existing state.

Tombstoning excludes an entry from new baseline selection, new comparison
requests, and ordinary history payload reads. Its identity, sequence, digest,
Run reference, tombstone reason, and tombstone timestamp remain as audit
metadata. A previously committed drift report remains valid and readable
through its recorded digests even if one input is later tombstoned.

### 5.2 Retention references

The following references prevent physical reclamation of a profile artifact:

- an Active profile history entry;
- a non-tombstoned ProfileHistoryEntry audit record whose retention policy
  still requires the payload;
- a committed `drift_report.v1` that records the profile digest as baseline or
  candidate and has not itself been physically reclaimed;
- any E5 Artifact or VerificationBundle membership that explicitly names the
  profile artifact.

Tombstoning is therefore a logical lifecycle operation, not permission to
delete bytes. Physical reclamation is a separate, reference-aware operation;
it must preserve the audit record and may run only after every retention
reference is gone. Q-D1 does not define a purge command or worker. A report
must never be made unreadable merely because its source profile is no longer
eligible for a new comparison.

### 5.3 History listing bounds

History reads are bounded and use an opaque cursor over the total order in
§3.2. The cursor is scoped to Workspace, Dataset, lifecycle filter, and sort
direction; reusing it for another scope is a typed error.

The fixed limits are:

| Limit | Value | Meaning |
| --- | ---: | --- |
| `DRIFT_MAX_HISTORY_PAGE_SIZE` | 100 | Maximum entries returned by one history read. |
| `DRIFT_MAX_HISTORY_REFERENCE_BYTES` | 1,048,576 | Maximum encoded metadata returned by one history page. |
| `DRIFT_MAX_HISTORY_FILTER_COLUMNS` | 256 | Maximum explicit column names in a history filter. |

`limit = 0`, a limit above the page ceiling, an invalid cursor, or a page
that would exceed the byte ceiling is rejected before reading. The service
returns a continuation cursor rather than an unbounded page.

## 6. Comparison input and deterministic policy

### 6.1 Request identity

A `DriftComparisonRequest` contains exactly:

1. Workspace and Dataset references;
2. one explicit candidate history ID;
3. one baseline mode from §4.1;
4. `threshold_policy_version`;
5. an optional deterministic observation window;
6. the requested report contract version.

The comparison key is the canonical digest of the resolved tuple:

```text
workspace_id,
dataset_id,
baseline_profile_digest,
candidate_profile_digest,
PROFILE_HISTORY_DRIFT_CONTRACT_VERSION,
threshold_policy_version,
observation_window
```

The resolved baseline digest, not the spelling of `LatestEligible`, is in the
key. Thus a later profile does not mutate an earlier comparison identity.

### 6.2 Threshold policy v1

`threshold_policy_version = 1` is closed and contains:

| Policy field | Value | Meaning |
| --- | ---: | --- |
| `numeric_histogram_l1_threshold` | `1/5` | Maximum allowed numeric histogram L1 distance. |
| `null_rate_delta_threshold` | `1/10` | Maximum allowed absolute null-rate delta. |
| `minimum_metric_rows` | 20 | Minimum non-null rows on each side for a distribution metric. |
| `max_findings` | 4096 | Maximum findings in one report. |

All fractions are reduced rationals, as required by ADR-003. For every
metric, the comparison is non-alerting when `observed_delta <= threshold` and
alerting only when `observed_delta > threshold`. Equality at the threshold
is therefore explicitly non-alerting; floating-point epsilon comparisons are
forbidden.

### 6.3 Observation window

An observation window is either absent or an explicit half-open Dataset
profile-sequence range:

```text
{ start_sequence: u64, end_sequence: u64 }
```

It must satisfy `start_sequence < end_sequence`. The window restricts
`LatestEligible` selection and validates that explicitly named baseline and
candidate entries lie in the interval; it does not rescan data or change the
metrics already materialized in either profile. It never derives a window from
the current clock, local timezone, process start time, or insertion timestamp.
A source event-time window, if later needed, must be supplied as an explicit
source-data value with its own declared timezone/unit and included in the
request digest. No wall-clock value enters a canonical drift body or
comparison result.

### 6.4 Input provenance

The resolved comparison records, outside the canonical report body:

- the two ProfileHistoryEntry IDs and profile artifact digests;
- their Run IDs, source input references, schema digests, scan scopes, and
  ProfileRequest/Policy identities;
- the threshold-policy version and resolved policy values;
- the observation window and selection mode;
- the comparison key and report digest.

Provenance is evidence about inputs, not a metric. It cannot change a finding,
its ordering, or the canonical report digest.

## 7. Deterministic drift semantics

### 7.1 Schema comparison

Schema columns are matched by exact canonical column name bytes. Names are
compared case-sensitively; locale-aware folding, fuzzy matching, and positional
matching are forbidden. The schema finding kinds are closed:

| Kind | Condition | Severity |
| --- | --- | --- |
| `schema.column_added` | Candidate contains a name absent from baseline. | `Warning` |
| `schema.column_removed` | Baseline contains a name absent from candidate. | `Warning` |
| `schema.column_type_changed` | A matched name has a different canonical logical type. | `Error` |
| `schema.column_nullability_changed` | A matched name changes its declared nullability. | `Warning` |

Schema findings are sorted by kind rank in the table above, then column name
bytes, then baseline type and candidate type bytes. A type change is one
finding even if nullability also changes; the nullability finding is emitted
as a second finding with its own stable kind when applicable.

No finding is emitted for column order alone. The schema digest remains in
provenance, and a future order-sensitive rule requires a new versioned
decision.

### 7.2 Numeric distribution metrics

Distribution comparison is defined only for matched numeric columns whose
baseline and candidate profiles both contain the ADR-003 histogram and whose
non-null counts are each at least `minimum_metric_rows`. It uses the declared
same histogram bucket count from §4.3.

For side `x`, let `N_x` be the exact sum of its finite histogram bucket counts
and `h_x[i]` the count in bucket `i`. The normalized bucket vector is the
exact rational `h_x[i] / N_x`. The numeric histogram L1 distance is:

```text
numeric_histogram_l1 = (1/2) * Σ_i |h_candidate[i]/N_candidate
                              - h_baseline[i]/N_baseline|
```

The result is reduced to a rational in `0..=1`. Histogram bucket indices,
counts, and the two profile digests are evidence; raw values and locale-formatted
numbers are never evidence. ADR-003's per-profile min/max and bucket-edge
rules remain unchanged; Q-D1 compares the already materialized bucket counts
and does not invent a second histogram.

For every matched column with available presence metrics, the null-rate delta
is:

```text
null_rate_delta = |null_count_candidate / row_count_candidate
                 - null_count_baseline / row_count_baseline|
```

Both fractions are exact and reduced. A null-rate finding is emitted only when
the delta is strictly greater than `null_rate_delta_threshold`. Numeric and
null-rate findings are independently evaluated; one does not suppress the
other.

The following are not distribution metrics in v1: text top-value comparison,
binary-value comparison, mean comparison, min/max distance, approximate
quantiles, or AI similarity. They may not be smuggled in under a generic
"distribution" label.

### 7.3 Missing and insufficient data

Each unavailable metric carries exactly one reason:

| Reason | Meaning |
| --- | --- |
| `no_baseline` | Latest-eligible selection found no baseline. |
| `no_rows` | A selected profile scanned zero rows. |
| `truncated_scan` | A selected profile stopped at an ADR-003 scan bound. |
| `unsupported_type` | The column has ADR-003 status `skipped_unsupported_type`. |
| `metric_absent` | The profile explicitly omitted the metric, such as distinct overflow or missing evidence. |
| `too_few_rows` | A metric side has fewer than `minimum_metric_rows` usable rows. |
| `incompatible_schema` | The matched columns cannot be compared because their logical type families differ. |
| `tombstoned_input` | A requested entry is tombstoned or its payload is unavailable for a new comparison. |
| `incompatible_version` | A required contract, policy, or canonicalization version is unknown or mismatched. |

`NO_BASELINE`, `INCOMPATIBLE_VERSION`, `TOMBSTONED_INPUT`, and invalid request
outcomes publish no report. `no_rows` and `truncated_scan` on an explicitly
selected input publish a report with `completeness = false`, no distribution
finding for the affected metric, and the reason in `missing_metrics`.

For a matched column with one unavailable side, the metric is absent for that
column and the report is incomplete. Schema findings remain valid and are not
discarded because distribution evidence is missing. A missing metric never
counts as zero, never passes a threshold by default, and never creates an
alert by itself.

### 7.4 Finding identity and order

Every finding has:

- a stable `finding_id` derived from the canonical tuple
  `(kind, column_name, baseline_digest, candidate_digest, metric_path)`;
- one closed `kind` from §7.1 or the threshold kinds
  `distribution.numeric_histogram_l1_exceeded` and
  `distribution.null_rate_delta_exceeded`;
- `severity`, `detector_id = "q-d1-v1"`, and this contract version;
- baseline/candidate profile digest references;
- exact observed and threshold rationals where a threshold was evaluated;
- ADR-003-compatible metric/histogram evidence references;
- a sanitized message containing no raw values or credentials.

The complete finding order is:

1. schema kind rank from §7.1;
2. distribution numeric histogram threshold;
3. distribution null-rate threshold;
4. canonical column-name bytes;
5. canonical metric path;
6. `finding_id` bytes as the final tie-break.

The order is applied before pagination and is identical across runs and batch
partitions. Findings are deterministic evidence. No AI proposal may be
stored as a `Deterministic` finding, alter this order, or alter a threshold.

## 8. Drift report contract

### 8.1 Canonical body

The report body is exactly a `drift_report.v1` object containing:

- `artifact_type = "drift_report.v1"`;
- `artifact_body_version = 1`;
- `profile_history_drift_contract_version = 1`;
- the resolved baseline and candidate canonical profile digests;
- `threshold_policy_version` and its exact rational values;
- the resolved observation window, or an explicit `none` value;
- `outcome` from the closed set
  `complete`, `partial`, `no_baseline`, `incompatible_version`,
  `tombstoned_input`, `invalid_comparison`, `output_limit_exceeded`;
- `completeness` and a deterministically ordered `missing_metrics` list;
- a deterministically ordered `findings` list;
- `canonical_input_digest`, the digest of the resolved comparison key.

The body contains no comparison ID, history sequence, Run ID, timestamps,
storage path, host/build data, request trace, provenance block, or envelope
metadata. `complete` means every requested metric was available and all
requested columns were evaluated; `partial` means a report is valid but at
least one requested metric is listed as missing.

Canonical JSON and digest rules are exactly ADR-003 §9: UTF-8, lexicographically
sorted object keys, no insignificant whitespace, exact integer/rational
encodings, and lowercase hexadecimal SHA-256 over the canonical bytes. The
report digest is therefore independent of Run ID, caller, time, host, and
storage location.

### 8.2 Report persistence and publication

A report becomes visible only after its complete canonical body, digest,
provenance, and all referenced profile artifacts have been committed. A
failure before commit exposes no partial finding list or partial page.

The report is an immutable Artifact owned by the E5 Run that performed the
comparison. Its ProfileHistoryEntry references are provenance/membership
references, not new owners. Re-reading a committed report verifies its digest
before returning content.

### 8.3 Bounded report reads

The fixed comparison bounds are:

| Limit | Value | Meaning |
| --- | ---: | --- |
| `DRIFT_MAX_COMPARE_COLUMNS` | 256 | Maximum resolved columns considered in one comparison. |
| `DRIFT_MAX_FINDINGS_PER_REPORT` | 4096 | Maximum findings retained in one report. |
| `DRIFT_MAX_MISSING_METRICS` | 256 | Maximum missing-metric records in one report. |
| `DRIFT_MAX_EVIDENCE_REFS_PER_FINDING` | 8 | Maximum evidence references per finding. |
| `DRIFT_MAX_REPORT_BYTES` | 2,097,152 | Maximum canonical report body size. |
| `DRIFT_MAX_REPORT_PAGE_SIZE` | 100 | Maximum findings returned by one report page. |

The report is rejected atomically with `OUTPUT_LIMIT_EXCEEDED` if any bound
would be exceeded. Finding pagination uses the stable finding order of §7.4;
its opaque cursor is scoped to report digest and page direction. A cursor
cannot be used to read a different report or bypass the report byte bound.

## 9. Idempotency, restart, and failure behavior

### 9.1 Comparison idempotency

The resolved comparison key in §6.1 is unique within Workspace. Repeating the
same request after success returns the original immutable report digest and
does not create a second report or finding set. Repeating it after a
transactional failure may recompute, but the first committed result wins and
all successful retries converge to the same digest. A request with the same
caller label but a different resolved input, threshold, or window is a new
comparison key.

### 9.2 Restart reconciliation

Profile history entries and drift reports use E5 durable Run state. On restart:

- an uncommitted history entry or report is invisible and may be retried;
- an Active entry with a committed artifact remains Active exactly once;
- a committed report remains readable exactly once;
- a tombstone remains Tombstoned and is never resurrected;
- a duplicate retry rechecks the stored digest and returns the existing result;
- no worker may select an entry or report that is still staging.

No wall-clock comparison, random ordering, implicit retry, or second scheduler
is allowed during reconciliation. Cancellation and terminal Run ownership
remain E5 semantics; Q-D1 never converts a failed comparison into a hidden
successful report.

### 9.3 Failure categories

The implementation must use typed outcomes for invalid scope, missing or
unreadable artifacts, version mismatch, incompatible input, output bounds,
and transactional failure. It must not return a partial report as success,
silently skip a column, or treat an unavailable metric as a clean comparison.

## 10. AI and evidence boundary

Q-D1 emits deterministic schema and metric findings only. Deterministic
findings and their evidence are the sole inputs to any later quality or
gating decision defined by the owning contract.

If a later task adds an AI proposal, it must use the ADR-002 effect boundary,
carry `origin = AiProposal` and model/effect provenance, remain separate from
the deterministic report, and never alter the canonical deterministic digest,
threshold result, completeness, or finding order. Raw profile values,
credentials, and bulk evidence remain forbidden in prompts, findings, logs,
and reports.

## 11. Delivery handoff

Q-D1 runtime implementation may begin only after this contract is accepted
and E5 artifact/Run ownership is available. It must:

- reuse E5 Job, Run, Event, Artifact, RequestContext, cancellation, and
  bounded-read primitives;
- consume committed ADR-003 `profile_report.v1` artifacts without changing
  their metric meanings or digest rules;
- implement the lifecycle, selection, comparison, bounds, and idempotency
  rules above in one semantic authority;
- add focused evidence for every acceptance case in §12.

Q-A1 may expose history and report reads only through bounded pagination and
must treat the typed outcomes above as domain results. It may not add a
second baseline selector, drift calculator, or job system.

## 12. Acceptance matrix

| Case | Required result |
| --- | --- |
| Record a committed profile | One Active entry; artifact ownership remains with its Run; digest verifies. |
| Replay the same profile identity | Same entry and sequence; no duplicate Active entry. |
| Explicit baseline | The named Active compatible entry is used, never silently replaced. |
| Latest eligible baseline | Highest eligible Dataset sequence is selected with the fixed ID tie-break. |
| No baseline | `NO_BASELINE`; no report and no finding. |
| Unknown/newer/mismatched profile or policy version | `INCOMPATIBLE_VERSION`; fail closed with no report. |
| Explicit tombstoned baseline or candidate | `TOMBSTONED_INPUT`; no new comparison. |
| Schema column added/removed | Exactly one deterministic finding per changed name, stable severity/order. |
| Schema type changed | `schema.column_type_changed` with `Error`; no distribution comparison for that column. |
| Nullability changed with type change | Type and nullability findings are both emitted once. |
| Numeric L1 delta below threshold | No numeric alert; exact observed rational may be recorded as evaluated evidence. |
| Numeric L1 delta equal to threshold | No numeric alert; equality is non-alerting. |
| Numeric L1 delta above threshold | Exactly one numeric distribution finding for that column. |
| Null-rate delta at or below / above threshold | Non-alerting at or below; one alert strictly above. |
| Unsupported type, absent metric, too few rows, empty, or truncated profile | A typed missing reason; `partial` when a report is otherwise valid; never zero and never an alert by itself. |
| Duplicate comparison submission | Original committed report/digest is returned; no duplicate report. |
| Crash before history/report commit | No visible partial object; retry converges to one committed result. |
| Finding/report/history bound exceeded | `OUTPUT_LIMIT_EXCEEDED` atomically; no partial success. |
| Report pagination | Stable order, bounded page, scoped opaque cursor, no skipped or duplicated finding. |
| Envelope changes only | Canonical body and digest remain unchanged. |
| Secret/raw-value inspection | No raw value or credential appears in history, evidence, message, or digest input. |

## 13. Verification obligations for the implementation PR

The later implementation PR must demonstrate, at minimum:

- one Dataset with repeated profiles and deterministic latest selection;
- explicit baseline, no-baseline, incompatible-version, tombstone, and
  restart/idempotency cases;
- schema add/remove/type/nullability findings and stable ordering;
- exact threshold tests for `<`, `=`, and `>` for both v1 metrics;
- empty, truncated, unsupported, overflow/absent, and too-few-row outcomes;
- canonical digest equality across Run IDs, timestamps, hosts, and batch
  partitions;
- bounded history/report pagination and atomic output-limit rejection;
- evidence that no second scheduler, queue, or AI deterministic path exists.

This file is the complete Q-D1-C0 docs-only deliverable. It changes no
runtime semantics until a later authorized implementation dispatch.
