# Issue #364: validation, deduplication and analysis result contract

> Status: Candidate frozen contract for acceptance by #364
> Risk: L1 — documentation and contract only; it freezes L3-class admission decisions but authorizes no runtime change
> Parent: #361 (Epic: complete the twelve NodeGraph data-processing capabilities)
> Depends on: #362 (merged, PR #382), design NX-P0 #346 (merged)
> Alignment: #346, the E4 contracts (`docs/issues/issue-054-validation-rejected-rows-contract.md`, `docs/issues/issue-314-svc-a2-verification-report-refs-contract.md`), ADR-002, and #363
> Base: `main@361d4e54bc30d2cc8b54f51ed96cd64d8597d147` (post-#382)
> Suggested branch: `agent/issue-364-nx-c2-validation-dedup-analysis-contract`

This document freezes how validation, deduplication and analysis results reach an existing
Verification, Profile, Quality and Artifact boundary: rule configuration, diagnostics, pass/rejected
rows, report binding, NULL and sampling semantics, resource and failure behaviour. It **admits** an
existing capability rather than building one: `Rule::Validate` and `Rule::Deduplicate` already exist and
already execute on the E4 path; they are rejected in the product path by three independent gates. It
registers no node and changes no execution behaviour by itself.

Per decision D4 of #362, this contract also **owns the input-version digest contract** (§8).

---

## 1. Decision and authority boundary

### 1.1 What this contract freezes

1. The three validation modes and where each result may go, including the rule that ordinary
   materialization never infers verification (§2).
2. The validity/closure rules and the `blocking` versus `isolated` diagnostic classes (§3).
3. The rule configuration and diagnostics matrix for required, format, range, uniqueness and anomaly
   rules (§4).
4. The pass-row / rejected-row / report routing matrix (§5).
5. The deduplication semantics this wave admits, including keep policy, ordering basis, NULL equality
   and duplicate statistics (§6).
6. The analysis and report contract, including full versus sampled scope, denominators and the
   no-fabrication law (§7).
7. The input-version digest contract: domain, what it binds, what it explicitly does **not** bind, and
   the fail-closed rules (§8).
8. The traceability binding from every report to its input, `PlanVersion`, `Run` and rule version (§9).
9. Resource, sampling, cancellation and failure behaviour for these capabilities (§10).
10. The per-slice boundary for #372, #373 and #374 (§11).

### 1.2 What this contract does not authorize

It does not admit `Validate` or `Deduplicate` to the product path (that is #372/#373 under this
contract), does not create an analysis node or a second report runtime (#374 decides the boundary), does
not change ordinary materialization or Preview semantics, does not change E3 or E4 behaviour, does not
add a report authority, artifact kind, storage law or API route, and does not lift any paused capability
from #362 §4 or the #93 XR HOLD.

### 1.3 Authority

E4 remains the **only** verification authority: row identity and routing, dedup index, report sections,
artifact kinds, `VerificationBundle` semantics, memory model, determinism and publication all stay as
frozen by the E4 contracts. Profile, Quality and Drift stay the only analysis/report runtimes. This
contract adds no second rule language, no second dedup index, no parallel report schema and no
publication path.

---

## 2. Validation modes and permitted results

The three modes are frozen vocabulary from #346 §2.1 (design vocabulary; not wire enums, database
states or API fields):

| Mode | Meaning | Permitted result |
| --- | --- | --- |
| `ReleaseValidation` | the graph is intended to produce a **publishable** plan; every node, edge, package, source binding, schema and semantic dependency must be valid, and **any error rejects** | a publishable plan, or rejection |
| `DraftPreview` | a user wants to inspect a target before the whole draft is publishable; the **target dependency closure** must be valid; safe non-dependent defects may be returned as isolated diagnostics but are **never executed** | a draft-only, explicitly **non-publishable** result, or rejection |
| `VerificationExecution` | a **full valid plan** explicitly enters E4 verification; the request carries an E4 context and uses only E4 bundle/artifact rules; ordinary materialization is **not** upgraded | E4 `VerificationBundle` / `ArtifactRef` outcomes |

`DraftPreview` is **not** a weaker release check: one target is evaluated safely against authorized
input, whereas release validation decides whether the whole graph becomes a durable published plan. A
draft preview never establishes the latter, and an `isolated` diagnostic is **not** success and never
permits publication.

**Routing law for this wave.** `Validate` and `Deduplicate` reach only `VerificationExecution`.
Ordinary materialization and ordinary Preview continue to reject them (today: E3 reject). No capability
in this contract may cause an implicit switch into verification, create a `VerificationBundle`, or
change Snapshot behaviour.

---

## 3. Validity, closure and diagnostics

### 3.1 Target dependency closure

`Closure(t)` is the bounded backward dependency closure of target `t`: the target plus its input ports,
every upstream node and edge needed to produce it, every source binding and authorized schema used,
every package/definition version, composite expansion and parameter binding used, plus the typed-port
checks proving one unambiguous input and target output. It is computed from explicit `NodePort`
identities and validated edges — **never** array order, display names or a guessed source — and the
validator must stop **before connector I/O** if the closure cannot be determined deterministically.

### 3.2 Order and zero-I/O law

Validation order is frozen: (1) decode the graph envelope with body/depth/count/byte/secret limits;
(2) graph/revision version, identity, duplicate tuples, endpoints, port direction/role/category/
cardinality and bounded slots; (3) target and `Closure(t)` without consulting a connector; (4) closed
node/package catalog with exact package version and content digest, and expansion bounds; (5) closure
semantics — shared typing, schema effects, `ColumnId` identity, target output mapping; (6) bind closure
sources to authorized schema identities and detect Schema drift; (7) mode-specific capability check;
(8) only then the existing read-only Preview path or the separately authorized E4 verification path.

No step may perform connector `inspect`, `read_batches`, stream polling, storage writes or publication
before its checks pass. All rejection paths in §4 below perform **zero** connector I/O.

### 3.3 Blocking versus isolated

* `blocking` — the requested mode cannot safely continue.
* `isolated` — a **non-closure** defect is reported without inspecting, reading or executing that
  defective region.

Always blocking, even outside the closure: unknown or newer graph/revision format; malformed or
duplicate node/edge/source identity; an endpoint that does not decode as `NodePort`; any request, body,
depth, diagnostic or compile-work limit exceeded; secret-safety rejection or an authorization failure
that would reveal protected resource existence; a missing or ambiguous target, a non-output target, or a
target unreachable from an authorized source; and a graph-level identity or port conflict that makes
target mapping non-deterministic.

Any invalidity **inside** `Closure(t)` blocks all three modes — including upstream bad configuration,
an unknown package version, a required cycle, an incompatible port/schema edge, a missing source
binding, source Schema drift and an indeterminate target output identity. There is no preview of a
"last known good" prefix and no partial `LogicalPlan` authority in that case.

Isolation requires **all** of: the offender outside `Closure(t)`; no edge, binding, package or schema
dependency crossing into the closure; deterministic target mapping; and a safe, bounded, sanitized
diagnostic. The isolation matrix is inherited unchanged from #346 §4, and the current single-source
linear version-1 behaviour stays as it is today.

---

## 4. Rule configuration and diagnostics

### 4.1 Rule surface

| Rule family | Configuration | Diagnostics |
| --- | --- | --- |
| Required / nullability | field reference; missing definition (NULL only, or the explicit missing-value definition frozen by #369) | which field, which rule ordinal, expected "present", actual "missing" |
| Format | field reference; closed format family (for example email-like, numeric text, date pattern) | expected format name, actual "did not match" — the offending value is never echoed |
| Range | field reference; inclusive/exclusive lower and upper bounds typed to the field | expected bound, actual "outside range" |
| Uniqueness | one or more key fields | duplicated key identity by `ColumnId` set and rule ordinal; counts in the report |
| Anomaly detection | numeric field; declared detector (for example IQR with an explicit multiplier) | detector name, threshold, and the row's position relative to it |

All configuration is typed and machine-readable at the node level; the rule vocabulary itself
(`Rule::Validate { predicate, severity, message }`, `Rule::Deduplicate { keys }`) is untouched by this
contract.

### 4.2 Severity and row outcome

The `Validate` mapping is frozen by #346 §7.2:

| Predicate result | `warning` | `error` |
| --- | --- | --- |
| `true` | keep row; no finding | keep row; no finding |
| `false` | keep row; one warning finding | **remove row**; one error finding and permitted rejected-row payload |
| `NULL` | same as `false` | same as `false` |

Rules run in declared order; a terminal error removes the row and **later rules cannot re-admit it**.
Findings, report `ColumnId`s, source row ordinals, node/rule identity and sanitization remain E4
authority. An ordinary Preview result can never masquerade as a `ValidationReport`.

### 4.3 Diagnostic location and sanitization

Every diagnostic carries the rule ordinal, `nodeId`, `columnId` where a field is involved, and the
field path, using the #335 diagnostic shape already implemented (`code`, `nodeId`, `columnId`,
`fieldPath`, `expected`, `actual`, `message`). Diagnostics are bounded and sanitized: never a raw cell
value, display name, credential, connector or filesystem path, third-party error text or backtrace. A
rejection never echoes the offending value.

---

## 5. Pass rows, rejected rows and reports

### 5.1 Routing matrix

| Output | Content | Binding |
| --- | --- | --- |
| Passing rows | the rows that survived all non-warning rules | available to the ordinary output path of the verification run; never published as a new artifact kind |
| Rejected rows | removed rows with the reserved rejected-row payload | `ArtifactKind::RejectedRows`, carrying input kind/id/**version digest**, source row ordinal, rejection kind, plan fingerprint, canonical plan digest, `nodeId` and rule ordinal |
| Validation report | per-rule evaluated/pass/fail counts and findings | `ArtifactKind::ValidationReport`, section `ValidationRuleSummary`, carrying input kind/id/**version digest**, plan fingerprint, canonical plan digest, `nodeId`, rule ordinal, message and counts |
| Deduplication report | per-rule key width, evaluated/unique/duplicate counts | `ArtifactKind::DeduplicationReport`, section `DedupRuleSummary`, carrying the same input/plan/node/rule identity plus `keyColumnCount`, `evaluatedCount`, `uniqueCount`, `duplicateCount` |

The reserved report columns above already exist in core; this contract freezes their meaning and
requires them to be populated with real values (never placeholders — §8.3).

### 5.2 Failure handling

A verification run that cannot complete its rules fails **closed**: no partial report, no partially
published bundle, no partially updated snapshot. Cancellation and deadline expiry before commit publish
nothing. A failed apply leaves no artifact reference behind, and a retry re-executes idempotently
(§10.4).

---

## 6. Deduplication

`Rule::Deduplicate` already exists; this contract adopts the E4 semantics as the admission baseline
(#346 §7.3, `docs/issues/issue-054-validation-rejected-rows-contract.md` §6):

1. **Keys.** An ordered, non-empty, typed tuple of existing `ColumnId`s. Duplicate keys within one rule
   are rejected at validation time. The key width is bounded by the E4 law (`MAX_DEDUP_KEY_COLUMNS`).
2. **Equality.** Exact and typed, including E4 NULL, NaN, signed-zero, timestamp, Utf8, Binary and
   timezone laws. NULL keys are equal to each other under the E4 dedup law (this differs deliberately
   from Join keys, where NULL never equals NULL — §11).
3. **Keep policy.** Keep-first, using the ascending logical Scan output ordinal. A different keep
   policy (keep-last, or "keep the row that wins an explicit ordering") requires an explicit ordering
   basis and is allowed only when that basis is declared in configuration; an undeclared basis is
   rejected rather than inferred.
4. **Statistics.** Duplicate counts are reported per rule and are consistent with the row reduction:
   `evaluatedCount = uniqueCount + duplicateCount`, and the number of removed rows equals the duplicate
   count.
5. **Resources.** Canonical key bytes are bounded **before** index insertion; the SQLite index and
   operator state stay inside the E4 memory/row/byte/page limits; over-limit behaviour is an explicit
   rejection, never an unbounded spill.
6. **No silent loss.** Rows are never dropped without a report entry and a rejected-row record.

---

## 7. Analysis and reports

1. **Scope.** Analysis reports distributions, frequencies, null rates and anomaly findings (for example
   IQR outliers) for declared fields. It **does not modify** materialized output and creates no
   synthetic rows.
2. **Full versus sampled.** A report declares its scope explicitly: `full` or `sampled`. A sampled
   result carries the sampling marker, the sampling strategy, the achieved sample size and the input
   fingerprint. Downstream consumers must be able to tell a sampled result from a full one without
   guessing.
3. **Denominators.** Every ratio declares its denominator: null rate is nulls over evaluated rows;
   frequency share is the group count over the stated total; a rule anomaly rate is findings over
   evaluated rows. A denominator may never be implicit.
4. **NULL, empty and non-numeric inputs.** NULL and empty-string handling is declared per metric and is
   never conflated with `0`; non-numeric values in a numeric metric are counted as excluded with the
   exclusion count reported, not silently coerced.
5. **Report binding.** Every analysis report binds to input kind/id/version digest, `PlanVersion`
   identity, `Run`, rule version and the analyzer version, per §9.
6. **No fabrication.** An analysis result never becomes a materialized row source and never enters the
   ordinary output of a run.

---

## 8. Input-version digest contract (decision D4)

### 8.1 The frozen descriptor

The digest is the logical-input identity of an authorized source:

```text
LOGICAL_INPUT_DIGEST_DOMAIN        = "stillflow.e4.logical-input.v1\0"
LOGICAL_INPUT_DESCRIPTOR_VERSION   = 1

asset_version_digest(asset_id, schema) =
    SHA-256( DOMAIN
             || 0x01
             || DESCRIPTOR_VERSION (u16 LE)
             || asset_id (16 bytes)
             || canonical_bytes(schema) )
```

The domain string and descriptor version are frozen. Any change to either is a breaking change requiring
its own contract and a new domain — an existing digest is never reinterpreted under a new domain.

### 8.2 What it binds — and what it does not

* It **binds** the asset identity and the canonical logical schema of the authorized input. It is
  deliberately independent of raw rows, locators, filesystem paths and connection credentials.
* It does **not** bind row content, row count, modification time or a content checksum. Two different
  row populations that share the same asset identity and logical schema produce the **same** digest.
* Therefore every report and rejected-row record that carries this value must describe it as the
  **authorized logical input identity**, never as a content or freshness guarantee. Acceptance evidence
  may not claim row-level provenance from it.

### 8.3 Naming and fail-closed rules

* One identity, three spellings already in use: `asset_version_digest` (core), `sourceVersionDigest`
  (the #346 §5 `PreviewBinding`), and `authorizedInputVersionDigest` (the #346 §7.1 verification
  execution context). This contract freezes them as **the same value**; implementations must not
  compute or compare them as if they were different identities.
* A binding, preview, verification target or report that requires the input version and does not have
  it **fails closed**. An absent, malformed or **all-zero** digest is never accepted as an authorized
  version, and a placeholder must never be written into a committed report: the current OpenShip
  placeholder is explicitly recorded as false provenance in `X44421/openship` →
  `docs/integration/stillflow-nodegraph-integration.md` ("Known gaps").
* The digest is a compile/execution input and a provenance value; it is never a graph-generated value
  and never a client-supplied authority.

### 8.4 Client exposure

The value must be obtainable by a client that has to present it (for example OpenShip's `materialize`
input): the client-visible asset view must expose it, or a dedicated route must return it. Today
`AssetMetadata` gains the field on the unmerged digest branch while `SourceAssetView` still carries no
digest, so the client gap is only half closed. Landing the digest branch and closing the exposure gap
are prerequisites for #373/#374 acceptance evidence and for the #381 G0 traceability flow.

### 8.5 Landing sequence

1. The digest implementation (currently unmerged, no PR) lands as its own L3 PR with the frozen domain
   in §8.1, exact-head CI and differential tests proving the digest is stable across restarts.
2. #364 (this contract) is the frozen contract that PR cites.
3. #373/#374 may then produce acceptance evidence whose reports carry a real input-version identity.

---

## 9. Traceability

Every validation, dedup and analysis result must be traceable to all of:

| Target | How |
| --- | --- |
| Input | input kind, input id, and the authorized logical input identity (§8) |
| Plan | `PlanVersion` identity plus plan fingerprint and canonical plan digest |
| Run | the `Run` that produced it, with the artifact referenced through an `ArtifactRef` |
| Rule | the rule ordinal plus its node identity, and the rule/message for validation findings |
| Analyzer | the analyzer/report version for analysis artifacts |

A report that cannot name any of these is not admissible as evidence. Digests and fingerprints are
**recomputed or verified server-side**; client-supplied values are never trusted as authority.

---

## 10. Resource, sampling, cancellation and failure behaviour

1. **Resource laws.** These capabilities reuse the E4 laws rather than defining new ones: bounded
   canonical dedup key bytes, the E4 index and operator-state limits, the E4 live-payload and peak-byte
   laws, bounded findings per row and message size, and the existing response budget. No new bound is
   introduced here, and none may be widened.
2. **Sampling.** Sampling is explicit, deterministic for a given declared strategy and input, and
   always disclosed (§7.2). A sampled report never claims full-scan accuracy, and a metric whose
   denominator changes under sampling must state both denominators.
3. **Cross-batch behaviour.** Rule evaluation, dedup state and report accumulation are invariant under
   input batch size: changing batch boundaries may not change counts, findings, kept rows or report
   contents.
4. **Cancellation and retry.** One request context and one deadline span the whole verification run.
   Cancellation or deadline expiry before commit publishes nothing. Retries are idempotent: re-running
   with identical input produces identical artifacts and identical counts, and no retry may duplicate a
   published artifact or a side effect.
5. **Over-limit.** Exceeding any bound fails the run with a typed error and publishes nothing; it never
   degrades into an unbounded or best-effort path.

---

## 11. Per-slice boundaries

| Slice | Issue | Gate | Minimum acceptance scope |
| --- | --- | --- | --- |
| Product admission of dedup | #372 | this contract accepted; #370 substrate | rows actually reduced with a verifiable keep policy; NULL keys; cross-batch duplicates; report/rejected rows bound to `Run`, `PlanVersion` and input identity; no silent row loss; ordinary materialization still rejects the rule |
| Product admission of validation | #373 | this contract accepted; #370 substrate | per-row pass/reject verification; diagnostics locate rule/`nodeId`/`columnId`/field path; report bound to input, `PlanVersion`, `Run`, rule version; failure, cancellation and over-limit publish nothing; no implicit mode switch |
| Report runtime boundary | #374 | this contract accepted; #370 substrate | full versus sampled scope with markers, explicit denominators and input fingerprint; NULL/empty/non-numeric rules; no fabricated data; materialized output unchanged; traceability per §9 |

**Deliberate semantic difference to keep visible:** `Join` keys treat NULL as *never equal* (#363 §7.1)
while `Deduplicate` keys treat NULL as equal under the E4 law (§6.2). Both are frozen deliberately;
documentation, catalog text and UI copy must not blur them.

---

## 12. Compatibility

* Ordinary materialization keeps rejecting `Validate` and `Deduplicate`; no existing graph, published
  `PlanVersion`, `Run` or artifact changes meaning.
* E3 Preview behaviour is unchanged; `DraftPreview` remains a gated candidate from #346 §6 and
  authorizes no code change here.
* E4 report schemas, reserved `ColumnId`s, artifact kinds and `VerificationBundle` semantics are reused,
  not redefined; a new artifact kind or section requires its own contract.
* The report columns that carry an input-version digest keep their identity; the change is that they are
  populated with a real value instead of a placeholder.
* The frozen version-1 regression corpus remains the compatibility evidence for the ordinary path.

---

## 13. Acceptance matrix for this delivery

| Issue #364 acceptance item | Where satisfied |
| --- | --- |
| Complete input, output, error, report and failure-handling matrix for validation, dedup and analysis | §4 (rules and diagnostics), §5 (routing and failure), §6 (dedup), §7 (analysis) |
| NULL, sampling, denominator, cross-batch, over-limit and cancellation behaviour is explicit | §4.2, §6.2, §7.2–§7.4, §10 |
| Reports are traceable to input, `PlanVersion`, `Run` and rule version | §9, with the concrete report columns in §5.1 |
| docs-only; ordinary materialization semantics unchanged | §1.2, §12, §14 |

---

## 14. Non-goals and stop conditions

This delivery does **not**: admit `Validate`/`Deduplicate` to the product path; create an analysis node or
a second report runtime; change E3, E4, `LogicalPlan`, `Rule`/`Expr`, NodeGraph, `GraphRevision`,
storage, API or publication behaviour; add an artifact kind, report section, route or storage law;
define a second rule language or dedup index; widen a bound; un-pause any capability from #362 §4; lift
the #93 XR HOLD; change OpenShip code (the digest exposure gap in §8.4 is recorded, not fixed here); or
run (or claim to have run) any Rust build, test, clippy or fmt.

**Stop and return to contract review** if a slice needs a public DTO/route, a persisted identity, a
changed E3/E4 semantic, a new storage or `ArtifactRef` law, a new executor or queue, a broadened package
capability, a resource or cancellation law not covered by the cited contracts, or a report that cannot
satisfy the traceability requirements of §9.
