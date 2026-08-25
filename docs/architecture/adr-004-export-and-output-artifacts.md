# ADR-004: Export and output artifacts

> Status: Proposed
> Date: 2026-08-25
> Decision owners: Stillflow maintainers
> Charter: [#104](https://github.com/X44421/stillflow/issues/104) (ledger [#81](https://github.com/X44421/stillflow/issues/81), Track X)
> Factual input: [`export-output-artifact-inventory.md`](../issues/export-output-artifact-inventory.md)
> (merged as X0-D0, issue [#66](https://github.com/X44421/stillflow/issues/66)), cited below as `X0-D0 §n`
> Supersedes: nothing. [ADR-001](adr-001-logical-physical-and-storage-boundaries.md)
> and [ADR-002](adr-002-deterministic-runtime-and-physical-executors.md) remain
> authority in full; this ADR freezes one previously open surface — exports and
> user-facing output artifacts — without altering any statement of either.

Citation discipline for this document:

- Statements about code that exists at `main@04966586192f8750a02790da988db71a28d82074`
  cite `path:symbol` (repo-root-relative, backend crates under `backend/crates/`)
  or `X0-D0 §n` for facts established by the accepted inventory. The export-relevant
  facts cited from X0-D0 were re-verified against this contract base; the storage
  deltas between the inventory base (`89aab255`) and this base are E4
  verification-bundle work that does not touch the surfaces cited here.
- Statements that introduce a new rule of this ADR are labeled **[Decision]**.
  Where this ADR resolves an interpretation question left open by X0-D0, the
  resolution is labeled **[Decision — interpretation]**.
- Items that cannot be settled today are stated in
  [Open questions](#open-questions) and nowhere else. No unknown is inferred away.
- This document freezes a contract only. Every named constant, type, and
  behavior it introduces is contract text; defining any of them in code requires
  a later dispatch (§10). Nothing here is an implementation authorization.

## Context

Today the product has no export path. X0-D0 §1 records that no symbol named
`ExportArtifact`, `ExportRequest`, `ExportFormat`, `ExportPolicy`,
`ExportInputRef`, `OutputLocation`, export manifest, export digest, download
API, or export job/run exists anywhere in the inventory base. The only write
capability in backend code is the connector byte layer
`ObjectStorageAccess::upload`, which is not wired into any product surface
(X0-D0 §4). Frontend export labels — `Export CSV`, node `n5`, dataset labels —
are browser-local mocks that write no file and call no backend (X0-D0 §7).

What does exist is a committed immutable Snapshot plane with strong integrity
discipline: visible-manifest reads that verify symlink absence, file length,
SHA-256 digest, canonical Parquet schema, row count, and single-batch shape per
partition (`store.rs:SnapshotStore.read_batches`, X0-D0 §2.2); staged writes with
`create_new`, rename installation, and atomic SQLite manifest commit
(`store.rs:SnapshotWriter.commit`, X0-D0 §2.3); tombstone and retention-cutoff
garbage collection plus publication recovery (`store.rs:tombstone_snapshot`,
`store.rs:recover`, `store.rs:collect_garbage`, X0-D0 §2.7); and reader/publisher
gates (`manifest.rs:MAX_ACTIVE_READERS = 64`, `manifest.rs:MAX_ACTIVE_PUBLISHERS
= 8`, X0-D0 §2.6). A snapshot becomes visible only after all partitions and
checksums are durable and its manifest is committed atomically (ADR-001,
persistence plane; invariant 6).

This ADR freezes what an export must mean before anyone builds it, so that the
first export implementation cannot quietly invent its own input rules, encoding
dialects, ordering story, bounds, or overwrite behavior.

Relationships held fixed by this ADR **[Decision]**: the E4 contract
([#54](https://github.com/X44421/stillflow/issues/54),
`issue-054-validation-rejected-rows-contract.md`) stays the sole authority for
Validate/Deduplicate semantics and its E4-S2 hold ([#91](https://github.com/X44421/stillflow/issues/91))
stands untouched; rejected rows and reports are not exportable inputs under this
ADR (§2). E5 Job/Run/Event/API integration remains blocked by E5 exactly as
X0-D0 §8 records. SQL Connector [#9](https://github.com/X44421/stillflow/issues/9)
remains Post-MVP and DuckDB preview SQL
[#10](https://github.com/X44421/stillflow/issues/10) remains Phase 1D exactly as
[`data-ingestion-architecture.md`](../data-ingestion-architecture.md)
§17–§18 state; neither is accelerated or blocked here, and
[#80](https://github.com/X44421/stillflow/issues/80)/[#91](https://github.com/X44421/stillflow/issues/91)
(E4-S2) remain out of scope and untouched.
No crate changes, no dependency-arrow change, and no AGENTS.md or prior-ADR edit
is made or authorized by this document.

## 1. Vocabulary and ownership

Each term below has exactly one meaning in this ADR and in all Track X delivery
tasks **[Decision]**.

| Term | Definition |
| --- | --- |
| Export | The bounded operation that reads exactly one committed immutable Snapshot through its visible manifest and produces Output Artifacts at a Destination. Never part of the cleaning path; never a second execution engine. |
| Committed Input | Exactly one committed Snapshot identified by `snapshot_id` whose manifest is `visible` under `store.rs:SnapshotStore.load_manifest` semantics (X0-D0 §2.1). Live engine state is not a Committed Input (§2). |
| Input Verification | The full per-partition integrity battery of `read_batches` (symlink/regular-file checks, stored byte count, SHA-256 digest, canonical schema, row count, single-batch shape; X0-D0 §2.2), applied before the first encoded output byte. |
| Output Artifact | One finalized file produced by an Export: a CSV, TSV, JSONL, or Parquet file (single-file artifact) or exactly one `part-*` member of a partitioned artifact set. Everything else — manifests, staging residue — is metadata or garbage, never an Artifact. |
| Partitioned Artifact Set | An artifact delivered as `part-<seq:010>.<ext>` files under one destination directory, sequences zero-based and contiguous, mirroring `SnapshotManifest::try_new` discipline (X0-D0 §2.3). |
| Export Manifest | The versioned provenance and integrity record persisted beside an artifact set (§7). Distinct from `SnapshotManifest`, which stays Snapshot-specific. |
| Export Digest | SHA-256 over complete finalized artifact file bytes, lowercase 64-character hex — the same discipline as `digest.rs:ContentDigest` (X0-D0 §5), applied to export files. |
| Destination | A location inside exactly one Allowed Root where artifacts are published: a managed local filesystem root, or (after E5 wiring exists) an object-store prefix. |
| Allowed Root | A registered absolute directory opened with no-follow semantics, rejecting symlinked components, exactly as local-tabular read roots behave today (X0-D0 §4). |
| Staging Area | The temporary per-export directory in which bytes are written before publication; keyed by caller-injected export id; never visible as output (§7). |
| Publication | The single visibility point: staged bytes installed by rename, then the Export Manifest committed. Readers observe all artifacts of a set or none (§7). |
| Retention | Policy data stating how long a tombstoned artifact's bytes remain recoverable before garbage collection; explicit, never implicit (§7). |

Ownership constraints **[Decision]** (binding on future implementers; no code is
moved or added by this document). These adopt the non-binding candidates of
X0-D0 §9 as constraints because they are the only assignment that preserves the
accepted dependency direction of AGENTS.md and ADR-001:

| Concern | Contract owner | Rationale |
| --- | --- | --- |
| Export domain values (request/format/policy/location/artifact identities) | stable public contracts in `stillflow-core` | lowest layer; API/engine/storage may all depend |
| Export Manifest persistence, digests, retention | `stillflow-storage` | control-plane SQLite and immutable payloads already owned there |
| Export encoding/writer runtime | `stillflow-engine` (or an engine-owned adapter) | engine owns execution, cancellation, and Snapshot publication today |
| Job/Run/Event/HTTP integration | `stillflow-engine` + `stillflow-api` | preserves dependency direction; blocked by E5 |

## 2. Committed input rules

**Only committed immutable Snapshots are eligible inputs** **[Decision]**. An
Export consumes exactly one Committed Input. The input identity recorded in the
Export Manifest is the tuple (`snapshot_id`, `dataset_id`, `session_id`,
`source_asset_id`, `LogicalSchemaFingerprint`, `DATASET_SNAPSHOT_VERSION`),
all of which exist today (`domain/snapshot.rs:DatasetSnapshot`,
`batch.rs:LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM`; X0-D0 §5). Nil identifiers are
rejected, mirroring snapshot identity discipline.

**Input Verification precedes encoding** **[Decision]**: before the first output
byte is written, every input partition passes the `read_batches` verification
battery named in §1. A failed check fails the whole export closed with a typed
error; no partial artifact is ever visible (§7). Digest mismatch, missing file,
length mismatch, schema mismatch, row-count mismatch, or extra batches are
failures, never warnings.

**Live execution state is not an export input** **[Decision]**. Exports read
committed storage; they never consume in-flight engine buffers, preview results,
or open `SnapshotWriter` state. A run that wants its output exported first
publishes a Snapshot, then exports it. This keeps exactly one publication and
recovery path per plane and honors ADR-002 §2 (executors and runtime do not
write storage outside `SnapshotWriter.commit`).

**One Snapshot per Export** **[Decision]**. Multi-snapshot, join-across-snapshots,
or filtered-subset inputs require a future amendment of this ADR; v1 has no such
surface, so none may be implemented under it.

**Rejected rows, VerificationBundles, quality findings, and profiles are not
eligible inputs** **[Decision — interpretation]**. X0-D0 §10 records these
objects as missing or `blocked by E4` on this base. They become exportable only
when the objects themselves exist under their own frozen contracts *and* an
amendment admits them as inputs. Until both conditions hold, an export request
naming them fails typed.

## 3. Format encoding semantics

These rules bind every writer, forever, regardless of library **[Decision]**.
Reader-side evidence cited is X0-D0 §3.

**Common rules** **[Decision]**:

- All text formats encode UTF-8 without BOM.
- Dates render as `%Y-%m-%d`. Timestamps render as RFC 3339 instants converted
  to UTC with the `Z` suffix, preserving the logical type's unit precision
  (millisecond/microsecond). Original offsets are not reconstructed in text;
  Parquet carries the instant natively.
- Non-finite floats (`NaN`, `+inf`, `-inf`) fail the export with a typed error
  in every format. No textual token for them exists. This is symmetric with the
  readers, which already reject non-finite floats (X0-D0 §3).
- Binary-typed columns are legal only in Parquet. CSV, TSV, and JSONL fail
  closed on binary columns with a typed error.
- Nested `List`/`Struct` columns encode natively in Parquet and as nested JSON
  values in JSONL; they fail closed in CSV/TSV v1.
- Null representation per format is fixed by the table below; writers have no
  null-style options.

| Concern | CSV | TSV | JSONL | Parquet |
| --- | --- | --- | --- | --- |
| Null value | empty field, unquoted | empty field, unquoted | `null` literal | physical null |
| Empty string | quoted empty `""` | quoted empty `""` | `""` | empty UTF-8 |
| Delimiter | `,` | tab (`\t`) | n/a | n/a |
| Quoting | RFC 4180 subset below | same rule as CSV with tab delimiter | n/a | n/a |
| Record separator | LF (`\n`) | LF (`\n`) | LF between lines, final LF present | n/a |
| Header row | always present | always present | none | Arrow schema |
| Binary column | typed failure | typed failure | typed failure | allowed |
| NaN / ±inf | typed failure | typed failure | typed failure | typed failure |

**CSV/TSV quoting law** **[Decision]**: a field is quoted if and only if it
contains the delimiter, a double quote, LF, or CR. Inside quotes, double quotes
are doubled. No other character triggers quoting. Header cells follow the same
rule. This predicate is total and mechanical, so two correct encoders agree
byte-for-byte.

**JSONL record law** **[Decision]**: one JSON object per line; fields appear
exactly once each, in §4 column order; strings use minimal RFC 8259 escaping
(`"`, `\`, and control characters below `0x20` via the fixed short escapes or
`\u00XX`); integers print exactly; floats print as the shortest decimal string
that round-trips, with the encoder pinned by version in the Export Manifest
(§7); blank lines are forbidden; the top-level value of every line is an object.

**Parquet writer law** **[Decision]**: export Parquet uses Snappy compression,
the canonical Arrow schema derived from the input LogicalSchema (the same
derivation storage verifies on read; X0-D0 §2.2), and row groups targeting at
most `MAX_BATCH_ROWS = 65_536` rows (`batch.rs:MAX_BATCH_ROWS`). File key-value
metadata records the schema fingerprint and the contract versions listed in §7.
This mirrors internal Snapshot Parquet discipline (X0-D0 §3) while remaining a
distinct, user-facing artifact.

**Deterministic-byte claims are version-pinned** **[Decision]**: identical
Committed Inputs, identical contract and encoder versions, and identical format
options yield identical artifact bytes. Any claim of byte identity across
encoder upgrades lapses until re-evidenced, exactly as ADR-002 §5 L3 requires
for snapshots.

## 4. Deterministic column and row ordering

**Column order is LogicalSchema declared field order** **[Decision]**: header
rows, JSONL field order, and Parquet column order follow the input schema's
field order, which `LogicalSchema` preserves (X0-D0 §10). Writers never sort,
reverse, or reorder columns, and v1 exposes no reordering option.

**Row order is partition-sequence order, then within-partition stored order**
**[Decision]**: rows are emitted ascending by zero-based contiguous partition
sequence (the order `SnapshotBatchReader` yields and `SnapshotManifest::try_new`
enforces; X0-D0 §2.2–§2.5), preserving each partition's physical row order.
Single-file formats concatenate partitions in that order into one stream.

**Exports never sort, shuffle, sample, or deduplicate rows** **[Decision]**. No
global row-sort contract exists in storage today (X0-D0 §2.5), and inventing one
inside the exporter would create a second ordering authority. Consumers who need
a specific order get a future, separately frozen feature.

**Ordering is reproducible** **[Decision]**: for a fixed Committed Input and
pinned versions, repeated exports yield identical column order, identical row
order, and — per §3 — identical bytes. Locale, timezone configuration, host,
load, and concurrency never influence order.

## 5. Output bounds

All bounds are hard ceilings **[Decision]**: exceeding one fails the export with
a typed error at or before the violation, leaving no visible partial artifact.
The constants are introduced as contract text here; defining them in code is a
later dispatch's obligation (§10).

| Constant | Value | Meaning |
| --- | --- | --- |
| `MAX_EXPORT_ROWS` | `10_000_000` | maximum total rows across all files of one artifact |
| `MAX_EXPORT_OUTPUT_BYTES` | `8 GiB` | maximum total finalized bytes across all files of one artifact |
| `MAX_EXPORT_SINGLE_FILE_BYTES` | `2 GiB` | maximum finalized size of one single-file artifact |
| `MAX_EXPORT_PARTITIONS` | `1_024` | maximum number of files in one partitioned artifact set |
| `EXPORT_DEFAULT_DEADLINE_SECONDS` | `600` | deadline applied when the caller supplies none via `request/mod.rs:RequestContext` |
| `MAX_EXPORT_TEMP_BYTES` | `16 GiB` | maximum live staging bytes per store root across concurrent exports |
| `MAX_ACTIVE_EXPORT_PUBLISHERS` | `4` | concurrent publications per store root (≤ `manifest.rs:MAX_ACTIVE_PUBLISHERS = 8`) |

**Partitioning policy** **[Decision]**: the default artifact is a single file.
If a single file would violate `MAX_EXPORT_SINGLE_FILE_BYTES` or
`MAX_EXPORT_PARTITIONS`-scale fan-out is explicitly requested, the writer
produces a Partitioned Artifact Set with `part-<seq:010>.<ext>` members with
zero-based contiguous sequences (§1). Row-to-partition assignment follows input
partition order without repartitioning logic in v1.

**Deadline and overshoot disclosure** **[Decision]**: the effective deadline
comes from `RequestContext` or the default above. An export whose deadline
expires during an uninterruptible region completes or aborts that region and
must disclose the overshoot in its result metadata rather than swallow it,
matching ADR-002 §4.

**Bounds compose, never relax** **[Decision]**: these ceilings sit alongside —
never above — existing envelope, engine, and store limits (`MAX_BATCH_BYTES`,
engine memory laws, `MAX_INPUT_ENVELOPES`, activity gates). No export path may
raise another plane's limit as a side effect.

## 6. Paths, filenames, roots, and overwrite policy

**Destinations live only inside registered Allowed Roots** **[Decision]**: roots
are absolute, opened with no-follow directory handles; symlinked components and
traversal attempts are rejected before any byte is written, exactly as
local-tabular read roots behave (X0-D0 §4). Relative destinations, root escape,
and home-relative shortcuts fail typed.

**Filename grammar** **[Decision]**: every path component below an Allowed Root
matches `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` (1–128 characters, initial character
alphanumeric); components `.`, `..`, and names beginning with `.` are reserved
and rejected; comparisons are byte-exact case-sensitive. Single-file artifacts
end in exactly `.csv`, `.tsv`, `.jsonl`, or `.parquet`, matching the negotiated
format. Partitioned sets use the §1 `part-<seq:010>.<ext>` scheme. Total depth
below the root is at most 8. Object-store keys, once E5 wiring exists, follow
the same component grammar.

**Overwrite is forbidden; publication is create-new** **[Decision]**: a
destination file or artifact directory that already exists causes a typed
failure. No truncation, replacement, suffixing, or move-aside occurs, closing
the gap X0-D0 §4 records in the upload byte layer ("no create-new guard"). A
name becomes publishable again only after its previous artifact is tombstoned
and collected (§7). Staging directories are keyed by the export id; a staging
collision is a typed failure, not a merge.

**Export identity** **[Decision]**: every export carries a caller-injected UUID
export id; nil is rejected; duplicate active ids fail typed — the same
identity discipline as `DatasetSnapshot` (X0-D0 §5).

## 7. Digest, provenance, retention, atomic publication, and recovery

**Export Digests** **[Decision]**: each finalized artifact file gets a SHA-256
over its complete finalized bytes, serialized lowercase hex (the `ContentDigest`
discipline of `digest.rs`; X0-D0 §5). The Export Manifest additionally carries a
set digest: SHA-256 over the UTF-8 line-joined sequence of lowercase-hex
per-file digests in partition order. Both are mechanically recomputable by any
conforming reader.

**Provenance is a versioned manifest, persisted beside the artifact**
**[Decision]**: every publication writes an Export Manifest carrying
`EXPORT_MANIFEST_VERSION` and: export id; the full Committed Input tuple (§2);
format identifier and format-contract version; encoder/storage versions;
producer `ENGINE_CONTRACT_VERSION`; created-at instant (UTC, RFC 3339); row and
byte totals; ordered per-file digests and sizes; and the destination root
reference. Manifests never contain secret material or credential values (AGENTS
rule 10); credential references stay references.

**Publication is atomic and is the single visibility point** **[Decision]**:
bytes are written to the Staging Area, fsynced, installed into position by
rename, and only then does the Export Manifest commit make the set visible.
Readers observe every file of a set or none — the ADR-001 invariant 6 discipline
applied to exports. A failure before manifest commit leaves no visible
artifact. There is no second publication path.

**Recovery sweeps residue; recovery never publishes** **[Decision]**: stale
staging directories and journal rows left by crashed exports are removed by a
maintenance sweep analogous to `store.rs:recover` (X0-D0 §2.7), which acquires
the maintenance gate excluding readers and publishers (X0-D0 §2.6). Recovery
deletes; it never completes a half-written publication. After recovery, the
affected export id is free, and a retry starts from zero.

**Retention is tombstone-first and explicit** **[Decision]**: deletion goes
through tombstone (invisible to ordinary reads) followed by garbage collection
of bytes after the retention cutoff, with candidate caps, mirroring
`tombstone_snapshot` / `collect_garbage` semantics (X0-D0 §2.7). Retention
duration is supplied policy data; absent policy, artifacts persist until
explicitly tombstoned. Silent background expiry of visible artifacts is
forbidden.

## 8. Cancellation, deadlines, and retry

**Cancellation is cooperative and checkpointed** **[Decision]**: export observes
`request/mod.rs:RequestContext` cancellation at defined checkpoints — before
input verification, after each input partition, after each output file append,
and before publication. Between checkpoints the exporter declares its
uninterruptible regions, as executors do under ADR-002 §4.

**A cancelled or deadline-exceeded export leaves nothing visible**
**[Decision]**: no artifact, no manifest, no partial file at the destination;
staging is removed best-effort immediately and definitively by the §7 sweep.
Callers distinguish `cancelled` from `failed-integrity` and from
`bound-exceeded` by typed error category — one taxonomy, mapped like
issue-046 §16, never stringly.

**Retry is deterministic and safe** **[Decision]**: retrying a failed or
cancelled export with the same Committed Input and versions reproduces
identical bytes (§3), so recovery-by-replay needs no special machinery. Retrying
an already-published export id at the same destination fails under §6
create-new unless the earlier artifact was tombstoned and collected.

**Overshoot honesty** **[Decision]**: deadline or cancel signals arriving inside
an uninterruptible region are disclosed in result metadata with magnitude when
measurable, never swallowed — the ADR-002 §4 obligation, inherited unchanged.

## 9. Instruction JSONL and Chat JSONL

**Out of scope entirely** **[Decision]**. Instruction JSONL and Chat JSONL have
no schema, type, reader, writer, or frontend option anywhere on this base
(X0-D0 §3, §8), and the roadmap permits them only behind separate typed-schema
approvals. Under this ADR they are not export formats: requesting them fails
typed, no encoder may recognize them, and no frontend label may imply them.
They enter Track X solely through an independent, separately dispatched typed
schema approval that amends this section; no other document, PR, or task can
activate them.

## 10. Scope, non-goals, and delivery gates

This ADR is a contract freeze. Its delivery is a Draft PR containing exactly
one new file — this document — and nothing else.

**Non-goals, restated as prohibitions** **[Decision]**:

- No Export runtime, streaming encoder, staging area, or publication code is
  implemented by the delivery that carries this ADR. Every §3–§8 mechanism is
  contract text until a later dispatch implements it.
- No crate changes of any kind — including `stillflow-connector-local-tabular`
  — and no new dependency arrows; AGENTS "Dependency direction" holds verbatim.
- No action on E4 or [#91](https://github.com/X44421/stillflow/issues/91): the
  E4-S2 hold stands; §2 merely refuses rejected-row inputs until E4 lands.
- No API or job-system change beyond this text: endpoints, job/run/event
  integration, and download transport remain blocked by E5 (X0-D0 §6, §8).
- No edit to ADR-001, ADR-002, AGENTS.md, or any other existing file; nothing
  herein supersedes anything.
- Not Ready, not merged, without independent architecture acceptance, which is
  a separate dispatch from this freeze.

**Delivery gates** **[Decision]**:

| Gate | Entry | Outcome | Stop conditions |
| --- | --- | --- | --- |
| X-C0 (this ADR) | charter #104; X0-D0 merged (#66); exact base `0496658` claimed in registry rev ≥ 88 | this document merged as Proposed via Draft PR after independent architecture acceptance | any runtime/crate/API file touched; Ready-without-review; base drift |
| X-R1 (implementation, later dispatch) | this ADR Approved; E5 surfaces available where required | define `EXPORT_MANIFEST_VERSION` and the §5 bound constants in code, core domain values, storage manifest/digest/retention, engine writer runtime per §§2–8 | semantic deviation from this text without amendment; second publication path; silent overwrite; uncheckpointed cancellation |
| Amendment-only items (§2, §9) | independent typed-schema approval (Instruction/Chat JSONL) or input-object admission (E4 outputs) | explicit ADR-004 amendment | activation by implication or by unrelated PR |

## Open questions

Stated, owned, and blocked on evidence — none is resolved by assumption here:

1. Whether Arrow IPC becomes a sanctioned export format later (X0-D0 §3 records
   it as a missing surface). Deferred to a future amendment; nothing in this
   ADR prescribes it.
2. The concrete shortest-round-trip float algorithm to pin for JSONL (§3) is
   named at implementation time by X-R1 and recorded in the Export Manifest;
   until then byte-identity claims for float-bearing JSONL are unclaimable, per
   the §3 pinning rule.
3. Whether multi-Snapshot or subset inputs are ever admitted (§2). Requires an
   amendment naming selection semantics; not decided here.
4. Object-store destinations inherit §6 grammar, but provider-side immutability
   guarantees (versioning, retention locks) are E5-scope facts not yet fixed.

## Consequences

### Benefits

- The first export implementation inherits one testable meaning for inputs,
  encodings, ordering, bounds, paths, publication, cancellation, and retention,
  instead of setting precedent by accident.
- Integrity discipline already proven for Snapshots (digest, atomicity,
  recovery, tombstones) extends to user-facing artifacts without new concepts.
- Deterministic ordering plus version-pinned encodings make "same input, same
  bytes" an evidence-backed claim, aligned with ADR-002 equivalence levels.
- Fail-closed rules for NaN/inf/binary/nested-in-text prevent silent data
  corruption at the most lossy boundary in the system.
- Instruction/Chat JSONL pressure is contained behind an explicit approval gate
  rather than ad-hoc format drift.

### Costs

- Strict create-new publication means callers manage naming and cleanup of
  superseded artifacts through explicit tombstones.
- CSV/TSV users cannot round-trip binary or nested columns; typed failures will
  surface where other tools emit lossy tokens.
- Seven new bound constants and a versioned manifest demand discipline from
  every implementing PR and add permanent conformance surface.
- Byte-identity claims expire on encoder-version bumps, forcing re-evidence.
- Until X-R1 lands, this document governs nothing executable; the gap between
  contract and capability remains visible in X0-D0 §8.

## Rejected alternatives

- **Accept live engine output as direct export input:** couples the exporter to
  executor internals, creates a second publication path, and breaks ADR-002 §2
  ownership; commit-then-export keeps one visibility point per plane.
- **Silent overwrite or replace-on-publish:** destroys user data and hides
  collisions; X0-D0 §4 records the upload byte layer's missing guard as a gap,
  not a pattern to copy.
- **Best-effort tokens for NaN/inf/binary in text formats:** produces files
  that our own readers reject; asymmetric with reader strictness and a silent
  corruption class.
- **Global row sorting inside the exporter:** invents a second ordering
  authority absent any storage-level sort contract (X0-D0 §2.5).
- **Locale- or environment-dependent formatting:** breaks reproducibility and
  the determinism laws this architecture is built on.
- **Admitting Instruction/Chat JSONL by writer convenience:** activates
  formats with no approved typed schema; explicitly gated behind independent
  approval by the roadmap (X0-D0 §8).
- **Implementing the runtime in this ADR's PR:** violates the dispatch scope
  and the risk gates; contract and implementation are separate deliveries.
- **Amending ADR-001/ADR-002/AGENTS.md here:** out of charter for #104; no
  statement of either conflicts with this ADR.

## Verification

Mechanically checkable enforcement for this ADR's own acceptance:

- Single-file diff, docs-only: `git diff --stat` against the base shows exactly
  one new file; `git diff --check` is clean; no unfinished-work marker of any
  kind appears.
- Dependency arrows: unchanged from AGENTS "Dependency direction"; no crate,
  lockfile, or frontend file is modified by this delivery.
- Links: relative links resolve within `docs/architecture/` and `docs/issues/`;
  referenced issues (#104, #81, #66, #54, #91, #80, #9, #10) match their
  subjects; cited symbols (`batch.rs:MAX_BATCH_ROWS`,
  `manifest.rs:MAX_ACTIVE_READERS`, `manifest.rs:MAX_ACTIVE_PUBLISHERS`,
  `digest.rs:ContentDigest`, `store.rs:SnapshotStore.*`,
  `domain/snapshot.rs:DatasetSnapshot`,
  `batch.rs:LOGICAL_SCHEMA_FINGERPRINT_ALGORITHM`,
  `request/mod.rs:RequestContext`) exist at the contract base.
- Acceptance-key mapping, each objectively testable:

| Registry acceptance key | Frozen by |
| --- | --- |
| `x-c0:committed-input-rules` | §2 |
| `x-c0:encoding-semantics` | §3 |
| `x-c0:deterministic-order` | §4 |
| `x-c0:bounds` | §5 |
| `x-c0:path-filename-overwrite` | §6 |
| `x-c0:digest-provenance-atomic` | §7 |
| `x-c0:cancel-recovery` | §8 |
| `x-c0:no-runtime` | Citation discipline ¶4; §10 prohibitions and gates |
