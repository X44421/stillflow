# Issue #137 Implementation Contract: E24-JSON-P5 fused selected Utf8

> Status: Frozen overlay of Issue #137
> Risk: High (connector JSON validation path)
> Issue: [#137](https://github.com/X44421/stillflow/issues/137)
> Authorized base: `main@04966586192f8750a02790da988db71a28d82074`
> Branch: `agent/e24-json-p5-selected-utf8-fused`
> Last updated: 2026-08-26

This document mirrors Issue #137. The Issue remains authoritative if they diverge.

## 1. Objective

Fuse deserialize + logical validation for **selected top-level Utf8 fields only**,
while still emitting the same `Value::String` / `Value::Null` for the existing
reencode/Polars tail.

## 2. Keep unchanged

- Framing, linear schema lookup, per-row selected `BTreeSet`, duplicate `seen`,
  missing-field scan, nested Struct, JSON array, reencode, Polars decode, reorder
- Default feature set / default runtime path

## 3. Authorized experiment

- Private feature `json-selected-utf8-fused` (off by default)
- Runtime `STILLFLOW_JSON_SELECTED_UTF8_FUSED=1` selects fused Utf8 seed
- Non-Utf8 selected fields and all non-selected fields stay on the existing path

## 4. Focused A/B

WSL2 ext4, `--release`, `--features json-selected-utf8-fused`:

- 10 × 100K NDJSON
- 100 × 100K NDJSON
- warmup both modes; 5 interleaved reps/mode
- schedule: `OFF, ON, ON, OFF, OFF, ON, ON, OFF, OFF, ON`

## 5. Verdict (exactly one)

- semantic mismatch: `FUSED_UTF8_SEMANTIC_REJECT`
- 100-col median gain <10%: `FOCUSED_FUSED_UTF8_WEAK`
- 100-col ≥10% and 10-col regression >5%: `FOCUSED_FUSED_UTF8_WIDE_ONLY`
- 100-col ≥10% and 10-col regression ≤5%: `PROMOTE_FUSED_UTF8_CANDIDATE`

No outcome authorizes merge or a default-path flip.
