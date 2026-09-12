# NX v1 compatibility baseline (NX-C0 contract §10.5)

This directory records **pre-change** behavior of the NodeGraph compiler for
the version-1 compatibility baseline. It is captured at the frozen base, never
regenerated from a candidate head. A v1 fixture may change only under a
contract that explicitly authorizes a change to version-1 execution identity.

- Base: `082f1e2282397a9514577c354b2a9a4dc845b66c` (merge of PR #347, NX-C0;
  the NX-C0 contract names `main@5bff563…` with rebind authorization, and
  #336's frozen capture point is the head its branch starts from)
- Capture command: `cargo test -p stillflow-plan --test nx_s1_semantics -- --ignored capture_v1_baseline`
- Toolchain: rustc 1.85.0 (4d91de4e4 2025-02-17), cargo 1.85.0
- Environment: Linux x86_64 (WSL2), workspace at `backend/`
- Files: one JSON document per corpus case under `cases/`, each storing the
  normalized input graph, the authorized source schema, the compile target,
  and the expected outcome (canonical bytes hex, fingerprint, per-node plan
  IDs and schemas, or the frozen error code and node ID).

The verify test in `tests/nx_s1_semantics.rs` rebuilds each case and compares
both the normalized input and the observed outcome against these files. The
same harness must produce byte-identical results on any candidate head; a
difference is a compatibility event that the implementing PR must fix or
justify under the NX-C0 contract.
