# E5-D0 Runtime Domain Inventory

> Status: discovery-only, non-binding
>
> Issue: #58
>
> Inventory base: `main@85502cbebb1fab461fe42d30fe019ad20613aa7c`
>
> Delivery: docs-only
>
> E5 contract status: not frozen

## 1. Scope and methodology

This document records repository facts observed at:

`main@85502cbebb1fab461fe42d30fe019ad20613aa7c`

It is an input to a future E5-C0 contract. It does not freeze E5 public
fields, lifecycle semantics, serialization formats, HTTP endpoints, crate
dependencies, or runtime implementation.

PR #53 and PR #57 are read-only references for this inventory. They are not
merged, rebased, or cherry-picked into this branch.

Repository findings in this document use the following implementation-status
vocabulary:

- `implemented`: a concrete definition and/or executable behavior exists on the
  inventory base.
- `placeholder`: a name, crate, type, or architectural slot exists, but the
  intended capability is not implemented.
- `missing`: no corresponding E5 domain capability is implemented on the
  inventory base.
- `blocked by E4`: the decision depends on E4 contracts or implementation that
  are not part of the inventory base.

Persistence is classified separately:

- `persisted`: concrete persistence write/read behavior exists.
- `runtime-only`: the value exists only during execution.
- `defined-but-not-persisted`: a domain definition exists, but no persistence
  path has been established by repository evidence.
- `unknown`: the inspected evidence is insufficient to classify persistence.

Serialization support alone is not considered evidence of persistence.

For each repository claim, this inventory records an exact repository path and
the inventory base SHA.

## 2. Current object inventory

### 2.1 Session

Pending investigation.

### 2.2 Dataset

Pending investigation.

### 2.3 DatasetSnapshot

Pending investigation.

### 2.4 SnapshotManifest

Pending investigation.

### 2.5 IngestionEvent

Pending investigation.

### 2.6 RequestContext

Pending investigation.

### 2.7 ExecutionIdentities

Pending investigation.

## 3. Missing object matrix

Pending investigation.

## 4. Crate ownership

### 4.1 Accepted dependency direction

```text
stillflow-api
    -> stillflow-engine
        -> stillflow-plan
        -> stillflow-connectors
        -> stillflow-storage
            -> stillflow-core