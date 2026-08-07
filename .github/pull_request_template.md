## Summary

<!-- What changed, why it is needed, and the architectural outcome. -->

Closes #

## Contract and risk

Risk: `risk:low | risk:medium | risk:high`

<!-- Required for medium/high risk. Use N/A for low risk. -->

Contract: `docs/issues/issue-NNN-implementation-contract.md`

## Scope

<!-- List touched crates/files and confirm explicit non-goals. -->

## Dependency changes

<!-- New dependencies, removals, or "None". -->

## Public API and data-format changes

<!-- Breaking/additive changes, compatibility decision, migration impact. -->

## Bounds, errors, and cancellation

<!-- Row/byte/batch limits, failure semantics, cancellation, or N/A. -->

## Tests

```bash
cd backend && cargo fmt --all -- --check
cd backend && cargo clippy --workspace --all-targets -- -D warnings
cd backend && cargo test --workspace
npm run typecheck
npm run build
```

<!-- Record exact pass/fail/not-run result for each command. -->

## Contract deviations

<!-- "None" or a linked follow-up issue with rationale. -->

## Completion report

- Modified files:
- New dependencies:
- Public API changes:
- `unwrap` / `expect` usage:
- TODO items:
- Test results:
- Remaining risks:
- Branch:
- Commit:

## Review checklist

- [ ] Built from the documented base branch; historical branches were read-only.
- [ ] Dependency arrows remain valid.
- [ ] No unbounded data operation or secret-bearing payload was added.
- [ ] Deterministic contracts have serialization/invariant tests.
- [ ] No unauthorized frontend layout/style/token change is included.
