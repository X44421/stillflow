# Issue #368: derived-computation expression extension and paused-capability decisions

> Status: Candidate frozen contract for acceptance by #368
> Risk: L1 — documentation and contract only; it freezes L3-class decisions but authorizes no runtime change by itself
> Parent: #361 (Epic: complete the twelve NodeGraph data-processing capabilities)
> Depends on: #362 (merged) §3.2 bucket B and §4 paused-capability ledger, #366 and #367 (merged) for the established extension pattern
> Base: `main@f84713ce364399372c711c1868e64629665198b2`
> Suggested branch: `agent/issue-368-nx-n4-expression-extension`

This document does two things the #362 contract requires before #368 can be implemented: it records the
**contracted decision to lift two paused capabilities** (the #362 §4 ledger forbids lifting a row
without one), and it freezes the **expression surface** that derived computation needs. It registers no
node, changes no execution behaviour, and authorizes no runtime work by itself.

---

## 1. Decision and authority boundary

### 1.1 What this contract freezes

1. The un-pause decision for `CheckedArithmeticPaused` (§2.1) and `ContainsPaused` (§2.2), with the
   exact semantics that replace the pause.
2. The rows that **stay paused**, so the un-pause cannot be read as a general relaxation (§2.3).
3. Three new closed expression variants — `Concat`, `Conditional`, `Substring` (§3).
4. Type inference, nullability and NULL-propagation laws for every new or newly admitted operator (§4).
5. That the existing expression resource bounds are reused unchanged (§5).
6. The compatibility argument that makes the change additive for every existing graph and stored plan (§6).
7. The tests the implementing slice must provide (§7).

### 1.2 What this contract does not authorize

It does not implement the operators, does not register or change any node, does not change
`Rule`/`Expr` variants by itself, does not add a regex or pattern language, does not add user code or a
second expression AST, does not widen a bound, does not lift any row of §2.3, and does not change
`LogicalPlan` serialization for existing plans.

### 1.3 Authority

`Expr` remains the only expression language and `LogicalPlan` the only execution authority. The new
variants are closed vocabulary: no free-form pattern, no callback, no backend-specific predicate. The
shared analyzer in `stillflow-plan::semantics` decides type and nullability once; the engine only
executes what the analyzer admitted.

---

## 2. Paused-capability decisions

The #362 §4 ledger states that lifting any row "requires its own contracted decision". This is that
decision for two rows, and only those two.

### 2.1 `CheckedArithmeticPaused` is lifted with checked semantics

Arithmetic becomes admissible with these frozen semantics:

| Operator | Operand rule | Result | Failure behaviour |
| --- | --- | --- | --- |
| `Add`, `Subtract`, `Multiply` | both operands the **same** numeric logical type | that type | integer overflow → **NULL** for that row; float follows IEEE-754 (`inf`/`NaN` are values) |
| `Divide`, `Modulo` | both operands the same numeric type | that type | division or modulo by zero → **NULL** for that row, for every numeric type |
| `Negate` (unary) | one numeric operand | that type | integer overflow (`MIN` negated) → **NULL**; float follows IEEE-754 |

Frozen consequences:

* **No implicit widening or coercion.** Mixed operand types are rejected by the analyzer
  (`IncompatibleType`); a caller that needs a different type uses the existing `Cast` expression, which
  keeps its explicit `onFailure` policy.
* **Nullable result.** Every arithmetic expression is nullable, because overflow and division by zero
  yield NULL. `Rule::DeriveColumn` already enforces that the declared output nullability is not narrower
  than the expression (`DerivedNullabilityNarrower`), so a caller must declare `nullable: true`.
* **NULL propagates.** A NULL operand yields NULL; arithmetic never substitutes a value for NULL.
* Determinism: the same operand values always produce the same result; no mode, environment or
  platform setting participates.

### 2.2 `ContainsPaused` is lifted as a **literal** containment test

`BinaryOperator::Contains` becomes admissible with a deliberately narrower meaning than the pause
comment anticipated:

* it is a **literal substring test** on `Utf8` operands and returns `Boolean`;
* the right operand is a value, never a pattern: **no regex, no wildcard, no character class**, and no
  Polars `regex` feature is enabled by this decision;
* NULL on either side yields NULL;
* non-`Utf8` operands are rejected by the analyzer.

The original pause was recorded as "paused until the regex polars feature is approved". This decision
resolves it the other way: the capability is admitted without regex, so the feature stays off and the
engine's existing note can be retired when the implementation lands.

### 2.3 Rows that stay paused

Unchanged and **not** lifted by this contract: `ListStructPaused`, `TimestampSecondPaused`,
`DateToUtf8CastPaused`, `BinaryCastUnauthorized`, and the ordered-comparison incompatibility laws.
#378 will need the list/struct decision, #367 already worked within the temporal constraints, and each
still requires its own contracted decision.

---

## 3. New expression surface

All three variants are additive closed vocabulary. Each is a new `Expr` variant, so the change is L3
under the #362 §3.2 bucket-B rules (additive only; wire tags frozen once published; schema law declared
before engine admission; catalog agreement where a node exposes it).

### 3.1 `Concat { expressions }`

* Concatenates `Utf8` operands in declared order into one `Utf8` value.
* **Arity**: at least 2 and at most 8 operands (bounded; the bound is part of the schema law).
* **NULL law**: NULL in any operand yields NULL (SQL `||` semantics). Substituting a default is the
  caller's explicit choice through the existing `Coalesce`.
* Empty strings concatenate normally (`""` is a value, not a NULL).

### 3.2 `Conditional { predicate, then, otherwise }`

* `predicate` must be `Boolean`; `then` and `otherwise` must share one logical type, which becomes the
  result type.
* **NULL predicate** takes the `otherwise` branch, consistent with the frozen `Validate` severity
  mapping in #364 §4.2 (`NULL` behaves as `false`).
* Result nullability is the logical OR of the two branch nullabilities.

### 3.3 `Substring { expression, start, length }`

* `expression` must be `Utf8`; `start` and `length` are **integer literals**, not dynamic expressions:
  `start >= 1` (1-based, SQL-like) and `length >= 0`.
* Out-of-range requests are clamped, never an error: a `start` beyond the end yields the empty string,
  and a `length` beyond the remaining text yields the remainder.
* NULL input yields NULL. An empty input yields the empty string.
* Indices count **Unicode scalar values**, not bytes, so extraction never splits a character.

---

## 4. Type inference and nullability laws

1. The result type of every new or newly admitted operator is decided by the shared analyzer before the
   engine sees it; the engine never infers a type from data.
2. `Concat` and `Substring` yield `Utf8`. `Contains` yields `Boolean`. Arithmetic yields its operand
   type. `Conditional` yields its branch type.
3. Nullability is conservative: `Concat` nullable if any operand is nullable; `Conditional` nullable if
   either branch is nullable; `Substring` nullable if its input is; `Contains` nullable if either
   operand is; arithmetic is always nullable (§2.1).
4. `DeriveColumn`'s declared output identity stays explicit and caller-supplied — id, name, data type
   and nullability are configuration, never inferred from the expression.
5. A newly admitted expression can only ever **admit** configurations that previously failed to compile;
   it can never change the inferred type or nullability of a configuration that already compiled.

---

## 5. Resource bounds

The existing expression bounds are reused unchanged: `MAX_EXPR_NODES = 1024`, `MAX_EXPR_DEPTH = 64`,
and the compile-work contribution of four units per expression node. Each new variant counts as one
expression node plus its children, so a `Concat` over eight operands costs one node plus the operand
subtrees. No bound is widened, and no new bound is introduced; the `Concat` arity bound in §3.1 is a
schema law, not a resource ceiling.

---

## 6. Compatibility

* **Additive AST.** No existing variant's name, wire tag, field meaning or ordering changes. Plans that
  serialize today keep serializing to byte-identical canonical bytes.
* **Un-pausing cannot change an accepted plan.** Expressions using the previously paused operators are
  rejected by the shared analyzer and again by engine preflight, so no executable plan contains them;
  admitting them cannot alter any existing plan's bytes, fingerprint or execution result.
* **No node change is required.** The existing `derive-column` node already declares `id`, unique
  `name`, `dataType`, `nullable` and an `expression`; extending the expression surface leaves its
  configuration and catalog entry unchanged.
* **Catalog agreement** applies where a node advertises expression constraints: the advertisement must
  keep describing exactly what the validator admits.

---

## 7. Required tests for the implementing slice

The implementation must provide, on real data:

1. **Arithmetic**: integer overflow → NULL per row; division by zero → NULL for integers and floats;
   float `inf`/`NaN` behaviour documented by an explicit test; `Negate` of the minimum integer → NULL;
   mixed operand types rejected by the analyzer.
2. **NULL propagation** for arithmetic, `Concat`, `Contains` and `Substring`, with NULL never becoming a
   value.
3. **`Concat`**: two and eight operands; empty strings preserved; arity bounds enforced (1 operand and 9
   operands rejected).
4. **`Conditional`**: both branches; NULL predicate takes `otherwise`; mismatched branch types rejected;
   branch nullability propagated.
5. **`Substring`**: 1-based clamping at both ends; empty result; Unicode scalar (not byte) indexing with
   a CJK and a non-BMP example; NULL input.
6. **`Contains`**: literal substring semantics — a value containing regex metacharacters must be matched
   literally and must not behave as a pattern.
7. **Schema agreement**: the compiled expression type and nullability equal the real preview schema, and
   a derived column whose declared nullability is too narrow is rejected.
8. **Regression**: existing `derive-column` configurations and the frozen version-1 corpus still compile
   to byte-identical plans.

---

## 8. Non-goals and stop conditions

This delivery does **not**: implement any operator; enable the Polars `regex` feature or any pattern
language; add user code, callbacks or a second expression AST; lift `ListStructPaused`,
`TimestampSecondPaused`, `DateToUtf8CastPaused` or `BinaryCastUnauthorized`; widen `MAX_EXPR_NODES`,
`MAX_EXPR_DEPTH` or the compile-work budget; change `Rule` variants, node definitions, catalog entries,
plan serialization, storage or API behaviour; change OpenShip code.

**Stop and return to contract review** if the implementation needs a wider operator set than §3, a
pattern language, a dynamic substring index, implicit numeric coercion, a new bound, or a semantic that
contradicts §2 or §4.
