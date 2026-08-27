# Agent artifact-safety norms (dispatch standard, adopted 2026-08-27)

Adopted after wave 2026-08-26 lost 4/5 subagent contexts to silent
context-poisoning death. These norms are binding for every executor and
reviewer dispatched through this registry unless a task contract explicitly
derogates (and any derogation must itself be justified in the task row).

1. **Never load a large artifact into model context.** Any file >= ~200KB is
   referenced by path + sha256 + generating command only. Oracle dumps,
   benchmark raw samples, fixture files (observed sizes: 8.5MB oracle JSONs,
   279MB NDJSON fixtures) are compared by PROGRAMS, not by agents reading them.
2. **Bounded tool output.** No bash command may emit unbounded output: redirect
   verbose runs to files and tail; prefer `head -c`, `jq 'length'`,
   `python3 -c` summaries, `wc`, `sha256sum`.
3. **Read long contracts once**, keep notes; do not re-dump issue bodies or
   large diffs repeatedly across turns.
4. **Checkpoint early**: commit audited work locally before long phases so a
   context death does not lose progress.
5. **Timeout discipline**: wrap potentially slow commands in `timeout`;
   transient network errors may be retried with backoff; taskctl exit 3
   (remote conflict) always means STOP, never blind retry.

Enforcement: dispatchers must paste these norms (or their substance) into every
executor/reviewer prompt alongside the task contract.
