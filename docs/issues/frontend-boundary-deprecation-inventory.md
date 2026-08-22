# Frontend Boundary and Deprecation Inventory (F0-D0)

| Field | Value |
| --- | --- |
| Issue | [#79](https://github.com/X44421/stillflow/issues/79) — `F0-D0-FRONTEND-DEPRECATION-INVENTORY` |
| Task type | Docs-only investigation. No source deletion, no movement, no dependency change, no CI change, no backend change. |
| Exact base | `main@9891dcf55875bb5e236e3573d17e50fae9caa091` — equal to `origin/main` at claim time and at branch creation; during the investigation window `origin/main` advanced to `f16666e59896e2d8bae3b79e188b8f567bb8c534` (merge of PR #74, `backend/**` only); this branch was not rebased and remains parented exactly on the contracted base (Appendix A) |
| Branch | `agent/issue-079-frontend-deprecation-inventory` |
| Claim | [issuecomment-5379280769](https://github.com/X44421/stillflow/issues/79#issuecomment-5379280769), 2026-08-22 |
| Deliverable | This document — the sole changed path in the PR |
| Working discipline | Isolated worktree outside the repository checkout; all analyzer output written under `/tmp`, never inside the repo |

**What this document freezes:** a `KEEP` / `REPLACE-FIRST` / `MIGRATE/ARCHIVE` / `DELETE-AFTER-PROOF` / `UNKNOWN-BLOCKED` boundary for every root frontend and build unit, backed by executed commands and two independent dead-code detection methods. **It authorizes nothing**: each future removal requires its own issue and PR per slice of Section 7.

## 1. Executive summary

1. The root frontend is 19 tracked files: 18 TS/TSX/CSS sources under `src/` (1,971 LOC total) plus root `index.html`. It was added in the repository's initial commit `9c451c9` ("initial commit: Vite + React + Tailwind project with DataCleaner Inspector panel", dated by commit metadata) and last touched in `a2a6b41` ("chore: optimize repository engineering configuration"); all E-series backend work landed without touching it.
2. The known chain `index.html → src/main.tsx → src/App.tsx → components/data/types/utils/duckdb` is verified and complete. Import-graph reachability (madge, cross-checked against a manual audit of every import statement) yields exactly 17 reachable modules from the entry and exactly 2 unreachable files: `src/components/ActivityPanel.tsx` and `src/utils/cn.ts`.
3. There is zero frontend→backend coupling at this base: no `fetch`, `axios`, `XMLHttpRequest`, `WebSocket`, `EventSource`, API base URL, or Vite proxy exists anywhere in `src/` (Section 4.5). The only network traffic is the jsDelivr CDN fetch performed inside `@duckdb/duckdb-wasm` at runtime.
4. The production build emits exactly one artifact, `dist/index.html`, 477,556 bytes (127.54 kB gzip). `vite-plugin-singlefile` **embeds** all JS and CSS into that file; it copies nothing else. `@duckdb/duckdb-wasm` JS bindings alone account for 204,253 bytes = 42.77% of it (isolated A/B stub-build method, Section 5.2). The actual execution payload is not in the repo or the bundle: each cold page load fetches `duckdb-mvp.wasm` (41,325,187 B measured) plus a worker script (839,642 B) from jsDelivr.
5. Consumers that currently require the root layout to stay buildable: the `frontend` job of `.github/workflows/ci.yml` (`npm ci` → `npm run typecheck` → `npm run build`), `.github/workflows/deploy.yml` (publishes `dist/` to GitHub Pages on push to main), the dependabot npm weekly group on `/`, the `AGENTS.md` required-verification block, the PR template tests block, and accepted architecture statements (`docs/data-ingestion-architecture.md` keeps the frontend at the repository root during Phase 1).
6. Whole-file deletion candidates proven by both methods: `src/components/ActivityPanel.tsx`, `src/utils/cn.ts` (whose removal unblocks dropping the `clsx` and `tailwind-merge` dependencies), and `src/icons/SearchIcon.tsx` (barrel-re-exported but never consumed). Additional path-specific dead units: three exports in `src/data.ts`, five exports in `src/types.ts`, five CSS rules in `src/index.css`, one dead window-event wire format in `Header.tsx`, and unused props in `DatasetPanel`/`DetailPanel`. Every candidate carries consumer evidence, preconditions, and a verification recipe (Section 6.4); none may be deleted automatically.
7. One functional risk is recorded as UNKNOWN/BLOCKED rather than asserted: `initDuckDB()` constructs `new Worker(bundle.mainWorker)` with an absolute cross-origin jsDelivr URL, while the installed package's own README wraps that URL in an `importScripts` blob before constructing the Worker. Static evidence indicates direct construction should throw a browser `SecurityError`, which would make "Run All" fail in a real session. Missing evidence: one executed browser session against the deployed build (Section 6.5).

## 2. Method and evidence protocol

### 2.1 Environment

- Investigation ran in an isolated git worktree checked out at the exact base SHA; the primary checkout and all other branches were untouched.
- All temporary artifacts (npm cache override, madge JSON, stub-build output, logs) were written under `/tmp/f0d0-*` and are reproducible; nothing temporary was added to the repository.
- Node v24.15.0 / npm on the investigation host; CI uses Node 20 (both workflows pin `node-version: 20`). No project file references the host Node version.

### 2.2 Commands executed at the base SHA (all reproducible)

| Purpose | Command | Result |
| --- | --- | --- |
| Clean install | `npm ci --no-audit --no-fund` | exit 243 — `EACCES` on root-owned `/home/owl/.npm/_cacache` (host environment limitation, not a project defect; see note below) |
| Clean install (workaround) | `npm ci --no-audit --no-fund --cache /tmp/f0d0-npm-cache` | exit 0; 211 lockfile package entries installed |
| Typecheck | `npm run typecheck` | exit 0 (`tsc --noEmit`, strict, `noUnusedLocals`, `noUnusedParameters`) |
| Production build | `npm run build` | exit 0; 829 modules transformed; single artifact `dist/index.html` 477.56 kB / gzip 127.54 kB |
| Import graph | `npx madge --extensions ts,tsx --ts-config tsconfig.json --json src/main.tsx` | module graph JSON (saved outside repo); 17 modules reachable |
| Reference search | `grep -rn <symbol-or-path>` over tracked files excluding `node_modules`, `dist`, `.git` | per-symbol counts in Sections 4–6 |
| Coupling search | `grep -rnE "fetch\(|axios|XMLHttpRequest|WebSocket|localhost:[0-9]+|/api/|EventSource" src/` | 0 matches |
| Bundle attribution | A/B stub build per Section 5.2 | baseline 477,556 B vs 273,303 B without `@duckdb/duckdb-wasm` |
| Provenance | `git log --diff-filter=A` / `git log -- src/` | initial add `9c451c9`; last frontend touch `a2a6b41`; 11 commits touched `src/` |

Note on the npm-cache failure: it is a host permission problem (`sudo chown -R 1000:1000 ~/.npm` would fix it locally). It is reported here as an environment limitation, never as a pass. GitHub runners use their own writable caches and are unaffected.

### 2.3 Dead-unit identification — two independent methods, both must agree

- **M-1 structural:** import-graph reachability from the sole entry `src/main.tsx`, computed twice: once by madge (tool-based) and once by manual audit of every import/export statement in `src/` (the two agree on every unit listed in this document).
- **M-2 textual:** whole-repo reference search for each basename and each exported symbol, including `docs/` and `.github/`, excluding `node_modules/`, `dist/`, `.git`.

A unit is listed as a deletion candidate only where M-1 shows it unreachable (or reachable-only-as-unused-export) and M-2 finds no consumer. Where a doc prose mention exists, it is called out explicitly because docs need a rider edit in the same cleanup PR.

## 3. Complete frontend surface inventory

### 3.1 Root files and configuration

| Path | Purpose | Generated? | In typecheck | In build/runtime | Consumers (verified) |
| --- | --- | --- | --- | --- | --- |
| `/index.html` | Vite HTML entry; `<div id="root">`; loads `/src/main.tsx` as module; title "DataFlow — Customer Data Cleaning" (differs from the in-app header text "Customer Data Cleaning" — cosmetic inconsistency, noted for a later rider) | hand-written | n/a | yes — runtime entry and build input | implicit Vite entry; built copy published by `deploy.yml` |
| `/package.json` | Manifest `stillflow@0.1.0` (private, ESM, node >=20); scripts `dev` / `build` / `typecheck` / `preview`; 6 runtime deps, 9 dev deps | hand-written | yes (`types:["node"]`) | toolchain | `npm ci` in both workflows; dependabot npm group; `AGENTS.md` verification block; PR template tests block |
| `/package-lock.json` | Lockfile, 211 package entries | generated by npm | — | — | same as `package.json` |
| `/vite.config.ts` | Plugins `@vitejs/plugin-react`, `@tailwindcss/vite`, `viteSingleFile`; alias `@ → src` (alias configured but unused: 0 `@/` imports in `src/`) | hand-written | yes (`include` lists it) | build config | `npm run build`; `deploy.yml` runs `npx vite build` |
| `/tsconfig.json` | Strict; `noUnusedLocals`/`noUnusedParameters`; `include: ["src","vite.config.ts"]`; paths `@/*` | hand-written | defines typecheck scope | — | `npm run typecheck` |
| `/.github/workflows/ci.yml` | Job `frontend`: checkout → setup-node 20 (`cache: npm`) → `npm ci` → `npm run typecheck` → `npm run build`; plus two backend jobs | hand-written | — | gates every PR and push to main | whole repository |
| `/.github/workflows/deploy.yml` | On push to main: checkout → setup-node 20 → `npm ci` → `npx vite build` → upload `dist/` → deploy GitHub Pages. Publishes the prototype UI publicly. | hand-written | — | publication consumer | GitHub Pages environment |
| `/.github/dependabot.yml` | npm ecosystem on `/`, weekly, single group `frontend-dependencies` pattern `*`; also cargo and github-actions groups | hand-written | — | — | recurring automated PRs touching `package.json` + lockfile (a standing contender for the Section 8 write locks) |
| `public/` assets, favicon, env files | Do not exist. No `public/` directory, no image/font assets anywhere outside `node_modules`; the only HTML file is `/index.html`; `.gitignore` covers `node_modules`, `dist`, `*.local` | — | — | — | — |

Governance consumers recorded for completeness: `AGENTS.md` rule 12 (frontend layout/components/CSS/tokens frozen without an explicit UI issue) and its Required verification block (`npm run typecheck`, `npm run build`); `.github/pull_request_template.md` tests block (same two commands) and review-checklist item "No unauthorized frontend layout/style/token change".

### 3.2 Source files (`src/**` — 19 files, 1,971 LOC)

| File | LOC | Purpose | Imported by (actual edges) | Reachable from entry | Dead content inside |
| --- | --- | --- | --- | --- | --- |
| `src/main.tsx` | 10 | `createRoot(...).render(<StrictMode><App/></StrictMode>)` | `index.html` | yes | none |
| `src/App.tsx` | 170 | Application shell; owns pipeline state; orchestrates Run All / Run From Here through `utils/duckdb`; wires Header, IconSidebar, DatasetPanel, PipelineCanvas, DetailPanel | `main.tsx` | yes | none — every declared handler is used |
| `src/components/Header.tsx` | 85 | Top bar: Run All trigger, progress %, search box, help/bell buttons, fake status labels ("Saved 2m ago", "Published") | `App.tsx` | yes | search input dispatches `window` CustomEvent `opencode:search-nodes` — zero listeners exist repo-wide (single occurrence = dispatcher itself); `onSearch` and `error` props declared and immediately discarded as `_onSearch`/`_error`; status/saved labels hard-coded mock values |
| `src/components/IconSidebar.tsx` | 64 | Left icon rail (7 icons + collapse button + avatar) | `App.tsx` | yes | props `activeView`, `assetsVisible`, `onViewChange`, `onToggleAssets` declared but never passed by the call site; collapse button has no handler |
| `src/components/DatasetPanel.tsx` | 208 | Dataset list grouped source/interim/output with tabs and search, fed from mock `data.ts` | `App.tsx` | yes | props `selectedId`, `importing`, `onImportCsv` declared but discarded (`_`-prefixed); "+" connect button merely clears the search box (no behavior) |
| `src/components/PipelineCanvas.tsx` | 241 | Vertical node list with connectors, zoom controls, palette popover, Delete-key handling | `App.tsx` | yes | toolbar buttons Sparkles/Grid/Settings/Expand/Undo/Redo and the canvas "Run" button render with no handlers (visual only); icon→node-type mapping re-implemented inline although `data.ts` exports an equivalent mapping (which is itself dead — Section 6.4 row 5) |
| `src/components/ObjectPalette.tsx` | 137 | Searchable/tabbed transform-object palette feeding node creation | `PipelineCanvas.tsx` | yes | none |
| `src/components/DetailPanel.tsx` | 405 | Node inspector: status, context, runtime metrics, quality cards, actions, editable config, menu, toast | `App.tsx` | yes | props `events`, `availableColumns`, `onPreview`, `onDuplicate` declared but never read; "Preview Result", "Duplicate node", "Copy node" show toasts only (no preview/duplicate capability exists — consistent with `docs/issues/profiling-quality-domain-inventory.md` conclusions); memory display comes from a deterministic estimate, not a measurement (see `utils/duckdb.ts`) |
| `src/components/ActivityPanel.tsx` | 17 | Event-count strip rendering `{events.length} event(s)` | nothing — 0 code importers | **no** (M-1 and M-2 agree) | whole file is dead; one prose mention exists at `docs/issues/profiling-quality-domain-inventory.md:442` |
| `src/data.ts` | 67 | Mock domain data: 10 datasets with fabricated sizes ("2.4M rows"), 5 initial nodes, 8 transform objects, plus name/mapping tables | `App.tsx` (`initialPipelineNodes`), `DatasetPanel.tsx` (`datasets`), `ObjectPalette.tsx` (`transformObjects`) | yes | exports `transformToNodeType`, `nodeDefaultName`, `nodeDefaultDescription` have zero references outside the file; displayed dataset sizes/statuses are fiction presented as state |
| `src/types.ts` | 82 | Shared interfaces/types | every module above | yes | exports `WorkspaceView`, `PreviewColumn`, `DataPreviewResult`, `PreviewLimit`, `EventLevel` have zero references outside `types.ts`; note `PreviewColumn` is cited as historical evidence at `docs/issues/profiling-quality-domain-inventory.md:491` |
| `src/icons/hero.tsx` | 61 | 31 named wrappers around `@heroicons/react/24/outline` + `/24/solid` (+2 hand-drawn fallbacks) | Header, IconSidebar, DatasetPanel, PipelineCanvas, ObjectPalette, DetailPanel | yes | none — programmatic check confirms all 31 exports are consumed by components (tree-shaking would drop any that were not) |
| `src/icons/index.ts` | 2 | Barrel re-exporting `CollapseButton` and `SearchIcon` | `IconSidebar.tsx` (imports `CollapseButton`) | yes | the `SearchIcon` leg of the barrel has no consumer anywhere |
| `src/icons/CollapseButton.tsx` | 24 | Hand-drawn SVG collapse glyph | barrel → `IconSidebar.tsx` | yes | rendered inside a button with no `onClick` (visual only) |
| `src/icons/SearchIcon.tsx` | 24 | Hand-drawn SVG search glyph | barrel only; no component imports `SearchIcon` | reachable-but-unused export | whole file deletable together with its barrel line; real search UIs use the `Search` wrapper from `hero.tsx` |
| `src/utils/duckdb.ts` | 198 | WASM DuckDB bootstrap from jsDelivr bundles; loads sample CSV; SQL executor for filter / deduplicate / normalize / export stages; metrics computation | `App.tsx` (`initDuckDB`, `loadSampleData`, `runFullPipeline`, type `PipelineMetrics`), `DetailPanel.tsx` (`formatRows`) | yes | builds stage SQL by string interpolation of user-editable config values (prototype-grade; blast radius limited to the user's own browser session); `memory` metric is a deterministic formula scaled by row count, explicitly commented as an estimate; duplicate-% branch double-computes the same expression in both arms |
| `src/utils/sample-customers.ts` | 101 | Inline CSV fixture: 100 customer rows with planted nulls, empty strings, and exact duplicates | `utils/duckdb.ts` (`CUSTOMERS_CSV`) | yes | load-bearing at runtime today — the only data the app can execute on |
| `src/utils/cn.ts` | 6 | `clsx` + `tailwind-merge` class-name helper | nothing — 0 references repo-wide including docs | **no** (M-1 and M-2 agree) | whole file is dead; its removal unblocks removing the `clsx` and `tailwind-merge` dependencies |
| `src/index.css` | 69 | `@import "tailwindcss"`; base layer (font smoothing, body font, scrollbar); components layer (`.node-card`, `.connector-line`, `.connector-arrow`, `.no-scrollbar`); keyframes `pulse-dot`, `progress-bar` | `main.tsx` | yes | rules `.node-card`, `.connector-line`, `.connector-arrow`, `.no-scrollbar`, `.animate-progress` have zero class-name occurrences in any tsx (verified against source and against the compiled bundle — Tailwind emits authored `@layer` CSS unconditionally, so they ship today); live rules: base styles and `.animate-pulse-dot` (used once in `PipelineCanvas`) |

### 3.3 Tests, mocks, fixtures, sample data, generated files

- **No test infrastructure exists**: no test runner in `dependencies`/`devDependencies`, no vitest/jest/playwright config, and a filesystem scan for `*.test.*`, `*.spec.*`, `__tests__`, `*.stories.*` returns nothing. The frontend's only automated gates are `tsc --noEmit` and `vite build`.
- **Sample data** = `src/utils/sample-customers.ts` (runtime fixture consumed by `loadSampleData()`). No external fixture files, mocks, or snapshots exist.
- **Generated artifacts**: `package-lock.json` (committed, npm-generated) and `dist/` (gitignored, produced by builds, published by `deploy.yml`). Nothing under `src/` is generated.

### 3.4 Documentation references to the frontend (verified lines)

| Document | Lines | Nature of reference |
| --- | --- | --- |
| `docs/data-ingestion-architecture.md` | :37 | "The existing frontend DuckDB WASM integration is a client-side capability, not the authoritative backend execution engine." |
| `docs/data-ingestion-architecture.md` | :56 | Non-goal: introducing new frontend navigation/panels/visual systems |
| `docs/data-ingestion-architecture.md` | :422 | "The frontend remains at the repository root during Phase 1. The backend is isolated under backend so the current Vite application can continue to build unchanged." — accepted placement decision |
| `docs/data-ingestion-architecture.md` | :525 | Gate: "Backend tests and the existing frontend build pass" |
| `docs/issues/profiling-quality-domain-inventory.md` | :267, :425–426, :432, :491–499 | Concludes frontend Quality display and null semantics are mock/local DuckDB computation, not backend-backed; cites `PreviewColumn` as existing evidence |
| `docs/issues/export-output-artifact-inventory.md` | :134, :140–147, :204–216 | Records absence of frontend export options; evidence keyed to `src` at base `89aab25` |
| `docs/development/ai-development-workflow.md` | :146–147 | Verification commands `npm run typecheck`, `npm run build` |
| `AGENTS.md` | :55, :100–101 | Rule 12 (UI freeze without explicit issue) and verification commands |
| `.github/pull_request_template.md` | :37–38, :65 | Same commands in the tests block; UI-token checklist item |

## 4. Runtime and build graph

### 4.1 Module graph (edges transcribed from actual import statements; reachability confirmed by madge)

```text
index.html
└─ src/main.tsx
   ├─ react, react-dom/client                        (npm)
   ├─ ./index.css ── @import "tailwindcss"           (compiled by @tailwindcss/vite)
   └─ ./App.tsx
      ├─ ./components/Header.tsx ──── ../icons/hero
      ├─ ./components/IconSidebar.tsx ─ ../icons/hero, ../icons (barrel → CollapseButton, SearchIcon*)
      ├─ ./components/DatasetPanel.tsx ─ ../icons/hero, ../types, ../data
      ├─ ./components/PipelineCanvas.tsx ─ ../icons/hero, ./ObjectPalette, ../types
      │    └─ ./components/ObjectPalette.tsx ─ ../icons/hero, ../data
      ├─ ./components/DetailPanel.tsx ─ ../icons/hero, ../utils/duckdb (formatRows), ../types
      ├─ ./data ── ./types
      └─ ./utils/duckdb ── @duckdb/duckdb-wasm, ./sample-customers (leaf)
./types (leaf)

Unreachable from entry (M-1 structural and M-2 textual agree):
   src/components/ActivityPanel.tsx
   src/utils/cn.ts
Reachable but exported-to-nobody:
   SearchIcon (via icons/index.ts barrel),
   data.ts: transformToNodeType, nodeDefaultName, nodeDefaultDescription,
   types.ts: WorkspaceView, PreviewColumn, DataPreviewResult, PreviewLimit, EventLevel
```

madge reports 17 modules in the reachable set (16 TS/TSX + `index.css`); 18 TS/TSX/CSS modules exist under `src/`. The two-file delta is exactly the pair listed above.

### 4.2 Browser runtime flow (DuckDB WASM)

1. `initDuckDB()` calls `getJsDelivrBundles()`, selects the `mvp` bundle, creates `new Worker(bundle.mainWorker)`, then `new AsyncDuckDB(logger, worker)` → `db.instantiate(bundle.mainModule)` → `db.connect()`.
2. `bundle.mainWorker` / `bundle.mainModule` are absolute CDN URLs assembled at runtime. The compiled bundle contains the constructor string `https://cdn.jsdelivr.net/npm/<pkg>@<version>/dist/…`; resolved installed version is `1.33.1-dev57.0` (manifest declares the floating range `^1.33.1-dev57.0`).
3. **Size consequence:** the repository and `dist/` carry zero WASM/worker bytes. Each cold page load fetches `duckdb-mvp.wasm` (41,325,187 B) and `duckdb-browser-mvp.worker.js` (839,642 B) from jsDelivr — sizes measured from the installed package's `dist/` files, which the CDN serves for the pinned version.
4. **Functionality risk (recorded, not assumed):** the Worker constructor is invoked directly with a cross-origin URL. The installed package's README (lines 22–28) documents wrapping the URL first: `URL.createObjectURL(new Blob(['importScripts("<worker url>")']))`. Per the HTML standard, constructing a Worker from a cross-origin URL throws a `SecurityError`. Static evidence therefore indicates pipeline execution fails in a real browser session. Missing evidence, and the reason this stays out of the frozen boundaries: one executed browser session (manual smoke or scripted trace) against a local or deployed build capturing console output during "Run All". See Section 6.5 item U1.
5. `loadSampleData()` wraps `CUSTOMERS_CSV` in a `Blob` object URL and issues `CREATE TABLE raw_customers AS SELECT * FROM read_csv_auto('<blob url>')`.
6. `runFullPipeline()` executes stages sequentially, each as `CREATE OR REPLACE TABLE stg_<type>_<epoch> AS …` reading the previous stage's table: filter drops NULL/empty keys; deduplicate implements Keep-first/Keep-last via `row_number() OVER (PARTITION … ORDER BY created_at)` and Merge-records via `string_agg(DISTINCT …)`; normalize trims/lowercases name/email with null-handling variants; export copies through. Metrics come from `count(*)`/conditional `sum()` queries plus a deterministic pseudo-memory formula (`96 + rowsOut/100*12`, commented as an estimate).

### 4.3 CSS / Tailwind / single-file build pipeline

- Tailwind v4 runs through the `@tailwindcss/vite` plugin; `src/index.css` contains the single `@import "tailwindcss"` plus `@layer base` / `@layer components` author rules and two keyframes blocks.
- Build log records `[plugin vite:singlefile] Inlining: index-ZczDtIaW.js` and `Inlining: style-DAf5TCT2.css`, after which `dist/` contains exactly one file, `index.html`. Answer to the issue's question: `vite-plugin-singlefile` **embeds** the JS and CSS into `index.html`; it does not copy or emit any other asset, so there is no partial-publication surface — every source change lands in that one file.
- Verified side effect: authored component-layer CSS ships even when unused — each dead rule from Section 3.2 appears once in the compiled bundle (`grep -c` = 1 per selector in `dist/index.html`; `.animate-pulse-dot` = 2 including its usage site).
- The `@ → src` alias is configured in both `vite.config.ts` and `tsconfig.json` but never used (0 `@/` imports).

### 4.4 CI, publication, and update consumers (exact steps)

| Consumer | Steps touching the frontend | Trigger |
| --- | --- | --- |
| `ci.yml` job `frontend` | `actions/checkout@v4` → `actions/setup-node@v4` (Node 20, `cache: npm`) → `npm ci` → `npm run typecheck` → `npm run build` | every pull request and every push to main |
| `deploy.yml` job `build-and-deploy` | same setup → `npx vite build` → `actions/upload-pages-artifact@v3` (`path: dist`) → `actions/deploy-pages@v4` | push to main; publishes the prototype publicly via GitHub Pages |
| `dependabot.yml` npm group | weekly `npm` ecosystem updates on `/`, all packages grouped as `frontend-dependencies` | recurring PRs that touch `package.json` + `package-lock.json` |
| Desktop / embed shells | none exist — repo-wide search for electron/tauri/webview/capacitor matches only the transitive `electron-to-chromium` browserslist entry inside `package-lock.json`, which is not an embedder | — |

### 4.5 Frontend→backend coupling: proof of absence

`grep -rnE "fetch\(|axios|XMLHttpRequest|WebSocket|localhost:[0-9]+|/api/|EventSource" src/` returns zero matches. `vite.config.ts` defines no `server.proxy`. No shared types, generated API client, OpenAPI artifact, or environment-variable endpoint exists. Conclusion: at base `9891dcf` the root frontend and `backend/` are fully decoupled; backend changes cannot break the frontend build and vice versa. The only shared resource is CI wall-clock time.

## 5. Dependency and bundle evidence

### 5.1 Command results at the base (clean tree)

| Command | Result |
| --- | --- |
| `npm ci` (default host cache) | exit 243 — `EACCES`, root-owned `/home/owl/.npm/_cacache` — environment limitation, recorded as such |
| `npm ci --no-audit --no-fund --cache /tmp/f0d0-npm-cache` | exit 0; 211 lockfile entries installed |
| `npm run typecheck` | exit 0 |
| `npm run build` | exit 0; 829 modules transformed; `dist/index.html` 477.56 kB (gzip 127.54 kB) |
| `npx vite build --outDir /tmp/f0d0-dist-noduckdb --emptyOutDir` (stub copy, Section 5.2) | exit 0; 273.30 kB (gzip 78.21 kB) |

### 5.2 Bundle attribution — A/B stub methodology and numbers

Method (reproducible, no repository mutation):

1. Baseline: `npm run build` at base → `dist/index.html` = **477,556 B**, gzip 127.54 kB.
2. Scratch copy: byte-copy `index.html`, `package.json`, `package-lock.json`, `tsconfig.json`, `vite.config.ts`, `src/` to `/tmp/f0d0-scratch` (outside the repo) with `node_modules` symlinked to the worktree's.
3. Single change in the scratch copy: rewrite `src/utils/duckdb.ts` as an API-signature-identical stub (same exported names and shapes) whose only material difference is the removal of the `@duckdb/duckdb-wasm` import; the `sample-customers` fixture stays imported so the delta isolates the DuckDB package alone.
4. `npx vite build --outDir /tmp/f0d0-dist-noduckdb --emptyOutDir` → **273,303 B**, gzip 78.21 kB.

Δ = 204,253 B = **42.77% of the baseline bundle** attributable to the bundled JS bindings of `@duckdb/duckdb-wasm` (tree-shaken down to the four imported symbols `getJsDelivrBundles`, `AsyncDuckDB`, `AsyncDuckDBConnection`, `ConsoleLogger`).

Additional runtime cost not visible in the bundle: ≈ 41.3 MB WASM + ≈ 0.84 MB worker fetched from jsDelivr per cold load (Sections 4.2/4.3).

Honesty caveats: the Δ isolates the DuckDB package's bundling cost only; React/ReactDOM dominate the remaining 273 kB and were not separately measured, and no performance or size-savings claim is made for any future slice beyond re-running this exact recipe post-change. Both build outputs remain available under `/tmp` for inspection during review.

### 5.3 Direct and indirect dependency usage (every dependency accounted for)

| Package (declared version) | Sole/primary importer | Status |
| --- | --- | --- |
| `react` 19.2.6, `react-dom` 19.2.6 | `main.tsx` + all components | used |
| `@duckdb/duckdb-wasm` ^1.33.1-dev57.0 (dev-range pin) | `utils/duckdb.ts` | used; REPLACE-FIRST classification (Section 6.2) |
| `@heroicons/react` ^2.2.0 | `icons/hero.tsx` (namespace imports of outline+solid sets) | used; all 31 wrappers consumed |
| `clsx` 2.1.1 | `utils/cn.ts` only | dead-with-host — removable only together with `cn.ts` |
| `tailwind-merge` 3.4.0 | `utils/cn.ts` only | dead-with-host — same |
| `tailwindcss` 4.1.17 + `@tailwindcss/vite` 4.1.17 | `vite.config.ts` plugin; CSS `@import` | used |
| `typescript` 5.9.3 | `typecheck` script | used |
| `vite` 7.3.2 | `build`/`dev`/`preview` scripts; `deploy.yml` `npx vite build` | used |
| `vite-plugin-singlefile` 2.3.0 | `vite.config.ts` | used |
| `@vitejs/plugin-react` 5.1.1 | `vite.config.ts` | used |
| `@types/react` / `@types/react-dom` | `tsconfig.json` | used |
| `@types/node` 22.19.17 | `tsconfig.json` `types:["node"]` for `vite.config.ts` (`path`, `url` imports) | used |

Indirect: the lockfile resolves 211 package entries total; no router, state library, query client, form library, or second UI framework exists — interactivity is hand-rolled `useState`/`useCallback`.

### 5.4 Dead-unit candidates — dual-method proof table

| Unit | M-1 structural evidence | M-2 textual evidence | Corroborating proof |
| --- | --- | --- | --- |
| `src/components/ActivityPanel.tsx` | absent from madge reachable set from `main.tsx`; absent from every manual import edge | repo-wide basename search: self + 1 prose mention (`profiling-quality-domain-inventory.md:442`) | `tsc` cannot flag it because `include:["src"]` compiles standalone files; absence from the graph is the operative fact |
| `src/utils/cn.ts` | unreachable | 0 references repo-wide including `docs/` | `clsx`/`tailwind-merge` have no other importer (checked via `grep` over `src/`) |
| `src/icons/SearchIcon.tsx` | reachable only as barrel re-export target | sole external occurrence = `icons/index.ts:2` re-export line | every real search input renders `hero.tsx`'s `Search` wrapper |
| `transformToNodeType`, `nodeDefaultName`, `nodeDefaultDescription` (`data.ts`) | exported, never imported | 0 refs outside defining file | `PipelineCanvas` re-implements an equivalent icon→type map inline, proving the export was superseded |
| `WorkspaceView`, `PreviewColumn`, `DataPreviewResult`, `PreviewLimit`, `EventLevel` (`types.ts`) | exported, never imported | 0 code refs outside `types.ts`; 1 doc citation (`profiling-quality-domain-inventory.md:491`) | types describe a preview feature that exists nowhere else in code |
| `.node-card`, `.connector-line`, `.connector-arrow`, `.no-scrollbar`, `.animate-progress` (`index.css`) | n/a (CSS) | 0 class-name occurrences across all tsx | each appears exactly once in the compiled bundle (authored CSS ships unconditionally) |
| `opencode:search-nodes` dispatch (`Header.tsx`) | n/a | single occurrence repo-wide = the dispatcher line; 0 `addEventListener("opencode…")` | dead event wire format left from prototyping |
| Unused props (`DatasetPanel`: `selectedId`, `importing`, `onImportCsv`; `DetailPanel`: `events`, `availableColumns`, `onPreview`, `onDuplicate`; `IconSidebar`: view/assets group; `Header`: `onSearch`, `error`) | typed-but-unread at call sites | occurrences limited to declaration sites | `noUnusedParameters` does not fire because interface members are exempt; hygiene items riding slice S3 |

Explicitly NOT candidates despite superficial appearance: `icons/hero.tsx` (all 31 exports consumed), `icons/CollapseButton.tsx` (consumed via barrel), `src/utils/sample-customers.ts` (runtime-load-bearing), `src/components/ObjectPalette.tsx` (imported by `PipelineCanvas`, not by `App` — demonstrating why import-graph evidence outranks filename intuition when auditing the chain `…→ components/…`).

## 6. Classification matrix (frozen boundary)

Legend: every unit gets a classification with justification, dependency blockers, removal preconditions, and rollback path. "Removal" always means a future, separate issue+PR; this document removes nothing.

### 6.1 KEEP — required for the intended product boundary, suitable as-is

| Unit | Justification | Dependency blockers | Preconditions to reclassify | Rollback |
| --- | --- | --- | --- | --- |
| `index.html`, `src/main.tsx`, `src/App.tsx` | Sole working entry/shell; consumed by CI job, Pages deployment, and every documented gate | none | S0 target-boundary decision supersedes them | revert the offending PR; single-commit history keeps this trivial |
| Live subset of components (`Header`, `IconSidebar`, `DatasetPanel`, `PipelineCanvas`, `ObjectPalette`, `DetailPanel`), `icons/hero.tsx`, `icons/CollapseButton.tsx`, `icons/index.ts` (CollapseButton leg), live subsets of `data.ts`/`types.ts`/`index.css` | They render the only product-facing UI today; `AGENTS.md` rule 12 forbids touching layout/components/CSS/tokens outside an explicit UI issue | none | a future UI contract replacing them | revert PR |
| `src/utils/sample-customers.ts` | Runtime fixture powering the only executable demo path | active until the DuckDB execution path is replaced | S4 replacement live in production build | restore from history |
| `package.json` (scripts + toolchain deps), `package-lock.json`, `vite.config.ts`, `tsconfig.json` | Toolchain consumed by CI, Pages, dependabot, `AGENTS.md`, PR template | none | S3/S4 dep edits within their scopes; S5 gate edits | revert PR |
| `ci.yml` frontend job, `deploy.yml`, `dependabot.yml` npm group | Release/publication/update infrastructure for the boundary being kept | none | S5, and only after S0/S3/S4 make a change necessary | revert workflow commit; Pages redeploy is automatic on next main push |
| Architecture statements at `docs/data-ingestion-architecture.md:37,:56,:422,:525` | Accepted Phase-1 positioning of the root frontend; changing them is contract-level work, outside F0-D0's scope | n/a | separate contract amendment referencing #79 | docs revert |

### 6.2 REPLACE-FIRST — obsolete relative to the accepted architecture; removal requires a working replacement first

| Unit | Why obsolete | Dependency blockers | Removal preconditions | Rollback |
| --- | --- | --- | --- | --- |
| Client-side execution role of `src/utils/duckdb.ts` + dependency `@duckdb/duckdb-wasm` (bundled bindings and CDN runtime payload) | `docs/data-ingestion-architecture.md:37` declares this integration a client-side capability, not the authoritative execution engine; cleaning-rule semantics belong to the backend engine (E3 preview runtime direction, E4 storage) | no backend preview/pipeline endpoint consumable by a browser client exists at this base; no auth story, no API contract designed here (explicit non-goals of F0-D0) | (a) backend endpoint live under an accepted contract; (b) frontend cutover merged (slice S4); (c) smoke/e2e green; (d) then remove the dependency and file internals in the same PR series | revert the cutover PR; `^1.33.1-dev57.0` reinstallable; CDN operation independent of repo history |
| Mock domain layer: fabricated dataset catalog and sizes (`data.ts`), hard-coded header labels (`Header.tsx` "Saved 2m ago"/"Published"), toast-only actions (`DetailPanel.tsx` Preview Result / Duplicate / Copy; `DatasetPanel.tsx` "+" connect) | Simulate capabilities that do not exist; `profiling-quality-domain-inventory.md:425–426` already concludes the frontend Quality display is mock/local computation | real dataset registry, preview, and event APIs absent | respective backend features shipped and UI rewired (separate product slices) | revert UI PRs |
| Linear-list pipeline presentation (`PipelineCanvas.tsx` vertical stack; `App.tsx` "Run From Here" assumes order == dataflow via `nodes.slice(0, idx+1)`) | Presents a linear chain where the accepted architecture is a plan DAG; no edges, positions, or branching exist | no UI/graph contract exists; new visual systems are an explicit non-goal here | UI contract issue accepted, then implementation slice | revert |

### 6.3 MIGRATE/ARCHIVE — prototype/demo evidence worth preserving outside the production entry

| Unit | Action | Justification | Preconditions | Rollback |
| --- | --- | --- | --- | --- |
| Prototype-as-shipped state (root Vite app at `9891dcf`) | Formalize with a lightweight annotated git tag (e.g. `prototype/root-frontend-9891dcf`) in slice S2 — no file movement | Preservation is already materially satisfied by complete git history plus the public GitHub Pages deployment record; a tag adds a cheap named anchor without path churn or import-graph disruption | maintainer accepts tag name | delete the tag |
| (deliberately empty otherwise) | No source unit requires migration today | Every `src/` unit is either load-bearing (KEEP) or holds no unrecoverable knowledge (the DELETE-AFTER-PROOF set totals ~47 LOC of trivial helpers/markup already preserved in git history) | — | — |

This row exists so the archival question is answered by evidence rather than silently skipped or blindly applied to everything.

### 6.4 DELETE-AFTER-PROOF — path-specific, consumer-backed; NO automatic deletion

Every row: all preconditions are required; the verification recipe must be re-run at the merge SHA; rollback is `git revert` of the single-purpose PR.

| # | Path / symbol | Consumer-proof (evidence at base) | Removal preconditions | Verification recipe | Rollback |
| --- | --- | --- | --- | --- | --- |
| D1 | `src/components/ActivityPanel.tsx` | 0 importers (madge set + edge audit); repo refs = self + 1 doc prose line | (a) rider edit annotating `docs/issues/profiling-quality-domain-inventory.md:442` in the same PR (doc describes past state); (b) grep-zero re-run; (c) `npm run typecheck` + `npm run build` green; (d) CI frontend job green | `grep -rn ActivityPanel --exclude-dir=node_modules --exclude-dir=dist --exclude-dir=.git` → expect only the annotated doc line | revert restores file |
| D2 | `src/utils/cn.ts` | 0 references anywhere | (a)–(d) as D1; (e) atomic same-PR removal of `clsx` + `tailwind-merge` from `package.json` with regenerated lockfile | `grep -rn "utils/cn"` → 0; `npm ls clsx tailwind-merge` → empty; full gate battery | revert restores file + deps together |
| D3 | `clsx@2.1.1`, `tailwind-merge@3.4.0` manifest entries + lockfile nodes | sole importer is `cn.ts` | same single PR as D2 (never separate — keeps every commit buildable) | `npm ci && npm run build` from clean cache | same revert |
| D4 | `src/icons/SearchIcon.tsx` + barrel line `src/icons/index.ts:2` | only external occurrence is the re-export itself | (a) drop barrel line in same PR; (b) grep-zero; (c) typecheck+build (note: `noUnusedLocals` does NOT catch unused barrel legs — the grep is the operative check); (d) CI | `grep -rn SearchIcon` → 0 | revert |
| D5 | `data.ts` exports `transformToNodeType`, `nodeDefaultName`, `nodeDefaultDescription` | 0 external references | delete the three declaration blocks; strict tsc catches any dangling internal use (none exist) | grep each symbol → defining-file-only; typecheck+build+CI | revert |
| D6 | `types.ts` exports `WorkspaceView`, `PreviewColumn`, `DataPreviewResult`, `PreviewLimit`, `EventLevel` | 0 code references; 1 doc citation (`…:491`) | (a) rider note or PR-body link covering the doc citation; (b)–(d) as D1. These types crossed no boundary (exported only within `src`), so no contract impact is possible | grep each symbol; typecheck+build+CI | revert |
| D7 | `index.css` rules `.node-card`, `.connector-line`, `.connector-arrow`, `.no-scrollbar`, `.animate-progress` (keep `.animate-pulse-dot` — live) | 0 tsx occurrences; present in compiled bundle (hygiene rationale, bytes negligible) | (a) remove rules; (b) rebuilt bundle still contains `.animate-pulse-dot` and all live classes; (c) manual smoke of canvas hover/pulse visuals; (d) CI | grep dist for removed selectors → 0; for `.animate-pulse-dot` → ≥1 | revert |
| D8 | `Header.tsx` `opencode:search-nodes` dispatch block + dead `onSearch`/`error` props | single occurrence; zero listeners | intra-file edit; typing in the header search box must behave identically (it currently affects nothing) | `grep -rn opencode` → 0; typecheck+build+CI; manual smoke | revert |
| D9 | Unused prop declarations listed in Section 5.4 row 8 (and `vite.config.ts`/`tsconfig.json` `@` alias, 0 uses) | declaration-site-only occurrences | optional hygiene riders inside slice S3 PRs touching those exact files; never standalone PRs | grep per symbol; typecheck+build+CI | revert |

### 6.5 UNKNOWN/BLOCKED — insufficient evidence; exact missing evidence stated

| ID | Question | Evidence in hand | Exactly what is missing | Impact if unresolved |
| --- | --- | --- | --- | --- |
| U1 | Does the deployed build execute pipelines successfully in a browser? | Static: cross-origin `new Worker(<jsDelivr URL>)` at `duckdb.ts` vs README-documented `importScripts` blob wrapper (README lines 22–28); HTML spec expectation: `SecurityError`. Repo contains no e2e or manual test that could have caught this | One executed browser session (manual smoke or scripted trace) against `vite preview` or the deployed Pages URL, capturing console output during "Run All" | None on S2/S3 safety; informs S4 priority and whether `deploy.yml` publishes a non-functional prototype (governance follow-up) |
| U2 | Is anyone using the public Pages deployment? | No analytics, access logs referenced, or workflow telemetry exist | Maintainer-provided traffic signal from the `github-pages` environment | Only affects urgency of prototype publication decisions; no technical consequence |
| U3 | Is jsDelivr dependence acceptable (offline/CSP/air-gapped contexts)? | Runtime URLs and payload sizes documented (Section 4.2) | Deployment CSP headers statement and distribution requirements | None on cut order; mooted entirely by S4 |

## 7. Safe cut plan — ordered, independently reviewable and reversible slices

Rules: one slice = one issue = one PR = one revert unit. No slice mixes architectural replacement with bulk deletion. Nothing executes without its own accepted issue; this document is planning evidence only. Slice dependency arrows are one-directional: S0 gates S4 and S5; S3 is independent of S0; S2 is independent; S6 follows S3+S4; S7 is last.

### S0 — Replacement/target boundary prerequisites (docs-only; parallel-safe with everything)

- Paths: `docs/issues/*` only.
- Content: decide and freeze what the root prototype gives way to (retain-root-Vite vs relocate vs rewrite), amending the Phase-1 statement at `docs/data-ingestion-architecture.md:422` through the normal contract process.
- Prerequisites: none. Tests: link and issue-number verification per AGENTS.md. Forbidden concurrent work: none. Stop condition: boundary decision merged; until then S4/S5 must not start (S3 is deliberately independent of S0).

### S2 — Prototype isolation/archival (optional, inert)

- Paths: none (git metadata only): annotated tag `prototype/root-frontend-9891dcf`.
- Prerequisites: none. Tests: tag points at expected SHA. Forbidden concurrent work: none (cannot conflict — touches no path). Stop condition: tag pushed and verifiable.

### S3 — Dead-code and dead-dependency cleanup (first executable slice; smallest blast radius)

- Paths, exact: delete `src/components/ActivityPanel.tsx`, `src/utils/cn.ts`, `src/icons/SearchIcon.tsx`; edit `src/icons/index.ts` (drop `SearchIcon` leg), `src/data.ts` (three exports), `src/types.ts` (five exports), `src/index.css` (five rules), `src/components/Header.tsx` (`opencode` block + two dead props), `package.json` + `package-lock.json` (remove `clsx`, `tailwind-merge` atomically); rider annotations for the two doc mentions (D1a, D6a).
- May be split into several small PRs along the D-row boundaries; every PR stays independently revertible and buildable.
- Prerequisites: none beyond this document. Tests: per-row verification recipes (Section 6.4); `npm ci` (temp-cache caveat applies locally), `npm run typecheck`, `npm run build`; CI `frontend` job green; manual smoke that the app boots and renders identically.
- Forbidden concurrent work: any other PR touching `package.json`/`package-lock.json` (including queued dependabot npm merges — sequence them around S3); any PR editing the listed `src/` files. Backend PRs unaffected (disjointness proof, Section 8).
- Size honesty: expected bundle delta is a few kilobytes (CSS rules, barrel, props); the dependency removal shrinks the install tree, not the bundle. No performance claim is made.
- Stop conditions: any verification recipe returns non-zero unexpectedly → halt and reclassify the unit UNKNOWN/BLOCKED; any gate regression → halt.

### S4 — Execution-path cutover (the only architectural slice; fenced behind S0 + backend reality)

- Paths: internals of `src/utils/duckdb.ts` become a backend API client; `src/App.tsx` orchestration updated; `package.json` + `package-lock.json` lose `@duckdb/duckdb-wasm` only after the last consumer is gone (same PR series, atomic commits); optional rider: `index.html` title alignment.
- Expected effect, stated as a methodology-bound projection: −204,253 B bundled (−42.77%, recipe in Section 5.2) and elimination of the ≈42.2 MB per-cold-load CDN fetch; to be re-measured and posted with the same recipe post-merge.
- Prerequisites: S0 merged; backend preview/pipeline endpoint accepted and deployed; secrets posture honored end-to-end (AGENTS.md rule 10 — `CredentialRef` only; no connection strings in client code).
- Tests: typecheck/build/CI; manual or scripted pipeline run against the staging backend; bundle re-measurement attached to the PR.
- Forbidden concurrent work: dependabot npm merges; other edits to `src/utils/` or `src/App.tsx`; any CI workflow edit (S5 territory).
- Stop conditions: endpoint unavailable or contract changed → halt. No interim state may ship both DuckDB and API paths — single cutover commit; rollback = revert that commit.

### S5 — CI/build gate update (only when S3/S4 outcomes require it)

- Paths: `.github/workflows/ci.yml` (frontend job steps unchanged unless the entry point moves), `.github/workflows/deploy.yml` (publication target decision from S0), `dependabot.yml` only if the root manifest moves.
- Prerequisites: S3 merged; S4 if its outcome affects gates. Tests: the workflows themselves run on the PR; verify a successful Pages deployment afterward. Forbidden concurrent work: any other workflow-editing PR; the backend jobs in `ci.yml` stay untouched. Stop condition: any gate red.

### S6 — Final dead-file sweep

- Re-run BOTH methods (fresh madge pass + repo-wide greps) at the then-head; remove whatever new dead units emerged using fresh D-style rows. Prerequisites: S3+S4 merged. Forbidden concurrent work: feature PRs touching `src/`. Stop condition: any ambiguity → leave the file in place and document it instead.

### S7 — Post-cut regression and rollback checks

- Full battery: typecheck, build, both workflow runs, Pages deployment smoke, a revert rehearsal of each merged cut PR on a throwaway branch, and updated bundle measurements posted to #79. Prerequisites: prior slices merged individually. Stop condition: any regression → execute the rehearsed revert.

## 8. Conflict analysis

Verified open PRs (file lists read via `gh pr view --json files` at claim time):

| PR | Head branch | Touched paths | Conflict with this deliverable |
| --- | --- | --- | --- |
| #53 (E3 preview runtime) | `agent/issue-052-node-preview-runtime` | `backend/crates/stillflow-connector-local-tabular/**`, `backend/crates/stillflow-engine/**`, `docs/development/ai-development-workflow.md`, `docs/issues/issue-050-node-preview-contract.md` | none — disjoint paths |
| #71 (E3 memory law) | `agent/issue-070-e3-memory-law` | `backend/crates/stillflow-engine/src/**` | none |
| #74 (E4-S1 storage) | `agent/issue-073-e4-artifact-verification-bundle-storage` | `backend/Cargo.toml`, `backend/Cargo.lock`, `backend/crates/stillflow-core/**`, `backend/crates/stillflow-storage/**` | none |
| #76 (O0-D0 backend inventory) | `agent/issue-075-backend-optimization-inventory` | `docs/issues/backend-code-simplification-performance-inventory.md` | none — sibling inventory document, different path (and a useful format precedent) |

- This branch adds exactly `docs/issues/frontend-boundary-deprecation-inventory.md` and touches no backend path, no root config, no workflow file. The diff is mechanically disjoint from all four PRs above.
- E-series experimental branches were not used as a base: the branch parent is `main@9891dcf55875bb5e236e3573d17e50fae9caa091` exactly; they remain read-only references.
- Parallelizable with backend work now: S0 (docs), S2 (tag), S3 (`src/**` + brief root-manifest exclusivity) — backend PRs touch `backend/**` exclusively, as proven above, so all can proceed concurrently with #53/#71/#74/#76 review and merge cycles.
- Exclusive write locks requiring serialization:
  - `package.json` + `package-lock.json` — contended by S3, S4, and standing weekly dependabot npm PRs; schedule cut merges in dependabot-free windows or hold dependabot briefly.
  - `.github/workflows/ci.yml`, `.github/workflows/deploy.yml` — S5 only.
  - `index.html`, `src/main.tsx`, `vite.config.ts`, `tsconfig.json` — riders of S4/S5 only; S3 avoids them by design.
  - `docs/issues/frontend-boundary-deprecation-inventory.md` — this task; later amendments via follow-up issues referencing #79.

## 9. Acceptance criteria self-check

| Criterion (from #79) | Evidence |
| --- | --- |
| Exactly one docs file changed | `git show --stat` at delivery lists only `docs/issues/frontend-boundary-deprecation-inventory.md` |
| Every deletion candidate path-specific and consumer-backed | Section 6.4 rows D1–D9 (paths + dual-method evidence + recipes) |
| No placeholder conclusions | Delivery check: case-insensitive search of this document for placeholder markers returns zero hits (recorded in Appendix A) |
| Build/typecheck executable and results recorded | Section 5.1, including the honest environment limitation |
| No automatic deletion; implementation requires separate Issues/PRs | Section 7 rules + per-row preconditions |
| `git diff --check` passes | Run at delivery, recorded in Appendix A |
| PR remains Draft; no merge/Ready action in F0-D0 | Opened as Draft; no Ready/merge action performed |

## Appendix A — delivery-time verification record

Recorded at commit time on the delivery branch (values pasted verbatim into the completion comment on #79):

- `git diff --check` → clean.
- Placeholder-marker scan over the deliverable (the marker set named by the issue's acceptance criteria, case-insensitive) → zero hits.
- Changed-path census → exactly one added file: `docs/issues/frontend-boundary-deprecation-inventory.md`.
- Base discipline: the dispatch stop-condition ("base moved before branch creation") did not trigger — `origin/main` equaled `9891dcf55875bb5e236e3573d17e50fae9caa091` at claim time and at branch creation. During the investigation window `origin/main` advanced to `f16666e59896e2d8bae3b79e188b8f567bb8c534` = merge of PR #74, touching `backend/**` exclusively (`git diff --name-status 9891dcf f16666e` lists only backend paths). This branch was deliberately NOT rebased onto the new tip: it remains parented on the exact contracted base, and its single added file under `docs/issues/` cannot textually conflict with any of those backend paths. Mergeability is reported on the Draft PR.
