import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownAZ,
  ArrowUpAZ,
  ChevronLeft,
  ChevronRight,
  Download,
  Info,
  Search,
  SlidersHorizontal,
  X,
} from "lucide-react";
import { isMissing, numberOf, pctLabel, type ColumnStats, type Row } from "../lib/csv";
import { rejectSummary, type CellChange, type RejectedRow } from "../lib/applyRules";
import { ColumnDetailCard, MiniChart, TypeIcon, ValidityBar, typeLabel } from "./ColumnSummary";

/* Object views decide WHAT the user looks at; display settings only decide
   how the Data view renders. The two concepts stay separate on purpose. */
export type ObjectView = "data" | "changes" | "rejected" | "profile" | "schema" | "quality";
type Density = "compact" | "detailed";
type Sort = { col: string; dir: "asc" | "desc" } | null;

const WIDE = new Set(["title", "subtitle", "ref", "tags", "licenseName", "creatorName"]);

/** All object views; Changes / Rejected are contextual (node scope only). */
export const OBJECT_VIEWS: [ObjectView, string][] = [
  ["data", "Data"],
  ["changes", "Changes"],
  ["rejected", "Rejected"],
  ["profile", "Profile"],
  ["schema", "Schema"],
  ["quality", "Quality"],
];

function fmt(v: string, s: ColumnStats) {
  if (isMissing(v)) return "";
  if ((s.type === "integer" || s.type === "decimal") && /^-?\d*\.?\d+$/.test(v)) {
    const n = Number(v);
    return s.type === "decimal" ? v : n.toLocaleString("en-US");
  }
  return v;
}

export function DataTable({
  columns,
  rows,
  stats,
  focusColumn,
  onFocusColumn,
  onDownload,
  changes,
  rejected,
  nodeName,
  objectView,
  onObjectViewChange,
  statsText,
}: {
  columns: string[];
  rows: Row[];
  stats: ColumnStats[];
  focusColumn?: string | null;
  onFocusColumn?: (column: string | null) => void;
  onDownload: () => void;
  /** Cell-level diffs produced by the selected node (sample). */
  changes?: CellChange[] | null;
  /** Rows removed by the selected node, with reasons (sample). */
  rejected?: RejectedRow[] | null;
  /** Name of the node the Changes / Rejected views describe. */
  nodeName?: string | null;
  /** Controlled object view — the tab strip lives in the preview header. */
  objectView: ObjectView;
  onObjectViewChange: (view: ObjectView) => void;
  /** Stage facts shown at the right of the toolbar (sample size, columns). */
  statsText?: string;
}) {
  const [density, setDensity] = useState<Density>("compact");
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<Sort>(null);
  const [page, setPage] = useState(0);
  const [perPage, setPerPage] = useState(25);
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  const [showCols, setShowCols] = useState(false);
  const [showDisplay, setShowDisplay] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const statMap = useMemo(() => Object.fromEntries(stats.map((s) => [s.name, s])), [stats]);

  const visible = columns.filter((c) => !hidden.has(c));

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => visible.some((c) => (r[c] ?? "").toLowerCase().includes(q)));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows, query, hidden]);

  const sorted = useMemo(() => {
    if (!sort) return filtered;
    const s = statMap[sort.col];
    const num = s && (s.type === "integer" || s.type === "decimal" || s.type === "date");
    const out = [...filtered].sort((a, b) => {
      const av = a[sort.col] ?? "";
      const bv = b[sort.col] ?? "";
      if (isMissing(av)) return 1;
      if (isMissing(bv)) return -1;
      const r = num ? numberOf(av, s.type) - numberOf(bv, s.type) : av.localeCompare(bv);
      return sort.dir === "asc" ? r : -r;
    });
    return out;
  }, [filtered, sort, statMap]);

  const pages = Math.max(1, Math.ceil(sorted.length / perPage));
  const clamped = Math.min(page, pages - 1);
  const slice = sorted.slice(clamped * perPage, clamped * perPage + perPage);

  useEffect(() => setPage(0), [query, perPage]);

  /* A focused field is a secondary focus: it scrolls into view and gets a
     quiet neutral highlight — never the primary selection blue. */
  useEffect(() => {
    if (!focusColumn || objectView !== "data") return;
    const el = scrollRef.current?.querySelector<HTMLElement>(`[data-col="${CSS.escape(focusColumn)}"]`);
    el?.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
  }, [focusColumn, objectView]);

  const toggleSort = (c: string) =>
    setSort((s) => (s?.col !== c ? { col: c, dir: "asc" } : s.dir === "asc" ? { col: c, dir: "desc" } : null));

  const focusInData = (column: string) => {
    onFocusColumn?.(column);
    onObjectViewChange("data");
  };

  return (
    <section className="flex h-full min-h-0 flex-col overflow-hidden bg-white">
      {/* ---------------------------- toolbar ---------------------------- */}
      <div className="flex flex-shrink-0 flex-wrap items-center gap-1.5 border-b border-[#edf2f6] px-3 py-1.5">
        {objectView === "data" && (
          <div className="relative">
            <Search className="pointer-events-none absolute top-1/2 left-2.5 h-3.5 w-3.5 -translate-y-1/2 text-[#9099a4]" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search this file..."
              className="h-7 w-[170px] rounded-md border border-[#dce2e8] pr-7 pl-8 text-[11.5px] text-[#171a1f] outline-none placeholder:text-[#9099a4] focus:border-[#2196d2] focus:ring-2 focus:ring-[rgba(33,150,210,.18)]"
            />
            {query && (
              <button
                onClick={() => setQuery("")}
                className="absolute top-1/2 right-2 -translate-y-1/2 text-[#9099a4] hover:text-[#171a1f]"
              >
                <X className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        )}

        <div className="relative">
          <button
            onClick={() => setShowCols((v) => !v)}
            title="Choose which columns are visible"
            className="flex h-7 items-center gap-1.5 rounded-md border border-[#dce2e8] px-2.5 text-[11.5px] text-[#39434e] transition-colors hover:bg-[#edf2f6]"
          >
            <SlidersHorizontal className="h-3.5 w-3.5" />
            {visible.length} of {columns.length} columns
          </button>
          {showCols && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setShowCols(false)} />
              <div className="kg-fade absolute left-0 z-50 mt-1 max-h-[320px] w-[240px] overflow-y-auto rounded-lg border border-[#dce2e8] bg-white py-1.5 shadow-[0_2px_8px_rgba(22,32,44,.08)]">
                {columns.map((c) => (
                  <label
                    key={c}
                    className="flex cursor-pointer items-center gap-2 px-3 py-1.5 text-[12.5px] text-[#39434e] hover:bg-[#edf2f6]"
                  >
                    <input
                      type="checkbox"
                      checked={!hidden.has(c)}
                      onChange={() =>
                        setHidden((h) => {
                          const n = new Set(h);
                          if (n.has(c)) n.delete(c);
                          else n.add(c);
                          return n.size === columns.length ? h : n;
                        })
                      }
                      className="accent-[#2196d2]"
                    />
                    <TypeIcon type={statMap[c].type} />
                    <span className="truncate">{c}</span>
                  </label>
                ))}
              </div>
            </>
          )}
        </div>

        {objectView === "data" && (
          <div className="relative">
            <button
              onClick={() => setShowDisplay((v) => !v)}
              title="Header display settings"
              className="flex h-7 items-center gap-1.5 rounded-md border border-[#dce2e8] px-2.5 text-[11.5px] text-[#39434e] transition-colors hover:bg-[#edf2f6]"
            >
              Display
              <span className="text-[#9099a4]">{density === "detailed" ? "Detailed" : "Compact"}</span>
            </button>
            {showDisplay && (
              <>
                <div className="fixed inset-0 z-40" onClick={() => setShowDisplay(false)} />
                <div className="kg-fade absolute left-0 z-50 mt-1 w-[220px] rounded-lg border border-[#dce2e8] bg-white py-1 shadow-[0_2px_8px_rgba(22,32,44,.08)]">
                  {(
                    [
                      ["compact", "Compact headers", "Name, type and one-line summary"],
                      ["detailed", "Detailed headers", "Completeness bar and distribution"],
                    ] as [Density, string, string][]
                  ).map(([value, label, hint]) => (
                    <button
                      key={value}
                      onClick={() => {
                        setDensity(value);
                        setShowDisplay(false);
                      }}
                      className={`flex w-full flex-col gap-0.5 px-3 py-2 text-left transition-colors hover:bg-[#edf2f6] ${
                        density === value ? "bg-[#f4f6f8]" : ""
                      }`}
                    >
                      <span className="text-[12px] font-medium text-[#171a1f]">{label}</span>
                      <span className="text-[11px] text-[#9099a4]">{hint}</span>
                    </button>
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        <div className="ml-auto" />

        {statsText && (
          <span className="hidden shrink-0 text-[11px] text-[#9099a4] lg:inline">
            {statsText}
          </span>
        )}

        <button
          onClick={onDownload}
          title="Export the current rows as CSV"
          className="flex h-7 items-center gap-1.5 rounded-md border border-[#dce2e8] px-2.5 text-[11.5px] text-[#39434e] transition-colors hover:bg-[#edf2f6]"
        >
          <Download className="h-3 w-3" />
          Export
        </button>
      </div>

      {/* --------------------------- content ----------------------------- */}
      {objectView === "changes" ? (
        <ChangesView changes={changes ?? null} nodeName={nodeName ?? null} />
      ) : objectView === "rejected" ? (
        <RejectedView rejected={rejected ?? null} nodeName={nodeName ?? null} columns={visible} />
      ) : objectView === "profile" ? (
        <div className="kg-scroll grid min-h-0 flex-1 grid-cols-1 content-start gap-2 overflow-y-auto p-3 md:grid-cols-2 xl:grid-cols-3">
          {stats
            .filter((s) => !hidden.has(s.name))
            .map((s) => (
              <button
                key={s.name}
                onClick={() => focusInData(s.name)}
                title={`Focus ${s.name} in the Data view`}
                className="rounded-md border border-[#dce2e8] p-3 text-left transition-colors hover:border-[#c9d1d9]"
              >
                <div className="flex items-center gap-1.5">
                  <TypeIcon type={s.type} />
                  <span className="truncate text-[12.5px] font-semibold text-[#171a1f]">{s.name}</span>
                  <span className="ml-auto rounded bg-[#f4f6f8] px-1.5 py-0.5 text-[10px] text-[#5e6874]">
                    {typeLabel(s.type)}
                  </span>
                </div>
                <div className="mt-2">
                  <ValidityBar s={s} />
                </div>
                <div className="mt-2">
                  <MiniChart s={s} />
                </div>
                <div className="mt-2 flex justify-between border-t border-[#edf2f6] pt-2 font-mono text-[10.5px] text-[#5e6874]">
                  <span className="text-[#171a1f]">valid {pctLabel((s.valid / s.total) * 100)}</span>
                  <span>mismatch {pctLabel((s.mismatched / s.total) * 100)}</span>
                  <span>missing {pctLabel((s.missing / s.total) * 100)}</span>
                  <span>{s.unique.toLocaleString()} uniq</span>
                </div>
              </button>
            ))}
        </div>
      ) : objectView === "schema" ? (
        <div className="kg-scroll min-h-0 flex-1 overflow-auto">
          <table className="w-full border-collapse text-[12px]">
            <thead className="sticky top-0 z-10 bg-white">
              <tr>
                {["Field", "Type", "Completeness", "Missing", "Unique"].map((h, i) => (
                  <th
                    key={h}
                    className={`border-b border-[#dce2e8] px-3 py-2 text-left text-[10.5px] font-semibold text-[#5e6874] ${
                      i >= 3 ? "text-right" : ""
                    }`}
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stats
                .filter((s) => !hidden.has(s.name))
                .map((s) => {
                  const complete = (s.valid / Math.max(1, s.total)) * 100;
                  const active = focusColumn === s.name;
                  return (
                    <tr
                      key={s.name}
                      onClick={() => focusInData(s.name)}
                      title={`Focus ${s.name} in the Data view`}
                      className={`cursor-pointer transition-colors ${
                        active ? "bg-[#f4f6f8]" : "hover:bg-[#f8fafb]"
                      }`}
                    >
                      <td className="border-b border-[#edf2f6] px-3 py-[7px]">
                        <span className="flex items-center gap-1.5">
                          <TypeIcon type={s.type} />
                          <span className="font-medium text-[#171a1f]">{s.name}</span>
                        </span>
                      </td>
                      <td className="border-b border-[#edf2f6] px-3 py-[7px] text-[#5e6874]">
                        {typeLabel(s.type)}
                      </td>
                      <td className="w-[220px] border-b border-[#edf2f6] px-3 py-[7px]">
                        <span className="flex items-center gap-2">
                          <span className="w-[120px]">
                            <ValidityBar s={s} />
                          </span>
                          <span className="font-mono text-[11px] text-[#5e6874]">{pctLabel(complete)}</span>
                        </span>
                      </td>
                      <td className="border-b border-[#edf2f6] px-3 py-[7px] text-right font-mono text-[11px] text-[#5e6874]">
                        {s.missing.toLocaleString()}
                      </td>
                      <td className="border-b border-[#edf2f6] px-3 py-[7px] text-right font-mono text-[11px] text-[#5e6874]">
                        {s.unique.toLocaleString()}
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      ) : objectView === "quality" ? (
        <QualityView stats={stats} onFocusColumn={focusInData} />
      ) : (
        /* --------------------------- data grid -------------------------- */
        <div ref={scrollRef} className="kg-scroll min-h-0 flex-1 overflow-auto">
          <table className="w-full table-fixed border-collapse text-[12px]">
            <colgroup>
              {visible.map((c) => (
                <col key={c} style={{ width: WIDE.has(c) ? 260 : 148 }} />
              ))}
            </colgroup>
            <thead className="sticky top-0 z-20 bg-white">
              <tr>
                {visible.map((c) => {
                  const s = statMap[c];
                  const active = sort?.col === c;
                  const focused = focusColumn === c;
                  return (
                    <th
                      key={c}
                      data-col={c}
                      className={`border-r border-b border-[#dce2e8] p-0 align-top last:border-r-0 ${
                        focused ? "bg-[#f4f6f8] shadow-[inset_0_-2px_0_#9099a4]" : "bg-white"
                      }`}
                    >
                      <div className="relative px-2.5 pt-1.5 pb-1.5 text-left">
                        <div className="flex items-center gap-1">
                          <TypeIcon type={s.type} className={focused ? "text-[#39434e]" : ""} />
                          <button
                            onClick={() => toggleSort(c)}
                            title={`Sort by ${c}`}
                            className="min-w-0 flex-1 truncate text-left text-[12.5px] font-semibold text-[#171a1f] hover:text-[#1686be]"
                          >
                            {c}
                          </button>
                          {active &&
                            (sort.dir === "asc" ? (
                              <ArrowUpAZ className="h-3.5 w-3.5 text-[#1686be]" />
                            ) : (
                              <ArrowDownAZ className="h-3.5 w-3.5 text-[#1686be]" />
                            ))}
                          {focused && (
                            <button
                              onClick={() => onFocusColumn?.(null)}
                              className="rounded p-0.5 text-[#9099a4] hover:bg-[#edf2f6] hover:text-[#171a1f]"
                              title="Clear field focus"
                            >
                              <X className="h-3 w-3" />
                            </button>
                          )}
                          <button
                            onClick={() => setOpenMenu(openMenu === c ? null : c)}
                            className="rounded p-0.5 text-[#9099a4] hover:bg-[#edf2f6] hover:text-[#171a1f]"
                            title="Column details"
                          >
                            <Info className="h-3.5 w-3.5" />
                          </button>
                        </div>

                        {density === "detailed" ? (
                          <div className="mt-1">
                            <ValidityBar s={s} />
                            <div className="mt-1">
                              <MiniChart s={s} />
                            </div>
                          </div>
                        ) : (
                          <div className="mt-0.5 truncate font-mono text-[10px] text-[#9099a4]">
                            {pctLabel((s.valid / Math.max(1, s.total)) * 100)} complete ·{" "}
                            {s.unique.toLocaleString()} unique
                          </div>
                        )}

                        {openMenu === c && (
                          <>
                            <div className="fixed inset-0 z-40" onClick={() => setOpenMenu(null)} />
                            <ColumnDetailCard s={s} />
                          </>
                        )}
                      </div>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {slice.map((r, i) => (
                <tr key={i} className="group hover:bg-[#f8fafb]">
                  {visible.map((c) => {
                    const s = statMap[c];
                    const raw = r[c] ?? "";
                    const num = s.type === "integer" || s.type === "decimal";
                    return (
                      <td
                        key={c}
                        title={raw}
                        className={`truncate border-r border-b border-[#edf2f6] px-2.5 py-[6px] last:border-r-0 ${
                          num || s.type === "date" ? "font-mono text-[12px]" : ""
                        } ${isMissing(raw) ? "bg-[#f8fafb]" : ""} ${focusColumn === c ? "bg-[#f4f6f8]" : ""}`}
                      >
                        {isMissing(raw) ? (
                          <span className="text-[11px] text-[#c9d1d9] italic">null</span>
                        ) : (
                          fmt(raw, s)
                        )}
                      </td>
                    );
                  })}
                </tr>
              ))}
              {!slice.length && (
                <tr>
                  <td colSpan={visible.length} className="py-16 text-center text-[13px] text-[#5e6874]">
                    No rows match “{query}”.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* --------------------------- pagination -------------------------- */}
      {objectView === "data" && (
        <div className="flex flex-shrink-0 flex-wrap items-center gap-3 border-t border-[#edf2f6] px-3 py-2 text-[12px] text-[#5e6874]">
          <span>
            Showing{" "}
            <b className="text-[#171a1f]">
              {sorted.length ? clamped * perPage + 1 : 0}–{Math.min(sorted.length, (clamped + 1) * perPage)}
            </b>{" "}
            of <b className="text-[#171a1f]">{sorted.length.toLocaleString()}</b> rows
            {query && <span className="text-[#9099a4]"> (filtered from {rows.length.toLocaleString()})</span>}
          </span>
          <label className="ml-auto flex items-center gap-1.5">
            Rows
            <select
              value={perPage}
              onChange={(e) => setPerPage(Number(e.target.value))}
              className="h-7 rounded-md border border-[#dce2e8] bg-white px-1.5 text-[12px] text-[#171a1f] outline-none focus:border-[#2196d2]"
            >
              {[10, 25, 50, 100].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </select>
          </label>
          <div className="flex items-center gap-0.5">
            <button
              disabled={clamped === 0}
              onClick={() => setPage(clamped - 1)}
              className="grid h-7 w-7 place-items-center rounded-md text-[#5e6874] hover:bg-[#edf2f6] disabled:opacity-35"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
            <span className="tabular">
              Page <b className="text-[#171a1f]">{clamped + 1}</b> / {pages}
            </span>
            <button
              disabled={clamped >= pages - 1}
              onClick={() => setPage(clamped + 1)}
              className="grid h-7 w-7 place-items-center rounded-md text-[#5e6874] hover:bg-[#edf2f6] disabled:opacity-35"
            >
              <ChevronRight className="h-4 w-4" />
            </button>
          </div>
        </div>
      )}
    </section>
  );
}

/* ---------------------------- Changes view --------------------------- */

function EmptyNodeHint({ nodeName, verb }: { nodeName: string | null; verb: string }) {
  return (
    <div className="flex h-full flex-col items-center justify-center gap-1.5 p-8 text-center">
      {nodeName ? (
        <>
          <span className="text-[13px] text-[#5e6874]">Not evaluable on this sample</span>
          <span className="text-[12px] text-[#9099a4]">
            {nodeName} references columns outside the displayed sample — run the
            pipeline to see the full-data impact.
          </span>
        </>
      ) : (
        <>
          <span className="text-[13px] text-[#5e6874]">No transformation selected</span>
          <span className="text-[12px] text-[#9099a4]">
            Select a node on the canvas to {verb}.
          </span>
        </>
      )}
    </div>
  );
}

function ChangesView({
  changes,
  nodeName,
}: {
  changes: CellChange[] | null;
  nodeName: string | null;
}) {
  if (!changes) return <EmptyNodeHint nodeName={nodeName} verb="see what it changes" />;
  if (changes.length === 0) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-1.5 p-8 text-center">
        <span className="text-[13px] text-[#5e6874]">No cell changes</span>
        <span className="text-[12px] text-[#9099a4]">
          {nodeName ?? "This node"} does not modify cell values in the current sample.
        </span>
      </div>
    );
  }
  const shown = changes.slice(0, 200);
  return (
    <div className="kg-scroll min-h-0 flex-1 overflow-auto">
      <div className="border-b border-[#edf2f6] px-3 py-2 text-[12px] text-[#5e6874]">
        <b className="text-[#171a1f]">{changes.length.toLocaleString()}</b> cells modified by{" "}
        <b className="text-[#171a1f]">{nodeName}</b> in the sample
        {changes.length > shown.length && ` · showing first ${shown.length}`}
      </div>
      <table className="w-full border-collapse text-[12px]">
        <thead className="sticky top-0 z-10 bg-white">
          <tr>
            {["Row", "Column", "Before", "After"].map((h) => (
              <th
                key={h}
                className="border-b border-[#dce2e8] px-3 py-2 text-left text-[10.5px] font-semibold text-[#5e6874]"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((change, index) => (
            <tr key={index} className="hover:bg-[#f8fafb]">
              <td className="w-[80px] border-b border-[#edf2f6] px-3 py-[6px] font-mono text-[11px] text-[#9099a4]">
                #{change.rowIndex + 1}
              </td>
              <td className="w-[180px] border-b border-[#edf2f6] px-3 py-[6px] font-medium text-[#171a1f]">
                {change.column}
              </td>
              <td className="border-b border-[#edf2f6] px-3 py-[6px] font-mono text-[11px] text-[#c95e62]">
                {change.from === "" ? <span className="italic text-[#c9d1d9]">empty</span> : change.from}
              </td>
              <td className="border-b border-[#edf2f6] px-3 py-[6px] font-mono text-[11px] text-[#4ba66a]">
                {change.to === "" ? <span className="italic text-[#c9d1d9]">empty</span> : change.to}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* --------------------------- Rejected view --------------------------- */

function RejectedView({
  rejected,
  nodeName,
  columns,
}: {
  rejected: RejectedRow[] | null;
  nodeName: string | null;
  columns: string[];
}) {
  if (!rejected) return <EmptyNodeHint nodeName={nodeName} verb="see which rows it removes" />;
  const summary = rejected.length > 0 ? rejectSummary({ rejected }) : [];
  const shown = rejected.slice(0, 200);
  const dataCols = columns.slice(0, 3);

  if (rejected.length === 0) {
    return (
      <div className="flex h-full flex-col items-center justify-center gap-1.5 p-8 text-center">
        <span className="text-[13px] text-[#5e6874]">No rejected rows</span>
        <span className="text-[12px] text-[#9099a4]">
          {nodeName ?? "This node"} keeps every row in the current sample.
        </span>
      </div>
    );
  }

  return (
    <div className="kg-scroll min-h-0 flex-1 overflow-auto">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-[#edf2f6] px-3 py-2 text-[12px] text-[#5e6874]">
        <span>
          <b className="text-[#171a1f]">{rejected.length.toLocaleString()}</b> rows removed by{" "}
          <b className="text-[#171a1f]">{nodeName}</b> in the sample
        </span>
        {summary.map(([reason, count]) => (
          <span key={reason} className="rounded bg-[#f4f6f8] px-1.5 py-0.5 text-[11px]">
            {count.toLocaleString()} · {reason}
          </span>
        ))}
      </div>
      <table className="w-full border-collapse text-[12px]">
        <thead className="sticky top-0 z-10 bg-white">
          <tr>
            {["Row", "Reason", ...dataCols].map((h) => (
              <th
                key={h}
                className="border-b border-[#dce2e8] px-3 py-2 text-left text-[10.5px] font-semibold text-[#5e6874]"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((rejectedRow, index) => (
            <tr key={index} className="hover:bg-[#f8fafb]">
              <td className="w-[80px] border-b border-[#edf2f6] px-3 py-[6px] font-mono text-[11px] text-[#9099a4]">
                #{rejectedRow.rowIndex + 1}
              </td>
              <td className="w-[240px] border-b border-[#edf2f6] px-3 py-[6px] text-[#c95e62]">
                {rejectedRow.reason}
              </td>
              {dataCols.map((column) => (
                <td
                  key={column}
                  className="truncate border-b border-[#edf2f6] px-3 py-[6px] font-mono text-[11px] text-[#5e6874]"
                  title={rejectedRow.row[column] ?? ""}
                >
                  {isMissing(rejectedRow.row[column] ?? "") ? (
                    <span className="italic text-[#c9d1d9]">null</span>
                  ) : (
                    rejectedRow.row[column]
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ---------------------------- Quality view ---------------------------- */

function QualityView({
  stats,
  onFocusColumn,
}: {
  stats: ColumnStats[];
  onFocusColumn: (column: string) => void;
}) {
  const total = stats[0]?.total ?? 0;
  const cells = total * Math.max(1, stats.length);
  const missingCells = stats.reduce((sum, s) => sum + s.missing + s.mismatched, 0);
  const completeness = cells > 0 ? ((cells - missingCells) / cells) * 100 : 100;
  const attention = stats
    .map((s) => ({
      stat: s,
      issuePct: ((s.missing + s.mismatched) / Math.max(1, s.total)) * 100,
    }))
    .filter((entry) => entry.issuePct > 0)
    .sort((a, b) => b.issuePct - a.issuePct)
    .slice(0, 6);

  return (
    <div className="kg-scroll min-h-0 flex-1 overflow-y-auto p-3">
      <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
        {[
          ["Rows sampled", total.toLocaleString()],
          ["Columns", String(stats.length)],
          ["Avg completeness", pctLabel(completeness)],
          ["Cells with issues", missingCells.toLocaleString()],
        ].map(([label, value]) => (
          <div key={label} className="rounded-md border border-[#dce2e8] px-3 py-2.5">
            <div className="text-[10.5px] font-semibold text-[#5e6874]">{label}</div>
            <div className="mt-0.5 text-[15px] font-semibold text-[#171a1f] tabular">{value}</div>
          </div>
        ))}
      </div>

      <h3 className="mt-4 mb-2 text-[10.5px] font-semibold text-[#5e6874]">
        Columns needing attention
      </h3>
      {attention.length === 0 ? (
        <p className="rounded-md border border-[#dce2e8] px-3 py-4 text-[12px] text-[#5e6874]">
          No missing or mismatched values detected in this sample.
        </p>
      ) : (
        <div className="space-y-1.5">
          {attention.map(({ stat, issuePct }) => (
            <button
              key={stat.name}
              onClick={() => onFocusColumn(stat.name)}
              title={`Focus ${stat.name} in the Data view`}
              className="flex w-full items-center gap-2.5 rounded-md border border-[#dce2e8] px-3 py-2 text-left transition-colors hover:border-[#c9d1d9]"
            >
              <TypeIcon type={stat.type} />
              <span className="w-[160px] truncate text-[12.5px] font-medium text-[#171a1f]">
                {stat.name}
              </span>
              <span className="min-w-0 flex-1">
                <ValidityBar s={stat} />
              </span>
              <span className="shrink-0 font-mono text-[11px] text-[#5e6874]">
                {stat.missing > 0 && `${pctLabel((stat.missing / stat.total) * 100)} missing`}
                {stat.missing > 0 && stat.mismatched > 0 && " · "}
                {stat.mismatched > 0 && `${pctLabel((stat.mismatched / stat.total) * 100)} mismatch`}
                {issuePct > 0 && issuePct < 1 ? "" : ""}
              </span>
            </button>
          ))}
        </div>
      )}
      <p className="mt-3 text-[11px] text-[#9099a4]">
        Quality signals are computed over the current sample. Rule-based checks and
        row-level issue tracking extend this view next.
      </p>
    </div>
  );
}
