import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownAZ,
  ArrowUpAZ,
  ChevronLeft,
  ChevronRight,
  Columns3,
  Download,
  Expand,
  Info,
  LayoutGrid,
  Rows3,
  Search,
  SlidersHorizontal,
  X,
} from "lucide-react";
import { isMissing, numberOf, pctLabel, type ColumnStats, type Row } from "../lib/csv";
import { ColumnDetailCard, MiniChart, TypeIcon, ValidityBar, typeLabel } from "./ColumnSummary";

type View = "detail" | "compact" | "column";
type Sort = { col: string; dir: "asc" | "desc" } | null;

const WIDE = new Set(["title", "subtitle", "ref", "tags", "licenseName", "creatorName"]);

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
  fileName,
  sizeLabel,
  focusColumn,
  onDownload,
}: {
  columns: string[];
  rows: Row[];
  stats: ColumnStats[];
  fileName: string;
  sizeLabel: string;
  focusColumn?: string | null;
  onDownload: () => void;
}) {
  const [view, setView] = useState<View>("detail");
  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<Sort>(null);
  const [page, setPage] = useState(0);
  const [perPage, setPerPage] = useState(25);
  const [hidden, setHidden] = useState<Set<string>>(new Set());
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  const [showCols, setShowCols] = useState(false);
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

  useEffect(() => {
    if (!focusColumn || view === "column") return;
    const el = scrollRef.current?.querySelector<HTMLElement>(`[data-col="${CSS.escape(focusColumn)}"]`);
    el?.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
  }, [focusColumn, view]);

  const toggleSort = (c: string) =>
    setSort((s) => (s?.col !== c ? { col: c, dir: "asc" } : s.dir === "asc" ? { col: c, dir: "desc" } : null));

  return (
    <section className="overflow-hidden rounded-xl border border-[#e3e6e8] bg-white">
      {/* ---------------------------- toolbar ---------------------------- */}
      <div className="flex flex-wrap items-center gap-2 border-b border-[#e3e6e8] px-4 py-2.5">
        <div className="mr-1 flex min-w-0 items-center gap-2">
          <span className="truncate text-[15px] font-semibold text-[#202124]">{fileName}</span>
          <span className="shrink-0 text-[13px] text-[#5f6368]">({sizeLabel})</span>
        </div>

        <div className="relative ml-auto">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 h-3.5 w-3.5 -translate-y-1/2 text-[#5f6368]" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search this file..."
            className="h-8 w-[190px] rounded-full border border-[#dadce0] pr-7 pl-8 text-[13px] outline-none focus:border-[#18181b] focus:ring-1 focus:ring-[#18181b]"
          />
          {query && (
            <button
              onClick={() => setQuery("")}
              className="absolute top-1/2 right-2 -translate-y-1/2 text-[#5f6368] hover:text-[#202124]"
            >
              <X className="h-3.5 w-3.5" />
            </button>
          )}
        </div>

        <div className="relative">
          <button
            onClick={() => setShowCols((v) => !v)}
            className="flex h-8 items-center gap-1.5 rounded-full border border-[#dadce0] px-3 text-[13px] text-[#3c4043] hover:bg-[#f1f3f4]"
          >
            <SlidersHorizontal className="h-3.5 w-3.5" />
            {visible.length} of {columns.length} columns
          </button>
          {showCols && (
            <>
              <div className="fixed inset-0 z-40" onClick={() => setShowCols(false)} />
              <div className="kg-fade absolute right-0 z-50 mt-1 max-h-[320px] w-[240px] overflow-y-auto rounded-lg border border-[#e3e6e8] bg-white py-1.5 shadow-[0_8px_24px_rgba(32,33,36,.16)]">
                {columns.map((c) => (
                  <label
                    key={c}
                    className="flex cursor-pointer items-center gap-2 px-3 py-1.5 text-[13px] hover:bg-[#f1f3f4]"
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
                      className="accent-[#18181b]"
                    />
                    <TypeIcon type={statMap[c].type} />
                    <span className="truncate">{c}</span>
                  </label>
                ))}
              </div>
            </>
          )}
        </div>

        <div className="flex h-8 items-center rounded-full border border-[#dadce0] p-0.5">
          {(
            [
              ["detail", Rows3, "Detail"],
              ["compact", LayoutGrid, "Compact"],
              ["column", Columns3, "Column"],
            ] as [View, typeof Rows3, string][]
          ).map(([v, Icon, label]) => (
            <button
              key={v}
              onClick={() => setView(v)}
              title={`${label} view`}
              className={`flex h-7 items-center gap-1.5 rounded-full px-2.5 text-[12.5px] transition ${
                view === v ? "bg-[#18181b] font-medium text-white" : "text-[#5f6368] hover:bg-[#f1f3f4]"
              }`}
            >
              <Icon className="h-3.5 w-3.5" />
              <span className="hidden sm:inline">{label}</span>
            </button>
          ))}
        </div>

        <button
          onClick={onDownload}
          title="Download filtered CSV"
          className="grid h-8 w-8 place-items-center rounded-full border border-[#dadce0] text-[#3c4043] hover:bg-[#f1f3f4]"
        >
          <Download className="h-3.5 w-3.5" />
        </button>
        <button
          title="Fullscreen"
          className="grid h-8 w-8 place-items-center rounded-full border border-[#dadce0] text-[#3c4043] hover:bg-[#f1f3f4]"
        >
          <Expand className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* ------------------------- column view --------------------------- */}
      {view === "column" ? (
        <div className="grid max-h-[640px] grid-cols-1 gap-3 overflow-y-auto p-4 md:grid-cols-2 xl:grid-cols-3">
          {stats
            .filter((s) => !hidden.has(s.name))
            .map((s) => (
              <div key={s.name} className="rounded-lg border border-[#e3e6e8] p-3 hover:shadow-sm">
                <div className="flex items-center gap-1.5">
                  <TypeIcon type={s.type} />
                  <span className="truncate text-[13px] font-semibold text-[#202124]">{s.name}</span>
                  <span className="ml-auto rounded bg-[#f1f3f4] px-1.5 py-0.5 text-[10px] text-[#5f6368]">
                    {typeLabel(s.type)}
                  </span>
                </div>
                <div className="mt-2">
                  <ValidityBar s={s} />
                </div>
                <div className="mt-2">
                  <MiniChart s={s} />
                </div>
                <div className="mt-2 flex justify-between border-t border-[#f1f3f4] pt-2 font-mono text-[10.5px] text-[#5f6368]">
                  <span className="text-[#18181b]">valid {pctLabel((s.valid / s.total) * 100)}</span>
                  <span className="text-[#71717a]">mismatch {pctLabel((s.mismatched / s.total) * 100)}</span>
                  <span>missing {pctLabel((s.missing / s.total) * 100)}</span>
                  <span>{s.unique.toLocaleString()} uniq</span>
                </div>
              </div>
            ))}
        </div>
      ) : (
        /* --------------------------- data grid -------------------------- */
        <div ref={scrollRef} className="kg-scroll max-h-[640px] overflow-auto">
          <table className="w-full table-fixed border-collapse text-[13px]">
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
                  return (
                    <th
                      key={c}
                      data-col={c}
                      className={`border-r border-b border-[#e3e6e8] bg-white p-0 align-top last:border-r-0 ${
                        focusColumn === c ? "bg-[#fafafa]" : ""
                      }`}
                    >
                      <div className="relative px-2.5 pt-2 pb-2 text-left">
                        <div className="flex items-center gap-1">
                          <TypeIcon type={s.type} />
                          <button
                            onClick={() => toggleSort(c)}
                            title={`Sort by ${c}`}
                            className="min-w-0 flex-1 truncate text-left text-[12.5px] font-semibold text-[#202124] hover:text-[#18181b]"
                          >
                            {c}
                          </button>
                          {active &&
                            (sort.dir === "asc" ? (
                              <ArrowUpAZ className="h-3.5 w-3.5 text-[#18181b]" />
                            ) : (
                              <ArrowDownAZ className="h-3.5 w-3.5 text-[#18181b]" />
                            ))}
                          <button
                            onClick={() => setOpenMenu(openMenu === c ? null : c)}
                            className="rounded p-0.5 text-[#80868b] hover:bg-[#f1f3f4] hover:text-[#202124]"
                            title="Column details"
                          >
                            <Info className="h-3.5 w-3.5" />
                          </button>
                        </div>

                        {view === "detail" && (
                          <div className="mt-1.5">
                            <ValidityBar s={s} />
                            <div className="mt-1.5">
                              <MiniChart s={s} />
                            </div>
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
                <tr key={i} className="group hover:bg-[#fafafa]">
                  {visible.map((c) => {
                    const s = statMap[c];
                    const raw = r[c] ?? "";
                    const num = s.type === "integer" || s.type === "decimal";
                    return (
                      <td
                        key={c}
                        title={raw}
                        className={`truncate border-r border-b border-[#eceff1] px-2.5 py-[7px] last:border-r-0 ${
                          num || s.type === "date" ? "font-mono text-[12px]" : ""
                        } ${isMissing(raw) ? "bg-[#fafafa]" : ""} ${focusColumn === c ? "bg-[#f0fbff]" : ""}`}
                      >
                        {isMissing(raw) ? (
                          <span className="text-[11px] text-[#bdc1c6] italic">null</span>
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
                  <td colSpan={visible.length} className="py-16 text-center text-[13px] text-[#5f6368]">
                    No rows match “{query}”.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* --------------------------- pagination -------------------------- */}
      <div className="flex flex-wrap items-center gap-3 border-t border-[#e3e6e8] px-4 py-2.5 text-[12.5px] text-[#5f6368]">
        <span>
          Showing{" "}
          <b className="text-[#202124]">
            {sorted.length ? clamped * perPage + 1 : 0}–{Math.min(sorted.length, (clamped + 1) * perPage)}
          </b>{" "}
          of <b className="text-[#202124]">{sorted.length.toLocaleString()}</b> rows
          {query && <span className="text-[#52525b]"> (filtered from {rows.length.toLocaleString()})</span>}
        </span>
        <label className="ml-auto flex items-center gap-1.5">
          Rows
          <select
            value={perPage}
            onChange={(e) => setPerPage(Number(e.target.value))}
            className="h-7 rounded border border-[#dadce0] bg-white px-1.5 text-[12.5px] text-[#202124] outline-none"
          >
            {[10, 25, 50, 100].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </label>
        <div className="flex items-center gap-1">
          <button
            disabled={clamped === 0}
            onClick={() => setPage(clamped - 1)}
            className="grid h-7 w-7 place-items-center rounded-full hover:bg-[#f1f3f4] disabled:opacity-35"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          <span className="tabular">
            Page <b className="text-[#202124]">{clamped + 1}</b> / {pages}
          </span>
          <button
            disabled={clamped >= pages - 1}
            onClick={() => setPage(clamped + 1)}
            className="grid h-7 w-7 place-items-center rounded-full hover:bg-[#f1f3f4] disabled:opacity-35"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
        </div>
      </div>
    </section>
  );
}
