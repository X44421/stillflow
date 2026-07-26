import { useState } from "react";
import { Calendar, CaseSensitive, Hash, ToggleLeft } from "lucide-react";
import { axisLabel, compact, pctLabel, type ColumnStats } from "../lib/csv";

export function TypeIcon({ type, className = "" }: { type: ColumnStats["type"]; className?: string }) {
  const c = `h-3.5 w-3.5 shrink-0 ${className}`;
  if (type === "integer" || type === "decimal") return <Hash className={`${c} text-[#1a73e8]`} />;
  if (type === "date") return <Calendar className={`${c} text-[#9334e6]`} />;
  if (type === "boolean") return <ToggleLeft className={`${c} text-[#e8710a]`} />;
  return <CaseSensitive className={`${c} text-[#188038]`} />;
}

export function typeLabel(t: ColumnStats["type"]) {
  return t === "integer" ? "Integer" : t === "decimal" ? "Decimal" : t === "date" ? "Date" : t === "boolean" ? "Boolean" : "String";
}

/* Thin valid / mismatched / missing bar shown under every column name. */
export function ValidityBar({ s, tall = false }: { s: ColumnStats; tall?: boolean }) {
  const t = Math.max(1, s.total);
  const seg = [
    { w: (s.valid / t) * 100, c: "#46a352", k: "Valid" },
    { w: (s.mismatched / t) * 100, c: "#e5534b", k: "Mismatched" },
    { w: (s.missing / t) * 100, c: "#c9cdd1", k: "Missing" },
  ].filter((x) => x.w > 0);
  return (
    <div className={`flex w-full overflow-hidden rounded-full bg-[#eceff1] ${tall ? "h-[6px]" : "h-[3px]"}`}>
      {seg.map((x) => (
        <div key={x.k} style={{ width: `${x.w}%`, background: x.c }} title={`${x.k} ${pctLabel(x.w)}`} />
      ))}
    </div>
  );
}

function Histogram({ s }: { s: ColumnStats }) {
  const [hover, setHover] = useState<number | null>(null);
  const max = Math.max(...s.buckets.map((b) => b.count), 1);
  const b = hover != null ? s.buckets[hover] : null;
  return (
    <div className="relative">
      {b && (
        <div className="kg-fade pointer-events-none absolute -top-9 left-1/2 z-30 -translate-x-1/2 rounded bg-[#202124] px-2 py-1 text-[11px] whitespace-nowrap text-white shadow-lg">
          {b.label} · {b.count.toLocaleString()}
        </div>
      )}
      <div className="flex h-[34px] items-end gap-[1.5px]" onMouseLeave={() => setHover(null)}>
        {s.buckets.map((bk, i) => (
          <div
            key={i}
            onMouseEnter={() => setHover(i)}
            className="flex h-full flex-1 cursor-crosshair items-end"
          >
            <div
              className="w-full rounded-[1px] transition-colors"
              style={{
                height: `${Math.max(bk.count ? 8 : 0, (bk.count / max) * 100)}%`,
                background: hover === i ? "#0f9ad6" : "#8ad9ff",
              }}
            />
          </div>
        ))}
      </div>
      <div className="mt-1 flex justify-between font-mono text-[10px] text-[#5f6368]">
        <span>{s.min != null ? axisLabel(s.min, s.type) : ""}</span>
        <span>{s.max != null ? axisLabel(s.max, s.type) : ""}</span>
      </div>
    </div>
  );
}

const CAT_COLORS = ["#20beff", "#8ad9ff", "#dfe3e6"];

function CategoryBar({ s }: { s: ColumnStats }) {
  const allUnique = s.unique >= s.valid * 0.98 && s.unique > 12;
  if (allUnique) {
    return (
      <div>
        <div className="flex h-[34px] items-end">
          <div className="h-[14px] w-full rounded-[2px] bg-[#dfe3e6]" />
        </div>
        <div className="mt-1 truncate font-mono text-[10px] text-[#5f6368]">
          {s.unique.toLocaleString()} unique values
        </div>
      </div>
    );
  }
  return (
    <div>
      <div className="flex h-[34px] items-end">
        <div className="flex h-[14px] w-full overflow-hidden rounded-[2px] bg-[#eceff1]">
          {s.categories.map((c, i) => (
            <div
              key={c.label}
              title={`${c.label} · ${c.count.toLocaleString()} (${pctLabel(c.pct)})`}
              style={{ width: `${c.pct}%`, background: c.other ? CAT_COLORS[2] : CAT_COLORS[i % 2] }}
              className="h-full border-r border-white last:border-0"
            />
          ))}
        </div>
      </div>
      <div className="mt-1 space-y-[1px]">
        {s.categories.slice(0, 2).map((c, i) => (
          <div key={c.label} className="flex items-center gap-1 text-[10px] text-[#5f6368]">
            <span
              className="h-[7px] w-[7px] shrink-0 rounded-[1px]"
              style={{ background: c.other ? CAT_COLORS[2] : CAT_COLORS[i % 2] }}
            />
            <span className="truncate">{c.label}</span>
            <span className="ml-auto shrink-0 font-mono">{pctLabel(c.pct)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function MiniChart({ s }: { s: ColumnStats }) {
  return s.buckets.length ? <Histogram s={s} /> : <CategoryBar s={s} />;
}

/* Rich popover with the full column profile (Kaggle shows this on click). */
export function ColumnDetailCard({ s }: { s: ColumnStats }) {
  const rows: [string, string][] = [["Type", typeLabel(s.type)]];
  if (s.min != null && s.type !== "date") {
    rows.push(["Min", compact(s.min)], ["Max", compact(s.max ?? 0)], ["Mean", compact(s.mean ?? 0)], ["Std dev", compact(s.std ?? 0)]);
  } else if (s.type === "date" && s.min != null) {
    rows.push(["Earliest", axisLabel(s.min, "date")], ["Latest", axisLabel(s.max ?? 0, "date")]);
  } else {
    rows.push(["Shortest", `${s.minLen ?? 0} chars`], ["Longest", `${s.maxLen ?? 0} chars`]);
    if (s.mostCommon) rows.push(["Most common", s.mostCommon.label]);
  }
  rows.push(["Unique", s.unique.toLocaleString()]);

  return (
    <div className="kg-fade absolute top-full left-0 z-50 mt-1 w-[264px] rounded-lg border border-[#e3e6e8] bg-white p-3 shadow-[0_8px_24px_rgba(32,33,36,.16)]">
      <div className="mb-2 flex items-center gap-1.5">
        <TypeIcon type={s.type} />
        <span className="truncate text-[13px] font-semibold text-[#202124]">{s.name}</span>
      </div>
      <ValidityBar s={s} tall />
      <div className="mt-2 grid grid-cols-3 gap-1 text-center">
        {[
          ["Valid", s.valid, "#46a352"],
          ["Mismatched", s.mismatched, "#e5534b"],
          ["Missing", s.missing, "#8a9199"],
        ].map(([k, v, c]) => (
          <div key={k as string} className="rounded bg-[#f8f9fa] py-1.5">
            <div className="text-[10px] text-[#5f6368]">{k as string}</div>
            <div className="text-[12px] font-semibold" style={{ color: c as string }}>
              {pctLabel(((v as number) / Math.max(1, s.total)) * 100)}
            </div>
            <div className="font-mono text-[10px] text-[#80868b]">{(v as number).toLocaleString()}</div>
          </div>
        ))}
      </div>
      <dl className="mt-2 divide-y divide-[#f1f3f4] text-[12px]">
        {rows.map(([k, v]) => (
          <div key={k} className="flex justify-between gap-3 py-1">
            <dt className="text-[#5f6368]">{k}</dt>
            <dd className="max-w-[150px] truncate font-medium text-[#202124]">{v}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
