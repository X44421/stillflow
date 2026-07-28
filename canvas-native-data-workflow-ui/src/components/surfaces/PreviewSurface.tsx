import { useState } from "react";
import { cn } from "../../utils/cn";
import { Icon } from "../../lib/icons";
import { nodeById, useWorkspace, type SurfaceId } from "../../lib/store";
import { PREVIEW_ROWS, PROFILE_COLS } from "../../lib/data";
import { Surface } from "../Surface";
import { IconBtn } from "../ui";

const COLS = [
  { k: "source", w: 92 },
  { k: "file_name", w: 124 },
  { k: "page", w: 46, num: true },
  { k: "title", w: 150 },
  { k: "content", w: 0 },
] as const;

type Tab = "data" | "profile" | "chart";

function Tabs({ value, onChange }: { value: Tab; onChange: (t: Tab) => void }) {
  const items: { id: Tab; label: string; icon: string }[] = [
    { id: "data", label: "Data", icon: "table" },
    { id: "profile", label: "Profile", icon: "profile" },
    { id: "chart", label: "Chart", icon: "chart" },
  ];
  return (
    <div className="flex h-[24px] items-center gap-0.5 rounded-[6px] border border-[#dce2e8] bg-[#f4f6f8] p-[2px]">
      {items.map((i) => (
        <button
          key={i.id}
          onClick={() => onChange(i.id)}
          className={cn(
            "flex h-[20px] items-center gap-1.5 rounded-[4px] px-2 text-[11px] transition-colors",
            value === i.id ? "bg-white text-t1 shadow-sm" : "text-t3 hover:text-t2",
          )}
        >
          <Icon name={i.icon} size={11} />
          {i.label}
        </button>
      ))}
    </div>
  );
}

function DataTable({ compact }: { compact: boolean }) {
  const { s, d } = useWorkspace();
  return (
    <div className="scroll min-h-0 flex-1 overflow-auto">
      <table className="w-full border-collapse text-left" style={{ minWidth: compact ? 520 : 700, tableLayout: "fixed" }}>
        <thead className="sticky top-0 z-10">
          <tr className="bg-[#f8fafb]">
            <th className="w-[34px] border-b border-div px-2 py-[5px] text-[9.5px] font-medium tracking-[0.1em] text-t4">#</th>
            {COLS.map((c) => (
              <th
                key={c.k}
                style={c.w ? { width: c.w } : undefined}
                className="border-b border-div px-2 py-[5px] text-[9.5px] font-medium tracking-[0.1em] text-t4 uppercase"
              >
                <span className="flex items-center gap-1">
                  {c.k}
                  <Icon name="chevDown" size={9} className="opacity-0" />
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {PREVIEW_ROWS.map((r, i) => {
            const sel = s.selectedRow === i;
            return (
              <tr
                key={i}
                onClick={() => d({ t: "selectRow", i })}
                className={cn("cursor-default border-b border-[#eef1f4]", sel ? "bg-[#e8f4fa]" : "hover:bg-[#f4f6f8]")}
              >
                <td className="tnum px-2 py-[5px] text-[10.5px] text-t4">{i + 1}</td>
                <td className="truncate px-2 py-[5px] text-[11px] text-t2">{r.source}</td>
                <td className="truncate px-2 py-[5px] text-[11px] text-t2">{r.file_name}</td>
                <td className="tnum px-2 py-[5px] text-[11px] text-t2">{r.page}</td>
                <td className="truncate px-2 py-[5px] text-[11px] text-t1">{r.title}</td>
                <td className="truncate px-2 py-[5px] text-[11px] text-t3">{r.content}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Profile() {
  const stats = [
    { k: "Rows", v: "3,412,000" },
    { k: "Columns", v: "7" },
    { k: "Missing cells", v: "0.61%" },
    { k: "Valid", v: "99.2%" },
    { k: "Unique", v: "87.4%" },
  ];
  return (
    <div className="scroll min-h-0 flex-1 overflow-auto">
      <div className="grid grid-cols-5 border-b border-div">
        {stats.map((st) => (
          <div key={st.k} className="border-r border-div px-3 py-2.5 last:border-r-0">
            <div className="text-[9.5px] font-medium tracking-[0.12em] text-t4 uppercase">{st.k}</div>
            <div className="tnum mt-1 text-[14px] font-medium text-t1">{st.v}</div>
          </div>
        ))}
      </div>
      <div className="px-3 py-2">
        <div className="flex items-center gap-3 pb-1.5 text-[9.5px] font-medium tracking-[0.12em] text-t4 uppercase">
          <span className="w-[104px]">Column</span>
          <span className="w-[58px]">Type</span>
          <span className="w-[92px]">Distribution</span>
          <span className="w-[54px] text-right">Missing</span>
          <span className="w-[54px] text-right">Unique</span>
          <span className="flex-1 text-right">Quality</span>
        </div>
        {PROFILE_COLS.map((c) => (
          <div key={c.name} className="flex items-center gap-3 border-t border-[#eef1f4] py-[6px] hover:bg-[#f4f6f8]">
            <span className="w-[104px] truncate text-[11.5px] text-t1">{c.name}</span>
            <span className="w-[58px] font-mono text-[10.5px] text-t3">{c.type}</span>
            <span className="flex h-[16px] w-[92px] items-end gap-[2px]">
              {c.hist.map((h, i) => (
                <span key={i} className="flex-1 rounded-[1px] bg-[#c9d1d9]" style={{ height: `${Math.max(8, h)}%` }} />
              ))}
            </span>
            <span className="tnum w-[54px] text-right text-[11px] text-t2">{c.missing}%</span>
            <span className="tnum w-[54px] text-right text-[11px] text-t2">{c.unique.toLocaleString()}</span>
            <span className="flex flex-1 items-center justify-end gap-2">
              <span className="h-[3px] w-[68px] overflow-hidden rounded-full bg-[#e5e9ed]">
                <span
                  className="block h-full rounded-full"
                  style={{ width: `${c.valid}%`, background: c.valid > 99 ? "#4ba66a" : c.valid > 97 ? "#7ab35e" : "#c58b32" }}
                />
              </span>
              <span className="tnum w-[38px] text-right text-[11px] text-t2">{c.valid}%</span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function Chart() {
  const bars = [4, 9, 17, 29, 44, 63, 82, 96, 88, 71, 55, 41, 30, 22, 15, 10, 6, 4, 3, 2];
  return (
    <div className="min-h-0 flex-1 px-4 py-3">
      <div className="flex items-baseline justify-between">
        <span className="text-[11px] text-t2">Chunk token length distribution</span>
        <span className="tnum text-[10.5px] text-t4">mean 742 · p95 800</span>
      </div>
      <div className="mt-3 flex h-[calc(100%-46px)] items-end gap-[3px]">
        {bars.map((b, i) => (
          <div key={i} className="flex-1 rounded-t-[2px]" style={{ height: `${b}%`, background: i === 7 ? "#2196d2" : "#dce2e8" }} />
        ))}
      </div>
      <div className="tnum mt-1.5 flex justify-between text-[9.5px] text-t4">
        <span>0</span>
        <span>400</span>
        <span>800</span>
        <span>1200</span>
      </div>
    </div>
  );
}

export function PreviewSurface({ id }: { id: Extract<SurfaceId, "preview" | "preview2"> }) {
  const { s, d } = useWorkspace();
  const secondary = id === "preview2";
  const node = nodeById(s, secondary ? s.preview2Node : s.previewNode);
  const [localTab, setLocalTab] = useState<Tab>("data");
  const tab: Tab = secondary ? localTab : s.previewTab;
  const setTab = (t: Tab) => (secondary ? setLocalTab(t) : d({ t: "previewTab", v: t }));
  const compact = s.surfaces[id].w < 620;

  const title = node ? (node.kind === "DATASET" ? node.name : `${node.name} · output`) : "Data Preview";
  const meta =
    node?.kind === "DATASET" ? "CSV · 1.2M rows × 24 columns" : "Chunk Dataset · 3.4M rows × 7 columns";

  return (
    <Surface
      id={id}
      icon="table"
      title={title}
      meta={meta}
      collapsedLabel={`${title} · ${meta.split(" · ")[1] ?? ""}`}
      extraMenu={[
        {
          label: secondary ? "Close comparison" : "Compare with output",
          icon: "columns",
          onClick: () =>
            secondary
              ? d({ t: "closeSurface", id: "preview2" })
              : (d({ t: "openSurface", id: "preview", rect: { x: 436, y: 548, w: 384, h: 356 } }), d({ t: "openSurface", id: "preview2" })),
        },
        { label: "Copy as CSV", icon: "copy" },
        { label: "Open query editor", icon: "command" },
      ]}
      headerRight={
        !secondary ? (
          <IconBtn
            icon="columns"
            size={22}
            iconSize={12.5}
            tip="Compare input / output"
            onClick={() => {
              d({ t: "openSurface", id: "preview", rect: { x: 436, y: 548, w: 384, h: 356 } });
              d({ t: "openSurface", id: "preview2" });
            }}
          />
        ) : undefined
      }
    >
      {/* toolbar */}
      <div className="flex h-[34px] shrink-0 items-center gap-2 border-b border-div px-2.5">
        <Tabs value={tab} onChange={setTab} />
        <span className="flex-1" />
        {!compact && (
          <div className="flex h-[24px] w-[132px] items-center gap-1.5 rounded-[6px] border border-[#dce2e8] bg-[#f4f6f8] px-1.5 focus-within:border-[#c9d1d9]">
            <Icon name="search" size={11.5} className="shrink-0 text-t4" />
            <input placeholder="Search columns" className="w-full bg-transparent text-[11px] text-t1" />
          </div>
        )}
        <IconBtn icon="filter" size={24} iconSize={13} tip="Filter rows" />
        <IconBtn icon="columns" size={24} iconSize={13} tip="Column settings" />
        <IconBtn icon="download" size={24} iconSize={13} tip="Export selection" />
      </div>

      {tab === "data" && <DataTable compact={compact} />}
      {tab === "profile" && <Profile />}
      {tab === "chart" && <Chart />}

      {/* status bar */}
      <div className="flex h-[26px] shrink-0 items-center gap-3 border-t border-div px-3 text-[10px] text-t4">
        <span className="tnum">
          {tab === "data" ? `${PREVIEW_ROWS.length} of 3,412,000 rows` : tab === "profile" ? "7 columns profiled" : "20 buckets"} · sampled
        </span>
        <span className="flex-1" />
        {s.selectedRow >= 0 && tab === "data" && <span className="tnum">row {s.selectedRow + 1} selected</span>}
        <span className="tnum">84 ms</span>
      </div>
    </Surface>
  );
}
