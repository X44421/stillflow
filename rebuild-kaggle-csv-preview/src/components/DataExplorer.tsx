import { useRef, useState } from "react";
import { ChevronDown, ChevronRight, FileSpreadsheet, Folder, Maximize2, RotateCcw, Upload } from "lucide-react";
import type { ColumnStats } from "../lib/csv";
import { TypeIcon } from "./ColumnSummary";

export function DataExplorer({
  fileName,
  sizeLabel,
  rowCount,
  stats,
  selected,
  onSelect,
  onUpload,
  onReset,
  custom,
}: {
  fileName: string;
  sizeLabel: string;
  rowCount: number;
  stats: ColumnStats[];
  selected: string | null;
  onSelect: (c: string | null) => void;
  onUpload: (f: File) => void;
  onReset: () => void;
  custom: boolean;
}) {
  const [openTree, setOpenTree] = useState(true);
  const [drag, setDrag] = useState(false);
  const input = useRef<HTMLInputElement>(null);

  return (
    <aside className="lg:sticky lg:top-[72px]">
      <div className="overflow-hidden rounded-xl border border-[#e3e6e8] bg-white">
        <div className="flex items-center justify-between border-b border-[#e3e6e8] px-3 py-2.5">
          <h2 className="text-[14px] font-semibold text-[#202124]">Data Explorer</h2>
          <Maximize2 className="h-3.5 w-3.5 text-[#5f6368]" />
        </div>

        <div className="px-3 py-2 text-[12px] text-[#5f6368]">
          Version 47 ({sizeLabel}) · {rowCount.toLocaleString()} rows
        </div>

        <div className="px-2 pb-2">
          <button
            onClick={() => setOpenTree((o) => !o)}
            className="flex w-full items-center gap-1 rounded px-1.5 py-1.5 text-left text-[13px] text-[#3c4043] hover:bg-[#f1f3f4]"
          >
            {openTree ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            <Folder className="h-4 w-4 text-[#5f6368]" />
            <span className="truncate font-medium">kaggle-datasets</span>
          </button>
          {openTree && (
            <button
              onClick={() => onSelect(null)}
              className="ml-5 flex w-[calc(100%-1.25rem)] items-center gap-1.5 rounded bg-[#e8f7fe] px-2 py-1.5 text-left text-[13px] font-medium text-[#0b6c96]"
            >
              <FileSpreadsheet className="h-4 w-4 shrink-0" />
              <span className="truncate">{fileName}</span>
            </button>
          )}
        </div>

        <div className="border-t border-[#e3e6e8] px-3 pt-2.5 pb-1">
          <div className="flex items-center justify-between">
            <span className="text-[11px] font-semibold tracking-wide text-[#5f6368] uppercase">Columns</span>
            <span className="text-[11px] text-[#80868b]">{stats.length}</span>
          </div>
        </div>
        <ul className="kg-scroll max-h-[360px] overflow-y-auto px-1.5 pb-2">
          {stats.map((s) => (
            <li key={s.name}>
              <button
                onMouseEnter={() => onSelect(s.name)}
                onFocus={() => onSelect(s.name)}
                onClick={() => onSelect(s.name)}
                className={`flex w-full items-center gap-1.5 rounded px-2 py-[5px] text-left text-[12.5px] transition-colors ${
                  selected === s.name ? "bg-[#f0fbff] text-[#0b6c96]" : "text-[#3c4043] hover:bg-[#f1f3f4]"
                }`}
              >
                <TypeIcon type={s.type} />
                <span className="truncate">{s.name}</span>
                <span className="ml-auto shrink-0 font-mono text-[10px] text-[#9aa0a6]">
                  {s.unique.toLocaleString()}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </div>

      {/* -------- functional extra: profile any CSV in this viewer -------- */}
      <div
        onDragOver={(e) => {
          e.preventDefault();
          setDrag(true);
        }}
        onDragLeave={() => setDrag(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDrag(false);
          const f = e.dataTransfer.files?.[0];
          if (f) onUpload(f);
        }}
        className={`mt-3 rounded-xl border border-dashed p-3 text-center transition ${
          drag ? "border-[#20beff] bg-[#f0fbff]" : "border-[#dadce0] bg-white"
        }`}
      >
        <Upload className="mx-auto h-4 w-4 text-[#5f6368]" />
        <p className="mt-1.5 text-[12px] leading-snug text-[#5f6368]">
          Drop a <b className="text-[#3c4043]">.csv</b> here to profile it with this explorer
        </p>
        <input
          ref={input}
          type="file"
          accept=".csv,text/csv"
          className="hidden"
          onChange={(e) => e.target.files?.[0] && onUpload(e.target.files[0])}
        />
        <div className="mt-2 flex justify-center gap-2">
          <button
            onClick={() => input.current?.click()}
            className="rounded-full border border-[#dadce0] px-3 py-1 text-[12px] font-medium text-[#3c4043] hover:bg-[#f1f3f4]"
          >
            Browse files
          </button>
          {custom && (
            <button
              onClick={onReset}
              className="flex items-center gap-1 rounded-full border border-[#dadce0] px-3 py-1 text-[12px] font-medium text-[#3c4043] hover:bg-[#f1f3f4]"
            >
              <RotateCcw className="h-3 w-3" /> Sample
            </button>
          )}
        </div>
      </div>
    </aside>
  );
}
