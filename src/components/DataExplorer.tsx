import { useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  FileSpreadsheet,
  Folder,
  Maximize2,
  RotateCcw,
  Upload,
} from "lucide-react";
import type { ColumnStats } from "../lib/csv";
import { TypeIcon } from "./ColumnSummary";

/**
 * Directly reused from rebuild-kaggle-csv-preview. The props connect its
 * explorer interactions to StillFlow's project-owned datasets and preview.
 */
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
  onSelect: (column: string | null) => void;
  onUpload: (file: File) => void;
  onReset: () => void;
  custom: boolean;
}) {
  const [openTree, setOpenTree] = useState(true);
  const [drag, setDrag] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const folderName = fileName.replace(/\.[^.]+$/, "") || "dataset";

  return (
    <aside className="flex h-full w-[272px] shrink-0 flex-col overflow-hidden bg-[#f5f7f8]">
      <div className="overflow-hidden border-b border-[#e3e6e8] bg-transparent">
        <div className="flex items-center justify-between border-b border-[#e3e6e8] px-3 py-2.5">
          <h2 className="text-[14px] font-semibold text-[#202124]">Data Explorer</h2>
          <Maximize2 className="h-3.5 w-3.5 text-[#5f6368]" />
        </div>

        <div className="px-3 py-2 text-[12px] text-[#5f6368]">
          Current file ({sizeLabel}) · {rowCount.toLocaleString()} rows
        </div>

        <div className="px-2 pb-2">
          <button
            onClick={() => setOpenTree((open) => !open)}
            className="flex w-full items-center gap-1 rounded px-1.5 py-1.5 text-left text-[13px] text-[#3c4043] hover:bg-[#f1f3f4]"
          >
            {openTree ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            <Folder className="h-4 w-4 text-[#5f6368]" />
            <span className="truncate font-medium">{folderName}</span>
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
      </div>

      <div className="flex min-h-0 flex-1 flex-col">
        <div className="flex items-center justify-between px-3 pt-2.5 pb-1">
          <span className="text-[11px] font-semibold tracking-wide text-[#5f6368] uppercase">Columns</span>
          <span className="text-[11px] text-[#80868b]">{stats.length}</span>
        </div>
        <ul className="kg-scroll min-h-0 flex-1 overflow-y-auto px-1.5 pb-2">
          {stats.map((stat) => (
            <li key={stat.name}>
              <button
                onMouseEnter={() => onSelect(stat.name)}
                onFocus={() => onSelect(stat.name)}
                onClick={() => onSelect(stat.name)}
                className={`flex w-full items-center gap-1.5 rounded px-2 py-[5px] text-left text-[12.5px] transition-colors ${
                  selected === stat.name ? "bg-[#f0fbff] text-[#0b6c96]" : "text-[#3c4043] hover:bg-[#f1f3f4]"
                }`}
              >
                <TypeIcon type={stat.type} />
                <span className="truncate">{stat.name}</span>
                <span className="ml-auto shrink-0 font-mono text-[10px] text-[#9aa0a6]">
                  {stat.unique.toLocaleString()}
                </span>
              </button>
            </li>
          ))}
        </ul>
      </div>

      <div
        onDragOver={(event) => {
          event.preventDefault();
          setDrag(true);
        }}
        onDragLeave={() => setDrag(false)}
        onDrop={(event) => {
          event.preventDefault();
          setDrag(false);
          const file = event.dataTransfer.files?.[0];
          if (file) onUpload(file);
        }}
        className={`m-3 mt-1 rounded-xl border border-dashed p-3 text-center transition ${
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
          onChange={(event) => event.target.files?.[0] && onUpload(event.target.files[0])}
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
              <RotateCcw className="h-3 w-3" /> Reset focus
            </button>
          )}
        </div>
      </div>
    </aside>
  );
}
