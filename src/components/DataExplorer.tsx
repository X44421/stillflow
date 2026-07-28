import { useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  FileSpreadsheet,
  Folder,
  Plus,
  Search,
  Upload,
} from "lucide-react";

/**
 * App-shell sidebar. It manages workspace objects only — field-level
 * structure lives in the central preview (Schema / Profile views), not here.
 */
export function DataExplorer({
  fileName,
  sizeLabel,
  rowCount,
  fileActive,
  onOpenPreview,
  onUpload,
}: {
  fileName: string;
  sizeLabel: string;
  rowCount: number;
  /** Whether this file is the current primary selection. */
  fileActive: boolean;
  onOpenPreview: () => void;
  onUpload: (file: File) => void;
}) {
  const [openTree, setOpenTree] = useState(true);
  const [query, setQuery] = useState("");
  const [drag, setDrag] = useState(false);
  const input = useRef<HTMLInputElement>(null);
  const folderName = fileName.replace(/\.[^.]+$/, "") || "dataset";

  const q = query.trim().toLowerCase();
  const fileVisible = !q || fileName.toLowerCase().includes(q);

  return (
    <aside
      onDragOver={(event) => {
        event.preventDefault();
        setDrag(true);
      }}
      onDragLeave={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget as Node)) {
          setDrag(false);
        }
      }}
      onDrop={(event) => {
        event.preventDefault();
        setDrag(false);
        const file = event.dataTransfer.files?.[0];
        if (file) onUpload(file);
      }}
      className="relative flex h-full w-[248px] shrink-0 flex-col overflow-hidden"
    >
      <div className="flex h-11 shrink-0 items-center px-1">
        <h2 className="text-[13px] font-semibold text-[#171a1f]">Data Explorer</h2>
      </div>

      <div className="flex shrink-0 items-center gap-1.5 pb-2">
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-2 h-3.5 w-3.5 -translate-y-1/2 text-[#9099a4]" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search objects..."
            className="h-8 w-full rounded-md border border-[#dce2e8] bg-white pr-2 pl-7 text-[12px] text-[#171a1f] outline-none placeholder:text-[#9099a4] focus:border-[#2196d2] focus:ring-2 focus:ring-[rgba(33,150,210,.18)]"
          />
        </div>
        <button
          onClick={() => input.current?.click()}
          title="Add workspace object — import a CSV file"
          className="grid h-8 w-8 shrink-0 place-items-center rounded-md border border-[#dce2e8] bg-white text-[#5e6874] transition-colors hover:bg-[#edf2f6]"
        >
          <Plus className="h-4 w-4" />
        </button>
      </div>

      <div className="kg-scroll min-h-0 flex-1 overflow-y-auto border-t border-[#dce2e8] py-2">
        <p className="px-2 pb-1 text-[10.5px] font-semibold text-[#9099a4]">Files</p>
        <button
          onClick={() => setOpenTree((open) => !open)}
          className="flex w-full items-center gap-1 rounded-md px-1.5 py-1.5 text-left text-[12.5px] text-[#39434e] transition-colors hover:bg-[#edf2f6]"
        >
          {openTree ? (
            <ChevronDown className="h-3.5 w-3.5 shrink-0 text-[#9099a4]" />
          ) : (
            <ChevronRight className="h-3.5 w-3.5 shrink-0 text-[#9099a4]" />
          )}
          <Folder className="h-4 w-4 shrink-0 text-[#9099a4]" />
          <span className="truncate font-medium">{folderName}</span>
        </button>
        {openTree && fileVisible && (
          <button
            onClick={onOpenPreview}
            aria-current={fileActive || undefined}
            className={`mt-0.5 ml-4 flex w-[calc(100%-1rem)] items-center gap-1.5 rounded-md px-2 py-1.5 text-left transition-colors ${
              fileActive ? "bg-[#e8f4fa]" : "hover:bg-[#edf2f6]"
            }`}
          >
            <FileSpreadsheet
              className={`h-4 w-4 shrink-0 ${fileActive ? "text-[#1686be]" : "text-[#9099a4]"}`}
            />
            <span className="min-w-0 flex-1">
              <span
                className={`block truncate text-[12.5px] font-medium ${
                  fileActive ? "text-[#171a1f]" : "text-[#39434e]"
                }`}
              >
                {fileName}
              </span>
              <span className="block text-[11px] text-[#5e6874]">
                {sizeLabel} · {rowCount.toLocaleString()} rows
              </span>
            </span>
          </button>
        )}
        {openTree && !fileVisible && (
          <p className="mt-2 px-2 text-[12px] text-[#9099a4]">No files match “{query}”.</p>
        )}
      </div>

      <div className="flex shrink-0 items-center gap-1.5 border-t border-[#dce2e8] pt-2">
        <button
          onClick={() => input.current?.click()}
          title="Import a CSV file or drop it anywhere in this panel"
          className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md border border-[#dce2e8] bg-white text-[12px] font-medium text-[#39434e] transition-colors hover:bg-[#edf2f6]"
        >
          <Upload className="h-3.5 w-3.5" />
          Import dataset
        </button>
      </div>

      <input
        ref={input}
        type="file"
        accept=".csv,text/csv"
        className="hidden"
        onChange={(event) => event.target.files?.[0] && onUpload(event.target.files[0])}
      />

      {drag && (
        <div className="pointer-events-none absolute inset-0 z-20 grid place-items-center rounded-lg border-2 border-dashed border-[#2196d2] bg-[#ddf2fc]/70">
          <div className="text-center">
            <Upload className="mx-auto h-5 w-5 text-[#1686be]" />
            <p className="mt-1.5 text-[12px] font-medium text-[#1686be]">Drop CSV to import</p>
          </div>
        </div>
      )}
    </aside>
  );
}