import { useState } from "react";
import { ChevronDown, Download, MoreVertical, NotebookPen, ArrowBigUp, Bookmark, Share2 } from "lucide-react";

const TABS = [
  { label: "Data Card", count: "" },
  { label: "Code", count: "142" },
  { label: "Discussion", count: "18" },
  { label: "Suggestions", count: "3" },
];

export function DatasetHeader({ onDownload }: { onDownload: () => void }) {
  const [tab, setTab] = useState("Data Card");
  const [voted, setVoted] = useState(false);

  return (
    <div className="border-b border-[#e3e6e8] bg-white">
      <div className="mx-auto max-w-[1440px] px-4 pt-5 sm:px-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start">
          <div className="hidden h-[104px] w-[168px] shrink-0 overflow-hidden rounded-lg bg-gradient-to-br from-[#20beff] via-[#3b82f6] to-[#1e3a8a] sm:block">
            <div className="grid h-full w-full place-items-center bg-[radial-gradient(circle_at_20%_20%,rgba(255,255,255,.35),transparent_45%)]">
              <span className="font-mono text-[11px] tracking-[0.2em] text-white/90">CSV · 16 COLS</span>
            </div>
          </div>

          <div className="min-w-0 flex-1">
            <h1 className="text-[28px] leading-tight font-semibold tracking-tight text-[#202124]">
              Kaggle Datasets
            </h1>
            <p className="mt-1 text-[15px] text-[#5f6368]">
              Metadata of every public dataset on Kaggle — refreshed weekly
            </p>
            <div className="mt-3 flex flex-wrap items-center gap-2 text-[13px] text-[#5f6368]">
              <div className="grid h-6 w-6 place-items-center rounded-full bg-[#f39c12] text-[10px] font-bold text-white">
                MW
              </div>
              <a className="font-medium text-[#202124] hover:underline" href="#">
                Morris Wong
              </a>
              <span>·</span>
              <span>Updated 3 days ago</span>
              <span className="ml-1 rounded bg-[#f1f3f4] px-1.5 py-0.5 text-[11px] font-medium text-[#3c4043]">
                Version 47
              </span>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              onClick={() => setVoted((v) => !v)}
              className={`flex items-center gap-1 rounded-full border px-3 py-2 text-[13px] font-medium transition ${
                voted
                  ? "border-[#20beff] bg-[#e8f7fe] text-[#0b6c96]"
                  : "border-[#dadce0] text-[#3c4043] hover:bg-[#f1f3f4]"
              }`}
            >
              <ArrowBigUp className="h-4 w-4" />
              {voted ? "1,205" : "1,204"}
            </button>
            <button className="rounded-full border border-[#dadce0] p-2 text-[#3c4043] hover:bg-[#f1f3f4]" title="Bookmark">
              <Bookmark className="h-4 w-4" />
            </button>
            <button className="rounded-full border border-[#dadce0] p-2 text-[#3c4043] hover:bg-[#f1f3f4]" title="Share">
              <Share2 className="h-4 w-4" />
            </button>
            <button className="flex items-center gap-1.5 rounded-full border border-[#dadce0] px-3.5 py-2 text-[13px] font-medium text-[#3c4043] hover:bg-[#f1f3f4]">
              <NotebookPen className="h-4 w-4" /> New Notebook
            </button>
            <button
              onClick={onDownload}
              className="flex items-center gap-1.5 rounded-full bg-[#20beff] px-3.5 py-2 text-[13px] font-medium text-white hover:bg-[#0f9ad6]"
            >
              <Download className="h-4 w-4" /> Download <span className="opacity-80">(38 MB)</span>
              <ChevronDown className="h-3.5 w-3.5 opacity-80" />
            </button>
            <button className="rounded-full p-2 text-[#5f6368] hover:bg-[#f1f3f4]">
              <MoreVertical className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="mt-4 flex gap-6 overflow-x-auto">
          {TABS.map((t) => {
            const on = tab === t.label;
            return (
              <button
                key={t.label}
                onClick={() => setTab(t.label)}
                className={`relative -mb-px shrink-0 border-b-2 pb-3 text-[14px] transition-colors ${
                  on
                    ? "border-[#20beff] font-semibold text-[#202124]"
                    : "border-transparent text-[#5f6368] hover:text-[#202124]"
                }`}
              >
                {t.label}
                {t.count && <span className="ml-1.5 text-[12px] text-[#80868b]">({t.count})</span>}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

export function AboutStrip() {
  const items = [
    ["Usability", "10.00"],
    ["License", "CC0: Public Domain"],
    ["Expected update frequency", "Weekly"],
  ];
  return (
    <div className="flex flex-wrap items-center gap-x-10 gap-y-3 rounded-xl border border-[#e3e6e8] bg-white px-4 py-3">
      {items.map(([k, v]) => (
        <div key={k}>
          <div className="text-[11px] tracking-wide text-[#5f6368] uppercase">{k}</div>
          <div className="text-[13px] font-medium text-[#202124]">{v}</div>
        </div>
      ))}
      <div className="ml-auto flex flex-wrap gap-1.5">
        {["datasets", "metadata", "internet", "beginner", "tabular"].map((t) => (
          <span
            key={t}
            className="rounded-full bg-[#f1f3f4] px-2.5 py-1 text-[12px] text-[#3c4043] hover:bg-[#e8eaed]"
          >
            {t}
          </span>
        ))}
      </div>
    </div>
  );
}
