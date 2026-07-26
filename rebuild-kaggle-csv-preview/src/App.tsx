import { useMemo, useState } from "react";
import { SideNav, TopNav } from "./components/Chrome";
import { AboutStrip, DatasetHeader } from "./components/DatasetHeader";
import { DataExplorer } from "./components/DataExplorer";
import { DataTable } from "./components/DataTable";
import { CSV_COLUMNS, FILE_META, buildRows } from "./data/kaggleDatasets";
import { parseCSV, profileAll, toCSV, type Row } from "./lib/csv";

interface Source {
  name: string;
  columns: string[];
  rows: Row[];
  custom: boolean;
}

const SAMPLE: Source = {
  name: FILE_META.name,
  columns: CSV_COLUMNS,
  rows: buildRows(1000),
  custom: false,
};

export default function App() {
  const [src, setSrc] = useState<Source>(SAMPLE);
  const [selected, setSelected] = useState<string | null>(null);

  const stats = useMemo(() => profileAll(src.columns, src.rows), [src]);

  const cells = src.rows.length * src.columns.length;
  const missing = stats.reduce((a, s) => a + s.missing + s.mismatched, 0);
  const sizeLabel = src.custom
    ? `${(toCSV(src.columns, src.rows).length / 1024).toFixed(1)} kB`
    : FILE_META.sizeLabel;

  const download = () => {
    const blob = new Blob([toCSV(src.columns, src.rows)], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = src.name;
    a.click();
    URL.revokeObjectURL(url);
  };

  const upload = (file: File) => {
    const reader = new FileReader();
    reader.onload = () => {
      const { columns, rows } = parseCSV(String(reader.result ?? ""));
      if (columns.length && rows.length) {
        setSrc({ name: file.name, columns, rows, custom: true });
        setSelected(null);
      }
    };
    reader.readAsText(file);
  };

  return (
    <div className="min-h-screen bg-[#fafafa] font-sans">
      <TopNav />
      <div className="flex">
        <SideNav />
        <main className="min-w-0 flex-1">
          <DatasetHeader onDownload={download} />

          <div className="mx-auto max-w-[1440px] space-y-4 px-4 py-4 sm:px-6">
            <AboutStrip />

            <div className="grid items-start gap-4 lg:grid-cols-[264px_minmax(0,1fr)]">
              <DataExplorer
                fileName={src.name}
                sizeLabel={sizeLabel}
                rowCount={src.rows.length}
                stats={stats}
                selected={selected}
                onSelect={setSelected}
                onUpload={upload}
                onReset={() => {
                  setSrc(SAMPLE);
                  setSelected(null);
                }}
                custom={src.custom}
              />

              <div className="min-w-0 space-y-4">
                <section className="rounded-xl border border-[#e3e6e8] bg-white p-4">
                  <h2 className="text-[15px] font-semibold text-[#202124]">About this file</h2>
                  <p className="mt-1.5 max-w-3xl text-[13.5px] leading-relaxed text-[#3c4043]">
                    {src.custom ? (
                      <>
                        Profiling <b>{src.name}</b> locally in your browser — nothing is uploaded. Column types,
                        histograms and valid / mismatched / missing ratios are computed on the fly.
                      </>
                    ) : (
                      <>
                        One row per public Kaggle dataset, scraped from the Kaggle API. Includes ownership,
                        engagement (views, downloads, votes), licensing, size on disk, the current version number
                        and a pipe-delimited tag list — everything you need to study what makes a dataset popular.
                      </>
                    )}
                  </p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {[
                      ["Rows", src.rows.length.toLocaleString()],
                      ["Columns", String(src.columns.length)],
                      ["Cells", cells.toLocaleString()],
                      ["Missing / mismatched", `${((missing / Math.max(1, cells)) * 100).toFixed(2)}%`],
                      ["Delimiter", "Comma"],
                      ["Encoding", "UTF-8"],
                    ].map(([k, v]) => (
                      <div
                        key={k}
                        className="rounded-lg border border-[#e3e6e8] bg-[#f8f9fa] px-3 py-1.5 text-[12px]"
                      >
                        <span className="text-[#5f6368]">{k}: </span>
                        <span className="font-semibold text-[#202124]">{v}</span>
                      </div>
                    ))}
                  </div>
                </section>

                <DataTable
                  columns={src.columns}
                  rows={src.rows}
                  stats={stats}
                  fileName={src.name}
                  sizeLabel={sizeLabel}
                  focusColumn={selected}
                  onDownload={download}
                />

                <p className="px-1 pb-6 text-[12px] leading-relaxed text-[#80868b]">
                  Green / red / grey bars under each column name show the share of{" "}
                  <span className="font-medium text-[#46a352]">valid</span>,{" "}
                  <span className="font-medium text-[#e5534b]">mismatched</span> and{" "}
                  <span className="font-medium text-[#5f6368]">missing</span> values. Click a column name to sort,
                  the ⓘ icon for the full profile, or hover a histogram bar for bucket counts.
                </p>
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
