import React, { useMemo, useState } from 'react';
import { Search, Upload } from '../icons/hero';
import type { DataPreviewResult, PreviewLimit } from '../types';

interface DataPreviewProps {
  preview: DataPreviewResult | null;
  loading?: boolean;
  label: string;
  limit: PreviewLimit;
  onLimitChange: (limit: PreviewLimit) => void | Promise<void>;
}

function displayValue(value: unknown): string {
  if (value === null || value === undefined || value === '') return 'null';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function escapeCsv(value: unknown): string {
  const text = value === null || value === undefined ? '' : String(value);
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

const DataPreview: React.FC<DataPreviewProps> = ({
  preview,
  loading = false,
  label,
  limit,
  onLimitChange,
}) => {
  const [activeTab, setActiveTab] = useState<'data' | 'profile'>('data');
  const [searchQuery, setSearchQuery] = useState('');

  const csv = useMemo(() => {
    if (!preview) return '';
    const header = preview.columns.map((column) => escapeCsv(column.name)).join(',');
    const lines = preview.rows.map((row) =>
      preview.columns.map((column) => escapeCsv(row[column.name])).join(',')
    );
    return [header, ...lines].join('\n');
  }, [preview]);

  const visible = useMemo(() => {
    if (!preview) return { columns: [], rows: [] };
    const query = searchQuery.trim().toLowerCase();
    if (!query) return { columns: preview.columns, rows: preview.rows };

    const matchingColumns = preview.columns.filter(
      (column) =>
        column.name.toLowerCase().includes(query) ||
        column.type.toLowerCase().includes(query)
    );
    const matchingRows = preview.rows.filter((row) =>
      preview.columns.some((column) =>
        displayValue(row[column.name]).toLowerCase().includes(query)
      )
    );

    return {
      columns: matchingColumns.length > 0 ? matchingColumns : preview.columns,
      rows: matchingRows,
    };
  }, [preview, searchQuery]);

  const downloadPreview = () => {
    if (!preview) return;
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `${label.replace(/\.[^.]+$/, '') || 'stillflow-result'}-preview.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <div className="flex-1 grid place-items-center bg-white">
        <div className="flex items-center gap-2 text-[13px] text-gray-500">
          <span className="h-3 w-3 rounded-full border-2 border-gray-300 border-t-gray-900 animate-spin" />
          Preparing data preview
        </div>
      </div>
    );
  }

  if (!preview) {
    return (
      <div className="flex-1 grid place-items-center bg-white">
        <div className="max-w-[300px] text-center">
          <div className="text-[13px] font-medium text-gray-800">No data result available</div>
          <div className="mt-1 text-[12px] leading-5 text-gray-500">
            Select a source or run the object graph to inspect its deterministic output.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 min-h-0 flex flex-col bg-white">
      <div className="h-10 border-b border-gray-200 px-3 flex items-center justify-between">
        <div className="flex h-full items-center gap-4">
          {(['data', 'profile'] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`h-full border-b-2 text-[12px] font-medium capitalize ${
                activeTab === tab
                  ? 'border-gray-900 text-gray-900'
                  : 'border-transparent text-gray-500 hover:text-gray-800'
              }`}
            >
              {tab}
            </button>
          ))}
          <span className="text-[11px] text-gray-400">
            {preview.totalRows.toLocaleString()} rows · {preview.columns.length} columns
          </span>
        </div>
        <div className="flex items-center gap-2">
          <label className="relative">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-gray-400"
            />
            <input
              type="search"
              placeholder="Search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              className="h-7 w-36 pl-7 pr-2 rounded border border-gray-200 bg-gray-50 text-[11px] outline-none focus:bg-white focus:border-gray-400"
            />
          </label>
          <select
            value={limit}
            onChange={(event) => void onLimitChange(Number(event.target.value) as PreviewLimit)}
            className="h-7 rounded border border-gray-200 bg-white px-2 text-[11px] text-gray-600 outline-none focus:border-gray-400"
            title="Preview row limit"
          >
            <option value={100}>100 rows</option>
            <option value={500}>500 rows</option>
          </select>
          <button
            onClick={downloadPreview}
            className="h-7 px-2 flex items-center gap-1.5 rounded border border-gray-200 text-[11px] font-medium text-gray-600 hover:bg-gray-50"
            title="Download visible rows as CSV"
          >
            <Upload size={13} />
            Export preview
          </button>
        </div>
      </div>

      {activeTab === 'data' ? (
        <div className="flex-1 min-h-0 overflow-auto">
          <table className="min-w-full border-collapse text-left">
            <thead className="sticky top-0 z-10 bg-gray-50">
              <tr>
                <th className="w-11 h-9 px-2 border-b border-r border-gray-200 text-[10px] font-medium text-gray-400">
                  #
                </th>
                {visible.columns.map((column) => (
                  <th
                    key={column.name}
                    className="min-w-[140px] h-9 px-3 border-b border-r border-gray-200 whitespace-nowrap"
                  >
                    <div className="text-[11px] font-semibold text-gray-800">{column.name}</div>
                    <div className="text-[9px] font-normal uppercase text-gray-400">{column.type}</div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visible.rows.map((row, rowIndex) => (
                <tr key={rowIndex} className="hover:bg-gray-50">
                  <td className="h-7 px-2 border-b border-r border-gray-100 text-[10px] text-gray-400 tabular-nums">
                    {rowIndex + 1}
                  </td>
                  {visible.columns.map((column) => {
                    const value = row[column.name];
                    const isNull = value === null || value === undefined || value === '';
                    return (
                      <td
                        key={column.name}
                        className={`max-w-[280px] h-7 px-3 border-b border-r border-gray-100 text-[11px] whitespace-nowrap overflow-hidden text-ellipsis ${
                          isNull ? 'italic text-gray-400' : 'text-gray-700'
                        }`}
                        title={displayValue(value)}
                      >
                        {displayValue(value)}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
          <table className="w-full border-collapse text-left">
            <thead className="sticky top-0 bg-gray-50">
              <tr className="text-[10px] uppercase text-gray-400">
                <th className="h-8 px-4 border-b border-gray-200 font-medium">Column</th>
                <th className="h-8 px-4 border-b border-gray-200 font-medium">Type</th>
                <th className="h-8 px-4 border-b border-gray-200 font-medium text-right">Distinct</th>
                <th className="h-8 px-4 border-b border-gray-200 font-medium text-right">Null</th>
                <th className="h-8 px-4 border-b border-gray-200 font-medium">Completeness</th>
              </tr>
            </thead>
            <tbody>
              {visible.columns.map((column) => {
                const complete =
                  preview.totalRows === 0
                    ? 100
                    : Math.round(((preview.totalRows - column.nullCount) / preview.totalRows) * 100);
                return (
                  <tr key={column.name} className="text-[12px] hover:bg-gray-50">
                    <td className="h-10 px-4 border-b border-gray-100 font-medium text-gray-800">
                      {column.name}
                    </td>
                    <td className="h-10 px-4 border-b border-gray-100 text-gray-500">{column.type}</td>
                    <td className="h-10 px-4 border-b border-gray-100 text-right tabular-nums text-gray-700">
                      {column.distinctCount.toLocaleString()}
                    </td>
                    <td className="h-10 px-4 border-b border-gray-100 text-right tabular-nums text-gray-700">
                      {column.nullCount.toLocaleString()}
                    </td>
                    <td className="h-10 px-4 border-b border-gray-100">
                      <div className="flex items-center gap-2">
                        <div className="h-1 flex-1 bg-gray-100">
                          <div className="h-full bg-gray-800" style={{ width: `${complete}%` }} />
                        </div>
                        <span className="w-9 text-right tabular-nums text-[11px] text-gray-500">
                          {complete}%
                        </span>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default DataPreview;
