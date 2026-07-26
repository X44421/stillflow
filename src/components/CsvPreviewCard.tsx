import React, { useEffect, useMemo, useState } from 'react';
import { ChevronDown, X } from '../icons/hero';
import type {
  DataPreviewResult,
  Dataset,
  PreviewColumn,
} from '../types';
import { previewBackendDataset } from '../utils/api';

const PAGE_SIZE = 12;

type PreviewTab = 'summary' | 'preview' | 'schema' | 'relations' | 'report';
type SortDirection = 'asc' | 'desc';

interface CsvPreviewCardProps {
  dataset: Dataset;
  onClose: () => void;
}

interface DistributionBar {
  label: string;
  count: number;
}

interface NumericRelation {
  left: string;
  right: string;
  correlation: number;
  points: Array<[number, number]>;
}

interface ReportInsight {
  tag: string;
  title: string;
  detail: string;
}

function isMissing(value: unknown): boolean {
  return value === null || value === undefined || String(value).trim() === '';
}

function formatCount(value: number): string {
  return new Intl.NumberFormat('en-US', {
    notation: value >= 10_000 ? 'compact' : 'standard',
    maximumFractionDigits: 1,
  }).format(value);
}

function formatPercent(value: number): string {
  return `${value.toFixed(value >= 99 ? 2 : 1)}%`;
}

function buildDistribution(
  rows: Record<string, unknown>[],
  column: PreviewColumn
): DistributionBar[] {
  const values = rows
    .map((row) => row[column.name])
    .filter((value) => !isMissing(value));

  if (column.type === 'number') {
    const numbers = values
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value));
    if (numbers.length === 0) return [];

    const minimum = Math.min(...numbers);
    const maximum = Math.max(...numbers);
    if (minimum === maximum) {
      return [{ label: String(minimum), count: numbers.length }];
    }

    const binCount = 8;
    const width = (maximum - minimum) / binCount;
    const counts = Array.from({ length: binCount }, () => 0);
    for (const value of numbers) {
      const index = Math.min(
        binCount - 1,
        Math.floor((value - minimum) / width)
      );
      counts[index] += 1;
    }
    return counts.map((count, index) => ({
      label: `${(minimum + index * width).toFixed(1)}`,
      count,
    }));
  }

  const counts = new Map<string, number>();
  for (const value of values) {
    const label = String(value);
    counts.set(label, (counts.get(label) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, 8)
    .map(([label, count]) => ({ label, count }));
}

function pearsonCorrelation(points: Array<[number, number]>): number {
  if (points.length < 3) return 0;
  const meanX =
    points.reduce((total, [value]) => total + value, 0) / points.length;
  const meanY =
    points.reduce((total, [, value]) => total + value, 0) / points.length;

  let numerator = 0;
  let sumX = 0;
  let sumY = 0;
  for (const [x, y] of points) {
    const deltaX = x - meanX;
    const deltaY = y - meanY;
    numerator += deltaX * deltaY;
    sumX += deltaX * deltaX;
    sumY += deltaY * deltaY;
  }

  const denominator = Math.sqrt(sumX * sumY);
  return denominator === 0 ? 0 : numerator / denominator;
}

function buildRelations(
  columns: PreviewColumn[],
  rows: Record<string, unknown>[]
): NumericRelation[] {
  const numericColumns = columns
    .filter((column) => column.type === 'number')
    .slice(0, 12);
  const relations: NumericRelation[] = [];

  for (let leftIndex = 0; leftIndex < numericColumns.length; leftIndex += 1) {
    for (
      let rightIndex = leftIndex + 1;
      rightIndex < numericColumns.length;
      rightIndex += 1
    ) {
      const left = numericColumns[leftIndex].name;
      const right = numericColumns[rightIndex].name;
      const points = rows
        .map((row): [number, number] | null => {
          if (isMissing(row[left]) || isMissing(row[right])) return null;
          const x = Number(row[left]);
          const y = Number(row[right]);
          return Number.isFinite(x) && Number.isFinite(y) ? [x, y] : null;
        })
        .filter((point): point is [number, number] => point !== null);

      if (points.length >= 3) {
        relations.push({
          left,
          right,
          correlation: pearsonCorrelation(points),
          points,
        });
      }
    }
  }

  return relations
    .sort(
      (left, right) =>
        Math.abs(right.correlation) - Math.abs(left.correlation)
    )
    .slice(0, 4);
}

function buildReport(
  preview: DataPreviewResult,
  relations: NumericRelation[]
): ReportInsight[] {
  const totalCells = preview.totalRows * preview.columns.length;
  const missingCells = preview.columns.reduce(
    (total, column) => total + column.nullCount,
    0
  );
  const completeness =
    totalCells === 0 ? 100 : ((totalCells - missingCells) / totalCells) * 100;
  const mostMissing = [...preview.columns].sort(
    (left, right) => right.nullCount - left.nullCount
  )[0];
  const highestCardinality = [...preview.columns].sort(
    (left, right) => right.distinctCount - left.distinctCount
  )[0];
  const numericColumns = preview.columns.filter(
    (column) => column.type === 'number'
  );
  const strongestRelation = relations[0];

  return [
    {
      tag: 'Completeness',
      title: `${formatPercent(completeness)} complete`,
      detail:
        missingCells === 0
          ? 'No empty cells were detected in the full dataset.'
          : `${formatCount(missingCells)} empty cells were detected across ${formatCount(totalCells)} cells.`,
    },
    {
      tag: 'Missing',
      title:
        mostMissing && mostMissing.nullCount > 0
          ? `${mostMissing.name} needs attention`
          : 'No missing-value hotspot',
      detail:
        mostMissing && mostMissing.nullCount > 0
          ? `${formatCount(mostMissing.nullCount)} rows are empty in this field.`
          : 'Every profiled column is complete.',
    },
    {
      tag: 'Cardinality',
      title: highestCardinality
        ? `${highestCardinality.name} has the most distinct values`
        : 'No columns available',
      detail: highestCardinality
        ? `${formatCount(highestCardinality.distinctCount)} distinct values across ${formatCount(preview.totalRows)} rows.`
        : 'Import a non-empty CSV to calculate cardinality.',
    },
    {
      tag: 'Numeric',
      title: `${numericColumns.length} numeric field${numericColumns.length === 1 ? '' : 's'}`,
      detail: strongestRelation
        ? `The strongest sampled relationship is ${strongestRelation.left} / ${strongestRelation.right} with R2 ${(
            strongestRelation.correlation ** 2
          ).toFixed(2)}.`
        : 'At least two populated numeric fields are required for correlation analysis.',
    },
    {
      tag: 'Coverage',
      title: `${formatCount(preview.rows.length)} rows loaded for inspection`,
      detail:
        preview.rows.length < preview.totalRows
          ? `The table and charts use the first ${formatCount(preview.rows.length)} rows; null and distinct counts cover all rows.`
          : 'The table, charts, and profiles cover the full dataset.',
    },
  ];
}

const ScatterPlot: React.FC<{ relation: NumericRelation }> = ({
  relation,
}) => {
  const points = relation.points.slice(0, 100);
  const xValues = points.map(([value]) => value);
  const yValues = points.map(([, value]) => value);
  const minimumX = Math.min(...xValues);
  const maximumX = Math.max(...xValues);
  const minimumY = Math.min(...yValues);
  const maximumY = Math.max(...yValues);
  const widthX = maximumX - minimumX || 1;
  const widthY = maximumY - minimumY || 1;

  return (
    <svg
      viewBox="0 0 300 120"
      className="h-[120px] w-full"
      role="img"
      aria-label={`${relation.left} and ${relation.right} scatter plot`}
    >
      <line x1="12" y1="108" x2="292" y2="108" stroke="#dedede" />
      <line x1="12" y1="8" x2="12" y2="108" stroke="#dedede" />
      {points.map(([x, y], index) => (
        <circle
          key={`${x}-${y}-${index}`}
          cx={12 + ((x - minimumX) / widthX) * 280}
          cy={108 - ((y - minimumY) / widthY) * 100}
          r="2"
          fill="#1c1c1a"
          opacity="0.55"
        />
      ))}
    </svg>
  );
};

const CsvPreviewCard: React.FC<CsvPreviewCardProps> = ({
  dataset,
  onClose,
}) => {
  const [activeTab, setActiveTab] = useState<PreviewTab>('summary');
  const [preview, setPreview] = useState<DataPreviewResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloadToken, setReloadToken] = useState(0);
  const [page, setPage] = useState(0);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] =
    useState<SortDirection>('asc');

  useEffect(() => {
    let current = true;
    setLoading(true);
    setError(null);
    setPreview(null);
    setActiveTab('summary');
    setPage(0);
    setSortColumn(null);

    void previewBackendDataset(dataset.id, 500)
      .then((result) => {
        if (!current) return;
        setPreview(result);
        setLoading(false);
      })
      .catch((previewError) => {
        if (!current) return;
        const message =
          previewError instanceof Error
            ? previewError.message
            : 'Dataset preview failed';
        setError(message);
        setLoading(false);
      });

    return () => {
      current = false;
    };
  }, [dataset.id, reloadToken]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  const relations = useMemo(
    () => (preview ? buildRelations(preview.columns, preview.rows) : []),
    [preview]
  );
  const report = useMemo(
    () => (preview ? buildReport(preview, relations) : []),
    [preview, relations]
  );
  const columnTypes = useMemo(
    () =>
      new Map(
        preview?.columns.map((column) => [column.name, column.type]) ?? []
      ),
    [preview]
  );
  const sortedRows = useMemo(() => {
    if (!preview || !sortColumn) return preview?.rows ?? [];
    const type = columnTypes.get(sortColumn);
    const direction = sortDirection === 'asc' ? 1 : -1;
    return [...preview.rows].sort((left, right) => {
      const leftValue = left[sortColumn];
      const rightValue = right[sortColumn];
      if (isMissing(leftValue) && isMissing(rightValue)) return 0;
      if (isMissing(leftValue)) return 1;
      if (isMissing(rightValue)) return -1;
      if (type === 'number') {
        return (Number(leftValue) - Number(rightValue)) * direction;
      }
      return (
        String(leftValue).localeCompare(String(rightValue), undefined, {
          numeric: true,
          sensitivity: 'base',
        }) * direction
      );
    });
  }, [columnTypes, preview, sortColumn, sortDirection]);

  const pageCount = Math.max(1, Math.ceil(sortedRows.length / PAGE_SIZE));
  const pageRows = sortedRows.slice(
    page * PAGE_SIZE,
    (page + 1) * PAGE_SIZE
  );
  const missingCells =
    preview?.columns.reduce(
      (total, column) => total + column.nullCount,
      0
    ) ?? 0;
  const totalCells = (preview?.totalRows ?? 0) * (preview?.columns.length ?? 0);
  const completeness =
    totalCells === 0 ? 100 : ((totalCells - missingCells) / totalCells) * 100;
  const uniqueColumns =
    preview?.columns.filter(
      (column) =>
        column.nullCount === 0 &&
        column.distinctCount === preview.totalRows &&
        preview.totalRows > 0
    ).length ?? 0;

  const tabs: Array<{ key: PreviewTab; label: string; count?: number }> = [
    { key: 'summary', label: 'Summary' },
    { key: 'preview', label: 'Preview', count: preview?.totalRows },
    { key: 'schema', label: 'Schema', count: preview?.columns.length },
    { key: 'relations', label: 'Relations' },
    { key: 'report', label: 'Report' },
  ];

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/20 p-4"
      onMouseDown={onClose}
    >
      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="csv-preview-title"
        className="max-h-[calc(100vh-32px)] w-full max-w-[960px] overflow-y-auto rounded-lg border border-gray-200 bg-white px-5 pb-5 pt-6 shadow-xl sm:px-7"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h2
              id="csv-preview-title"
              className="truncate text-[18px] font-bold leading-6 text-[#1c1c1a]"
            >
              {dataset.name}
            </h2>
            <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-gray-500">
              <span>
                <strong className="font-semibold text-gray-800">
                  {preview ? formatCount(preview.totalRows) : '--'}
                </strong>{' '}
                rows
              </span>
              <span>
                <strong className="font-semibold text-gray-800">
                  {preview ? preview.columns.length : '--'}
                </strong>{' '}
                columns
              </span>
              <span>{dataset.type.toUpperCase()}</span>
              {dataset.source && (
                <span className="capitalize">{dataset.source} source</span>
              )}
              <span className="capitalize">{dataset.category} dataset</span>
            </div>
          </div>
          <button
            type="button"
            className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700"
            aria-label="Close CSV preview"
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </header>

        <nav
          className="mt-4 flex overflow-x-auto border-b border-gray-200"
          aria-label="CSV preview views"
        >
          {tabs.map((tab) => (
            <button
              key={tab.key}
              type="button"
              className={`relative h-9 flex-shrink-0 rounded-t-md px-3 text-[12px] transition-colors ${
                activeTab === tab.key
                  ? 'font-semibold text-[#1c1c1a] after:absolute after:bottom-[-1px] after:left-1 after:right-1 after:h-0.5 after:bg-[#1c1c1a]'
                  : 'font-medium text-gray-500 hover:bg-gray-50 hover:text-gray-800'
              }`}
              onClick={() => {
                setActiveTab(tab.key);
                setPage(0);
              }}
            >
              {tab.label}
              {tab.count !== undefined && (
                <span className="ml-1 text-[9px] font-normal text-gray-400">
                  {formatCount(tab.count)}
                </span>
              )}
            </button>
          ))}
        </nav>

        {loading && (
          <div className="flex min-h-[320px] items-center justify-center text-[13px] text-gray-500">
            Loading CSV preview...
          </div>
        )}

        {!loading && error && (
          <div className="flex min-h-[320px] flex-col items-center justify-center gap-3 text-center">
            <p className="text-[13px] text-red-600">{error}</p>
            <button
              type="button"
              className="h-8 rounded-md border border-gray-200 px-3 text-[12px] font-medium text-gray-700 hover:bg-gray-50"
              onClick={() => setReloadToken((current) => current + 1)}
            >
              Retry
            </button>
          </div>
        )}

        {!loading && !error && preview && activeTab === 'summary' && (
          <div className="min-h-[320px] pt-4">
            <p className="mb-3 text-[12px] text-gray-500">
              Select a metric to open its detailed view.
            </p>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
              {[
                {
                  value: formatCount(preview.totalRows),
                  label: 'records',
                  hint: 'view table',
                  width: 100,
                  target: 'preview' as PreviewTab,
                },
                {
                  value: String(preview.columns.length),
                  label: 'fields',
                  hint: 'view schema',
                  width: 100,
                  target: 'schema' as PreviewTab,
                },
                {
                  value: formatCount(missingCells),
                  label: 'missing',
                  hint: 'all cells',
                  width:
                    totalCells === 0 ? 0 : (missingCells / totalCells) * 100,
                  target: 'schema' as PreviewTab,
                },
                {
                  value: formatPercent(completeness),
                  label: 'complete',
                  hint: 'view report',
                  width: completeness,
                  target: 'report' as PreviewTab,
                },
                {
                  value: String(uniqueColumns),
                  label: 'unique fields',
                  hint: 'candidate keys',
                  width:
                    preview.columns.length === 0
                      ? 0
                      : (uniqueColumns / preview.columns.length) * 100,
                  target: 'report' as PreviewTab,
                },
              ].map((stat) => (
                <button
                  key={stat.label}
                  type="button"
                  className="rounded-lg bg-gray-50 px-3.5 py-3 text-left transition-colors hover:bg-gray-100"
                  onClick={() => setActiveTab(stat.target)}
                >
                  <span className="block text-[22px] font-extrabold leading-6 text-[#1c1c1a]">
                    {stat.value}
                  </span>
                  <span className="mt-1 block text-[10px] text-gray-500">
                    {stat.label}
                  </span>
                  <span className="block text-[9px] text-gray-400">
                    {stat.hint}
                  </span>
                  <span className="mt-2 block h-0.5 overflow-hidden rounded-full bg-gray-200">
                    <span
                      className="block h-full rounded-full bg-[#1c1c1a]"
                      style={{ width: `${Math.max(0, stat.width)}%` }}
                    />
                  </span>
                </button>
              ))}
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              <span className="rounded-full bg-[#1c1c1a] px-3 py-1.5 text-[11px] font-medium capitalize text-white">
                {dataset.category}
              </span>
              <span className="rounded-full bg-gray-200 px-3 py-1.5 text-[11px] font-medium text-gray-700">
                {dataset.type.toUpperCase()}
              </span>
              <span className="rounded-full bg-gray-100 px-3 py-1.5 text-[11px] font-medium text-gray-500">
                {formatCount(preview.rows.length)} rows loaded
              </span>
            </div>
            <p className="mt-4 text-[9px] font-medium uppercase tracking-[0.08em] text-gray-400">
              Dataset profile based on persisted CSV data
            </p>
          </div>
        )}

        {!loading && !error && preview && activeTab === 'preview' && (
          <div className="min-h-[320px] pt-4">
            <p className="mb-3 text-[12px] text-gray-500">
              {preview.rows.length < preview.totalRows
                ? `First ${formatCount(preview.rows.length)} of ${formatCount(preview.totalRows)} rows, sortable.`
                : `All ${formatCount(preview.totalRows)} rows, sortable.`}
            </p>
            <div className="max-h-[440px] overflow-auto">
              <table className="w-full min-w-max border-collapse whitespace-nowrap text-[12px]">
                <thead className="sticky top-0 z-10 bg-white">
                  <tr>
                    {preview.columns.map((column) => {
                      const isSorted = sortColumn === column.name;
                      return (
                        <th
                          key={column.name}
                          className="border-b border-gray-200 px-2 py-2 text-left text-[10px] font-semibold uppercase tracking-[0.06em] text-gray-500"
                        >
                          <button
                            type="button"
                            className="flex w-full items-center gap-1 hover:text-gray-900"
                            onClick={() => {
                              setPage(0);
                              if (isSorted) {
                                setSortDirection((current) =>
                                  current === 'asc' ? 'desc' : 'asc'
                                );
                              } else {
                                setSortColumn(column.name);
                                setSortDirection('asc');
                              }
                            }}
                          >
                            {column.name}
                            <span className="rounded bg-gray-100 px-1 text-[8px] font-medium normal-case tracking-normal text-gray-400">
                              {column.type}
                            </span>
                            {isSorted && (
                              <ChevronDown
                                size={11}
                                className={
                                  sortDirection === 'asc'
                                    ? 'ml-auto rotate-180 text-gray-800'
                                    : 'ml-auto text-gray-800'
                                }
                              />
                            )}
                          </button>
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {pageRows.map((row, rowIndex) => (
                    <tr key={`${page}-${rowIndex}`} className="group">
                      {preview.columns.map((column) => {
                        const value = row[column.name];
                        return (
                          <td
                            key={column.name}
                            className={`max-w-[240px] truncate border-b border-gray-100 px-2 py-1.5 group-hover:bg-gray-50 ${
                              column.type === 'number'
                                ? 'text-right font-medium tabular-nums'
                                : 'text-left'
                            }`}
                            title={isMissing(value) ? 'Empty' : String(value)}
                          >
                            {isMissing(value) ? (
                              <span className="text-gray-300">--</span>
                            ) : (
                              String(value)
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
              {pageRows.length === 0 && (
                <div className="py-16 text-center text-[12px] text-gray-400">
                  This CSV contains no data rows.
                </div>
              )}
            </div>
            <div className="mt-3 flex items-center gap-2 text-[11px] text-gray-500">
              <button
                type="button"
                className="h-7 rounded-md border border-gray-200 px-2.5 font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-default disabled:opacity-30"
                disabled={page === 0}
                onClick={() => setPage((current) => Math.max(0, current - 1))}
              >
                Prev
              </button>
              <span>
                {sortedRows.length === 0 ? 0 : page + 1} /{' '}
                {sortedRows.length === 0 ? 0 : pageCount}
              </span>
              <button
                type="button"
                className="h-7 rounded-md border border-gray-200 px-2.5 font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-default disabled:opacity-30"
                disabled={page >= pageCount - 1 || sortedRows.length === 0}
                onClick={() =>
                  setPage((current) => Math.min(pageCount - 1, current + 1))
                }
              >
                Next
              </button>
              <span className="ml-auto">
                {pageRows.length > 0
                  ? `${page * PAGE_SIZE + 1}-${page * PAGE_SIZE + pageRows.length} of ${formatCount(sortedRows.length)} loaded`
                  : '0 rows'}
              </span>
            </div>
            <p className="mt-3 text-[9px] font-medium uppercase tracking-[0.08em] text-gray-400">
              Sortable and paginated preview
            </p>
          </div>
        )}

        {!loading && !error && preview && activeTab === 'schema' && (
          <div className="min-h-[320px] pt-4">
            <p className="mb-3 text-[12px] text-gray-500">
              Full-column profiles with sampled value distributions.
            </p>
            <div className="grid gap-3 md:grid-cols-2">
              {preview.columns.map((column) => {
                const distribution = buildDistribution(preview.rows, column);
                const maximum = Math.max(
                  1,
                  ...distribution.map((bar) => bar.count)
                );
                const columnCompleteness =
                  preview.totalRows === 0
                    ? 100
                    : ((preview.totalRows - column.nullCount) /
                        preview.totalRows) *
                      100;
                return (
                  <article
                    key={column.name}
                    className="rounded-lg bg-gray-50 px-3.5 py-3"
                  >
                    <div className="flex items-center justify-between gap-3">
                      <h3 className="truncate text-[11px] font-bold uppercase tracking-[0.04em] text-[#1c1c1a]">
                        {column.name}
                      </h3>
                      <span className="text-[10px] text-gray-500">
                        {column.type}
                      </span>
                    </div>
                    <p className="mt-1 text-[10px] text-gray-700">
                      <span className="text-gray-500">unique</span>{' '}
                      {formatCount(column.distinctCount)}
                      <span className="text-gray-400">
                        {' '}
                        / {formatCount(column.nullCount)} missing
                      </span>
                    </p>
                    <div className="mt-1 h-0.5 overflow-hidden rounded-full bg-gray-200">
                      <div
                        className="h-full rounded-full bg-[#1c1c1a]"
                        style={{ width: `${columnCompleteness}%` }}
                      />
                    </div>
                    {distribution.length > 0 ? (
                      <div className="mt-3 flex h-9 items-end gap-1">
                        {distribution.map((bar, index) => (
                          <div
                            key={`${bar.label}-${index}`}
                            className="min-h-0.5 flex-1 rounded-sm bg-gray-400"
                            style={{
                              height: `${Math.max(8, (bar.count / maximum) * 100)}%`,
                            }}
                            title={`${bar.label}: ${bar.count}`}
                          />
                        ))}
                      </div>
                    ) : (
                      <p className="mt-3 text-[10px] text-gray-400">
                        No non-empty values to chart.
                      </p>
                    )}
                  </article>
                );
              })}
            </div>
            <p className="mt-4 text-[9px] font-medium uppercase tracking-[0.08em] text-gray-400">
              {preview.columns.length} columns, auto-profiled
            </p>
          </div>
        )}

        {!loading && !error && preview && activeTab === 'relations' && (
          <div className="min-h-[320px] pt-4">
            <p className="mb-3 text-[12px] text-gray-500">
              Sampled relationships between populated numeric fields.
            </p>
            {relations.length > 0 ? (
              <div className="grid gap-3 md:grid-cols-2">
                {relations.map((relation) => (
                  <article
                    key={`${relation.left}-${relation.right}`}
                    className="rounded-lg bg-gray-50 p-3.5"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <h3 className="truncate text-[11px] font-bold uppercase tracking-[0.04em] text-[#1c1c1a]">
                          {relation.left} / {relation.right}
                        </h3>
                        <p className="mt-1 text-[10px] text-gray-500">
                          {formatCount(relation.points.length)} paired values
                        </p>
                      </div>
                      <span className="text-[18px] font-extrabold text-[#1c1c1a]">
                        R2 {(relation.correlation ** 2).toFixed(2)}
                      </span>
                    </div>
                    <ScatterPlot relation={relation} />
                    <p className="text-[10px] text-gray-500">
                      {Math.abs(relation.correlation) >= 0.7
                        ? 'Strong'
                        : Math.abs(relation.correlation) >= 0.4
                          ? 'Moderate'
                          : 'Weak'}{' '}
                      {relation.correlation >= 0 ? 'positive' : 'negative'}{' '}
                      sampled correlation
                    </p>
                  </article>
                ))}
              </div>
            ) : (
              <div className="flex min-h-[240px] items-center justify-center text-center text-[12px] text-gray-400">
                Relations require at least two populated numeric columns.
              </div>
            )}
            <p className="mt-4 text-[9px] font-medium uppercase tracking-[0.08em] text-gray-400">
              Deterministic Pearson correlation
            </p>
          </div>
        )}

        {!loading && !error && preview && activeTab === 'report' && (
          <div className="min-h-[320px] pt-4">
            <p className="mb-2 text-[12px] text-gray-500">
              Five deterministic findings from this dataset.
            </p>
            <div>
              {report.map((insight, index) => (
                <article
                  key={insight.tag}
                  className="flex gap-3 border-b border-gray-100 py-3 last:border-b-0"
                >
                  <span className="mt-0.5 flex h-[18px] w-[18px] flex-shrink-0 items-center justify-center rounded-full bg-[#1c1c1a] text-[9px] font-bold text-white">
                    {index + 1}
                  </span>
                  <div className="text-[12px] leading-5 text-gray-600">
                    <span className="mr-1.5 inline-block rounded bg-gray-100 px-1.5 text-[9px] font-semibold uppercase text-gray-500">
                      {insight.tag}
                    </span>
                    <strong className="font-semibold text-[#1c1c1a]">
                      {insight.title}.
                    </strong>{' '}
                    {insight.detail}
                  </div>
                </article>
              ))}
            </div>
            <p className="mt-3 text-[9px] font-medium uppercase tracking-[0.08em] text-gray-400">
              Generated from persisted profile metrics
            </p>
          </div>
        )}
      </section>
    </div>
  );
};

export default CsvPreviewCard;
