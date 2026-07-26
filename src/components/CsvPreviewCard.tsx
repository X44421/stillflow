import React, { useEffect, useMemo, useState } from 'react';
import {
  Check,
  ChevronDown,
  Copy,
  Filter,
  Maximize2,
  Minimize2,
  Minus,
  Search,
  Type,
  X,
} from '../icons/hero';
import { defaultConfig } from '../data';
import type {
  DataPreviewResult,
  Dataset,
  PipelineNode,
  PreviewColumn,
} from '../types';
import { previewBackendDataset } from '../utils/api';

const PAGE_SIZE_OPTIONS = [25, 50, 100];

type PreviewTab = 'data' | 'profile' | 'relations' | 'issues';
type SortDirection = 'asc' | 'desc';
type PreviewDisplayMode = 'docked' | 'minimized' | 'fullscreen';
type IssueKind = 'missing' | 'duplicates' | 'whitespace';
type IssueSeverity = 'high' | 'medium' | 'low';

interface CsvPreviewCardProps {
  dataset: Dataset;
  onClose: () => void;
  onCreateNode?: (node: PipelineNode) => void;
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

interface QualityIssue {
  id: string;
  kind: IssueKind;
  severity: IssueSeverity;
  title: string;
  detail: string;
  affected: number;
  column?: string;
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

function formatMetric(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '--';
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 2,
  }).format(value);
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
      return [{ label: formatMetric(minimum), count: numbers.length }];
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
      label: formatMetric(minimum + index * width),
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

  return relations.sort(
    (left, right) =>
      Math.abs(right.correlation) - Math.abs(left.correlation)
  );
}

function severityFor(count: number, totalRows: number): IssueSeverity {
  const ratio = totalRows === 0 ? 0 : count / totalRows;
  if (ratio >= 0.15) return 'high';
  if (ratio >= 0.05) return 'medium';
  return 'low';
}

function buildIssues(preview: DataPreviewResult): QualityIssue[] {
  const issues: QualityIssue[] = [];

  if (preview.duplicateRows > 0) {
    issues.push({
      id: 'duplicate-rows',
      kind: 'duplicates',
      severity: severityFor(preview.duplicateRows, preview.totalRows),
      title: `${formatCount(preview.duplicateRows)} duplicate rows`,
      detail: 'Exact duplicate records can be removed deterministically.',
      affected: preview.duplicateRows,
    });
  }

  for (const column of preview.columns) {
    if (column.nullCount > 0) {
      issues.push({
        id: `missing:${column.name}`,
        kind: 'missing',
        severity: severityFor(column.nullCount, preview.totalRows),
        title: `${column.name} contains missing values`,
        detail: `${formatCount(column.nullCount)} rows have an empty value in this field.`,
        affected: column.nullCount,
        column: column.name,
      });
    }
    if (column.whitespaceCount > 0) {
      issues.push({
        id: `whitespace:${column.name}`,
        kind: 'whitespace',
        severity: severityFor(column.whitespaceCount, preview.totalRows),
        title: `${column.name} contains surrounding whitespace`,
        detail: `${formatCount(column.whitespaceCount)} values can be normalized without changing their meaning.`,
        affected: column.whitespaceCount,
        column: column.name,
      });
    }
  }

  const severityRank: Record<IssueSeverity, number> = {
    high: 0,
    medium: 1,
    low: 2,
  };
  return issues.sort(
    (left, right) =>
      severityRank[left.severity] - severityRank[right.severity] ||
      right.affected - left.affected
  );
}

function nodeForIssue(issue: QualityIssue): PipelineNode {
  const id = `n${Date.now()}-${issue.kind}`;
  if (issue.kind === 'missing') {
    return {
      id,
      type: 'filter',
      name: `Filter empty ${issue.column}`,
      description: `Remove rows missing ${issue.column}`,
      rows: '',
      status: 'pending',
      config: {
        ...defaultConfig,
        column: issue.column ?? '',
        nullHandling: 'Remove null rows',
      },
    };
  }
  if (issue.kind === 'duplicates') {
    return {
      id,
      type: 'deduplicate',
      name: 'Remove duplicate rows',
      description: 'Keep the first exact record',
      rows: '',
      status: 'pending',
      config: {
        ...defaultConfig,
        column: '',
        strategy: 'Keep first',
      },
    };
  }
  return {
    id,
    type: 'normalize',
    name: `Normalize ${issue.column}`,
    description: `Trim values in ${issue.column}`,
    rows: '',
    status: 'pending',
    config: {
      ...defaultConfig,
      column: issue.column ?? '',
    },
  };
}

function issueActionLabel(issue: QualityIssue): string {
  if (issue.kind === 'missing') return 'Create filter node';
  if (issue.kind === 'duplicates') return 'Create deduplicate node';
  return 'Create normalize node';
}

function relationStrength(correlation: number): string {
  const absolute = Math.abs(correlation);
  if (absolute >= 0.7) return 'Strong';
  if (absolute >= 0.4) return 'Moderate';
  return 'Weak';
}

const ScatterPlot: React.FC<{ relation: NumericRelation }> = ({
  relation,
}) => {
  const points = relation.points.slice(0, 160);
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
      viewBox="0 0 520 180"
      className="h-[180px] w-full"
      role="img"
      aria-label={`${relation.left} and ${relation.right} scatter plot`}
    >
      <line x1="18" y1="160" x2="506" y2="160" stroke="#dedede" />
      <line x1="18" y1="12" x2="18" y2="160" stroke="#dedede" />
      {points.map(([x, y], index) => (
        <circle
          key={`${x}-${y}-${index}`}
          cx={18 + ((x - minimumX) / widthX) * 488}
          cy={160 - ((y - minimumY) / widthY) * 148}
          r="2.25"
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
  onCreateNode,
}) => {
  const [activeTab, setActiveTab] = useState<PreviewTab>('data');
  const [preview, setPreview] = useState<DataPreviewResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [updating, setUpdating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [reloadToken, setReloadToken] = useState(0);
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] =
    useState<SortDirection>('asc');
  const [searchInput, setSearchInput] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedColumn, setSelectedColumn] = useState('');
  const [relationIndex, setRelationIndex] = useState(0);
  const [createdIssues, setCreatedIssues] = useState<Set<string>>(new Set());
  const [displayMode, setDisplayMode] =
    useState<PreviewDisplayMode>('docked');

  useEffect(() => {
    setActiveTab('data');
    setPreview(null);
    setLoading(true);
    setError(null);
    setPage(0);
    setSortColumn(null);
    setSortDirection('asc');
    setSearchInput('');
    setSearchQuery('');
    setSelectedColumn('');
    setRelationIndex(0);
    setCreatedIssues(new Set());
  }, [dataset.id]);

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setPage(0);
      setSearchQuery(searchInput.trim());
    }, 250);
    return () => window.clearTimeout(timeout);
  }, [searchInput]);

  useEffect(() => {
    let current = true;
    const hasCurrentPreview = preview?.tableName === dataset.id;
    setLoading(!hasCurrentPreview);
    setUpdating(hasCurrentPreview);
    setError(null);

    void previewBackendDataset(dataset.id, {
      offset: page * pageSize,
      limit: pageSize,
      sortBy: sortColumn ?? undefined,
      sortDirection: sortColumn ? sortDirection : undefined,
      search: searchQuery || undefined,
    })
      .then((result) => {
        if (!current) return;
        setPreview(result);
        setSelectedColumn((column) =>
          result.columns.some((item) => item.name === column)
            ? column
            : result.columns[0]?.name ?? ''
        );
        setLoading(false);
        setUpdating(false);
      })
      .catch((previewError) => {
        if (!current) return;
        const message =
          previewError instanceof Error
            ? previewError.message
            : 'Dataset preview failed';
        setError(message);
        setLoading(false);
        setUpdating(false);
      });

    return () => {
      current = false;
    };
  }, [
    dataset.id,
    page,
    pageSize,
    preview?.tableName,
    reloadToken,
    searchQuery,
    sortColumn,
    sortDirection,
  ]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      if (displayMode === 'fullscreen') {
        setDisplayMode('docked');
        return;
      }
      onClose();
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [displayMode, onClose]);

  const sampleRows = preview?.sampleRows ?? [];
  const relations = useMemo(
    () => (preview ? buildRelations(preview.columns, sampleRows) : []),
    [preview, sampleRows]
  );
  const issues = useMemo(
    () => (preview ? buildIssues(preview) : []),
    [preview]
  );
  const selectedProfile =
    preview?.columns.find((column) => column.name === selectedColumn) ??
    preview?.columns[0] ??
    null;
  const selectedDistribution = useMemo(
    () =>
      selectedProfile
        ? buildDistribution(sampleRows, selectedProfile)
        : [],
    [sampleRows, selectedProfile]
  );
  const selectedRelation =
    relations[Math.min(relationIndex, Math.max(0, relations.length - 1))] ??
    null;
  const pageCount = Math.max(
    1,
    Math.ceil((preview?.filteredRows ?? 0) / pageSize)
  );
  const missingCells =
    preview?.columns.reduce(
      (total, column) => total + column.nullCount,
      0
    ) ?? 0;
  const whitespaceCells =
    preview?.columns.reduce(
      (total, column) => total + (column.whitespaceCount ?? 0),
      0
    ) ?? 0;
  const totalCells = (preview?.totalRows ?? 0) * (preview?.columns.length ?? 0);
  const completeness =
    totalCells === 0 ? 100 : ((totalCells - missingCells) / totalCells) * 100;
  const qualityPenalty =
    totalCells === 0
      ? 0
      : (missingCells / totalCells) * 60 +
        (whitespaceCells / totalCells) * 20 +
        ((preview?.duplicateRows ?? 0) / Math.max(1, preview?.totalRows ?? 0)) *
          20;
  const qualityScore = Math.max(0, 100 - qualityPenalty);
  const minimized = displayMode === 'minimized';
  const fullscreen = displayMode === 'fullscreen';
  const selectedRelationIndex = selectedRelation
    ? relations.indexOf(selectedRelation)
    : 0;

  const tabs: Array<{ key: PreviewTab; label: string; count?: number }> = [
    {
      key: 'data',
      label: 'Data',
      count: preview?.filteredRows ?? preview?.totalRows,
    },
    { key: 'profile', label: 'Profile', count: preview?.columns.length },
    { key: 'relations', label: 'Relations', count: relations.length },
    { key: 'issues', label: 'Issues', count: issues.length },
  ];

  const handleSort = (column: PreviewColumn) => {
    setPage(0);
    if (sortColumn === column.name) {
      setSortDirection((current) => (current === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortColumn(column.name);
    setSortDirection('asc');
  };

  const handleCreateIssueNode = (issue: QualityIssue) => {
    if (!onCreateNode || createdIssues.has(issue.id)) return;
    onCreateNode(nodeForIssue(issue));
    setCreatedIssues((current) => new Set(current).add(issue.id));
  };

  return (
    <section
      role={fullscreen ? 'dialog' : 'region'}
      aria-modal={fullscreen ? true : undefined}
      aria-labelledby="csv-preview-title"
      className={
        fullscreen
          ? 'fixed inset-0 z-50 flex min-h-0 flex-col overflow-hidden bg-white'
          : minimized
            ? 'h-11 flex-shrink-0 overflow-hidden border-t border-gray-200 bg-white shadow-[0_-8px_24px_rgba(16,24,40,0.04)]'
            : 'flex h-[min(44vh,520px)] min-h-[280px] flex-shrink-0 flex-col overflow-hidden border-t border-gray-200 bg-white shadow-[0_-8px_24px_rgba(16,24,40,0.04)]'
      }
    >
      <header
        className={`flex flex-shrink-0 items-center justify-between gap-4 px-5 sm:px-7 ${
          minimized ? 'h-11' : 'pb-3 pt-4'
        }`}
      >
        <div className="min-w-0">
          <h2
            id="csv-preview-title"
            className={`truncate font-bold text-[#1c1c1a] ${
              minimized ? 'text-[13px] leading-5' : 'text-[18px] leading-6'
            }`}
          >
            {dataset.name}
          </h2>
          {!minimized && (
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
              {updating && <span>Updating...</span>}
            </div>
          )}
        </div>
        <div className="flex flex-shrink-0 items-center gap-0.5">
          {minimized ? (
            <button
              type="button"
              className="flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700"
              aria-label="Expand CSV preview"
              title="Expand preview"
              onClick={() => setDisplayMode('docked')}
            >
              <ChevronDown size={16} className="rotate-180" />
            </button>
          ) : (
            <button
              type="button"
              className="flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700"
              aria-label="Minimize CSV preview"
              title="Minimize preview"
              onClick={() => setDisplayMode('minimized')}
            >
              <Minus size={16} />
            </button>
          )}
          <button
            type="button"
            className="flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700"
            aria-label={fullscreen ? 'Restore CSV preview' : 'Fullscreen CSV preview'}
            title={fullscreen ? 'Restore preview' : 'Fullscreen preview'}
            onClick={() =>
              setDisplayMode((current) =>
                current === 'fullscreen' ? 'docked' : 'fullscreen'
              )
            }
          >
            {fullscreen ? <Minimize2 size={16} /> : <Maximize2 size={16} />}
          </button>
          <button
            type="button"
            className="flex h-8 w-8 items-center justify-center rounded-md text-gray-400 transition-colors hover:bg-gray-100 hover:text-gray-700"
            aria-label="Close CSV preview"
            title="Close preview"
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </div>
      </header>

      {!minimized && (
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden px-5 pb-5 sm:px-7">
          <nav
            className="flex flex-shrink-0 overflow-x-auto border-b border-gray-200"
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
                onClick={() => setActiveTab(tab.key)}
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

          <div className="min-h-0 flex-1 overflow-y-auto">
            {loading && (
              <div className="flex min-h-[260px] items-center justify-center text-[13px] text-gray-500">
                Loading CSV preview...
              </div>
            )}

            {!loading && error && (
              <div className="flex min-h-[260px] flex-col items-center justify-center gap-3 text-center">
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

            {!loading && !error && preview && activeTab === 'data' && (
              <div className="min-h-[260px] pt-4">
                <div className="mb-3 flex flex-wrap items-center gap-2">
                  <label className="relative min-w-[220px] flex-1">
                    <Search
                      size={14}
                      className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400"
                    />
                    <input
                      type="search"
                      value={searchInput}
                      placeholder="Search all columns"
                      className="h-8 w-full rounded-md border border-gray-200 bg-white pl-8 pr-8 text-[12px] text-gray-800 outline-none transition-colors placeholder:text-gray-400 focus:border-gray-400"
                      onChange={(event) => setSearchInput(event.target.value)}
                    />
                    {searchInput && (
                      <button
                        type="button"
                        className="absolute right-1 top-1/2 flex h-6 w-6 -translate-y-1/2 items-center justify-center rounded text-gray-400 hover:bg-gray-100 hover:text-gray-700"
                        aria-label="Clear search"
                        onClick={() => setSearchInput('')}
                      >
                        <X size={12} />
                      </button>
                    )}
                  </label>
                  <select
                    value={pageSize}
                    className="h-8 rounded-md border border-gray-200 bg-white px-2 text-[11px] text-gray-600 outline-none focus:border-gray-400"
                    aria-label="Rows per page"
                    onChange={(event) => {
                      setPage(0);
                      setPageSize(Number(event.target.value));
                    }}
                  >
                    {PAGE_SIZE_OPTIONS.map((size) => (
                      <option key={size} value={size}>
                        {size} rows
                      </option>
                    ))}
                  </select>
                  <span className="text-[11px] text-gray-400">
                    {preview.filteredRows === preview.totalRows
                      ? `${formatCount(preview.totalRows)} total`
                      : `${formatCount(preview.filteredRows)} of ${formatCount(preview.totalRows)}`}
                  </span>
                </div>

                <div className="max-h-[360px] overflow-auto">
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
                                onClick={() => handleSort(column)}
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
                    <tbody className={updating ? 'opacity-60' : undefined}>
                      {preview.rows.map((row, rowIndex) => (
                        <tr
                          key={preview.offset + rowIndex}
                          className="group"
                        >
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
                  {preview.rows.length === 0 && (
                    <div className="py-14 text-center text-[12px] text-gray-400">
                      {searchQuery
                        ? 'No rows match this search.'
                        : 'This CSV contains no data rows.'}
                    </div>
                  )}
                </div>

                <div className="mt-3 flex items-center gap-2 text-[11px] text-gray-500">
                  <button
                    type="button"
                    className="h-7 rounded-md border border-gray-200 px-2.5 font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-default disabled:opacity-30"
                    disabled={page === 0 || updating}
                    onClick={() => setPage((current) => Math.max(0, current - 1))}
                  >
                    Prev
                  </button>
                  <span>
                    {preview.filteredRows === 0 ? 0 : page + 1} /{' '}
                    {preview.filteredRows === 0 ? 0 : pageCount}
                  </span>
                  <button
                    type="button"
                    className="h-7 rounded-md border border-gray-200 px-2.5 font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-default disabled:opacity-30"
                    disabled={
                      page >= pageCount - 1 ||
                      preview.filteredRows === 0 ||
                      updating
                    }
                    onClick={() =>
                      setPage((current) => Math.min(pageCount - 1, current + 1))
                    }
                  >
                    Next
                  </button>
                  <span className="ml-auto">
                    {preview.rows.length > 0
                      ? `${preview.offset + 1}-${preview.offset + preview.rows.length} of ${formatCount(preview.filteredRows)}`
                      : '0 rows'}
                  </span>
                </div>
              </div>
            )}

            {!loading && !error && preview && activeTab === 'profile' && (
              <div className="min-h-[260px] pt-4">
                <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                  {[
                    [formatCount(preview.totalRows), 'records'],
                    [String(preview.columns.length), 'fields'],
                    [formatPercent(completeness), 'complete'],
                    [formatMetric(qualityScore), 'quality score'],
                  ].map(([value, label]) => (
                    <div key={label} className="rounded-lg bg-gray-50 px-3.5 py-3">
                      <span className="block text-[20px] font-extrabold leading-6 text-[#1c1c1a]">
                        {value}
                      </span>
                      <span className="mt-1 block text-[10px] text-gray-500">
                        {label}
                      </span>
                    </div>
                  ))}
                </div>

                <div className="mt-4 grid overflow-hidden rounded-lg border border-gray-200 md:grid-cols-[220px_minmax(0,1fr)]">
                  <div className="max-h-[300px] overflow-y-auto border-b border-gray-200 md:border-b-0 md:border-r">
                    {preview.columns.map((column) => (
                      <button
                        key={column.name}
                        type="button"
                        className={`flex w-full items-center justify-between gap-3 border-b border-gray-100 px-3 py-2.5 text-left last:border-b-0 ${
                          selectedProfile?.name === column.name
                            ? 'bg-gray-100'
                            : 'hover:bg-gray-50'
                        }`}
                        onClick={() => setSelectedColumn(column.name)}
                      >
                        <span className="min-w-0">
                          <span className="block truncate text-[11px] font-semibold text-gray-800">
                            {column.name}
                          </span>
                          <span className="block text-[9px] text-gray-400">
                            {column.type}
                          </span>
                        </span>
                        {column.nullCount > 0 && (
                          <span className="text-[9px] tabular-nums text-gray-400">
                            {formatCount(column.nullCount)} empty
                          </span>
                        )}
                      </button>
                    ))}
                  </div>

                  {selectedProfile && (
                    <div className="min-w-0 p-4">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <h3 className="truncate text-[13px] font-bold text-[#1c1c1a]">
                            {selectedProfile.name}
                          </h3>
                          <p className="mt-0.5 text-[10px] text-gray-400">
                            {selectedProfile.type} field
                          </p>
                        </div>
                        <button
                          type="button"
                          className="text-[10px] font-medium text-gray-500 hover:text-gray-900"
                          onClick={() => setActiveTab('issues')}
                        >
                          View issues
                        </button>
                      </div>

                      <div className="mt-3 grid grid-cols-3 gap-3 text-[10px]">
                        <div>
                          <span className="block text-gray-400">Distinct</span>
                          <strong className="mt-0.5 block font-semibold text-gray-800">
                            {formatCount(selectedProfile.distinctCount)}
                          </strong>
                        </div>
                        <div>
                          <span className="block text-gray-400">Missing</span>
                          <strong className="mt-0.5 block font-semibold text-gray-800">
                            {formatCount(selectedProfile.nullCount)}
                          </strong>
                        </div>
                        <div>
                          <span className="block text-gray-400">Whitespace</span>
                          <strong className="mt-0.5 block font-semibold text-gray-800">
                            {formatCount(selectedProfile.whitespaceCount)}
                          </strong>
                        </div>
                      </div>

                      {selectedProfile.type === 'number' && (
                        <div className="mt-3 grid grid-cols-3 gap-3 border-t border-gray-100 pt-3 text-[10px]">
                          <div>
                            <span className="block text-gray-400">Minimum</span>
                            <strong className="mt-0.5 block font-semibold text-gray-800">
                              {formatMetric(selectedProfile.minimum)}
                            </strong>
                          </div>
                          <div>
                            <span className="block text-gray-400">Average</span>
                            <strong className="mt-0.5 block font-semibold text-gray-800">
                              {formatMetric(selectedProfile.average)}
                            </strong>
                          </div>
                          <div>
                            <span className="block text-gray-400">Maximum</span>
                            <strong className="mt-0.5 block font-semibold text-gray-800">
                              {formatMetric(selectedProfile.maximum)}
                            </strong>
                          </div>
                        </div>
                      )}

                      <div className="mt-4">
                        <p className="mb-2 text-[9px] font-semibold uppercase tracking-[0.06em] text-gray-400">
                          Sample distribution
                        </p>
                        {selectedDistribution.length > 0 ? (
                          <div className="space-y-1.5">
                            {selectedDistribution.map((bar, index) => {
                              const maximum = Math.max(
                                ...selectedDistribution.map((item) => item.count)
                              );
                              return (
                                <div
                                  key={`${bar.label}-${index}`}
                                  className="grid grid-cols-[72px_minmax(0,1fr)_36px] items-center gap-2 text-[9px]"
                                >
                                  <span className="truncate text-gray-500" title={bar.label}>
                                    {bar.label}
                                  </span>
                                  <span className="h-1.5 overflow-hidden rounded-full bg-gray-100">
                                    <span
                                      className="block h-full rounded-full bg-gray-500"
                                      style={{
                                        width: `${(bar.count / Math.max(1, maximum)) * 100}%`,
                                      }}
                                    />
                                  </span>
                                  <span className="text-right tabular-nums text-gray-400">
                                    {formatCount(bar.count)}
                                  </span>
                                </div>
                              );
                            })}
                          </div>
                        ) : (
                          <p className="text-[10px] text-gray-400">
                            No non-empty values to profile.
                          </p>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {!loading && !error && preview && activeTab === 'relations' && (
              <div className="min-h-[260px] pt-4">
                {selectedRelation ? (
                  <>
                    <div className="mb-3 flex flex-wrap items-center gap-2">
                      <select
                        value={selectedRelationIndex}
                        className="h-8 min-w-[220px] rounded-md border border-gray-200 bg-white px-2 text-[11px] text-gray-700 outline-none focus:border-gray-400"
                        aria-label="Numeric field pair"
                        onChange={(event) =>
                          setRelationIndex(Number(event.target.value))
                        }
                      >
                        {relations.map((relation, index) => (
                          <option
                            key={`${relation.left}:${relation.right}`}
                            value={index}
                          >
                            {relation.left} / {relation.right}
                          </option>
                        ))}
                      </select>
                      <span className="text-[10px] text-gray-400">
                        Based on {formatCount(sampleRows.length)} sampled rows
                      </span>
                    </div>

                    <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_220px]">
                      <div className="min-w-0 rounded-lg bg-gray-50 p-4">
                        <div className="flex items-start justify-between gap-4">
                          <div className="min-w-0">
                            <h3 className="truncate text-[12px] font-bold text-[#1c1c1a]">
                              {selectedRelation.left} / {selectedRelation.right}
                            </h3>
                            <p className="mt-1 text-[10px] text-gray-500">
                              {relationStrength(selectedRelation.correlation)}{' '}
                              {selectedRelation.correlation >= 0
                                ? 'positive'
                                : 'negative'}{' '}
                              relationship
                            </p>
                          </div>
                          <div className="text-right">
                            <strong className="block text-[20px] font-extrabold text-[#1c1c1a]">
                              {selectedRelation.correlation.toFixed(2)}
                            </strong>
                            <span className="text-[9px] text-gray-400">
                              Pearson r
                            </span>
                          </div>
                        </div>
                        <ScatterPlot relation={selectedRelation} />
                        <p className="text-[9px] text-gray-400">
                          {formatCount(selectedRelation.points.length)} paired
                          values, R2{' '}
                          {(selectedRelation.correlation ** 2).toFixed(2)}
                        </p>
                      </div>

                      <div>
                        <p className="mb-2 text-[9px] font-semibold uppercase tracking-[0.06em] text-gray-400">
                          Strongest pairs
                        </p>
                        {relations.slice(0, 6).map((relation, index) => (
                          <button
                            key={`${relation.left}:${relation.right}`}
                            type="button"
                            className={`flex w-full items-center justify-between gap-3 border-b border-gray-100 py-2 text-left ${
                              index === selectedRelationIndex
                                ? 'text-gray-900'
                                : 'text-gray-500 hover:text-gray-900'
                            }`}
                            onClick={() => setRelationIndex(index)}
                          >
                            <span className="min-w-0 truncate text-[10px]">
                              {relation.left} / {relation.right}
                            </span>
                            <strong className="text-[10px] tabular-nums">
                              {relation.correlation.toFixed(2)}
                            </strong>
                          </button>
                        ))}
                      </div>
                    </div>
                  </>
                ) : (
                  <div className="flex min-h-[220px] items-center justify-center text-center text-[12px] text-gray-400">
                    Relations require at least two populated numeric columns.
                  </div>
                )}
              </div>
            )}

            {!loading && !error && preview && activeTab === 'issues' && (
              <div className="min-h-[260px] pt-4">
                <div className="mb-3 flex items-end justify-between gap-4">
                  <div>
                    <strong className="block text-[24px] font-extrabold leading-7 text-[#1c1c1a]">
                      {formatMetric(qualityScore)}
                    </strong>
                    <span className="text-[10px] text-gray-400">
                      deterministic quality score
                    </span>
                  </div>
                  <span className="text-[11px] text-gray-500">
                    {issues.length === 0
                      ? 'No actionable issues'
                      : `${issues.length} actionable issue${issues.length === 1 ? '' : 's'}`}
                  </span>
                </div>

                {issues.length > 0 ? (
                  <div>
                    {issues.map((issue) => {
                      const created = createdIssues.has(issue.id);
                      const IssueIcon =
                        issue.kind === 'missing'
                          ? Filter
                          : issue.kind === 'duplicates'
                            ? Copy
                            : Type;
                      return (
                        <article
                          key={issue.id}
                          className="flex flex-wrap items-center gap-3 border-b border-gray-100 py-3 last:border-b-0"
                        >
                          <span className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md bg-gray-100 text-gray-600">
                            <IssueIcon size={15} />
                          </span>
                          <div className="min-w-[220px] flex-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <h3 className="text-[12px] font-semibold text-[#1c1c1a]">
                                {issue.title}
                              </h3>
                              <span className="text-[9px] font-medium uppercase text-gray-400">
                                {issue.severity}
                              </span>
                            </div>
                            <p className="mt-0.5 text-[10px] text-gray-500">
                              {issue.detail}
                            </p>
                          </div>
                          {issue.column && (
                            <button
                              type="button"
                              className="h-7 px-2 text-[10px] font-medium text-gray-500 hover:text-gray-900"
                              onClick={() => {
                                setSelectedColumn(issue.column ?? '');
                                setActiveTab('profile');
                              }}
                            >
                              Inspect field
                            </button>
                          )}
                          <button
                            type="button"
                            className="flex h-7 items-center gap-1.5 rounded-md border border-gray-200 px-2.5 text-[10px] font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-default disabled:bg-gray-50 disabled:text-gray-400"
                            disabled={!onCreateNode || created}
                            title={
                              onCreateNode
                                ? undefined
                                : 'Open the active source dataset to add cleaning nodes'
                            }
                            onClick={() => handleCreateIssueNode(issue)}
                          >
                            {created && <Check size={12} />}
                            {created ? 'Node added' : issueActionLabel(issue)}
                          </button>
                        </article>
                      );
                    })}
                  </div>
                ) : (
                  <div className="flex min-h-[180px] flex-col items-center justify-center text-center">
                    <Check size={20} className="text-gray-500" />
                    <p className="mt-2 text-[12px] font-medium text-gray-700">
                      No deterministic cleaning issues found.
                    </p>
                    <p className="mt-1 text-[10px] text-gray-400">
                      Missing values, exact duplicates, and surrounding
                      whitespace were checked.
                    </p>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </section>
  );
};

export default CsvPreviewCard;
