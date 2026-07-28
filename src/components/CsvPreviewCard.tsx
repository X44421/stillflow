import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Check,
  ChevronDown,
  ChevronRight,
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

/* ── Mono tokens (lieflat-charts mono-tokens.js) ─────────────── */
const INK = '#1C1C1A';
const PAPER = '#F0EFEB';
const MUTED = '#8F8E88';
const FAINT = '#C6C5BF';
const GRID = '#DEDDD6';

/** Deterministic pseudo-random — refreshes must look identical. */
function rnd(i: number, k: number): number {
  return Math.abs(((i * 73856093) ^ (k * 19349663)) % 1000) / 1000;
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

function truncateLabel(label: string, max = 14): string {
  return label.length > max ? `${label.slice(0, max - 1)}…` : label;
}

/** Mono source line: uppercase, letterspaced, faint. */
const MonoSrc: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <p className="mt-2 text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
    {children}
  </p>
);

/* ════ F5 · Tick Rows ════
   横向条形:Lupi 语法——一行 = 一个分布桶,长度 ∝ 行数,
   tick 密度来自单位分解(1 tick ≈ K rows,K 写进副标题)。 */
const TickRows: React.FC<{ bars: DistributionBar[]; sampleNote: string }> = ({
  bars,
  sampleNote,
}) => {
  const maximum = Math.max(...bars.map((bar) => bar.count), 1);
  const unit = Math.max(1, Math.ceil(maximum / 34));
  const rowHeight = 26;
  const top = 10;
  const height = top + bars.length * rowHeight + 24;
  const X0 = 108;
  const PX = 6.6;
  const maxTicks = Math.ceil(maximum / unit);

  return (
    <svg
      viewBox={`0 0 400 ${height}`}
      className="h-auto w-full"
      role="img"
      aria-label="Sample distribution"
    >
      {bars.map((bar, i) => {
        const y = top + i * rowHeight;
        const ticks = Math.max(1, Math.round(bar.count / unit));
        return (
          <g key={`${bar.label}-${i}`}>
            <text
              x={98}
              y={y + 3}
              fontSize={8}
              fontWeight={700}
              fill="#6A6963"
              textAnchor="end"
              letterSpacing="0.06em"
              className="mono-fade"
              style={{ animationDelay: `${i * 0.08}s` }}
            >
              {truncateLabel(bar.label)}
            </text>
            <line
              x1={X0}
              y1={y + 9}
              x2={X0 + maxTicks * PX}
              y2={y + 9}
              stroke={GRID}
              strokeWidth={0.6}
              className="mono-fade"
              style={{ animationDelay: `${i * 0.08}s` }}
            />
            {Array.from({ length: ticks }, (_, k) => {
              const x = X0 + k * PX + PX / 2;
              const h = 9 + rnd(k + 1, i + 2) * 6;
              return (
                <g key={k}>
                  <line
                    x1={x}
                    y1={y + 9}
                    x2={x}
                    y2={y + 9 - h}
                    stroke={INK}
                    strokeWidth={0.9}
                    opacity={0.55 + rnd(k + 3, i + 5) * 0.45}
                    className="mono-fade"
                    style={{ animationDelay: `${i * 0.08 + k * 0.012}s` }}
                  />
                  {k % 5 === 4 && (
                    <circle
                      cx={x}
                      cy={y + 13}
                      r={0.8}
                      fill={FAINT}
                      className="mono-fade"
                      style={{ animationDelay: `${i * 0.08 + k * 0.012}s` }}
                    />
                  )}
                </g>
              );
            })}
            <text
              x={X0 + ticks * PX + 8}
              y={y + 4}
              fontSize={11}
              fontWeight={800}
              fill={INK}
              className="mono-fade"
              style={{ animationDelay: `${0.4 + i * 0.08}s` }}
            >
              {formatCount(bar.count)}
              <title>{`${bar.label} — ${formatCount(bar.count)} rows`}</title>
            </text>
          </g>
        );
      })}
      <text
        x={200}
        y={height - 6}
        fontSize={7}
        fontWeight={600}
        fill="#B0AFA9"
        textAnchor="middle"
        letterSpacing="0.12em"
        className="mono-fade"
        style={{ animationDelay: '0.9s' }}
      >
        {`ONE TICK ≈ ${formatCount(unit)} ROWS · DOT MARKS EVERY FIFTH · ${sampleNote}`}
      </text>
    </svg>
  );
};

/* ════ F8 · Plumb Scatter ════
   散点:每个点垂一根发丝铅垂线到 barcode 地板,
   x 读线脚、y 读高度;最高/最低两个点放大标值。 */
const PlumbScatter: React.FC<{ relation: NumericRelation }> = ({
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
  const X0 = 52;
  const X1 = 500;
  const base = 200;
  const mapX = (x: number) => X0 + ((x - minimumX) / widthX) * (X1 - X0);
  const mapY = (y: number) => base - ((y - minimumY) / widthY) * 168;
  const heroHigh = points.reduce((a, b) => (b[1] > a[1] ? b : a), points[0]);
  const heroLow = points.reduce((a, b) => (b[1] < a[1] ? b : a), points[0]);

  return (
    <svg
      viewBox="0 0 520 252"
      className="h-auto w-full"
      role="img"
      aria-label={`${relation.left} and ${relation.right} scatter plot`}
    >
      {Array.from({ length: 21 }, (_, g) => {
        const x = X0 + (g / 20) * (X1 - X0);
        return (
          <line
            key={g}
            x1={x}
            y1={base}
            x2={x}
            y2={base - (g % 5 === 0 ? 7 : 4)}
            stroke="#CFCEC7"
            strokeWidth={0.6}
            className="mono-fade"
            style={{ animationDelay: `${g * 0.01}s` }}
          />
        );
      })}
      <line
        x1={X0 - 6}
        y1={base}
        x2={X1 + 6}
        y2={base}
        stroke={GRID}
        strokeWidth={0.8}
        className="mono-fade"
      />
      <text
        x={X0}
        y={base + 16}
        fontSize={7}
        fontWeight={600}
        fill={FAINT}
        className="mono-fade"
      >
        {formatMetric(minimumX)}
      </text>
      <text
        x={X1}
        y={base + 16}
        fontSize={7}
        fontWeight={600}
        fill={FAINT}
        textAnchor="end"
        className="mono-fade"
      >
        {formatMetric(maximumX)}
      </text>
      <text
        x={18}
        y={mapY(maximumY)}
        fontSize={7}
        fontWeight={600}
        fill={FAINT}
        textAnchor="end"
        letterSpacing="0.08em"
        transform={`rotate(-90 18 ${mapY(maximumY)})`}
        className="mono-fade"
      >
        {`${relation.right.toUpperCase()} ↑`}
      </text>
      {points.map(([x, y], index) => {
        const px = mapX(x);
        const py = mapY(y);
        const hero =
          (x === heroHigh[0] && y === heroHigh[1]) ||
          (x === heroLow[0] && y === heroLow[1]);
        return (
          <g key={`${x}-${y}-${index}`}>
            <line
              x1={px}
              y1={base}
              x2={px}
              y2={py}
              stroke="#B0AFA9"
              strokeWidth={0.55}
              opacity={0.6}
              className="mono-fade"
              style={{ animationDelay: `${0.2 + Math.min(index, 40) * 0.02}s` }}
            />
            <circle
              cx={px}
              cy={py}
              r={hero ? 4.2 : 2.2}
              fill={hero ? INK : '#55554F'}
              className="mono-pop"
              style={{ animationDelay: `${0.25 + Math.min(index, 40) * 0.02}s` }}
            >
              <title>{`${relation.left} ${formatMetric(x)} · ${relation.right} ${formatMetric(y)}`}</title>
            </circle>
            {hero && (
              <text
                x={px}
                y={py - 9}
                fontSize={8.5}
                fontWeight={800}
                fill={INK}
                textAnchor="middle"
                className="mono-fade"
                style={{
                  animationDelay: '0.8s',
                  paintOrder: 'stroke',
                  stroke: PAPER,
                  strokeWidth: 3,
                }}
              >
                {formatMetric(y)}
              </text>
            )}
          </g>
        );
      })}
      <text
        x={260}
        y={246}
        fontSize={7}
        fontWeight={600}
        fill="#B0AFA9"
        textAnchor="middle"
        letterSpacing="0.12em"
        className="mono-fade"
        style={{ animationDelay: '1s' }}
      >
        EVERY DOT HANGS A PLUMB LINE · ONE DOT = ONE SAMPLED ROW
      </text>
    </svg>
  );
};

/* ════ F11 · Tick Gauge ════
   单值进度:100 根 tick 弯成 210° 表盘,1 tick = 1%,
   上墨 = 已得;里程碑 25/50/75/100 点标 + 小字。 */
const TickGauge: React.FC<{ score: number }> = ({ score }) => {
  const goal = Math.max(0, Math.min(100, Math.round(score)));
  const cx = 110;
  const cy = 112;
  const R0 = 78;
  const A0 = -195;
  const SW = 210;
  const D2R = Math.PI / 180;
  const pol = (r: number, deg: number): [number, number] => [
    cx + r * Math.cos(deg * D2R),
    cy + r * Math.sin(deg * D2R),
  ];

  return (
    <svg
      viewBox="0 0 220 150"
      className="h-auto w-full"
      role="img"
      aria-label={`Quality score ${formatMetric(score)}`}
    >
      {Array.from({ length: 100 }, (_, k) => {
        const angle = A0 + (k / 100) * SW;
        const inked = k < goal;
        const length = inked ? 11 + rnd(k + 1, 3) * 5 : 4 + rnd(k + 1, 7) * 2.5;
        const [x1, y1] = pol(R0, angle);
        const [x2, y2] = pol(R0 + length, angle);
        return (
          <line
            key={k}
            x1={x1}
            y1={y1}
            x2={x2}
            y2={y2}
            stroke={inked ? INK : '#CFCEC7'}
            strokeWidth={inked ? 1 : 0.6}
            className="mono-fade"
            style={{ animationDelay: `${k * 0.012}s` }}
          />
        );
      })}
      {[25, 50, 75, 100].map((milestone) => {
        const angle = A0 + (milestone / 100) * SW;
        const [dx, dy] = pol(R0 - 6, angle);
        const [tx, ty] = pol(R0 - 16, angle);
        return (
          <g key={milestone}>
            <circle
              cx={dx}
              cy={dy}
              r={1}
              fill="#B0AFA9"
              className="mono-fade"
              style={{ animationDelay: '0.8s' }}
            />
            <text
              x={tx}
              y={ty + 2.5}
              fontSize={7}
              fontWeight={600}
              fill={FAINT}
              textAnchor="middle"
              className="mono-fade"
              style={{ animationDelay: '0.85s' }}
            >
              {milestone}
            </text>
          </g>
        );
      })}
      {(() => {
        const [ex, ey] = pol(R0 + 17, A0 + (goal / 100) * SW);
        return (
          <circle
            cx={ex}
            cy={ey}
            r={2.4}
            fill={INK}
            className="mono-pop"
            style={{ animationDelay: '1.1s' }}
          />
        );
      })()}
      <text
        x={cx}
        y={cy - 2}
        fontSize={28}
        fontWeight={800}
        fill={INK}
        textAnchor="middle"
        className="mono-fade"
        style={{ animationDelay: '1s' }}
      >
        {formatMetric(score)}
      </text>
      <text
        x={cx}
        y={cy + 15}
        fontSize={7}
        fontWeight={600}
        fill={MUTED}
        textAnchor="middle"
        letterSpacing="0.1em"
        className="mono-fade"
        style={{ animationDelay: '1.05s' }}
      >
        {`${100 - goal} TICKS TO GO`}
      </text>
      <text
        x={cx}
        y={146}
        fontSize={7}
        fontWeight={600}
        fill="#B0AFA9"
        textAnchor="middle"
        letterSpacing="0.12em"
        className="mono-fade"
        style={{ animationDelay: '1.2s' }}
      >
        ONE TICK = 1% · INKED = EARNED
      </text>
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
  const peakBin =
    selectedDistribution.length > 0
      ? selectedDistribution.reduce((a, b) => (b.count > a.count ? b : a))
      : null;

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

  const headerButtonClass =
    'rounded-md p-1.5 text-zinc-500 transition-colors hover:bg-zinc-100 hover:text-zinc-700';

  /* ── Vertical resize ─────────────────────────────────────── */
  const [panelHeight, setPanelHeight] = useState(420);
  const dragRef = useRef<{ startY: number; startH: number } | null>(null);

  const handleResizeStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      dragRef.current = { startY: e.clientY, startH: panelHeight };

      const handleMove = (ev: MouseEvent) => {
        if (!dragRef.current) return;
        const delta = dragRef.current.startY - ev.clientY;
        const newH = Math.max(
          200,
          Math.min(window.innerHeight * 0.85, dragRef.current.startH + delta)
        );
        setPanelHeight(newH);
      };

      const handleUp = () => {
        dragRef.current = null;
        document.removeEventListener('mousemove', handleMove);
        document.removeEventListener('mouseup', handleUp);
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      };

      document.body.style.cursor = 'row-resize';
      document.body.style.userSelect = 'none';
      document.addEventListener('mousemove', handleMove);
      document.addEventListener('mouseup', handleUp);
    },
    [panelHeight]
  );

  return (
    <section
      role={fullscreen ? 'dialog' : 'region'}
      aria-modal={fullscreen ? true : undefined}
      aria-labelledby="csv-preview-title"
      className={
        fullscreen
          ? 'fixed inset-0 z-50 flex min-h-0 flex-col overflow-hidden bg-white'
          : minimized
            ? 'h-12 flex-shrink-0 overflow-hidden rounded-t-xl border border-zinc-200 bg-white'
            : 'flex min-h-[200px] flex-shrink-0 flex-col overflow-hidden rounded-t-xl border border-zinc-200 bg-white shadow-[0_-4px_16px_rgba(0,0,0,0.04)]'
      }
      style={
        !fullscreen && !minimized ? { height: panelHeight } : undefined
      }
    >
      {/* Resize handle */}
      {!fullscreen && !minimized && (
        <div
          className="group flex h-2.5 flex-shrink-0 cursor-row-resize items-center justify-center"
          onMouseDown={handleResizeStart}
        >
          <div className="h-1 w-8 rounded-full bg-zinc-200 transition-colors group-hover:bg-zinc-400" />
        </div>
      )}
      <header
        className={`flex flex-shrink-0 items-center justify-between border-b border-zinc-200 px-4 ${
          minimized ? 'h-12' : 'py-2.5'
        }`}
      >
        <h2
          id="csv-preview-title"
          className={`truncate font-semibold text-zinc-900 ${
            minimized ? 'text-[13px]' : 'text-sm'
          }`}
        >
          {dataset.name}
        </h2>
        <div className="flex items-center gap-2">
          {!minimized && (
            <span className="text-[13px] text-zinc-500">
              {preview ? formatCount(preview.totalRows) : '--'} rows ·{' '}
              {preview ? preview.columns.length : '--'} columns
              {updating ? ' · Updating…' : ''}
            </span>
          )}
          {minimized ? (
            <button
              type="button"
              className={headerButtonClass}
              aria-label="Expand CSV preview"
              title="Expand preview"
              onClick={() => setDisplayMode('docked')}
            >
              <ChevronDown size={14} className="rotate-180" />
            </button>
          ) : (
            <button
              type="button"
              className={headerButtonClass}
              aria-label="Minimize CSV preview"
              title="Minimize preview"
              onClick={() => setDisplayMode('minimized')}
            >
              <Minus size={14} />
            </button>
          )}
          <button
            type="button"
            className={headerButtonClass}
            aria-label={fullscreen ? 'Restore CSV preview' : 'Fullscreen CSV preview'}
            title={fullscreen ? 'Restore preview' : 'Fullscreen preview'}
            onClick={() =>
              setDisplayMode((current) =>
                current === 'fullscreen' ? 'docked' : 'fullscreen'
              )
            }
          >
            {fullscreen ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
          </button>
          <button
            type="button"
            className={headerButtonClass}
            aria-label="Close CSV preview"
            title="Close preview"
            onClick={onClose}
          >
            <X size={14} />
          </button>
        </div>
      </header>

      {!minimized && (
        <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <nav
            className="flex h-10 flex-shrink-0 overflow-x-auto border-b border-zinc-200 px-4"
            aria-label="CSV preview views"
          >
            {tabs.map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={`relative h-10 flex-shrink-0 px-3 text-[13px] transition-colors first:pl-0 ${
                  activeTab === tab.key
                    ? 'font-semibold text-zinc-900 after:absolute after:bottom-[-1px] after:left-0 after:right-3 after:h-0.5 after:bg-zinc-900 first:after:left-0'
                    : 'font-medium text-zinc-500 hover:text-zinc-700'
                }`}
                onClick={() => setActiveTab(tab.key)}
              >
                {tab.label}
                {tab.count !== undefined && (
                  <span className="ml-1 text-[10px] font-normal text-zinc-400">
                    {formatCount(tab.count)}
                  </span>
                )}
              </button>
            ))}
          </nav>

          <div className="min-h-0 flex-1 overflow-y-auto">
            {loading && (
              <div className="flex min-h-[260px] items-center justify-center text-[13px] text-zinc-400">
                Loading CSV preview...
              </div>
            )}

            {!loading && error && (
              <div className="flex min-h-[260px] flex-col items-center justify-center gap-3 text-center">
                <p className="text-[13px] text-zinc-600">{error}</p>
                <button
                  type="button"
                  className="h-8 rounded-md border border-zinc-300 px-3 text-[12px] font-medium text-zinc-600 transition-colors hover:bg-zinc-50"
                  onClick={() => setReloadToken((current) => current + 1)}
                >
                  Retry
                </button>
              </div>
            )}

            {!loading && !error && preview && activeTab === 'data' && (
              <div className="flex h-full min-h-[260px] flex-col">
                {/* Toolbar */}
                <div className="flex flex-wrap items-center gap-2 border-b border-zinc-100 px-4 py-2">
                  <label className="relative min-w-[200px] flex-1">
                    <Search
                      size={14}
                      className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-zinc-400"
                    />
                    <input
                      type="search"
                      value={searchInput}
                      placeholder="Search all columns"
                      className="h-7 w-full rounded-md border border-zinc-200 bg-white pl-8 pr-8 text-[13px] text-zinc-800 outline-none transition-colors placeholder:text-zinc-400 focus:border-zinc-400"
                      onChange={(event) => setSearchInput(event.target.value)}
                    />
                    {searchInput && (
                      <button
                        type="button"
                        className="absolute right-1 top-1/2 flex h-5 w-5 -translate-y-1/2 items-center justify-center rounded text-zinc-400 hover:bg-zinc-100 hover:text-zinc-600"
                        aria-label="Clear search"
                        onClick={() => setSearchInput('')}
                      >
                        <X size={12} />
                      </button>
                    )}
                  </label>
                  <select
                    value={pageSize}
                    className="h-7 rounded-md border border-zinc-200 bg-white px-2 text-[12px] text-zinc-600 outline-none focus:border-zinc-400"
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
                </div>

                {/* Table */}
                <div className="min-h-0 flex-1 overflow-auto">
                  <table className="w-full border-collapse text-[13px]">
                    <thead className="sticky top-0 z-10">
                      <tr className="bg-zinc-50 text-left">
                        <th className="w-10 border-b border-r border-zinc-200 px-3 py-2 font-medium text-zinc-500">
                          #
                        </th>
                        {preview.columns.map((column) => {
                          const isSorted = sortColumn === column.name;
                          return (
                            <th
                              key={column.name}
                              className="border-b border-r border-zinc-200 px-3 py-1.5"
                            >
                              <button
                                type="button"
                                className="flex w-full items-center gap-1.5 hover:text-zinc-900"
                                onClick={() => handleSort(column)}
                              >
                                <span className="font-semibold text-zinc-800">
                                  {column.name}
                                </span>
                                <span className="text-[11px] font-normal text-zinc-400">
                                  {column.type}
                                </span>
                                {isSorted && (
                                  <ChevronDown
                                    size={11}
                                    className={
                                      sortDirection === 'asc'
                                        ? 'ml-auto rotate-180 text-zinc-700'
                                        : 'ml-auto text-zinc-700'
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
                          className="hover:bg-zinc-50"
                        >
                          <td className="border-b border-r border-zinc-100 px-3 py-2 text-zinc-400">
                            {preview.offset + rowIndex + 1}
                          </td>
                          {preview.columns.map((column) => {
                            const value = row[column.name];
                            return (
                              <td
                                key={column.name}
                                className={`max-w-[240px] truncate border-b border-r border-zinc-100 px-3 py-2 text-zinc-800 ${
                                  column.type === 'number'
                                    ? 'text-right font-medium tabular-nums'
                                    : 'text-left'
                                }`}
                                title={isMissing(value) ? 'Empty' : String(value)}
                              >
                                {isMissing(value) ? (
                                  <span className="text-zinc-300">--</span>
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
                    <div className="py-14 text-center text-[13px] text-zinc-400">
                      {searchQuery
                        ? 'No rows match this search.'
                        : 'This CSV contains no data rows.'}
                    </div>
                  )}
                </div>

                {/* Pagination */}
                <div className="flex shrink-0 items-center justify-between border-t border-zinc-200 px-4 py-2">
                  <span className="text-[13px] text-zinc-500">
                    {preview.rows.length > 0
                      ? `Showing ${preview.offset + 1} to ${preview.offset + preview.rows.length} of ${formatCount(preview.filteredRows)} rows`
                      : '0 rows'}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      type="button"
                      className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-100 disabled:opacity-40"
                      disabled={page === 0 || updating}
                      onClick={() => setPage((current) => Math.max(0, current - 1))}
                    >
                      <ChevronRight size={14} className="rotate-180" />
                    </button>
                    {Array.from({ length: Math.min(pageCount, 3) }, (_, i) => (
                      <button
                        key={i}
                        type="button"
                        className={`h-7 min-w-7 rounded-md px-1.5 text-[13px] ${
                          page === i
                            ? 'bg-zinc-900 font-medium text-white'
                            : 'text-zinc-600 hover:bg-zinc-100'
                        }`}
                        onClick={() => setPage(i)}
                      >
                        {i + 1}
                      </button>
                    ))}
                    {pageCount > 3 && (
                      <span className="px-1 text-[13px] text-zinc-400">…</span>
                    )}
                    {pageCount > 3 && (
                      <button
                        type="button"
                        className={`h-7 min-w-7 rounded-md px-1.5 text-[13px] ${
                          page === pageCount - 1
                            ? 'bg-zinc-900 font-medium text-white'
                            : 'text-zinc-600 hover:bg-zinc-100'
                        }`}
                        onClick={() => setPage(pageCount - 1)}
                      >
                        {pageCount}
                      </button>
                    )}
                    <button
                      type="button"
                      className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 hover:bg-zinc-100 disabled:opacity-40"
                      disabled={page >= pageCount - 1 || preview.filteredRows === 0 || updating}
                      onClick={() => setPage((current) => Math.min(pageCount - 1, current + 1))}
                    >
                      <ChevronRight size={14} />
                    </button>
                  </div>
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
                    <div key={label} className="rounded-2xl bg-white px-4 py-3">
                      <span className="block text-[20px] font-extrabold leading-6 text-ink">
                        {value}
                      </span>
                      <span className="mt-1 block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                        {label}
                      </span>
                    </div>
                  ))}
                </div>

                <div className="mt-4 grid overflow-hidden rounded-2xl bg-white md:grid-cols-[220px_minmax(0,1fr)]">
                  <div className="max-h-[300px] overflow-y-auto border-b border-[#e8e7e2] md:border-b-0 md:border-r">
                    {preview.columns.map((column) => (
                      <button
                        key={column.name}
                        type="button"
                        className={`flex w-full items-center justify-between gap-3 border-b border-[#efeee9] px-3 py-2.5 text-left last:border-b-0 ${
                          selectedProfile?.name === column.name
                            ? 'bg-paper'
                            : 'hover:bg-paper/60'
                        }`}
                        onClick={() => setSelectedColumn(column.name)}
                      >
                        <span className="min-w-0">
                          <span className="block truncate text-[11px] font-semibold text-ink">
                            {column.name}
                          </span>
                          <span className="block text-[9px] text-ink-5">
                            {column.type}
                          </span>
                        </span>
                        {column.nullCount > 0 && (
                          <span className="text-[9px] tabular-nums text-ink-5">
                            {formatCount(column.nullCount)} empty
                          </span>
                        )}
                      </button>
                    ))}
                  </div>

                  {selectedProfile && (
                    <div className="min-w-0 p-5">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <h3 className="truncate text-[13px] font-bold text-ink">
                            {selectedProfile.name}
                          </h3>
                          <p className="mt-0.5 text-[10px] text-ink-5">
                            {selectedProfile.type} field
                          </p>
                        </div>
                        <button
                          type="button"
                          className="text-[10px] font-medium text-ink-4 hover:text-ink"
                          onClick={() => setActiveTab('issues')}
                        >
                          View issues
                        </button>
                      </div>

                      <div className="mt-3 grid grid-cols-3 gap-3">
                        <div>
                          <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                            Distinct
                          </span>
                          <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                            {formatCount(selectedProfile.distinctCount)}
                          </strong>
                        </div>
                        <div>
                          <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                            Missing
                          </span>
                          <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                            {formatCount(selectedProfile.nullCount)}
                          </strong>
                        </div>
                        <div>
                          <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                            Whitespace
                          </span>
                          <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                            {formatCount(selectedProfile.whitespaceCount)}
                          </strong>
                        </div>
                      </div>

                      {selectedProfile.type === 'number' && (
                        <div className="mt-3 grid grid-cols-3 gap-3 border-t border-[#efeee9] pt-3">
                          <div>
                            <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                              Minimum
                            </span>
                            <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                              {formatMetric(selectedProfile.minimum)}
                            </strong>
                          </div>
                          <div>
                            <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                              Average
                            </span>
                            <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                              {formatMetric(selectedProfile.average)}
                            </strong>
                          </div>
                          <div>
                            <span className="block text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                              Maximum
                            </span>
                            <strong className="mt-0.5 block text-[13px] font-extrabold text-ink">
                              {formatMetric(selectedProfile.maximum)}
                            </strong>
                          </div>
                        </div>
                      )}

                      <div className="mt-5">
                        {selectedDistribution.length > 0 && peakBin ? (
                          <>
                            <h4 className="text-[12.5px] font-bold tracking-[-0.01em] text-ink">
                              {`“${truncateLabel(peakBin.label, 20)}” leads with ${formatCount(peakBin.count)} rows`}
                            </h4>
                            <p className="mb-1 mt-0.5 text-[10px] text-ink-4">
                              sample distribution · bar length ∝ rows in bin
                            </p>
                            <TickRows
                              bars={selectedDistribution}
                              sampleNote={`${formatCount(sampleRows.length)} SAMPLED ROWS`}
                            />
                            <MonoSrc>
                              {`tick rows · mono-basic · ${dataset.name}`}
                            </MonoSrc>
                          </>
                        ) : (
                          <p className="text-[10px] text-ink-5">
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
                        className="h-8 min-w-[220px] rounded-md border border-grid bg-white px-2 text-[11px] text-ink-2 outline-none focus:border-ink-4"
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
                      <span className="text-[10px] text-ink-5">
                        Based on {formatCount(sampleRows.length)} sampled rows
                      </span>
                    </div>

                    <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_220px]">
                      <div className="min-w-0 rounded-2xl bg-white p-5">
                        <div className="flex items-start justify-between gap-4">
                          <div className="min-w-0">
                            <h3 className="truncate text-[13px] font-bold tracking-[-0.01em] text-ink">
                              {`${relationStrength(selectedRelation.correlation)} ${selectedRelation.correlation >= 0 ? 'positive' : 'negative'}: ${selectedRelation.left} × ${selectedRelation.right}`}
                            </h3>
                            <p className="mt-0.5 text-[10px] text-ink-4">
                              every dot hangs a plumb line · one dot = one
                              sampled row
                            </p>
                          </div>
                          <div className="text-right">
                            <strong className="block text-[20px] font-extrabold leading-6 text-ink">
                              {selectedRelation.correlation.toFixed(2)}
                            </strong>
                            <span className="text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                              Pearson r
                            </span>
                          </div>
                        </div>
                        <PlumbScatter relation={selectedRelation} />
                        <MonoSrc>
                          {`plumb scatter · mono-basic · ${formatCount(selectedRelation.points.length)} paired values · R2 ${(selectedRelation.correlation ** 2).toFixed(2)}`}
                        </MonoSrc>
                      </div>

                      <div>
                        <p className="mb-2 text-[9.5px] font-medium uppercase tracking-[0.08em] text-ink-6">
                          Strongest pairs
                        </p>
                        {relations.slice(0, 6).map((relation, index) => (
                          <button
                            key={`${relation.left}:${relation.right}`}
                            type="button"
                            className={`flex w-full items-center justify-between gap-3 border-b border-[#e8e7e2] py-2 text-left ${
                              index === selectedRelationIndex
                                ? 'text-ink'
                                : 'text-ink-4 hover:text-ink'
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
                  <div className="flex min-h-[220px] items-center justify-center text-center text-[12px] text-ink-5">
                    Relations require at least two populated numeric columns.
                  </div>
                )}
              </div>
            )}

            {!loading && !error && preview && activeTab === 'issues' && (
              <div className="min-h-[260px] pt-4">
                <div className="mb-2 grid items-center gap-4 md:grid-cols-[220px_minmax(0,1fr)]">
                  <div className="rounded-2xl bg-white p-4">
                    <h3 className="text-[12.5px] font-bold tracking-[-0.01em] text-ink">
                      Data quality
                    </h3>
                    <p className="mt-0.5 text-[10px] text-ink-4">
                      one tick = 1% · inked = earned
                    </p>
                    <TickGauge score={qualityScore} />
                    <MonoSrc>
                      {`tick gauge · mono-basic · deterministic score`}
                    </MonoSrc>
                  </div>
                  <div>
                    <strong className="block text-[15px] font-bold text-ink">
                      {issues.length === 0
                        ? 'No actionable issues'
                        : `${issues.length} actionable issue${issues.length === 1 ? '' : 's'}`}
                    </strong>
                    <p className="mt-1 text-[11px] leading-5 text-ink-4">
                      Missing values, exact duplicates, and surrounding
                      whitespace were checked deterministically across{' '}
                      {formatCount(preview.totalRows)} rows.
                    </p>
                  </div>
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
                          className="flex flex-wrap items-center gap-3 border-b border-[#e8e7e2] py-3 last:border-b-0"
                        >
                          <span className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-md bg-white text-ink-3">
                            <IssueIcon size={15} />
                          </span>
                          <div className="min-w-[220px] flex-1">
                            <div className="flex flex-wrap items-center gap-2">
                              <h3 className="text-[12px] font-semibold text-ink">
                                {issue.title}
                              </h3>
                              <span className="text-[9px] font-medium uppercase tracking-[0.08em] text-ink-5">
                                {issue.severity}
                              </span>
                            </div>
                            <p className="mt-0.5 text-[10px] text-ink-4">
                              {issue.detail}
                            </p>
                          </div>
                          {issue.column && (
                            <button
                              type="button"
                              className="h-7 px-2 text-[10px] font-medium text-ink-4 hover:text-ink"
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
                            className="flex h-7 items-center gap-1.5 rounded-md border border-grid px-2.5 text-[10px] font-medium text-ink-2 transition-colors hover:bg-white disabled:cursor-default disabled:border-[#e8e7e2] disabled:text-ink-5"
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
                    <Check size={20} className="text-ink-3" />
                    <p className="mt-2 text-[12px] font-medium text-ink">
                      No deterministic cleaning issues found.
                    </p>
                    <p className="mt-1 text-[10px] text-ink-5">
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
