import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  X,
  MoreHorizontal,
  Copy,
  Filter,
  Type,
  Eye,
  Play,
  FileText,
  Upload,
  ChevronRight,
} from '../icons/hero';
import { formatRows } from '../utils/format';
import type { NodeImpact } from '../lib/applyRules';
import type { PipelineNode, NodeType, WorkspaceEvent } from '../types';

const TYPE_ICON: Record<NodeType, React.ReactNode> = {
  source: <FileText size={15} />,
  filter: <Filter size={15} />,
  deduplicate: <Copy size={15} />,
  normalize: <Type size={15} />,
  export: <Upload size={15} />,
};

const TYPE_LABEL: Record<NodeType, string> = {
  source: 'Source',
  filter: 'Transformation',
  deduplicate: 'Process',
  normalize: 'Transformation',
  export: 'Output',
};

const STATUS_META: Record<
  PipelineNode['status'],
  { label: string; dot: string }
> = {
  completed: { label: 'Completed', dot: 'bg-[#4ba66a]' },
  running: { label: 'Running', dot: 'bg-[#2196d2] animate-pulse-dot' },
  failed: { label: 'Failed', dot: 'bg-[#c95e62]' },
  pending: { label: 'Draft', dot: 'bg-[#c9d1d9]' },
  disabled: { label: 'Disabled', dot: 'bg-[#c9d1d9]' },
};

/** Truthful output labels — never the rule description ("Keep matched rows"). */
const OUTPUT_LABEL: Record<NodeType, string> = {
  source: '',
  filter: 'Filtered preview',
  deduplicate: 'Deduplicated preview',
  normalize: 'Normalized preview',
  export: 'CSV export',
};

const FILTER_OPERATORS = [
  'is not empty',
  'is empty',
  'equals',
  'not equals',
  'contains',
  'not contains',
  'greater than',
  'less than',
];
const EMPTINESS_OPERATORS = new Set(['is empty', 'is not empty']);

const FILTER_MODES = ['Keep matching rows', 'Remove matching rows'];
const FILTER_NULL_HANDLING = ['Treat as non-match', 'Treat as match'];
const DEDUP_STRATEGIES = ['Keep first', 'Keep last', 'Merge records'];
const DEDUP_NULL_HANDLING = ['Ignore', 'Treat as duplicate', 'Remove null rows'];
const NORMALIZE_NULL_HANDLING = ['Ignore', 'Remove null rows'];

function Section({
  title,
  action,
  children,
  last = false,
}: {
  title: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  last?: boolean;
}) {
  return (
    <div className={`px-3 py-3 ${last ? '' : 'border-b border-[#edf2f6]'}`}>
      <div className="mb-2 flex items-center justify-between">
        <h4 className="text-[10.5px] font-semibold text-[#5e6874]">{title}</h4>
        {action}
      </div>
      <div className="space-y-1.5">{children}</div>
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex min-h-[20px] items-center justify-between gap-3">
      <span className="shrink-0 text-[12px] text-[#5e6874]">{label}</span>
      {children}
    </div>
  );
}

function Value({ children }: { children: React.ReactNode }) {
  return (
    <span className="truncate text-[12px] font-medium text-[#171a1f]">
      {children}
    </span>
  );
}

const CONTROL_CLASS =
  'h-7 w-full rounded-md border border-[#dce2e8] bg-white px-1.5 text-[12px] font-medium text-[#171a1f] outline-none focus:border-[#2196d2] focus:ring-2 focus:ring-[rgba(33,150,210,.18)]';

function SelectControl({
  value,
  options,
  onChange,
  placeholder,
}: {
  value: string;
  options: string[];
  onChange: (value: string) => void;
  placeholder?: string;
}) {
  return (
    <select
      className={CONTROL_CLASS}
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      {placeholder !== undefined && <option value="">{placeholder}</option>}
      {options.map((option) => (
        <option key={option} value={option}>
          {option}
        </option>
      ))}
    </select>
  );
}

interface DetailPanelProps {
  node: PipelineNode;
  nodes: PipelineNode[];
  events?: WorkspaceEvent[];
  availableColumns?: string[];
  /** Name of the dataset feeding the pipeline (source stage label). */
  datasetName?: string;
  /** Name of the pipeline's output asset (chain terminal label). */
  assetName?: string;
  /** Live rule impact computed on the displayed sample. */
  sampleImpact?: NodeImpact | null;
  onClose: () => void;
  onRun: (nodeId: string) => void | Promise<void>;
  onViewInput?: () => void;
  onUpdate: (nodeId: string, patch: Partial<PipelineNode>) => void;
  onDelete: (nodeId: string) => void;
  onDuplicate: (nodeId: string) => void;
}

const DetailPanel: React.FC<DetailPanelProps> = ({
  node,
  nodes,
  availableColumns = [],
  datasetName = '',
  assetName = 'Output dataset',
  sampleImpact = null,
  onClose,
  onRun,
  onViewInput,
  onUpdate,
  onDelete,
  onDuplicate,
}) => {
  // The currently-running status comes from `node` (App-owned).
  const running = node.status === 'running';
  const disabled = node.status === 'disabled';
  const [showMenu, setShowMenu] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const toastTimer = useRef<number | undefined>(undefined);
  const showToast = useCallback((message: string) => {
    setToast(message);
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 1600);
  }, []);
  useEffect(() => () => window.clearTimeout(toastTimer.current), []);

  // Upstream / downstream context relative to the selected node.
  const idx = nodes.findIndex((n) => n.id === node.id);
  const prevNode = idx > 0 ? nodes[idx - 1] : null;
  const nextNode = idx >= 0 && idx < nodes.length - 1 ? nodes[idx + 1] : null;

  const metrics = node.metrics;
  const hasRun = Boolean(metrics);

  const setConfig = (patch: Partial<PipelineNode['config']>) => {
    onUpdate(node.id, { config: { ...node.config, ...patch } });
  };

  const handleRun = async () => {
    if (running) return;
    if (disabled) {
      showToast('Enable the node before running');
      return;
    }
    await onRun(node.id);
    showToast('Preview ready');
  };

  const handleDisable = () => {
    if (node.type === 'source') {
      showToast('Source node is required');
      return;
    }
    onUpdate(node.id, {
      status: disabled ? 'pending' : 'disabled',
      metrics: undefined,
      error: undefined,
    });
    showToast(disabled ? 'Node enabled' : 'Node disabled');
  };

  const handleMenuAction = (item: { label: string; delete?: boolean }) => {
    setShowMenu(false);
    if (item.delete) {
      onDelete(node.id);
      showToast('Node deleted');
      return;
    }
    if (item.label === 'Duplicate node') {
      onDuplicate(node.id);
      showToast('Node duplicated');
      return;
    }
    if (item.label === 'Disable node' || item.label === 'Enable node') {
      handleDisable();
      return;
    }
    if (item.label === 'Copy node') {
      void navigator.clipboard?.writeText(node.name);
    }
    showToast(item.label);
  };

  const progressPct =
    node.status === 'completed' ? 100 : node.status === 'running' ? 67 : 0;

  const status = STATUS_META[node.status];

  /* ── Rule completeness: an incomplete rule must not offer a confident
        primary action. ─────────────────────────────────────────────── */
  const operator = node.config.operator ?? 'is not empty';
  const needsValue = !EMPTINESS_OPERATORS.has(operator);
  const ruleIncomplete =
    node.type === 'filter' &&
    (!node.config.column.trim() || (needsValue && !node.config.value?.trim()));

  /* ── Rule validation: soundness of the rule plus its sample impact ── */
  interface RuleCheck {
    state: 'pass' | 'warn' | 'fail';
    label: string;
  }
  const ruleChecks: RuleCheck[] = [];
  if (node.type === 'filter') {
    const column = node.config.column.trim();
    if (!column) {
      ruleChecks.push({ state: 'fail', label: 'Select a column for the rule' });
    } else if (
      availableColumns.length > 0 &&
      !availableColumns.some((c) => c.toLowerCase() === column.toLowerCase())
    ) {
      ruleChecks.push({
        state: 'fail',
        label: `Column "${column}" does not exist in the dataset`,
      });
    } else if (needsValue && !node.config.value?.trim()) {
      ruleChecks.push({ state: 'fail', label: 'Enter a comparison value' });
    } else {
      ruleChecks.push({ state: 'pass', label: 'Rule is valid' });
    }
    if (sampleImpact && sampleImpact.rowsIn > 0) {
      const removed = sampleImpact.rowsIn - sampleImpact.rowsOut;
      const pct = (removed / sampleImpact.rowsIn) * 100;
      if (pct > 5) {
        ruleChecks.push({
          state: pct > 50 ? 'fail' : 'warn',
          label: `Removes ${formatRows(removed)} sample rows (${pct.toFixed(1)}%)`,
        });
      }
    }
  } else if (node.type === 'deduplicate') {
    ruleChecks.push({ state: 'pass', label: 'Rule is valid' });
    if (sampleImpact && sampleImpact.rejected.length === 0) {
      ruleChecks.push({ state: 'pass', label: 'No duplicates in the sample' });
    }
  } else if (node.type === 'normalize') {
    ruleChecks.push({ state: 'pass', label: 'Rule is valid' });
    if (sampleImpact && sampleImpact.changes.length === 0) {
      ruleChecks.push({
        state: 'pass',
        label: 'No values need normalization in the sample',
      });
    }
  }

  const inputLabel =
    node.type === 'source'
      ? 'Local file'
      : (prevNode?.name ?? datasetName) || null;
  const outputLabel =
    node.type === 'source'
      ? datasetName || null
      : (OUTPUT_LABEL[node.type] ?? null);

  return (
    <div className="relative flex w-[320px] flex-shrink-0 flex-col overflow-hidden rounded-lg border border-[#dce2e8] bg-white">
      <div
        className="flex-1 overflow-y-auto"
        style={{ scrollbarWidth: 'thin', scrollbarColor: '#c9d1d9 transparent' }}
      >
        {/* Header */}
        <div className="flex items-center gap-2.5 border-b border-[#edf2f6] px-3 py-2.5">
          <div className="grid h-7 w-7 shrink-0 place-items-center rounded-md bg-[#f4f6f8] text-[#5e6874]">
            {TYPE_ICON[node.type]}
          </div>
          <div className="min-w-0 flex-1">
            <h3 className="truncate text-[13px] leading-[17px] font-semibold text-[#171a1f]">
              {node.name}
            </h3>
            <p className="flex items-center gap-1.5 text-[11px] leading-[14px] text-[#5e6874]">
              {TYPE_LABEL[node.type]}
              <span className="text-[#c9d1d9]">·</span>
              <span className={`h-[5px] w-[5px] rounded-full ${status.dot}`} />
              {status.label}
            </p>
          </div>
          <div className="flex shrink-0 items-center gap-0.5">
            <button
              onClick={() => setShowMenu((m) => !m)}
              className="grid h-7 w-7 place-items-center rounded-md text-[#5e6874] transition-colors hover:bg-[#edf2f6] hover:text-[#171a1f]"
              aria-label="More actions"
            >
              <MoreHorizontal size={15} />
            </button>
            <button
              onClick={onClose}
              className="grid h-7 w-7 place-items-center rounded-md text-[#5e6874] transition-colors hover:bg-[#edf2f6] hover:text-[#171a1f]"
              aria-label="Close Inspector"
            >
              <X size={15} />
            </button>
          </div>
        </div>

        {/* Actions — Preview changes is the primary verb; an incomplete rule
            disables it instead of pretending the node can run. */}
        <Section title="Actions">
          <div className="flex gap-1.5">
            <button
              onClick={handleRun}
              disabled={running || disabled || ruleIncomplete}
              title={
                ruleIncomplete
                  ? 'Complete the rule before previewing'
                  : 'Run up to this node and preview the result'
              }
              className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md bg-[#2196d2] text-[12.5px] font-medium text-white transition-colors hover:bg-[#1686be] disabled:cursor-not-allowed disabled:bg-[#c9d1d9]"
            >
              <Play size={13} fill="currentColor" />
              <span>{running ? 'Running…' : 'Preview changes'}</span>
            </button>
            {node.type !== 'source' && (
              <button
                onClick={() => {
                  onViewInput?.();
                  showToast('Input preview opened');
                }}
                title="Preview the data entering this node"
                className="flex h-8 flex-1 items-center justify-center gap-1.5 rounded-md border border-[#dce2e8] bg-[#f4f6f8] text-[12.5px] font-medium text-[#39434e] transition-colors hover:bg-[#edf2f6]"
              >
                <Eye size={13} />
                View input
              </button>
            )}
          </div>
          {ruleIncomplete && (
            <p className="pt-1 text-[11px] leading-[15px] text-[#9099a4]">
              {node.config.column.trim()
                ? 'Enter a comparison value to complete the rule.'
                : 'Select a column to complete the rule.'}
            </p>
          )}
          {running && (
            <div className="flex items-center gap-2 pt-1.5">
              <div className="h-1 flex-1 overflow-hidden rounded-full bg-[#edf2f6]">
                <div
                  className="h-full rounded-full bg-[#2196d2] transition-[width] duration-150 ease-out"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <span className="text-[11px] font-medium text-[#5e6874]">
                {progressPct}%
              </span>
            </div>
          )}
        </Section>

        {/* Rule — configuration is always editable and type-specific. */}
        {node.type === 'filter' && (
          <Section title="Rule">
            <Row label="Mode">
              <div className="w-40">
                <SelectControl
                  value={node.config.mode ?? FILTER_MODES[0]}
                  options={FILTER_MODES}
                  onChange={(mode) => setConfig({ mode })}
                />
              </div>
            </Row>
            <div className="pt-1 pb-0.5 text-[10.5px] font-semibold text-[#9099a4]">
              Conditions
            </div>
            <Row label="Column">
              <div className="w-40">
                {availableColumns.length > 0 ? (
                  <SelectControl
                    value={node.config.column}
                    options={availableColumns}
                    placeholder="Select column"
                    onChange={(column) => setConfig({ column })}
                  />
                ) : (
                  <input
                    className={CONTROL_CLASS}
                    placeholder="Column name"
                    value={node.config.column}
                    onChange={(event) =>
                      setConfig({ column: event.target.value })
                    }
                  />
                )}
              </div>
            </Row>
            <Row label="Operator">
              <div className="w-40">
                <SelectControl
                  value={operator}
                  options={FILTER_OPERATORS}
                  onChange={(next) => setConfig({ operator: next })}
                />
              </div>
            </Row>
            {needsValue && (
              <Row label="Value">
                <div className="w-40">
                  <input
                    className={CONTROL_CLASS}
                    placeholder='e.g. "US" or 100000'
                    value={node.config.value ?? ''}
                    onChange={(event) => setConfig({ value: event.target.value })}
                  />
                </div>
              </Row>
            )}
            <Row label="Null handling">
              <div className="w-40">
                <SelectControl
                  value={node.config.nullHandling}
                  options={FILTER_NULL_HANDLING}
                  onChange={(nullHandling) => setConfig({ nullHandling })}
                />
              </div>
            </Row>
          </Section>
        )}

        {node.type === 'deduplicate' && (
          <Section title="Rule">
            <Row label="Column">
              <div className="w-40">
                {availableColumns.length > 0 ? (
                  <SelectControl
                    value={node.config.column}
                    options={availableColumns}
                    placeholder="Entire row"
                    onChange={(column) => setConfig({ column })}
                  />
                ) : (
                  <input
                    className={CONTROL_CLASS}
                    placeholder="Entire row"
                    value={node.config.column}
                    onChange={(event) =>
                      setConfig({ column: event.target.value })
                    }
                  />
                )}
              </div>
            </Row>
            <Row label="Strategy">
              <div className="w-40">
                <SelectControl
                  value={node.config.strategy}
                  options={DEDUP_STRATEGIES}
                  onChange={(strategy) => setConfig({ strategy })}
                />
              </div>
            </Row>
            <Row label="Null handling">
              <div className="w-40">
                <SelectControl
                  value={node.config.nullHandling}
                  options={DEDUP_NULL_HANDLING}
                  onChange={(nullHandling) => setConfig({ nullHandling })}
                />
              </div>
            </Row>
          </Section>
        )}

        {node.type === 'normalize' && (
          <Section title="Rule">
            <Row label="Column">
              <div className="w-40">
                {availableColumns.length > 0 ? (
                  <SelectControl
                    value={node.config.column}
                    options={availableColumns}
                    placeholder="All text columns"
                    onChange={(column) => setConfig({ column })}
                  />
                ) : (
                  <input
                    className={CONTROL_CLASS}
                    placeholder="All text columns"
                    value={node.config.column}
                    onChange={(event) =>
                      setConfig({ column: event.target.value })
                    }
                  />
                )}
              </div>
            </Row>
            <Row label="Null handling">
              <div className="w-40">
                <SelectControl
                  value={node.config.nullHandling}
                  options={NORMALIZE_NULL_HANDLING}
                  onChange={(nullHandling) => setConfig({ nullHandling })}
                />
              </div>
            </Row>
            <p className="pt-1 text-[11px] leading-[15px] text-[#9099a4]">
              Trims whitespace and lowercases email values.
            </p>
          </Section>
        )}

        {node.type === 'source' && (
          <Section title="Source">
            <Row label="File">
              <Value>{node.name}</Value>
            </Row>
            {node.description && (
              <Row label="Detail">
                <Value>{node.description}</Value>
              </Row>
            )}
            {node.rows && (
              <Row label="Rows">
                <Value>{node.rows}</Value>
              </Row>
            )}
          </Section>
        )}

        {node.type === 'export' && (
          <Section title="Rule">
            <Row label="Format">
              <Value>CSV (UTF-8)</Value>
            </Row>
            <Row label="File name">
              <Value>{node.name}</Value>
            </Row>
          </Section>
        )}

        {/* Input / Output — only rows with a real object are shown. */}
        {(inputLabel || outputLabel) && (
          <Section title="Input / Output">
            {inputLabel && (
              <Row label="Input">
                <Value>{inputLabel}</Value>
              </Row>
            )}
            {outputLabel && (
              <Row label="Output">
                <Value>{outputLabel}</Value>
              </Row>
            )}
          </Section>
        )}

        {/* Impact — run results once they exist; a live estimate computed
            on the displayed sample while the rule is still a draft. */}
        {hasRun && metrics ? (
          <Section title="Impact">
            <Row label="Rows evaluated">
              <Value>{formatRows(metrics.rowsIn)}</Value>
            </Row>
            {node.type === 'filter' ? (
              <>
                <Row label="Matched">
                  <Value>{formatRows(metrics.rowsOut)}</Value>
                </Row>
                <Row label="Removed">
                  <Value>{formatRows(metrics.rowsIn - metrics.rowsOut)}</Value>
                </Row>
              </>
            ) : node.type === 'deduplicate' ? (
              <>
                <Row label="Duplicates removed">
                  <Value>{formatRows(metrics.duplicates)}</Value>
                </Row>
                <Row label="Rows out">
                  <Value>{formatRows(metrics.rowsOut)}</Value>
                </Row>
              </>
            ) : (
              <Row label="Rows out">
                <Value>{formatRows(metrics.rowsOut)}</Value>
              </Row>
            )}
            <Row label="Errors">
              <Value>{node.error ? '1' : '0'}</Value>
            </Row>
          </Section>
        ) : sampleImpact && node.type !== 'source' && node.type !== 'export' ? (
          <Section title="Impact (sample)">
            <Row label="Rows in">
              <Value>{formatRows(sampleImpact.rowsIn)}</Value>
            </Row>
            {node.type === 'filter' ? (
              <>
                <Row label="Matched">
                  <Value>{formatRows(sampleImpact.rowsOut)}</Value>
                </Row>
                <Row label="Removed">
                  <Value>
                    {formatRows(sampleImpact.rowsIn - sampleImpact.rowsOut)}
                  </Value>
                </Row>
              </>
            ) : node.type === 'deduplicate' ? (
              <>
                <Row label="Duplicates removed">
                  <Value>{formatRows(sampleImpact.rejected.length)}</Value>
                </Row>
                <Row label="Rows out">
                  <Value>{formatRows(sampleImpact.rowsOut)}</Value>
                </Row>
              </>
            ) : (
              <>
                <Row label="Values changed">
                  <Value>{formatRows(sampleImpact.changes.length)}</Value>
                </Row>
                <Row label="Rows out">
                  <Value>{formatRows(sampleImpact.rowsOut)}</Value>
                </Row>
              </>
            )}
            <p className="pt-1 text-[11px] leading-[15px] text-[#9099a4]">
              Estimated live on the displayed sample.
            </p>
          </Section>
        ) : null}

        {/* Validation — is the rule itself sound, and what will it do? */}
        {(node.type === 'filter' ||
          node.type === 'deduplicate' ||
          node.type === 'normalize') && (
          <Section title="Validation">
            {ruleChecks.map((check) => (
              <div key={check.label} className="flex items-start gap-2">
                <span
                  className={`w-3.5 shrink-0 text-[12px] font-semibold ${
                    check.state === 'pass'
                      ? 'text-[#4ba66a]'
                      : check.state === 'warn'
                        ? 'text-[#c58b32]'
                        : 'text-[#c95e62]'
                  }`}
                >
                  {check.state === 'pass' ? '✓' : check.state === 'warn' ? '⚠' : '✕'}
                </span>
                <span className="text-[12px] leading-[16px] text-[#39434e]">
                  {check.label}
                </span>
              </div>
            ))}
          </Section>
        )}

        {/* Last run — execution facts, visible only once they exist. */}
        {hasRun && metrics && (
          <Section title="Last run">
            <Row label="Duration">
              <Value>
                {metrics.duration < 1000
                  ? `${Math.round(metrics.duration)}ms`
                  : `${(metrics.duration / 1000).toFixed(1)}s`}
              </Value>
            </Row>
            <Row label="Memory">
              <Value>{metrics.memory} MB</Value>
            </Row>
          </Section>
        )}

        {/* Context — where this node sits in the chain. */}
        <Section title="Context" last>
          <div className="flex flex-wrap items-center gap-x-1 gap-y-0.5 text-[11.5px] leading-[16px]">
            <span className="text-[#9099a4]">
              {prevNode?.name ??
                (node.type === 'source' ? 'Local file' : datasetName)}
            </span>
            <ChevronRight size={11} className="text-[#c9d1d9]" />
            <span className="font-medium text-[#171a1f]">{node.name}</span>
            <ChevronRight size={11} className="text-[#c9d1d9]" />
            <span className="text-[#9099a4]">{nextNode?.name ?? assetName}</span>
          </div>
        </Section>
      </div>

      {/* More Menu */}
      {showMenu && (
        <>
          <div className="fixed inset-0 z-20" onClick={() => setShowMenu(false)} />
          <div className="absolute top-12 right-3 z-30 w-[160px] rounded-lg border border-[#dce2e8] bg-white p-1 shadow-[0_2px_8px_rgba(22,32,44,.08)]">
            {[
              { label: 'Duplicate node' },
              { label: 'Copy node' },
              { label: disabled ? 'Enable node' : 'Disable node' },
              { label: 'Delete node', delete: true },
            ].map((item) => (
              <button
                key={item.label}
                onClick={() => handleMenuAction(item)}
                className={`h-8 w-full cursor-pointer rounded-md border-0 bg-transparent px-2.5 text-left text-[12px] transition-colors hover:bg-[#edf2f6] ${
                  item.delete ? 'text-[#c95e62]' : 'text-[#39434e]'
                }`}
              >
                {item.label}
              </button>
            ))}
          </div>
        </>
      )}

      {/* Toast */}
      <div
        className={`pointer-events-none fixed bottom-6 left-1/2 z-[100] rounded-md bg-[#171a1f] px-3 py-2 text-[12px] text-white shadow-[0_2px_8px_rgba(22,32,44,.16)] transition-all duration-150 ease-out ${
          toast ? 'opacity-100' : 'opacity-0 translate-y-4'
        }`}
        style={{
          transform: toast
            ? 'translateX(-50%)'
            : 'translateX(-50%) translateY(16px)',
        }}
        role="status"
        aria-live="polite"
      >
        {toast}
      </div>
    </div>
  );
};

export default DetailPanel;