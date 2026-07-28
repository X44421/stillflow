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
} from '../icons/hero';
import type { PipelineNode, NodeType, WorkspaceEvent } from '../types';
import { formatRows } from '../utils/duckdb';

const TYPE_ICON: Record<NodeType, React.ReactNode> = {
  source: <FileText size={20} />,
  filter: <Filter size={20} />,
  deduplicate: <Copy size={20} />,
  normalize: <Type size={20} />,
  export: <Upload size={20} />,
};

const TYPE_BG: Record<NodeType, string> = {
  deduplicate: 'bg-violet-50 text-violet-600',
  normalize: 'bg-orange-50 text-orange-600',
  filter: 'bg-purple-50 text-purple-600',
  source: 'bg-green-50 text-green-600',
  export: 'bg-amber-50 text-amber-600',
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
  completed: { label: 'Completed', dot: 'bg-green-600' },
  running: { label: 'Running', dot: 'bg-gray-900' },
  failed: { label: 'Failed', dot: 'bg-red-500' },
  pending: { label: 'Draft', dot: 'bg-gray-400' },
  disabled: { label: 'Disabled', dot: 'bg-gray-300' },
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

const CONTROL_CLASS =
  'text-[13px] font-medium text-gray-900 text-right bg-transparent border border-gray-300 rounded-md px-2 py-0.5 w-36 outline-none focus:border-gray-400';

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
    <div className={`p-4 ${last ? '' : 'border-b border-gray-100'}`}>
      <div className="mb-3 flex items-center justify-between">
        <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider">{title}</h4>
        {action}
      </div>
      <div className="space-y-2.5">{children}</div>
    </div>
  );
}

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex min-h-[22px] items-center justify-between">
      <span className="text-[13px] text-gray-500">{label}</span>
      {children}
    </div>
  );
}

function Value({ children }: { children: React.ReactNode }) {
  return (
    <span className="truncate text-[13px] font-medium text-gray-900">
      {children}
    </span>
  );
}

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
  datasetName?: string;
  onClose: () => void;
  onRun: (nodeId: string) => void | Promise<void>;
  onPreview?: () => void;
  onUpdate: (nodeId: string, patch: Partial<PipelineNode>) => void;
  onDuplicate?: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
}

const DetailPanel: React.FC<DetailPanelProps> = ({
  node,
  nodes,
  availableColumns = [],
  datasetName = '',
  onClose,
  onRun,
  onUpdate,
  onDelete,
}) => {
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
    showToast('Run finished');
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
    if (item.label === 'Disable node' || item.label === 'Enable node') {
      handleDisable();
      return;
    }
    showToast(item.label);
  };

  const progressPct =
    node.status === 'completed' ? 100 : node.status === 'running' ? 67 : 0;

  const operator = node.config.operator ?? 'is not empty';
  const needsValue = !EMPTINESS_OPERATORS.has(operator);
  const ruleIncomplete =
    node.type === 'filter' &&
    (!node.config.column.trim() || (needsValue && !node.config.value?.trim()));

  const inputLabel =
    node.type === 'source'
      ? 'Local file'
      : (prevNode?.name ?? datasetName) || null;
  const outputLabel =
    node.type === 'source'
      ? datasetName || null
      : (OUTPUT_LABEL[node.type] ?? null);

  return (
    <div className="w-[356px] bg-white border-l border-gray-200 flex flex-col flex-shrink-0 overflow-hidden relative shadow-[-12px_0_32px_rgba(16,24,40,0.05)]">
      <div className="flex-1 overflow-y-auto" style={{ scrollbarWidth: 'thin', scrollbarColor: '#d9dadd transparent' }}>
        {/* Header */}
        <div className="p-4 pb-3 border-b border-gray-100">
          <div className="flex items-start justify-between mb-1">
            <div className="flex items-center gap-3">
              <div className={`w-10 h-10 rounded-xl flex items-center justify-center ${TYPE_BG[node.type]}`}>
                {TYPE_ICON[node.type]}
              </div>
              <div>
                <h3 className="text-[15px] font-semibold text-gray-900">{node.name}</h3>
                <p className="text-[12px] text-gray-500">{TYPE_LABEL[node.type]}</p>
              </div>
            </div>
            <div className="flex items-center gap-0.5">
              <button onClick={() => setShowMenu((m) => !m)} className="p-1.5 hover:bg-gray-100 rounded-md transition-colors" aria-label="More actions">
                <MoreHorizontal size={16} className="text-gray-500" />
              </button>
              <button onClick={onClose} className="p-1.5 hover:bg-gray-100 rounded-md transition-colors" aria-label="Close Inspector">
                <X size={16} className="text-gray-500" />
              </button>
            </div>
          </div>
          <div className="flex items-center gap-2 mt-3">
            <span
              className={`inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full ${
                node.status === 'completed'
                  ? 'text-green-700 bg-green-50'
                  : node.status === 'running'
                  ? 'text-white bg-gray-900'
                  : 'text-gray-600 bg-gray-100'
              }`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${
                node.status === 'completed' ? 'bg-green-600' :
                node.status === 'running' ? 'bg-white animate-pulse' :
                'bg-gray-400'
              }`} />
              {node.status === 'completed' ? 'Completed' : node.status === 'running' ? 'Running' : 'Draft'}
            </span>
            {node.error && (
              <span className="text-[12px] text-red-600">Error</span>
            )}
          </div>
        </div>

        {/* Actions — Preview changes is the primary verb */}
        <Section title="Actions">
          <button
            onClick={handleRun}
            disabled={running || disabled || ruleIncomplete}
            title={
              ruleIncomplete
                ? 'Complete the rule before running'
                : 'Run up to this node and preview the result'
            }
            className="w-full bg-gray-900 text-white text-[13px] font-medium py-2.5 rounded-lg flex items-center justify-center gap-2 hover:bg-gray-800 transition-colors mb-2 disabled:opacity-55 disabled:cursor-wait"
          >
            <Play size={14} fill="white" />
            <span>{running ? 'Running…' : 'Preview changes'}</span>
          </button>
          {ruleIncomplete && (
            <p className="text-[12px] text-gray-400">
              {node.config.column.trim()
                ? 'Enter a comparison value to complete the rule.'
                : 'Select a column to complete the rule.'}
            </p>
          )}
          {running && (
            <div className="flex items-center gap-2 pt-1">
              <div className="flex-1 h-1 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gray-900 rounded-full transition-[width] duration-150 ease-out"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <span className="text-[11px] text-gray-500 font-medium">{progressPct}%</span>
            </div>
          )}
        </Section>

        {/* Rule — per type */}
        {node.type === 'filter' && (
          <Section title="Rule">
            <Row label="Mode">
              <div className="w-36">
                <SelectControl
                  value={node.config.mode ?? FILTER_MODES[0]}
                  options={FILTER_MODES}
                  onChange={(mode) => setConfig({ mode })}
                />
              </div>
            </Row>
            <div className="pt-1 pb-0.5 text-[11px] font-medium text-gray-400">Conditions</div>
            <Row label="Column">
              <div className="w-36">
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
                    onChange={(event) => setConfig({ column: event.target.value })}
                  />
                )}
              </div>
            </Row>
            <Row label="Operator">
              <div className="w-36">
                <SelectControl
                  value={operator}
                  options={FILTER_OPERATORS}
                  onChange={(next) => setConfig({ operator: next })}
                />
              </div>
            </Row>
            {needsValue && (
              <Row label="Value">
                <div className="w-36">
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
              <div className="w-36">
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
              <div className="w-36">
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
                    onChange={(event) => setConfig({ column: event.target.value })}
                  />
                )}
              </div>
            </Row>
            <Row label="Strategy">
              <div className="w-36">
                <SelectControl
                  value={node.config.strategy}
                  options={DEDUP_STRATEGIES}
                  onChange={(strategy) => setConfig({ strategy })}
                />
              </div>
            </Row>
            <Row label="Null handling">
              <div className="w-36">
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
              <div className="w-36">
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
                    onChange={(event) => setConfig({ column: event.target.value })}
                  />
                )}
              </div>
            </Row>
            <Row label="Null handling">
              <div className="w-36">
                <SelectControl
                  value={node.config.nullHandling}
                  options={NORMALIZE_NULL_HANDLING}
                  onChange={(nullHandling) => setConfig({ nullHandling })}
                />
              </div>
            </Row>
            <p className="pt-1 text-[12px] text-gray-400">
              Trims whitespace and lowercases email values.
            </p>
          </Section>
        )}

        {node.type === 'source' && (
          <Section title="Source">
            <Row label="File"><Value>{node.name}</Value></Row>
            {node.description && <Row label="Detail"><Value>{node.description}</Value></Row>}
            {node.rows && <Row label="Rows"><Value>{node.rows}</Value></Row>}
          </Section>
        )}

        {node.type === 'export' && (
          <Section title="Rule">
            <Row label="Format"><Value>CSV (UTF-8)</Value></Row>
            <Row label="File name"><Value>{node.name}</Value></Row>
          </Section>
        )}

        {/* Input / Output */}
        {(inputLabel || outputLabel) && (
          <Section title="Input / Output">
            {inputLabel && <Row label="Input"><Value>{inputLabel}</Value></Row>}
            {outputLabel && <Row label="Output"><Value>{outputLabel}</Value></Row>}
          </Section>
        )}

        {/* Impact — after a run */}
        {hasRun && metrics && (
          <Section title="Preview impact">
            <Row label="Sample evaluated"><Value>{formatRows(metrics.rowsIn)}</Value></Row>
            {node.type === 'filter' ? (
              <>
                <Row label="Matched"><Value>{formatRows(metrics.rowsOut)}</Value></Row>
                <Row label="Removed"><Value>{formatRows(metrics.rowsIn - metrics.rowsOut)}</Value></Row>
              </>
            ) : node.type === 'deduplicate' ? (
              <>
                <Row label="Duplicates removed"><Value>{formatRows(metrics.duplicates)}</Value></Row>
                <Row label="Rows out"><Value>{formatRows(metrics.rowsOut)}</Value></Row>
              </>
            ) : (
              <Row label="Rows out"><Value>{formatRows(metrics.rowsOut)}</Value></Row>
            )}
            <Row label="Errors"><Value>{node.error ? '1' : '0'}</Value></Row>
          </Section>
        )}

        {/* Last run */}
        {hasRun && metrics && (
          <Section title="Last run">
            <Row label="Duration">
              <Value>
                {metrics.duration < 1000
                  ? `${Math.round(metrics.duration)}ms`
                  : `${(metrics.duration / 1000).toFixed(1)}s`}
              </Value>
            </Row>
            <Row label="Memory"><Value>{metrics.memory} MB</Value></Row>
          </Section>
        )}

        {/* Relationships — only shown when both sides exist */}
        {(prevNode || nextNode) && (
          <Section title="Relationships" last>
            {prevNode && <Row label="Upstream"><Value>{prevNode.name}</Value></Row>}
            {nextNode && <Row label="Downstream"><Value>{nextNode.name}</Value></Row>}
          </Section>
        )}
      </div>

      {/* More Menu */}
      {showMenu && (
        <>
          <div className="fixed inset-0 z-20" onClick={() => setShowMenu(false)} />
          <div className="absolute top-14 right-4 w-[156px] p-[6px] border border-gray-200 rounded-lg bg-white shadow-xl z-30">
            {[
              { label: 'Duplicate node' },
              { label: 'Copy node' },
              { label: disabled ? 'Enable node' : 'Disable node' },
              { label: 'Delete node', delete: true },
            ].map((item) => (
              <button
                key={item.label}
                onClick={() => handleMenuAction(item)}
                className={`w-full h-[34px] px-[9px] border-0 rounded-md bg-transparent text-left text-[11.5px] cursor-pointer hover:bg-gray-100 ${item.delete ? 'text-red-600' : ''}`}
              >
                {item.label}
              </button>
            ))}
          </div>
        </>
      )}

      {/* Toast */}
      <div
        className={`fixed left-1/2 bottom-6 z-[100] px-[13px] py-[10px] rounded-lg bg-gray-900 text-white shadow-xl text-[12px] pointer-events-none transition-all duration-150 ease-out ${
          toast ? 'opacity-100' : 'opacity-0 translate-y-4'
        }`}
        style={{ transform: toast ? 'translateX(-50%)' : 'translateX(-50%) translateY(16px)' }}
        role="status"
        aria-live="polite"
      >
        {toast}
      </div>
    </div>
  );
};

export default DetailPanel;
