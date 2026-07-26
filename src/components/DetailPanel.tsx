import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  X,
  MoreHorizontal,
  Copy,
  Filter,
  Type,
  Eye,
  Play,
  Clock,
} from '../icons/hero';
import { formatRows } from '../utils/format';
import type { PipelineNode, NodeType, PipelineMetrics, WorkspaceEvent } from '../types';

const TYPE_ICON: Record<NodeType, React.ReactNode> = {
  deduplicate: <Copy size={20} />,
  normalize: <Type size={20} />,
  filter: <Filter size={20} />,
  source: <Copy size={20} />,
  export: <Copy size={20} />,
};

const TYPE_BG: Record<NodeType, string> = {
  deduplicate: 'bg-[#e8f7fe] text-[#0b6c96]',
  normalize: 'bg-[#e8f7fe] text-[#0b6c96]',
  filter: 'bg-[#e8f7fe] text-[#0b6c96]',
  source: 'bg-[#e8f7fe] text-[#0b6c96]',
  export: 'bg-[#e8f7fe] text-[#0b6c96]',
};

const TYPE_LABEL: Record<NodeType, string> = {
  source: 'Source Node',
  filter: 'Transform Node',
  deduplicate: 'Process Node',
  normalize: 'Transform Node',
  export: 'Output Node',
};

const CONFIG_OPTIONS: Record<string, string[]> = {
  strategy: ['Keep first', 'Keep last', 'Merge records'],
  scope: ['Current dataset', 'Selected branch', 'Entire pipeline'],
  nullHandling: ['Ignore', 'Treat as duplicate', 'Remove null rows'],
};

interface DetailPanelProps {
  node: PipelineNode;
  nodes: PipelineNode[];
  events?: WorkspaceEvent[];
  availableColumns?: string[];
  onClose: () => void;
  onRun: (nodeId: string) => void | Promise<void>;
  onPreview?: () => void;
  onUpdate: (nodeId: string, patch: Partial<PipelineNode>) => void;
  onDelete: (nodeId: string) => void;
  onDuplicate: (nodeId: string) => void;
}

const DetailPanel: React.FC<DetailPanelProps> = ({
  node,
  nodes,
  onClose,
  onRun,
  onPreview,
  onUpdate,
  onDelete,
  onDuplicate,
}) => {
  // The currently-running status comes from `node` (App-owned).
  const running = node.status === 'running';
  const disabled = node.status === 'disabled';
  const [editing, setEditing] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  const [editConfig, setEditConfig] = useState({ ...node.config });
  useEffect(() => {
    setEditConfig({ ...node.config });
  }, [node.id, node.config]);

  const toastTimer = useRef<number | undefined>(undefined);
  const showToast = useCallback((message: string) => {
    setToast(message);
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 1600);
  }, []);
  useEffect(() => () => window.clearTimeout(toastTimer.current), []);

  // Determine Input / Output context from the nodes list, relative to the selected
  const idx = nodes.findIndex((n) => n.id === node.id);
  const prevNode = idx > 0 ? nodes[idx - 1] : null;
  const nextNode = idx >= 0 && idx < nodes.length - 1 ? nodes[idx + 1] : null;

  // Loaded metrics are whatever the App recorded for this node.
  const metrics: PipelineMetrics = node.metrics ?? {
    rowsIn: 0,
    rowsOut: parseFloat((node.rows || '').replace(/[A-Za-z]/g, '')) || 0,
    duplicates: 0,
    missing: 0,
    nullColumns: 0,
    qualityScore: 0,
    duration: 0,
    memory: 0,
  };

  const hasRun = Boolean(node.metrics);

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

  const handleEdit = () => {
    if (editing) {
      onUpdate(node.id, { config: { ...editConfig } });
      showToast('Configuration saved');
    }
    setEditing((e) => !e);
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
    if (item.label === 'Copy node') {
      void navigator.clipboard?.writeText(node.name);
    }
    showToast(item.label);
  };

  // Progress driven by App's node status — for visual continuity show 0% when pending,
  // 100% when completed, indeterminate-ish for running.
  const progressPct = node.status === 'completed' ? 100 : node.status === 'running' ? 67 : 0;

  const rowsOutDisplay = formatRows(metrics.rowsOut);

  return (
    <div className="w-[356px] bg-white border-l border-[#e3e6e8] flex flex-col flex-shrink-0 overflow-hidden relative shadow-[-12px_0_32px_rgba(32,33,36,0.05)]">
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
          <div className="flex items-center gap-3 mt-3">
            <span
              className={`inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full ${
                node.status === 'completed'
                  ? 'text-green-700 bg-green-50'
                  : node.status === 'running'
                  ? 'text-white bg-[#20beff]'
                  : 'text-[#5f6368] bg-[#f1f3f4]'
              }`}
            >
              {node.status === 'completed' ? (
                <span className="w-1.5 h-1.5 bg-green-600 rounded-full" />
              ) : node.status === 'running' ? (
                <span className="w-1.5 h-1.5 rounded-full border-2 border-white border-t-transparent animate-spin" />
              ) : (
                <span className="w-1.5 h-1.5 bg-gray-400 rounded-full" />
              )}
              {node.status === 'completed'
                ? 'Completed'
                : node.status === 'running'
                  ? 'Running'
                  : node.status === 'disabled'
                    ? 'Disabled'
                    : node.status === 'failed'
                      ? 'Failed'
                      : 'Pending'}
            </span>
            <span className="text-[11px] text-gray-400 flex items-center gap-1">
              <Clock size={12} />
              {hasRun ? 'Updated just now' : 'Not run yet'}
            </span>
          </div>
        </div>

        {/* Context */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Context</h4>
          <div className="space-y-2.5">
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Input</span>
              <span className="text-[13px] font-medium text-gray-900">
                {prevNode ? prevNode.name : '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Output</span>
              <span className="text-[13px] font-medium text-gray-900">
                {nextNode ? nextNode.name : '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Relation</span>
              <span className="inline-flex items-center min-h-[22px] px-[9px] bg-gray-100 rounded-full text-[11px] font-medium text-gray-900">
                transforms
              </span>
            </div>
          </div>
        </div>

        {/* Runtime */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Runtime</h4>
          <div className="space-y-2.5">
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Rows</span>
              <span className="text-[13px] font-medium text-gray-900">
                {hasRun ? `${formatRows(metrics.rowsIn)} / ${formatRows(metrics.rowsOut)}` : '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Duration</span>
              <span className="text-[13px] font-medium text-gray-900">
                {hasRun ? (metrics.duration < 1000 ? `${metrics.duration}ms` : `${(metrics.duration / 1000).toFixed(1)}s`) : '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Memory</span>
              <span className="text-[13px] font-medium text-gray-900">
                {hasRun ? `${metrics.memory} MB` : '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Mode</span>
              <span className="text-[13px] font-medium text-gray-900">Incremental</span>
            </div>
          </div>
          <div className="mt-3">
            <div className="flex items-center justify-between mb-1.5">
              <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className="h-full bg-[#20beff] rounded-full transition-[width] duration-150 ease-out"
                  style={{ width: `${progressPct}%` }}
                />
              </div>
              <span className="text-[11px] text-gray-500 ml-2 font-medium">{progressPct}%</span>
            </div>
          </div>
        </div>

        {/* Metrics */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Metrics</h4>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-[#f5f7f8] rounded-lg p-3">
              <div className="text-[11px] text-gray-500 mb-1">Rows</div>
              <div className="text-lg font-bold text-gray-900">{hasRun ? rowsOutDisplay : '—'}</div>
              <div className="text-[11px] text-gray-400">output rows</div>
            </div>
            <div className="bg-[#f5f7f8] rounded-lg p-3 relative">
              <div className="text-[11px] text-gray-500 mb-1">Duplicates</div>
              <div className="text-lg font-bold text-gray-900">{hasRun ? `${metrics.duplicates}%` : '—'}</div>
              {hasRun && metrics.duplicates > 0 && (
                <div className="text-[11px] text-green-600 font-medium absolute right-3 bottom-3">↓ {(metrics.duplicates * 0.38).toFixed(1)}%</div>
              )}
            </div>
            <div className="bg-[#f5f7f8] rounded-lg p-3">
              <div className="text-[11px] text-gray-500 mb-1">Missing</div>
              <div className="text-lg font-bold text-gray-900">{hasRun ? `${metrics.missing}%` : '—'}</div>
              <div className="text-[11px] text-gray-400">across columns</div>
            </div>
            <div className="bg-[#f5f7f8] rounded-lg p-3">
              <div className="text-[11px] text-gray-500 mb-1">Quality Score</div>
              <div className="text-lg font-bold text-gray-900">{hasRun ? metrics.qualityScore : '—'}</div>
              {hasRun && (
                <div className="text-[11px] text-green-600 font-medium flex items-center gap-1">
                  <span className="w-1.5 h-1.5 bg-green-500 rounded-full" />
                  {metrics.qualityScore >= 80 ? 'Good' : metrics.qualityScore >= 60 ? 'Fair' : 'Poor'}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Actions</h4>
          <button
            onClick={handleRun}
            disabled={running || disabled}
            className="w-full bg-[#20beff] text-white text-[13px] font-medium py-2.5 rounded-lg flex items-center justify-center gap-2 hover:bg-[#0f9ad6] transition-colors mb-2 disabled:opacity-55 disabled:cursor-wait"
          >
            <Play size={14} fill="white" />
            <span>{running ? 'Running…' : 'Run From Here'}</span>
          </button>
          <div className="grid grid-cols-2 gap-2">
            <button
              onClick={() => {
                onPreview?.();
                showToast('Result preview opened');
              }}
              className="flex items-center justify-center gap-1.5 text-[12px] font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 py-2 rounded-lg transition-colors"
            >
              <Eye size={13} />
              Preview Result
            </button>
            <button
              onClick={handleDisable}
              className={`flex items-center justify-center gap-1.5 text-[12px] font-medium py-2 rounded-lg transition-colors ${
                disabled ? 'bg-gray-900 text-white hover:bg-gray-800' : 'text-gray-700 bg-gray-100 hover:bg-gray-200'
              }`}
            >
              <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                <rect x="6" y="4" width="12" height="16" rx="2" />
                <path d="M10 8h4m-4 4h4m-4 4h4" />
              </svg>
              <span>{disabled ? 'Enable' : 'Disable'}</span>
            </button>
          </div>
        </div>

        {/* Configuration */}
        <div className="p-4">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider">Configuration</h4>
            <button onClick={handleEdit} className="text-[12px] text-gray-900 font-medium hover:text-gray-700 transition-colors">
              {editing ? 'Save' : 'Edit'}
            </button>
          </div>
          <div className="space-y-2.5">
            {(['column', 'strategy', 'scope', 'nullHandling'] as const).map((field) => (
              <div key={field} className="flex items-center justify-between min-h-[28px]">
                <span className="text-[13px] text-gray-500">
                  {field === 'nullHandling' ? 'Null Handling' : field.charAt(0).toUpperCase() + field.slice(1)}
                </span>
                {editing ? (
                  field === 'column' ? (
                    <input
                      className="text-[13px] font-medium text-gray-900 text-right bg-transparent border border-gray-300 rounded-md px-2 py-0.5 w-36 outline-none focus:border-gray-400"
                      value={editConfig[field]}
                      onChange={(e) => setEditConfig((c) => ({ ...c, [field]: e.target.value }))}
                    />
                  ) : (
                    <select
                      className="text-[13px] font-medium text-gray-900 text-right bg-transparent border border-gray-300 rounded-md px-2 py-0.5 w-36 outline-none focus:border-gray-400"
                      value={editConfig[field]}
                      onChange={(e) => setEditConfig((c) => ({ ...c, [field]: e.target.value }))}
                    >
                      {CONFIG_OPTIONS[field].map((opt) => (
                        <option key={opt} value={opt}>{opt}</option>
                      ))}
                    </select>
                  )
                ) : (
                  <span className="text-[13px] font-medium text-gray-900">{node.config[field]}</span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* More Menu */}
      {showMenu && (
        <>
          <div className="fixed inset-0 z-20" onClick={() => setShowMenu(false)} />
          <div className="absolute top-14 right-4 w-[156px] p-[6px] border border-gray-200 rounded-lg bg-white shadow-xl z-30">
            {[
              { label: 'Duplicate node' },
              { label: 'Copy node' },
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
