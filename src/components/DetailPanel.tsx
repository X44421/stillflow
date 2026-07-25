import React, { useCallback, useEffect, useRef, useState } from 'react';
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
  ChevronDown,
  ChevronRight,
} from '../icons/hero';
import { formatRows } from '../utils/duckdb';
import type { NodeType, PipelineNode, WorkspaceEvent } from '../types';

const TYPE_ICON: Record<NodeType, React.ReactNode> = {
  source: <FileText size={16} />,
  filter: <Filter size={16} />,
  deduplicate: <Copy size={16} />,
  normalize: <Type size={16} />,
  export: <Upload size={16} />,
};

const TYPE_LABEL: Record<NodeType, string> = {
  source: 'Atomic · Source',
  filter: 'Atomic · Transform',
  deduplicate: 'Atomic · Transform',
  normalize: 'Atomic · Transform',
  export: 'Atomic · Output',
};

const CONFIG_OPTIONS: Record<string, string[]> = {
  strategy: ['Keep first', 'Keep last'],
  scope: ['Current dataset', 'Selected branch', 'Entire pipeline'],
  nullHandling: ['Ignore', 'Treat as duplicate', 'Remove null rows'],
};

interface DetailPanelProps {
  node: PipelineNode;
  nodes: PipelineNode[];
  events: WorkspaceEvent[];
  availableColumns?: string[];
  onClose: () => void;
  onRun: (nodeId: string) => void | Promise<void>;
  onPreview: () => void;
  onUpdate: (nodeId: string, patch: Partial<PipelineNode>) => void;
  onDuplicate: (nodeId: string) => void;
  onDelete: (nodeId: string) => void;
}

const DetailPanel: React.FC<DetailPanelProps> = ({
  node,
  nodes,
  events,
  availableColumns = [],
  onClose,
  onRun,
  onPreview,
  onUpdate,
  onDuplicate,
  onDelete,
}) => {
  const running = node.status === 'running';
  const disabled = node.status === 'disabled';
  const [editing, setEditing] = useState(false);
  const [showMenu, setShowMenu] = useState(false);
  const [showInsight, setShowInsight] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [editConfig, setEditConfig] = useState({ ...node.config });
  const toastTimer = useRef<number | undefined>(undefined);

  useEffect(() => {
    setEditConfig({ ...node.config });
    setEditing(false);
  }, [node.id, node.config]);

  const showToast = useCallback((message: string) => {
    setToast(message);
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 1600);
  }, []);

  useEffect(() => () => window.clearTimeout(toastTimer.current), []);

  const index = nodes.findIndex((item) => item.id === node.id);
  const previousNode = index > 0 ? nodes[index - 1] : null;
  const nextNode = index >= 0 && index < nodes.length - 1 ? nodes[index + 1] : null;
  const objectEvents = events.filter((event) => event.objectId === node.id).slice(0, 3);
  const metrics = node.metrics;

  const handleRun = async () => {
    if (running || disabled) return;
    await onRun(node.id);
  };

  const handleEdit = () => {
    if (editing) {
      if (availableColumns.length > 0 && !availableColumns.includes(editConfig.column)) {
        showToast('Choose an available column');
        return;
      }
      onUpdate(node.id, { config: { ...editConfig }, status: 'pending', error: undefined });
      showToast('Configuration saved');
    }
    setEditing((value) => !value);
  };

  const handleToggleDisabled = () => {
    onUpdate(node.id, {
      status: disabled ? 'pending' : 'disabled',
      error: undefined,
    });
    showToast(disabled ? 'Object enabled' : 'Object disabled');
  };

  const statusLabel =
    node.status === 'completed'
      ? 'Ready'
      : node.status === 'running'
        ? 'Running'
        : node.status === 'failed'
          ? 'Failed'
          : node.status === 'disabled'
            ? 'Disabled'
            : 'Configured';

  return (
    <aside className="inspector-panel w-[360px] bg-white border-l border-gray-200 flex flex-col flex-shrink-0 overflow-hidden relative">
      <div className="flex-1 overflow-y-auto">
        <header className="p-4 border-b border-gray-200">
          <div className="flex items-start gap-3">
            <div className="w-8 h-8 grid place-items-center rounded border border-gray-200 text-gray-600">
              {TYPE_ICON[node.type]}
            </div>
            <div className="min-w-0 flex-1">
              <h3 className="truncate text-[14px] font-semibold text-gray-900">{node.name}</h3>
              <p className="text-[10px] uppercase text-gray-400">{TYPE_LABEL[node.type]}</p>
            </div>
            <button
              onClick={() => setShowMenu((visible) => !visible)}
              className="w-7 h-7 grid place-items-center rounded hover:bg-gray-100"
              aria-label="More actions"
            >
              <MoreHorizontal size={15} />
            </button>
            <button
              onClick={onClose}
              className="w-7 h-7 grid place-items-center rounded hover:bg-gray-100"
              aria-label="Close Inspector"
            >
              <X size={15} />
            </button>
          </div>
          <div className="mt-3 flex items-center gap-2">
            <span className="inline-flex items-center gap-1.5 text-[10px] font-medium px-2 py-1 rounded border border-gray-200 text-gray-700">
              <span
                className={`w-1.5 h-1.5 rounded-full ${
                  node.status === 'failed'
                    ? 'bg-gray-900'
                    : node.status === 'running'
                      ? 'bg-gray-700 animate-pulse-dot'
                      : 'bg-gray-400'
                }`}
              />
              {statusLabel}
            </span>
            <span className="text-[10px] text-gray-400">Object ID · {node.id}</span>
          </div>
          {node.error && (
            <div className="mt-3 px-2.5 py-2 border border-gray-300 bg-gray-50 rounded text-[11px] leading-4 text-gray-800">
              {node.error}
            </div>
          )}
        </header>

        <section className="p-4 border-b border-gray-200">
          <h4 className="section-label">Context</h4>
          <dl className="inspector-list">
            <div><dt>Mission</dt><dd>Customer Data Cleaning</dd></div>
            <div><dt>Consumes</dt><dd>{previousNode?.name ?? 'Workspace source'}</dd></div>
            <div><dt>Produces</dt><dd>{nextNode?.name ?? 'Published output'}</dd></div>
            <div><dt>Relationship</dt><dd>{node.type === 'export' ? 'publishes' : 'transforms'}</dd></div>
          </dl>
        </section>

        <section className="p-4 border-b border-gray-200">
          <h4 className="section-label">Runtime</h4>
          <dl className="inspector-list">
            <div>
              <dt>Rows</dt>
              <dd>{metrics ? `${formatRows(metrics.rowsIn)} → ${formatRows(metrics.rowsOut)}` : 'Not run'}</dd>
            </div>
            <div>
              <dt>Quality</dt>
              <dd>{metrics ? `${metrics.qualityScore}/100` : 'Not measured'}</dd>
            </div>
            <div>
              <dt>Duration</dt>
              <dd>{metrics ? `${metrics.duration} ms` : '—'}</dd>
            </div>
            <div>
              <dt>Memory</dt>
              <dd>{metrics ? `${metrics.memory} MB` : '—'}</dd>
            </div>
          </dl>
        </section>

        <section className="p-4 border-b border-gray-200">
          <h4 className="section-label">Actions</h4>
          <div className="grid grid-cols-2 gap-2">
            <button
              onClick={handleRun}
              disabled={running || disabled}
              className="h-8 col-span-2 bg-gray-900 text-white text-[11px] font-medium rounded flex items-center justify-center gap-1.5 hover:bg-black disabled:bg-gray-300 disabled:cursor-not-allowed"
            >
              <Play size={12} />
              {running ? 'Running' : 'Run from here'}
            </button>
            <button
              onClick={onPreview}
              disabled={!metrics}
              className="h-8 border border-gray-200 text-[11px] font-medium text-gray-700 rounded flex items-center justify-center gap-1.5 hover:bg-gray-50 disabled:text-gray-300"
            >
              <Eye size={13} />
              Preview
            </button>
            <button
              onClick={handleToggleDisabled}
              disabled={node.type === 'source'}
              className="h-8 border border-gray-200 text-[11px] font-medium text-gray-700 rounded hover:bg-gray-50 disabled:text-gray-300 disabled:hover:bg-white"
            >
              {disabled ? 'Enable' : 'Disable'}
            </button>
          </div>
        </section>

        <section className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-3">
            <h4 className="section-label mb-0">Configuration</h4>
            <button onClick={handleEdit} className="text-[10px] font-semibold text-gray-700 hover:text-black">
              {editing ? 'Save' : 'Edit'}
            </button>
          </div>
          <div className="space-y-2">
            {(['column', 'strategy', 'scope', 'nullHandling'] as const).map((field) => (
              <div key={field} className="min-h-7 flex items-center justify-between gap-3">
                <span className="text-[11px] text-gray-500">
                  {field === 'nullHandling'
                    ? 'Null handling'
                    : field.charAt(0).toUpperCase() + field.slice(1)}
                </span>
                {editing ? (
                  field === 'column' && availableColumns.length > 0 ? (
                    <select
                      className="w-40 h-7 px-2 rounded border border-gray-300 text-[11px] outline-none focus:border-gray-500"
                      value={editConfig[field]}
                      onChange={(event) =>
                        setEditConfig((current) => ({ ...current, [field]: event.target.value }))
                      }
                    >
                      {availableColumns.map((column) => (
                        <option key={column}>{column}</option>
                      ))}
                    </select>
                  ) : field === 'column' ? (
                    <input
                      className="w-40 h-7 px-2 rounded border border-gray-300 text-right text-[11px] outline-none focus:border-gray-500"
                      value={editConfig[field]}
                      onChange={(event) =>
                        setEditConfig((current) => ({ ...current, [field]: event.target.value }))
                      }
                    />
                  ) : (
                    <select
                      className="w-40 h-7 px-2 rounded border border-gray-300 text-[11px] outline-none focus:border-gray-500"
                      value={editConfig[field]}
                      onChange={(event) =>
                        setEditConfig((current) => ({ ...current, [field]: event.target.value }))
                      }
                    >
                      {CONFIG_OPTIONS[field].map((option) => (
                        <option key={option}>{option}</option>
                      ))}
                    </select>
                  )
                ) : (
                  <span className="max-w-44 truncate text-right text-[11px] font-medium text-gray-800">
                    {node.config[field]}
                  </span>
                )}
              </div>
            ))}
          </div>
        </section>

        <section className="border-b border-gray-200">
          <button
            onClick={() => setShowInsight((visible) => !visible)}
            className="w-full h-10 px-4 flex items-center justify-between hover:bg-gray-50"
          >
            <span className="section-label mb-0">AI Insight</span>
            {showInsight ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          </button>
          {showInsight && (
            <div className="px-4 pb-4 text-[11px] leading-5 text-gray-600">
              {metrics
                ? metrics.qualityScore < 90
                  ? `Review ${metrics.nullColumns} missing values before publishing. This is a suggestion; execution remains deterministic.`
                  : 'The current output is structurally consistent. Validate the business rules before publishing.'
                : 'Run this object to generate a context-aware suggestion.'}
            </div>
          )}
        </section>

        <section className="p-4">
          <h4 className="section-label">Recent Events</h4>
          {objectEvents.length === 0 ? (
            <div className="text-[11px] text-gray-400">No events for this object.</div>
          ) : (
            <div>
              {objectEvents.map((event) => (
                <div key={event.id} className="py-2 border-b border-gray-100 last:border-0">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-medium text-gray-700">{event.action}</span>
                    <span className="text-[10px] text-gray-400">{event.timestamp}</span>
                  </div>
                  <div className="mt-0.5 truncate text-[10px] text-gray-500">{event.detail}</div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>

      {showMenu && (
        <>
          <div className="fixed inset-0 z-20" onClick={() => setShowMenu(false)} />
          <div className="absolute top-12 right-4 z-30 w-36 p-1 border border-gray-200 rounded bg-white">
            <button
              onClick={() => {
                setShowMenu(false);
                onDuplicate(node.id);
              }}
              className="w-full h-8 px-2 rounded text-left text-[11px] hover:bg-gray-100"
            >
              Duplicate object
            </button>
            <button
              onClick={() => {
                setShowMenu(false);
                onDelete(node.id);
              }}
              disabled={node.type === 'source'}
              className="w-full h-8 px-2 rounded text-left text-[11px] hover:bg-gray-100 disabled:text-gray-300 disabled:hover:bg-white"
            >
              Delete object
            </button>
          </div>
        </>
      )}

      <div
        className={`fixed left-1/2 bottom-6 z-[100] px-3 py-2 rounded bg-gray-900 text-white text-[11px] pointer-events-none transition-all ${
          toast ? 'opacity-100' : 'opacity-0 translate-y-3'
        }`}
        style={{ transform: toast ? 'translateX(-50%)' : 'translateX(-50%) translateY(12px)' }}
        role="status"
        aria-live="polite"
      >
        {toast}
      </div>
    </aside>
  );
};

export default DetailPanel;
