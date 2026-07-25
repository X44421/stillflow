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
} from 'lucide-react';

interface DetailPanelProps {
  onClose: () => void;
}

const CONFIG_OPTIONS: Record<string, string[]> = {
  strategy: ['Keep first', 'Keep last', 'Merge records'],
  scope: ['Current dataset', 'Selected branch', 'Entire pipeline'],
  nullHandling: ['Ignore', 'Treat as duplicate', 'Remove null rows'],
};

const DetailPanel: React.FC<DetailPanelProps> = ({ onClose }) => {
  const [running, setRunning] = useState(false);
  const [disabled, setDisabled] = useState(false);
  const [editing, setEditing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [showMenu, setShowMenu] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [status, setStatus] = useState<'running' | 'completed'>('running');
  const [updatedAt, setUpdatedAt] = useState('Updated 2m ago');
  const [config, setConfig] = useState({
    column: 'customer_id',
    strategy: 'Keep first',
    scope: 'Current dataset',
    nullHandling: 'Ignore',
  });
  const [editConfig, setEditConfig] = useState({ ...config });

  const toastTimer = useRef<number | undefined>(undefined);

  const showToast = useCallback((message: string) => {
    setToast(message);
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 1600);
  }, []);

  useEffect(() => {
    return () => window.clearTimeout(toastTimer.current);
  }, []);

  const handleRun = () => {
    if (running || disabled) {
      if (disabled) showToast('Enable the node before running');
      return;
    }

    setRunning(true);
    setStatus('running');
    setProgress(0);

    let p = 0;
    const timer = window.setInterval(() => {
      p = Math.min(100, p + 8);
      setProgress(p);

      if (p >= 100) {
        window.clearInterval(timer);
        setRunning(false);
        setStatus('completed');
        setUpdatedAt('Updated just now');
        showToast('Node completed successfully');
      }
    }, 120);
  };

  const handleDisable = () => {
    const next = !disabled;
    setDisabled(next);
    showToast(next ? 'Node disabled' : 'Node enabled');
  };

  const handleEdit = () => {
    if (editing) {
      setConfig({ ...editConfig });
      showToast('Configuration saved');
    }
    setEditing((e) => !e);
  };

  const handleMenuAction = (message: string) => {
    showToast(message);
    setShowMenu(false);
  };

  return (
    <div className="w-[356px] bg-white border-l border-gray-200 flex flex-col flex-shrink-0 overflow-hidden relative shadow-[-12px_0_32px_rgba(16,24,40,0.05)]">
      <div className="flex-1 overflow-y-auto" style={{ scrollbarWidth: 'thin', scrollbarColor: '#d9dadd transparent' }}>
        {/* Header */}
        <div className="p-4 pb-3 border-b border-gray-100">
          <div className="flex items-start justify-between mb-1">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 bg-violet-50 rounded-xl flex items-center justify-center text-violet-600">
                <Copy size={20} />
              </div>
              <div>
                <h3 className="text-[15px] font-semibold text-gray-900">Deduplicate</h3>
                <p className="text-[12px] text-gray-500">Process Node</p>
              </div>
            </div>
            <div className="flex items-center gap-0.5">
              <button
                onClick={() => setShowMenu((m) => !m)}
                className="p-1.5 hover:bg-gray-100 rounded-md transition-colors"
                aria-label="More actions"
              >
                <MoreHorizontal size={16} className="text-gray-500" />
              </button>
              <button
                onClick={onClose}
                className="p-1.5 hover:bg-gray-100 rounded-md transition-colors"
                aria-label="Close Inspector"
              >
                <X size={16} className="text-gray-500" />
              </button>
            </div>
          </div>
          <div className="flex items-center gap-3 mt-3">
            <span
              className={`inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full ${
                status === 'completed'
                  ? 'text-green-700 bg-green-50'
                  : 'text-white bg-gray-900'
              }`}
            >
              {status === 'completed' ? (
                <span className="w-1.5 h-1.5 bg-green-600 rounded-full" />
              ) : (
                <span className="w-1.5 h-1.5 rounded-full border-2 border-white border-t-transparent animate-spin" />
              )}
              {status === 'completed' ? 'Completed' : 'Running'}
            </span>
            <span className="text-[11px] text-gray-400 flex items-center gap-1">
              <Clock size={12} />
              {updatedAt}
            </span>
          </div>
        </div>

        {/* Context */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Context</h4>
          <div className="space-y-2.5">
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Input</span>
              <span className="flex items-center gap-1.5 text-[13px] font-medium text-gray-900">
                <div className="w-4 h-4 bg-green-50 rounded flex items-center justify-center text-green-600">
                  <Filter size={10} />
                </div>
                Filter
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Output</span>
              <span className="flex items-center gap-1.5 text-[13px] font-medium text-gray-900">
                <div className="w-4 h-4 bg-blue-50 rounded flex items-center justify-center text-blue-600">
                  <Type size={10} />
                </div>
                Normalize Text
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
              <span className="text-[13px] font-medium text-gray-900">1.2M / 1.8M</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Duration</span>
              <span className="text-[13px] font-medium text-gray-900" id="durationValue">2.4s</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[13px] text-gray-500">Memory</span>
              <span className="text-[13px] font-medium text-gray-900">186 MB</span>
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
                  className="h-full bg-gray-900 rounded-full transition-[width] duration-150 ease-out"
                  style={{ width: `${progress || 67}%` }}
                />
              </div>
              <span className="text-[11px] text-gray-500 ml-2 font-medium">{progress || 67}%</span>
            </div>
          </div>
        </div>

        {/* Metrics */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Metrics</h4>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-gray-50 rounded-xl p-3">
              <div className="text-[11px] text-gray-500 mb-1">Rows</div>
              <div className="text-lg font-bold text-gray-900">1.2M</div>
              <div className="text-[11px] text-gray-400">after dedup</div>
            </div>
            <div className="bg-gray-50 rounded-xl p-3 relative">
              <div className="text-[11px] text-gray-500 mb-1">Duplicates</div>
              <div className="text-lg font-bold text-gray-900">8.4%</div>
              <div className="text-[11px] text-green-600 font-medium absolute right-3 bottom-3">↓ 3.2%</div>
            </div>
            <div className="bg-gray-50 rounded-xl p-3">
              <div className="text-[11px] text-gray-500 mb-1">Missing</div>
              <div className="text-lg font-bold text-gray-900">12.1%</div>
              <div className="text-[11px] text-gray-400">email column</div>
            </div>
            <div className="bg-gray-50 rounded-xl p-3">
              <div className="text-[11px] text-gray-500 mb-1">Quality Score</div>
              <div className="text-lg font-bold text-gray-900">91</div>
              <div className="text-[11px] text-green-600 font-medium flex items-center gap-1">
                <span className="w-1.5 h-1.5 bg-green-500 rounded-full" />
                Good
              </div>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="p-4 border-b border-gray-100">
          <h4 className="text-[12px] font-semibold text-gray-500 uppercase tracking-wider mb-3">Actions</h4>
          <button
            onClick={handleRun}
            disabled={running}
            className="w-full bg-gray-900 text-white text-[13px] font-medium py-2.5 rounded-lg flex items-center justify-center gap-2 hover:bg-gray-800 transition-colors mb-2 disabled:opacity-55 disabled:cursor-wait"
          >
            <Play size={14} fill="white" />
            <span>{running ? 'Running…' : 'Run From Here'}</span>
          </button>
          <div className="grid grid-cols-2 gap-2">
            <button
              onClick={() => showToast('Result preview opened')}
              className="flex items-center justify-center gap-1.5 text-[12px] font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 py-2 rounded-lg transition-colors"
            >
              <Eye size={13} />
              Preview Result
            </button>
            <button
              onClick={handleDisable}
              className={`flex items-center justify-center gap-1.5 text-[12px] font-medium py-2 rounded-lg transition-colors ${
                disabled
                  ? 'bg-gray-900 text-white hover:bg-gray-800'
                  : 'text-gray-700 bg-gray-100 hover:bg-gray-200'
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
                  <span className="text-[13px] font-medium text-gray-900">{config[field]}</span>
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
              { label: 'Duplicate node', message: 'Node duplicated' },
              { label: 'Copy node', message: 'Node copied' },
              { label: 'Delete node', message: 'Delete action requested', danger: true },
            ].map((item) => (
              <button
                key={item.label}
                onClick={() => handleMenuAction(item.message)}
                className={`w-full h-[34px] px-[9px] border-0 rounded-md bg-transparent text-left text-[11.5px] cursor-pointer hover:bg-gray-100 ${item.danger ? 'text-red-600' : ''}`}
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
