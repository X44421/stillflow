import React from 'react';
import { HelpCircle, Bell, ChevronDown, Check, Play, Search } from '../icons/hero';

interface HeaderProps {
  running?: boolean;
  progress?: number;
  onRunAll?: () => void;
  savedLabel?: string;
  statusLabel?: string;
}

const Header: React.FC<HeaderProps> = ({
  running = false,
  progress = 0,
  onRunAll,
  savedLabel = 'Saved 2m ago',
  statusLabel = 'Published',
}) => {
  return (
    <header className="h-14 bg-white border-b border-gray-200 flex items-center justify-between px-4 flex-shrink-0">
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 bg-gray-900 rounded-lg flex items-center justify-center">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <rect x="1" y="1" width="6" height="6" rx="1" fill="white" />
            <rect x="9" y="1" width="6" height="6" rx="1" fill="white" />
            <rect x="1" y="9" width="6" height="6" rx="1" fill="white" />
            <rect x="9" y="9" width="6" height="6" rx="1" fill="white" />
          </svg>
        </div>
        <div className="flex items-center gap-2 cursor-pointer">
          <h1 className="text-[15px] font-semibold text-gray-900">Customer Data Cleaning</h1>
          <ChevronDown size={16} className="text-gray-500" />
        </div>
        <span className="flex items-center gap-1.5 bg-green-50 text-green-700 text-xs font-medium px-2.5 py-1 rounded-full border border-green-200">
          <span className="w-1.5 h-1.5 bg-green-500 rounded-full"></span>
          {statusLabel}
        </span>
        <span className="flex items-center gap-1.5 text-gray-500 text-xs ml-1">
          <Check size={14} className="text-gray-400" />
          {savedLabel}
        </span>
      </div>
      <div className="flex items-center gap-1 flex-1 justify-end max-w-[420px]">
        <div className="relative hidden sm:block">
          <Search size={16} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search nodes…"
            className="h-8 w-44 pl-8 pr-3 text-[13px] border border-gray-200 rounded-lg bg-gray-50 focus:bg-white focus:border-gray-300 focus:outline-none transition-colors placeholder:text-gray-400"
            onChange={(e) => {
              const ev = new CustomEvent('opencode:search-nodes', { detail: e.target.value });
              window.dispatchEvent(ev);
            }}
          />
        </div>
        <button className="p-2 hover:bg-gray-100 rounded-lg transition-colors">
          <HelpCircle size={18} className="text-gray-600" />
        </button>
        <button className="p-2 hover:bg-gray-100 rounded-lg transition-colors relative">
          <Bell size={18} className="text-gray-600" />
        </button>
        {running && progress > 0 && progress < 100 && (
          <span className="text-[11px] text-gray-500 font-medium mx-1">
            {progress}%
          </span>
        )}
        <button
          onClick={onRunAll}
          disabled={running}
          className={`ml-2 text-white text-sm font-medium px-4 py-2 rounded-lg flex items-center gap-2 transition-colors ${
            running ? 'bg-gray-700 cursor-wait' : 'bg-gray-900 hover:bg-gray-800'
          }`}
        >
          <Play size={14} fill="white" />
          {running ? 'Running…' : 'Run All'}
        </button>
      </div>
    </header>
  );
};

export default Header;