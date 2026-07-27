import React from 'react';
import { LayoutGrid, Table, BarChart3, Calendar, Sparkles, Library, Settings, HelpCircle, FileText } from 'lucide-react';

export type RailMode =
  | 'assets'
  | 'search'
  | 'switcher'
  | 'canvas'
  | 'preview'
  | 'quality'
  | 'events'
  | 'ai'
  | 'library';

const items: { id: RailMode; label: string; icon: React.ComponentType<{ size?: number }> }[] = [
  { id: 'assets', label: 'Assets', icon: FileText },
  { id: 'search', label: 'Search', icon: SearchPlaceholder },
  { id: 'switcher', label: 'Switcher', icon: SwitcherPlaceholder },
  { id: 'canvas', label: 'Canvas', icon: LayoutGrid },
  { id: 'preview', label: 'Data Preview', icon: Table },
  { id: 'quality', label: 'Quality Review', icon: BarChart3 },
  { id: 'events', label: 'Events', icon: Calendar },
  { id: 'ai', label: 'AI Context', icon: Sparkles },
  { id: 'library', label: 'Capability Library', icon: Library },
];

function SearchPlaceholder({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="11" cy="11" r="8" />
      <path d="m21 21-4.35-4.35" />
    </svg>
  );
}

function SwitcherPlaceholder({ size = 16 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M4 7V4h3M4 17v3h3M20 7V4h-3M20 17v3h-3M9 9h6v6H9z" />
    </svg>
  );
}

export function NavRail({ mode, onMode }: { mode: RailMode; onMode: (m: RailMode) => void }) {
  return (
    <div className="flex w-[46px] shrink-0 flex-col items-center gap-[2px] border-r border-[#e3e6e8] bg-[#f5f7f8] py-2">
      {items.map((it) => (
        <Tip key={it.id} label={it.label}>
          <button
            onClick={() => onMode(it.id)}
            className="focus-ring relative flex h-[32px] w-[32px] items-center justify-center rounded-[6px] transition-colors duration-100"
            style={{
              color: mode === it.id ? '#202124' : '#5f6368',
              background: mode === it.id ? '#e3e6e8' : 'transparent',
            }}
            onMouseEnter={(e) => {
              if (mode !== it.id) e.currentTarget.style.background = '#e3e6e8';
            }}
            onMouseLeave={(e) => {
              if (mode !== it.id) e.currentTarget.style.background = 'transparent';
            }}
          >
            {mode === it.id && (
              <span className="absolute left-[-6px] top-1/2 h-[14px] w-[2px] -translate-y-1/2 rounded-full bg-[#18181b]" />
            )}
            <it.icon size={16} />
          </button>
        </Tip>
      ))}
      <div className="flex-1" />
      <Tip label="Settings">
        <button className="focus-ring relative flex h-[32px] w-[32px] items-center justify-center rounded-[6px] text-[#5f6368] hover:bg-[#e3e6e8]">
          <Settings size={16} />
        </button>
      </Tip>
      <Tip label="Help">
        <button className="focus-ring relative flex h-[32px] w-[32px] items-center justify-center rounded-[6px] text-[#5f6368] hover:bg-[#e3e6e8]">
          <HelpCircle size={16} />
        </button>
      </Tip>
    </div>
  );
}

function Tip({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <span className="group/tip relative inline-flex">
      {children}
      <span className="pointer-events-none absolute left-full top-1/2 z-50 ml-2 -translate-y-1/2 whitespace-nowrap rounded-[4px] border border-[#e3e6e8] bg-white px-1.5 py-[3px] text-[10.5px] text-[#5f6368] opacity-0 shadow-lg transition-opacity duration-100 group-hover/tip:opacity-100 group-hover/tip:delay-300">
        {label}
      </span>
    </span>
  );
}
