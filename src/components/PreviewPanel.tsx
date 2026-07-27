import type React from 'react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { ChevronDown, Minus, X } from 'lucide-react';

const MIN_HEIGHT = 160;
const MAX_HEIGHT_RATIO = 0.75;
const DEFAULT_HEIGHT = 420;
const MINIMIZED_HEIGHT = 40;

export function PreviewPanel({
  title,
  subtitle,
  children,
  isOpen,
  onClose,
  emptyHint,
}: {
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  isOpen: boolean;
  onClose: () => void;
  emptyHint?: string;
}) {
  const [expanded, setExpanded] = useState(true);
  const [height, setHeight] = useState(DEFAULT_HEIGHT);
  const [isDragging, setIsDragging] = useState(false);
  const dragRef = useRef<{ startY: number; startH: number } | null>(null);

  useEffect(() => {
    if (isOpen) setExpanded(true);
  }, [isOpen]);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragRef.current = { startY: e.clientY, startH: height };
    setIsDragging(true);
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  }, [height]);

  useEffect(() => {
    const handleMove = (e: MouseEvent) => {
      if (!dragRef.current) return;
      const delta = dragRef.current.startY - e.clientY;
      const maxH = window.innerHeight * MAX_HEIGHT_RATIO;
      const newH = Math.max(MIN_HEIGHT, Math.min(maxH, dragRef.current.startH + delta));
      setHeight(newH);
      if (!expanded) setExpanded(true);
    };
    const handleUp = () => {
      dragRef.current = null;
      setIsDragging(false);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    if (isDragging) {
      document.addEventListener('mousemove', handleMove);
      document.addEventListener('mouseup', handleUp);
    }
    return () => {
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };
  }, [isDragging, expanded]);

  if (!isOpen) return null;

  const displayHeight = expanded ? height : MINIMIZED_HEIGHT;

  return (
    <div
      className="flex-shrink-0 overflow-hidden rounded-b-xl border border-t-0 border-[#e3e6e8] bg-white shadow-[0_-4px_16px_rgba(0,0,0,0.04)] transition-[height] duration-200 ease-out"
      style={{ height: displayHeight }}
    >
      {/* Drag handle */}
      <div
        onMouseDown={handleResizeStart}
        className="group flex h-2.5 flex-shrink-0 cursor-row-resize items-center justify-center"
      >
        <div className="h-1 w-8 rounded-full bg-[#d4d4d8] transition-colors group-hover:bg-[#71717a]" />
      </div>

      {/* Header */}
      <div className="flex h-11 flex-shrink-0 items-center border-b border-[#e3e6e8] px-3">
        <div className="min-w-0 flex-1">
          <span className="truncate text-[13px] font-semibold text-[#202124]">{title}</span>
          {subtitle && <span className="ml-2 text-[12px] text-[#5f6368]">({subtitle})</span>}
        </div>
        <div className="ml-auto flex items-center gap-1">
          <button
            type="button"
            title={expanded ? 'Minimize preview' : 'Expand preview'}
            aria-label={expanded ? 'Minimize preview' : 'Expand preview'}
            onClick={() => setExpanded((e) => !e)}
            className="grid h-8 w-8 place-items-center rounded-full text-[#5f6368] hover:bg-[#f1f3f4]"
          >
            {expanded ? <Minus className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
          </button>
          <button
            type="button"
            title="Close preview"
            aria-label="Close preview"
            onClick={onClose}
            className="grid h-8 w-8 place-items-center rounded-full text-[#5f6368] hover:bg-[#f1f3f4]"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="min-h-0 flex-1 overflow-hidden">
        {children ? (
          <div className="h-full overflow-auto">{children}</div>
        ) : (
          <div className="flex h-full flex-col items-center justify-center gap-2 p-6 text-center">
            <span className="text-[13px] text-[#5f6368]">{emptyHint ?? 'No preview available'}</span>
            <span className="text-[12px] text-[#a1a1aa]">
              Import or select a dataset to see a preview
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
