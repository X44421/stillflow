import React from 'react';
import { X } from 'lucide-react';

export function BottomPanel({
  title,
  children,
  onClose,
}: {
  title: string;
  children: React.ReactNode;
  onClose?: () => void;
}) {
  return (
    <div className="flex h-[200px] shrink-0 flex-col border-t border-[#e3e6e8] bg-white">
      <div className="flex h-[30px] shrink-0 items-center border-b border-[#e3e6e8] px-3">
        <span className="text-[12px] font-medium text-[#202124]">{title}</span>
        <div className="ml-auto flex items-center gap-1">
          {onClose && (
            <button
              onClick={onClose}
              className="grid h-6 w-6 place-items-center rounded-full text-[#5f6368] hover:bg-[#f1f3f4]"
              aria-label="Close panel"
            >
              <X size={12} />
            </button>
          )}
        </div>
      </div>
      <div className="min-h-0 flex-1 overflow-hidden">{children}</div>
    </div>
  );
}
