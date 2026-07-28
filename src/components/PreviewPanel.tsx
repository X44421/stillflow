import type React from 'react';
import { ChevronRight } from 'lucide-react';

export interface PreviewStage {
  id: string;
  label: string;
  active: boolean;
  onSelect: () => void;
}

/**
 * The data preview is a fixed workspace region, not a floating window:
 * no minimize / close controls — its height is negotiated with the canvas
 * through the draggable divider owned by the App shell.
 *
 * The header is a single 44px strip: identity (title + version badge),
 * stage path, and the object-view tabs share one row. Stage facts live
 * in the data toolbar below instead of a second text line.
 */
export function PreviewPanel({
  title,
  badge,
  stages,
  tabs,
  children,
  showToggle,
  toggleMode,
  onToggleMode,
  outputAvailable,
  emptyHint,
}: {
  title: string;
  /** Small version chip next to the title (e.g. "v1", "Draft v2"). */
  badge?: string;
  stages?: PreviewStage[];
  /** Object-view tab strip, rendered between the stage path and the toggle. */
  tabs?: React.ReactNode;
  children: React.ReactNode;
  showToggle?: boolean;
  toggleMode?: 'input' | 'output';
  onToggleMode?: (mode: 'input' | 'output') => void;
  outputAvailable?: boolean;
  emptyHint?: string;
}) {
  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-lg border border-[#dce2e8] bg-white">
      {/* Header — one strip: title + badge · stage path · view tabs · toggle */}
      <div className="flex h-11 flex-shrink-0 items-center gap-2.5 border-b border-[#edf2f6] px-3">
        <span className="max-w-[220px] truncate text-[13px] font-semibold leading-[18px] text-[#171a1f]">
          {title}
        </span>
        {badge && (
          <span className="shrink-0 rounded bg-[#f4f6f8] px-1.5 py-0.5 text-[10.5px] font-medium text-[#5e6874]">
            {badge}
          </span>
        )}

        {stages && stages.length > 0 && (
          <div className="flex shrink-0 items-center gap-0.5 border-l border-[#edf2f6] pl-2.5">
            {stages.map((stage, index) => (
              <span key={stage.id} className="flex items-center gap-0.5">
                {index > 0 && (
                  <ChevronRight className="h-3 w-3 shrink-0 text-[#c9d1d9]" />
                )}
                <button
                  type="button"
                  onClick={stage.onSelect}
                  title={`Preview ${stage.label}`}
                  className={`h-[22px] rounded-[4px] px-1.5 text-[11px] font-medium transition-colors ${
                    stage.active
                      ? 'bg-[#e8f4fa] text-[#1686be]'
                      : 'text-[#5e6874] hover:bg-[#edf2f6]'
                  }`}
                >
                  {stage.label}
                </button>
              </span>
            ))}
          </div>
        )}

        <div className="min-w-0 flex-1" />

        {tabs && (
          <div className="flex shrink-0 items-center gap-0.5">{tabs}</div>
        )}

        {showToggle && (
          <div className="flex shrink-0 items-center rounded-[4px] border border-[#dce2e8] p-px">
            <button
              type="button"
              onClick={() => onToggleMode?.('input')}
              className={`h-[22px] rounded-[3px] px-2 text-[10.5px] font-medium transition-colors ${
                toggleMode === 'input'
                  ? 'bg-[#e8f4fa] text-[#1686be]'
                  : 'text-[#9099a4] hover:bg-[#edf2f6]'
              }`}
            >
              Input
            </button>
            <button
              type="button"
              onClick={() => {
                if (outputAvailable) onToggleMode?.('output');
              }}
              className={`h-[22px] rounded-[3px] px-2 text-[10.5px] font-medium transition-colors ${
                toggleMode === 'output'
                  ? 'bg-[#e8f4fa] text-[#1686be]'
                  : outputAvailable
                    ? 'text-[#9099a4] hover:bg-[#edf2f6]'
                    : 'cursor-default text-[#c9d1d9]'
              }`}
              title={
                outputAvailable ? 'View output rows' : 'Preview changes to see output'
              }
            >
              Output
            </button>
          </div>
        )}
      </div>

      {/* Content */}
      <div className="min-h-0 flex-1 overflow-hidden">
        {children ? (
          <div className="flex h-full flex-col overflow-hidden">{children}</div>
        ) : (
          <div className="flex h-full flex-col items-center justify-center gap-2 p-6 text-center">
            <span className="text-[13px] text-[#5e6874]">{emptyHint ?? 'No preview available'}</span>
            <span className="text-[12px] text-[#9099a4]">
              Import or select a dataset to see a preview
            </span>
          </div>
        )}
      </div>
    </div>
  );
}
