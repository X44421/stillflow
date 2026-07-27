export function StatusStrip({
  message = 'Ready',
  selectionCount = 0,
}: {
  message?: string;
  selectionCount?: number;
}) {
  return (
    <div className="flex h-[24px] shrink-0 items-center gap-4 border-t border-[#e3e6e8] bg-[#f5f7f8] px-3 text-[11px] text-[#5f6368]">
      <span>{message}</span>
      <span className="flex-1" />
      <span>
        selection — {selectionCount ? `${selectionCount} object${selectionCount > 1 ? 's' : ''}` : 'none'}
      </span>
    </div>
  );
}
