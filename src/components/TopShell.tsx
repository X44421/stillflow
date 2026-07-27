export function TopShell() {
  return (
    <div className="flex h-[38px] shrink-0 items-center gap-2 border-b border-[#e3e6e8] bg-white px-3">
      <div className="flex items-center gap-2">
        <span className="flex h-[18px] w-[18px] items-center justify-center rounded-[4px] bg-[#18181b] text-[10px] font-semibold text-white">
          S
        </span>
        <span className="text-[13px] font-semibold text-[#202124]">StillFlow</span>
      </div>
      <span className="text-[11px] text-[#5f6368]">/</span>
      <span className="text-[12px] text-[#5f6368]">My Workspace</span>
      <div className="flex-1" />
      <div className="flex items-center gap-2">
        <span className="text-[12px] text-[#5f6368]">Search or run a command</span>
      </div>
    </div>
  );
}
