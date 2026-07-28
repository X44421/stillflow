import { useEffect, useRef, type ReactNode } from "react";
import { cn } from "../utils/cn";
import { Icon } from "../lib/icons";
import { STATUS_COLOR } from "../lib/data";

export function useOutside<T extends HTMLElement>(ref: React.RefObject<T | null>, cb: () => void) {
  useEffect(() => {
    const h = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) cb();
    };
    window.addEventListener("mousedown", h, true);
    return () => window.removeEventListener("mousedown", h, true);
  }, [ref, cb]);
}

export function Tip({
  label,
  keys,
  side = "bottom",
  children,
}: {
  label: string;
  keys?: string;
  side?: "bottom" | "top" | "right" | "left";
  children: ReactNode;
}) {
  const pos =
    side === "bottom"
      ? "top-[calc(100%+8px)] left-1/2 -translate-x-1/2"
      : side === "top"
        ? "bottom-[calc(100%+8px)] left-1/2 -translate-x-1/2"
        : side === "right"
          ? "left-[calc(100%+8px)] top-1/2 -translate-y-1/2"
          : "right-[calc(100%+8px)] top-1/2 -translate-y-1/2";
  return (
    <span className="group/tip relative inline-flex">
      {children}
      <span
        className={cn(
          "pointer-events-none absolute z-[999] hidden whitespace-nowrap rounded-[6px] border border-[#dce2e8] bg-white px-2 py-1 text-[10.5px] text-t2 shadow-lg group-hover/tip:block",
          pos,
        )}
      >
        {label}
        {keys && <span className="ml-1.5 text-t4 tnum">{keys}</span>}
      </span>
    </span>
  );
}

export function IconBtn({
  icon,
  onClick,
  active,
  size = 28,
  iconSize = 15,
  tip,
  keys,
  side,
  tone = "default",
  className,
  disabled,
}: {
  icon: string;
  onClick?: (e: React.MouseEvent) => void;
  active?: boolean;
  size?: number;
  iconSize?: number;
  tip?: string;
  keys?: string;
  side?: "bottom" | "top" | "right" | "left";
  tone?: "default" | "quiet" | "danger";
  className?: string;
  disabled?: boolean;
}) {
  const btn = (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      onPointerDown={(e) => e.stopPropagation()}
      className={cn(
        "grid shrink-0 place-items-center rounded-[6px] transition-colors duration-100",
        tone === "quiet" ? "text-t4 hover:text-t2" : "text-t3 hover:text-t1",
        tone === "danger" && "hover:text-[#c95e62]",
        !disabled && "hover:bg-[#edf2f6]",
        disabled && "cursor-default text-t4/50",
        active && "bg-[#2196d2]/[0.1] text-[#1686be] shadow-[inset_0_0_0_1px_rgba(33,150,210,0.25)] hover:bg-[#2196d2]/[0.14]",
        className,
      )}
      style={{ width: size, height: size }}
    >
      <Icon name={icon} size={iconSize} />
    </button>
  );
  return tip ? (
    <Tip label={tip} keys={keys} side={side}>
      {btn}
    </Tip>
  ) : (
    btn
  );
}

export interface MenuItem {
  label?: string;
  icon?: string;
  keys?: string;
  sep?: boolean;
  danger?: boolean;
  disabled?: boolean;
  check?: boolean;
  onClick?: () => void;
}

export function Menu({
  items,
  onClose,
  className,
  style,
}: {
  items: MenuItem[];
  onClose: () => void;
  className?: string;
  style?: React.CSSProperties;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useOutside(ref, onClose);
  useEffect(() => {
    const h = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [onClose]);
  return (
    <div
      ref={ref}
      style={style}
      onPointerDown={(e) => e.stopPropagation()}
      className={cn(
        "fade-up absolute z-[900] min-w-[186px] rounded-[8px] border border-[#dce2e8] bg-white p-1 shadow-[0_20px_44px_-20px_rgba(0,0,0,0.15)]",
        className,
      )}
    >
      {items.map((it, i) =>
        it.sep ? (
          <div key={i} className="my-1 h-px bg-div" />
        ) : (
          <button
            key={i}
            disabled={it.disabled}
            onClick={() => {
              it.onClick?.();
              onClose();
            }}
            className={cn(
              "flex w-full items-center gap-2.5 rounded-[5px] px-2 py-[5px] text-left text-[11.5px] transition-colors",
              it.disabled ? "text-t4" : "text-t2 hover:bg-[#edf2f6] hover:text-t1",
              it.danger && !it.disabled && "hover:text-[#c95e62]",
            )}
          >
            <span className="grid w-4 place-items-center text-t3">{it.icon && <Icon name={it.icon} size={13} />}</span>
            <span className="flex-1">{it.label}</span>
            {it.check && <Icon name="check" size={11} className="text-[#2196d2]" />}
            {it.keys && <span className="tnum text-[10px] text-t4">{it.keys}</span>}
          </button>
        ),
      )}
    </div>
  );
}

export function StatusDot({ status, size = 6, pulse }: { status: string; size?: number; pulse?: boolean }) {
  const c = STATUS_COLOR[status] ?? "#51555d";
  return (
    <span
      className={cn("inline-block shrink-0 rounded-full", pulse && "pulse-dot")}
      style={{ width: size, height: size, background: c, boxShadow: `0 0 0 2.5px ${c}1f` }}
    />
  );
}

export function Kbd({ children }: { children: ReactNode }) {
  return (
    <span className="tnum rounded-[4px] border border-[#dce2e8] bg-[#f4f6f8] px-1.5 py-[1px] text-[10px] leading-[15px] text-t3">
      {children}
    </span>
  );
}

export function SectionLabel({ children, right }: { children: ReactNode; right?: ReactNode }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-[10px] font-medium tracking-[0.13em] text-t4 uppercase">{children}</span>
      {right}
    </div>
  );
}

export function Row({ k, v, mono, tone }: { k: string; v: ReactNode; mono?: boolean; tone?: string }) {
  return (
    <div className="flex items-baseline justify-between gap-3 py-[5px]">
      <span className="text-[11px] text-t3">{k}</span>
      <span className={cn("tnum text-[11.5px] text-t1", mono && "font-mono text-[11px]")} style={tone ? { color: tone } : undefined}>
        {v}
      </span>
    </div>
  );
}
