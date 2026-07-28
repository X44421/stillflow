import { useRef, useState, type ReactNode } from "react";
import { cn } from "../utils/cn";
import { Icon } from "../lib/icons";
import { toFrame } from "../lib/scale";
import { useWorkspace, type SurfaceId } from "../lib/store";
import { IconBtn, Menu, type MenuItem } from "./ui";

export function Surface({
  id,
  icon,
  title,
  meta,
  collapsedLabel,
  extraMenu = [],
  headerRight,
  children,
  bodyClass,
}: {
  id: SurfaceId;
  icon?: string;
  title: ReactNode;
  meta?: string;
  collapsedLabel?: string;
  extraMenu?: MenuItem[];
  headerRight?: ReactNode;
  children: ReactNode;
  bodyClass?: string;
}) {
  const { s, d } = useWorkspace();
  const w = s.surfaces[id];
  const [menu, setMenu] = useState(false);
  const drag = useRef<{ sx: number; sy: number; ox: number; oy: number } | null>(null);
  const rs = useRef<{ sx: number; sy: number; ow: number; oh: number } | null>(null);

  if (!w.open || w.mode === "minimized") return null;

  const active = s.active === id;
  const collapsed = w.mode === "collapsed";
  const width = collapsed ? Math.min(w.w, 336) : w.w;
  const height = collapsed ? 40 : w.h;

  const menuItems: MenuItem[] = [
    ...extraMenu,
    ...(extraMenu.length ? [{ sep: true }] : []),
    { label: "Dock left", icon: "dockLeft", onClick: () => d({ t: "mode", id, mode: "docked", dock: "left" }) },
    { label: "Dock right", icon: "dockLeft", onClick: () => d({ t: "mode", id, mode: "docked", dock: "right" }) },
    { label: "Dock bottom", icon: "dockBottom", onClick: () => d({ t: "mode", id, mode: "docked", dock: "bottom" }) },
    {
      label: w.mode === "maximized" ? "Restore size" : "Maximize",
      icon: w.mode === "maximized" ? "restore" : "maximize",
      keys: "⇧↵",
      onClick: () => d({ t: "mode", id, mode: w.mode === "maximized" ? "floating" : "maximized" }),
    },
    { sep: true },
    { label: "Minimize to dock", icon: "minimize", onClick: () => d({ t: "mode", id, mode: "minimized" }) },
    { label: "Close surface", icon: "x", keys: "⌘W", danger: true, onClick: () => d({ t: "closeSurface", id }) },
  ];

  return (
    <div
      onPointerDown={() => !active && d({ t: "focusSurface", id })}
      className={cn(
        "absolute flex flex-col overflow-hidden rounded-[10px] border transition-[border-color,background-color] duration-150",
        active ? "border-[#c9d1d9] bg-surfa surface-shadow" : "border-line bg-surf island-shadow",
      )}
      style={{ left: w.x, top: w.y, width, height, zIndex: 40 + w.z }}
    >
      {/* header */}
      <div
        onPointerDown={(e) => {
          if (e.button !== 0) return;
          e.currentTarget.setPointerCapture(e.pointerId);
          drag.current = { sx: e.clientX, sy: e.clientY, ox: w.x, oy: w.y };
        }}
        onPointerMove={(e) => {
          const g = drag.current;
          if (!g) return;
          d({ t: "moveSurface", id, x: g.ox + toFrame(e.clientX - g.sx), y: g.oy + toFrame(e.clientY - g.sy) });
        }}
        onPointerUp={() => (drag.current = null)}
        onDoubleClick={() => d({ t: "mode", id, mode: collapsed ? "floating" : "collapsed" })}
        className={cn(
          "nosel relative flex h-10 shrink-0 cursor-grab items-center gap-2 px-3 active:cursor-grabbing",
          !collapsed && "border-b border-div",
        )}
      >
        {icon && (
          <span className={cn("shrink-0", active ? "text-t2" : "text-t3")}>
            <Icon name={icon} size={13.5} />
          </span>
        )}
        <div className="flex min-w-0 flex-1 items-baseline gap-2">
          <span className={cn("truncate text-[12.5px] font-medium tracking-[-0.01em]", active ? "text-t1" : "text-t2")}>
            {collapsed ? (collapsedLabel ?? title) : title}
          </span>
          {!collapsed && meta && <span className="tnum truncate text-[10.5px] text-t4">{meta}</span>}
        </div>
        <div className="flex shrink-0 items-center gap-0.5">
          {!collapsed && headerRight}
          <div className="relative">
            <IconBtn icon="more" size={22} iconSize={13} onClick={() => setMenu((v) => !v)} active={menu} />
            {menu && <Menu items={menuItems} onClose={() => setMenu(false)} style={{ right: 0, top: 26 }} />}
          </div>
          <IconBtn
            icon={collapsed ? "expand" : "collapse"}
            size={22}
            iconSize={13}
            onClick={() => d({ t: "mode", id, mode: collapsed ? "floating" : "collapsed" })}
          />
          <IconBtn icon="x" size={22} iconSize={13} onClick={() => d({ t: "closeSurface", id })} />
        </div>
      </div>

      {!collapsed && <div className={cn("flex min-h-0 flex-1 flex-col", bodyClass)}>{children}</div>}

      {/* resize handle — only on the active surface */}
      {!collapsed && active && w.mode === "floating" && (
        <div
          onPointerDown={(e) => {
            e.stopPropagation();
            e.currentTarget.setPointerCapture(e.pointerId);
            rs.current = { sx: e.clientX, sy: e.clientY, ow: w.w, oh: w.h };
          }}
          onPointerMove={(e) => {
            const g = rs.current;
            if (!g) return;
            d({ t: "resizeSurface", id, w: g.ow + toFrame(e.clientX - g.sx), h: g.oh + toFrame(e.clientY - g.sy) });
          }}
          onPointerUp={() => (rs.current = null)}
          className="absolute right-0 bottom-0 z-10 h-4 w-4 cursor-nwse-resize"
        >
          <svg viewBox="0 0 16 16" className="h-4 w-4 text-[#c9d1d9]">
            <path d="M15 9 9 15M15 13l-2 2" stroke="currentColor" strokeWidth="1.1" fill="none" strokeLinecap="round" />
          </svg>
        </div>
      )}
    </div>
  );
}
