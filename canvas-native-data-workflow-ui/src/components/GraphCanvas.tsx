import { useEffect, useRef, useState } from "react";
import { cn } from "../utils/cn";
import { Icon } from "../lib/icons";
import { scaleRef } from "../lib/scale";
import { useWorkspace, type WState } from "../lib/store";
import {
  ANNOTATION,
  COMMENT_PIN,
  GROUP_FRAME,
  ICON_BY_KIND,
  NODE_H,
  NODE_W,
  STATUS_COLOR,
  type GEdge,
  type GNode,
} from "../lib/data";
import { IconBtn, Menu, type MenuItem } from "./ui";

/* ------------------------------------------------------------------ */
/* geometry                                                            */
/* ------------------------------------------------------------------ */

type Pt = { x: number; y: number };

function portPos(n: GNode, port: string): Pt {
  if (port === "left") return { x: n.x, y: n.y + NODE_H / 2 };
  if (port === "right") return { x: n.x + NODE_W, y: n.y + NODE_H / 2 };
  if (port === "top") return { x: n.x + NODE_W / 2, y: n.y };
  return { x: n.x + NODE_W / 2, y: n.y + NODE_H };
}

function edgePath(a: Pt, b: Pt, vertical: boolean) {
  const gap = 7;
  if (vertical) {
    if (Math.abs(a.x - b.x) < 1) return `M${a.x} ${a.y} L${b.x} ${b.y - gap}`;
    const my = (a.y + b.y) / 2;
    const r = 9;
    const dir = b.x > a.x ? 1 : -1;
    return `M${a.x} ${a.y} V${my - r} Q${a.x} ${my} ${a.x + r * dir} ${my} H${b.x - r * dir} Q${b.x} ${my} ${b.x} ${my + r} V${b.y - gap}`;
  }
  if (Math.abs(a.y - b.y) < 1) return `M${a.x} ${a.y} L${b.x - gap} ${b.y}`;
  const mx = (a.x + b.x) / 2;
  const r = 9;
  const dir = b.y > a.y ? 1 : -1;
  return `M${a.x} ${a.y} H${mx - r} Q${mx} ${a.y} ${mx} ${a.y + r * dir} V${b.y - r * dir} Q${mx} ${b.y} ${mx + r} ${b.y} H${b.x - gap}`;
}

function arrow(b: Pt, vertical: boolean) {
  return vertical
    ? `M${b.x - 3.4} ${b.y - 7.5} L${b.x} ${b.y - 1} L${b.x + 3.4} ${b.y - 7.5} Z`
    : `M${b.x - 7.5} ${b.y - 3.4} L${b.x - 1} ${b.y} L${b.x - 7.5} ${b.y + 3.4} Z`;
}

function edgeEnds(s: WState, e: GEdge) {
  const a = s.nodes.find((n) => n.id === e.from);
  const b = s.nodes.find((n) => n.id === e.to);
  if (!a || !b) return null;
  return { a, b, p1: portPos(a, e.fromPort), p2: portPos(b, e.toPort) };
}

/* ------------------------------------------------------------------ */
/* node                                                                */
/* ------------------------------------------------------------------ */

function Port({ side, visible, compatible }: { side: string; visible: boolean; compatible?: boolean }) {
  const pos: Record<string, string> = {
    left: "left-0 top-1/2 -translate-x-1/2 -translate-y-1/2",
    right: "right-0 top-1/2 translate-x-1/2 -translate-y-1/2",
    top: "left-1/2 top-0 -translate-x-1/2 -translate-y-1/2",
    bottom: "left-1/2 bottom-0 -translate-x-1/2 translate-y-1/2",
  };
  return (
    <span className={cn("absolute grid h-[22px] w-[22px] place-items-center", pos[side])}>
      <span
        className={cn(
          "block h-[9px] w-[9px] rounded-full border transition-all duration-150",
          visible
            ? compatible
              ? "border-[#2196d2] bg-[#ddf2fc]"
              : "border-[#c9d1d9] bg-white"
            : "border-transparent bg-[#dce2e8]/70 scale-[0.55]",
        )}
      />
    </span>
  );
}

function NodeCard({ n }: { n: GNode }) {
  const { s, d } = useWorkspace();
  const selected = s.selected === n.id;
  const hovered = s.hovered === n.id;
  const drag = useRef<{ sx: number; sy: number; moved: boolean } | null>(null);
  const color = STATUS_COLOR[n.status];
  const running = n.status === "running";
  const failed = n.status === "failed";
  const waiting = n.status === "waiting";

  const connecting = s.tool === "connect";
  const ports = selected || hovered || connecting;

  return (
    <div
      data-node
      onPointerDown={(e) => {
        if (e.button !== 0) return;
        e.stopPropagation();
        e.currentTarget.setPointerCapture(e.pointerId);
        drag.current = { sx: e.clientX, sy: e.clientY, moved: false };
        if (!selected) d({ t: "select", id: n.id });
      }}
      onPointerMove={(e) => {
        const g = drag.current;
        if (!g) return;
        const dx = (e.clientX - g.sx) / (scaleRef.current * s.zoom);
        const dy = (e.clientY - g.sy) / (scaleRef.current * s.zoom);
        if (Math.abs(dx) + Math.abs(dy) < 0.4) return;
        g.sx = e.clientX;
        g.sy = e.clientY;
        g.moved = true;
        d({ t: "moveNode", id: n.id, dx, dy });
      }}
      onPointerUp={() => (drag.current = null)}
      onMouseEnter={() => d({ t: "hover", id: n.id })}
      onMouseLeave={() => d({ t: "hover", id: null })}
      onDoubleClick={() => {
        d({ t: "previewTarget", id: n.id });
        d({ t: "openSurface", id: "preview" });
      }}
      onContextMenu={(e) => {
        e.preventDefault();
        e.stopPropagation();
        const r = (e.currentTarget.closest("[data-canvas]") as HTMLElement).getBoundingClientRect();
        d({ t: "select", id: n.id });
        d({
          t: "menu",
          v: {
            x: (e.clientX - r.left) / scaleRef.current,
            y: (e.clientY - r.top) / scaleRef.current,
            target: "node",
            nodeId: n.id,
          },
        });
      }}
      className={cn(
        "group absolute cursor-default rounded-[8px] border transition-[border-color,background-color,box-shadow] duration-150",
        selected
          ? "border-[#2196d2]/50 bg-nodesel shadow-[0_0_0_1px_rgba(33,150,210,0.18),0_10px_26px_-16px_rgba(0,0,0,0.1)]"
          : hovered
            ? "border-[#c9d1d9] bg-[#f4f6f8]"
            : "border-line bg-node",
        waiting && "opacity-[0.62]",
      )}
      style={{ left: n.x, top: n.y, width: NODE_W, height: NODE_H }}
    >
      <div className="flex h-full flex-col px-3 py-[11px]">
        {/* 1 — sequence + type */}
        <div className="flex items-center justify-between">
          <span className={cn("tnum text-[10px] font-medium tracking-[0.06em]", n.aux ? "text-[#c58b32]" : "text-t4")}>{n.seq}</span>
          <span className="flex items-center gap-[5px] text-t3">
            <Icon name={ICON_BY_KIND[n.kind]} size={11} />
            <span className="text-[9.5px] font-medium tracking-[0.15em]">{n.kind}</span>
          </span>
        </div>

        {/* 2 — identity + status */}
        <div className="mt-[7px] flex items-start justify-between gap-2">
          <span
            className={cn(
              "truncate text-[14px] leading-[17px] font-medium tracking-[-0.012em]",
              waiting ? "text-t2" : "text-t1",
            )}
          >
            {n.name}
          </span>
          <span className="mt-[5px] flex shrink-0 items-center gap-1.5">
            {running && <span className="tnum text-[10px] text-[#2196d2]">{Math.round(n.progress ?? 0)}%</span>}
            <span
              className={cn("block h-[6px] w-[6px] rounded-full", running && "pulse-dot")}
              style={{ background: color, boxShadow: `0 0 0 2.5px ${color}1f` }}
            />
          </span>
        </div>

        {/* 3 — core operational result */}
        <div className="mt-auto">
          <div className="tnum text-[11.5px] leading-[14px] text-t2">{n.metric}</div>
          {/* 4 — critical behaviour */}
          <div
            className={cn("tnum mt-[5px] truncate text-[10.5px] leading-[13px]", failed ? "" : "text-t3")}
            style={failed ? { color: "#c95e62" } : undefined}
            title={n.behavior}
          >
            {n.status === "warning" && !n.aux ? <span className="text-[#c58b32]">{n.behavior}</span> : n.behavior}
          </div>
        </div>
      </div>

      {/* running progress line */}
      {running && (
        <div className="absolute inset-x-0 bottom-0 h-[2px] overflow-hidden rounded-b-[7px] bg-[#e5e9ed]">
          <div className="h-full transition-[width] duration-200" style={{ width: `${n.progress ?? 0}%`, background: "#2196d2" }} />
        </div>
      )}

      <Port side="left" visible={ports} compatible={connecting} />
      <Port side="right" visible={ports} compatible={connecting} />
      {(n.kind === "PIPE" || n.kind === "VALIDATE") && (
        <Port side={n.kind === "PIPE" ? "bottom" : "top"} visible={ports} compatible={connecting} />
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* canvas                                                              */
/* ------------------------------------------------------------------ */

export function GraphCanvas() {
  const { s, d } = useWorkspace();
  const ref = useRef<HTMLDivElement>(null);
  const pan = useRef<{ sx: number; sy: number } | null>(null);
  const [dropPt, setDropPt] = useState<Pt | null>(null);
  const [nodeMenu, setNodeMenu] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const cx = (e.clientX - r.left) / scaleRef.current;
      const cy = (e.clientY - r.top) / scaleRef.current;
      if (e.ctrlKey || e.metaKey) {
        d({ t: "zoomBy", d: -e.deltaY * 0.0035 * s.zoom * 4, cx, cy });
      } else {
        d({ t: "pan", dx: -e.deltaX / scaleRef.current, dy: -e.deltaY / scaleRef.current });
      }
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [d, s.zoom]);

  const sel = s.nodes.find((n) => n.id === s.selected) ?? null;
  const toScreen = (p: Pt) => ({ x: s.pan.x + p.x * s.zoom, y: s.pan.y + p.y * s.zoom });

  const nodeMenuItems: MenuItem[] = [
    { label: "Run from here", icon: "play", keys: "⌘↵", onClick: () => d({ t: "run" }) },
    { label: "Open data preview", icon: "eye", onClick: () => sel && (d({ t: "previewTarget", id: sel.id }), d({ t: "openSurface", id: "preview" })) },
    {
      label: "Compare with output",
      icon: "columns",
      onClick: () => {
        d({ t: "openSurface", id: "preview", rect: { x: 436, y: 548, w: 384, h: 356 } });
        d({ t: "openSurface", id: "preview2" });
      },
    },
    { sep: true },
    { label: "Duplicate object", icon: "copy", keys: "⌘D" },
    { label: "Disable object", icon: "power" },
    { label: "Disconnect inputs", icon: "link" },
    { label: "Reset runtime state", icon: "refresh", onClick: () => d({ t: "resetRun" }) },
    { sep: true },
    { label: "Delete object", icon: "trash", keys: "⌫", danger: true },
  ];

  return (
    <div
      ref={ref}
      data-canvas
      onPointerDown={(e) => {
        if ((e.target as HTMLElement).closest("[data-node]")) return;
        if (e.button === 2) return;
        e.currentTarget.setPointerCapture(e.pointerId);
        pan.current = { sx: e.clientX, sy: e.clientY };
        d({ t: "menu", v: null });
        if (s.tool === "select") d({ t: "select", id: null });
      }}
      onPointerMove={(e) => {
        const g = pan.current;
        if (!g) return;
        d({ t: "pan", dx: (e.clientX - g.sx) / scaleRef.current, dy: (e.clientY - g.sy) / scaleRef.current });
        g.sx = e.clientX;
        g.sy = e.clientY;
      }}
      onPointerUp={() => (pan.current = null)}
      onContextMenu={(e) => {
        if ((e.target as HTMLElement).closest("[data-node]")) return;
        e.preventDefault();
        const r = e.currentTarget.getBoundingClientRect();
        d({ t: "menu", v: { x: (e.clientX - r.left) / scaleRef.current, y: (e.clientY - r.top) / scaleRef.current, target: "canvas" } });
      }}
      onDragOver={(e) => {
        e.preventDefault();
        const r = e.currentTarget.getBoundingClientRect();
        setDropPt({ x: (e.clientX - r.left) / scaleRef.current, y: (e.clientY - r.top) / scaleRef.current });
      }}
      onDragLeave={() => setDropPt(null)}
      onDrop={(e) => {
        e.preventDefault();
        const raw = e.dataTransfer.getData("application/x-dcos");
        setDropPt(null);
        if (!raw) return;
        const payload = JSON.parse(raw) as { name: string; kind: GNode["kind"]; metric: string; behavior: string };
        const r = e.currentTarget.getBoundingClientRect();
        const x = ((e.clientX - r.left) / scaleRef.current - s.pan.x) / s.zoom - NODE_W / 2;
        const y = ((e.clientY - r.top) / scaleRef.current - s.pan.y) / s.zoom - NODE_H / 2;
        d({ t: "addNode", kind: payload.kind, name: payload.name, x, y, metric: payload.metric, behavior: payload.behavior });
        d({ t: "openSurface", id: "preview" });
      }}
      className={cn(
        "absolute inset-0 overflow-hidden rounded-[12px] bg-ws",
        s.tool === "pan" ? "cursor-grab active:cursor-grabbing" : "cursor-default",
      )}
    >
      {/* layer 0 — grid */}
      <div
        className="dotgrid pointer-events-none absolute inset-0"
        style={{
          backgroundSize: `${24 * s.zoom}px ${24 * s.zoom}px`,
          backgroundPosition: `${s.pan.x}px ${s.pan.y}px`,
          opacity: Math.min(1, s.zoom),
        }}
      />
      <div
        className="dotgrid-coarse pointer-events-none absolute inset-0"
        style={{ backgroundSize: `${120 * s.zoom}px ${120 * s.zoom}px`, backgroundPosition: `${s.pan.x}px ${s.pan.y}px` }}
      />
      <div
        className="pointer-events-none absolute inset-0"
        style={{ background: "radial-gradient(1100px 620px at 52% 26%, rgba(33,150,210,0.04), transparent 70%)" }}
      />

      {/* layer 1 — domain objects */}
      <div
        className="absolute origin-top-left"
        style={{ transform: `translate3d(${s.pan.x}px, ${s.pan.y}px, 0) scale(${s.zoom})`, width: 1, height: 1 }}
      >
        {/* group frame */}
        <div
          className="absolute rounded-[10px] border border-dashed border-[#dce2e8]"
          style={{ left: GROUP_FRAME.x, top: GROUP_FRAME.y, width: GROUP_FRAME.w, height: GROUP_FRAME.h }}
        >
          <div className="absolute -top-[19px] left-0 flex items-center gap-1.5 text-[9.5px] tracking-[0.14em] text-t4 uppercase">
            <Icon name="group" size={10} />
            {GROUP_FRAME.label}
            <span className="text-[#c9d1d9]">·</span>
            <span className="tnum tracking-normal normal-case">{GROUP_FRAME.meta}</span>
          </div>
        </div>

        {/* relationships */}
        <svg
          className="pointer-events-none absolute overflow-visible"
          style={{ left: -800, top: -800, width: 3400, height: 2400 }}
          viewBox="-800 -800 3400 2400"
        >
          {s.edges.map((e) => {
            const ends = edgeEnds(s, e);
            if (!ends) return null;
            const vertical = e.fromPort === "bottom";
            const touching = s.selected === e.from || s.selected === e.to;
            const flowing = ends.b.status === "running" || (ends.a.status === "running" && e.kind === "aux");
            const stroke = flowing ? "#2196d2" : touching ? "#9099a4" : e.kind === "aux" ? "#dce2e8" : "#c9d1d9";
            return (
              <g key={e.id}>
                <path
                  d={edgePath(ends.p1, ends.p2, vertical)}
                  fill="none"
                  stroke={stroke}
                  strokeWidth={1.1}
                  strokeDasharray={e.kind === "aux" ? "3 4" : undefined}
                  className={flowing ? "dash-flow" : undefined}
                />
                <path d={arrow(ends.p2, vertical)} fill={stroke} />
                <circle cx={ends.p1.x} cy={ends.p1.y} r={2} fill={stroke} />
              </g>
            );
          })}
        </svg>

        {/* annotation object */}
        <div
          className="absolute rounded-[8px] border border-[#e5e9ed] bg-white px-3 py-2.5"
          style={{ left: ANNOTATION.x, top: ANNOTATION.y, width: ANNOTATION.w }}
        >
          <div className="flex items-center gap-1.5 text-[9px] font-medium tracking-[0.14em] text-t4 uppercase">
            <Icon name="annotate" size={10} />
            {ANNOTATION.title}
          </div>
          <p className="mt-1.5 text-[10.5px] leading-[15px] text-t2">{ANNOTATION.body}</p>
          <div className="mt-2 text-[9.5px] text-t4">{ANNOTATION.author}</div>
        </div>

        {/* comment pin */}
        <div className="absolute flex items-center gap-1.5" style={{ left: COMMENT_PIN.x, top: COMMENT_PIN.y }}>
          <span className="grid h-[22px] w-[22px] place-items-center rounded-full rounded-bl-[3px] border border-[#dce2e8] bg-[#f4f6f8] text-[9.5px] font-medium text-t2">
            {COMMENT_PIN.initials}
          </span>
          <span className="tnum text-[9.5px] text-t4">{COMMENT_PIN.count}</span>
        </div>

        {s.nodes.map((n) => (
          <NodeCard key={n.id} n={n} />
        ))}
      </div>

      {/* layer 3 — screen-space overlays */}
      {s.edges.map((e) => {
        const ends = edgeEnds(s, e);
        if (!ends || !e.label) return null;
        if (s.selected !== e.from && s.selected !== e.to) return null;
        const vertical = e.fromPort === "bottom";
        const mid = toScreen({ x: (ends.p1.x + ends.p2.x) / 2, y: (ends.p1.y + ends.p2.y) / 2 });
        return (
          <div
            key={e.id}
            className="tnum pointer-events-none absolute z-20 -translate-x-1/2 -translate-y-1/2 rounded-[5px] border border-[#dce2e8] bg-white px-1.5 py-[2px] text-[9.5px] whitespace-nowrap text-t3"
            style={{ left: mid.x, top: mid.y + (vertical ? 0 : -9) }}
          >
            {e.label}
          </div>
        );
      })}

      {/* node quick actions */}
      {sel && (
        <div
          className="fade-up absolute z-30 flex h-8 items-center gap-0.5 rounded-[8px] border border-[#dce2e8] bg-white px-1 island-shadow"
          style={{
            left: toScreen({ x: sel.x + NODE_W / 2, y: sel.y }).x,
            top: toScreen({ x: sel.x, y: sel.y }).y - 40,
            transform: "translateX(-50%)",
          }}
        >
          <IconBtn icon="play" size={26} iconSize={13} tip="Run from here" keys="⌘↵" side="top" onClick={() => d({ t: "run" })} />
          <IconBtn
            icon="eye"
            size={26}
            iconSize={13}
            tip="Preview output"
            side="top"
            onClick={() => {
              d({ t: "previewTarget", id: sel.id });
              d({ t: "openSurface", id: "preview" });
            }}
          />
          <span className="mx-[1px] h-4 w-px bg-[#e5e9ed]" />
          <div className="relative">
            <IconBtn icon="more" size={26} iconSize={13} tip="More actions" side="top" onClick={() => setNodeMenu((v) => !v)} active={nodeMenu} />
            {nodeMenu && <Menu items={nodeMenuItems} onClose={() => setNodeMenu(false)} style={{ left: -70, top: 32 }} />}
          </div>
        </div>
      )}

      {/* drop guide */}
      {dropPt && (
        <div
          className="pointer-events-none absolute z-30 rounded-[8px] border border-dashed border-[#2196d2]/50 bg-[#2196d2]/[0.04]"
          style={{ left: dropPt.x - (NODE_W * s.zoom) / 2, top: dropPt.y - (NODE_H * s.zoom) / 2, width: NODE_W * s.zoom, height: NODE_H * s.zoom }}
        >
          <div className="absolute -top-5 left-0 text-[10px] text-[#2196d2]">Create dataset object</div>
        </div>
      )}

      {/* context menu */}
      {s.menu && (
        <Menu
          style={{ left: s.menu.x, top: s.menu.y, zIndex: 300 }}
          onClose={() => d({ t: "menu", v: null })}
          items={
            s.menu.target === "node"
              ? nodeMenuItems
              : [
                  { label: "Add dataset object", icon: "database", keys: "⌘1" },
                  { label: "Add transform", icon: "filter", keys: "⌘2" },
                  { label: "Add validation", icon: "validate", keys: "⌘3" },
                  { sep: true },
                  { label: "Paste object", icon: "copy", keys: "⌘V", disabled: true },
                  { label: "Add annotation", icon: "annotate" },
                  { label: "Add comment", icon: "comment" },
                  { sep: true },
                  { label: "Auto layout graph", icon: "layout", keys: "⇧L" },
                  { label: "Fit to view", icon: "fit", keys: "⇧1", onClick: () => d({ t: "fit" }) },
                ]
          }
        />
      )}
    </div>
  );
}
