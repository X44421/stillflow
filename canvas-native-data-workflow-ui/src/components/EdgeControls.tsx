import { useState } from "react";
import { cn } from "../utils/cn";
import { Icon } from "../lib/icons";
import { useWorkspace, type SurfaceId, type ToolId } from "../lib/store";
import { RUN_STEPS } from "../lib/data";
import { IconBtn, Kbd, Menu, StatusDot, Tip } from "./ui";

const ISLAND = "flex items-center rounded-[8px] border border-[#dce2e8] bg-white island-shadow";

/* ------------------------------ identity ------------------------------ */

export function Identity() {
  const [open, setOpen] = useState(false);
  return (
    <div className={cn(ISLAND, "absolute top-4 left-4 z-[200] h-[42px] gap-2.5 pr-1.5 pl-2.5")}>
      <span className="grid h-[22px] w-[22px] shrink-0 place-items-center rounded-[6px] border border-[#dce2e8] bg-[#f4f6f8]">
        <svg viewBox="0 0 16 16" className="h-[13px] w-[13px]">
          <path d="M8 2.2 13.2 5v6L8 13.8 2.8 11V5z" fill="none" stroke="#2196d2" strokeWidth="1.1" strokeLinejoin="round" />
          <path d="M8 6.1 10.6 7.5v3L8 11.9 5.4 10.5v-3z" fill="#2196d2" fillOpacity="0.35" />
        </svg>
      </span>
      <span className="flex flex-col leading-none">
        <span className="text-[9.5px] font-medium tracking-[0.11em] text-t4 uppercase">DataCleaner OS</span>
        <span className="mt-[3px] text-[12.5px] font-medium tracking-[-0.01em] text-t1">RAG Knowledge Base</span>
      </span>
      <span className="mx-0.5 h-[22px] w-px bg-[#e5e9ed]" />
      <span className="flex items-center gap-1.5 text-[11px] text-t2">
        <Icon name="branch" size={12} className="text-t4" />
        Build RAG Knowledge Base
      </span>
      <div className="relative">
        <IconBtn icon="chevDown" size={26} iconSize={12} onClick={() => setOpen((v) => !v)} active={open} />
        {open && (
          <Menu
            style={{ left: -220, top: 30 }}
            onClose={() => setOpen(false)}
            items={[
              { label: "Switch workspace…", icon: "folder", keys: "⌘⇧O" },
              { label: "New mission", icon: "add" },
              { label: "Mission settings", icon: "settings" },
              { sep: true },
              { label: "Members & access", icon: "lock" },
              { label: "Export graph as JSON", icon: "download" },
              { sep: true },
              { label: "Command palette", icon: "command", keys: "⌘K" },
            ]}
          />
        )}
      </div>
    </div>
  );
}

/* ------------------------------ toolbar ------------------------------ */

const TOOLS: { id: ToolId; icon: string; label: string; keys: string }[] = [
  { id: "select", icon: "cursor", label: "Select", keys: "V" },
  { id: "pan", icon: "hand", label: "Pan", keys: "H" },
  { id: "add", icon: "add", label: "Add object", keys: "A" },
  { id: "connect", icon: "connect", label: "Connect", keys: "C" },
  { id: "group", icon: "group", label: "Group", keys: "G" },
  { id: "annotate", icon: "annotate", label: "Annotate", keys: "T" },
  { id: "comment", icon: "comment", label: "Comment", keys: "M" },
  { id: "layout", icon: "layout", label: "Auto layout", keys: "⇧L" },
];

export function CanvasToolbar() {
  const { s, d } = useWorkspace();
  return (
    <div className={cn(ISLAND, "absolute top-4 left-1/2 z-[200] h-[42px] -translate-x-1/2 gap-1 px-1.5")}>
      {TOOLS.map((t, i) => (
        <span key={t.id} className="flex items-center gap-1">
          {(i === 4 || i === 7) && <span className="mx-1 h-[20px] w-px bg-[#e5e9ed]" />}
          <IconBtn
            icon={t.icon}
            size={32}
            iconSize={16}
            tip={t.label}
            keys={t.keys}
            active={s.tool === t.id}
            onClick={() => d({ t: "tool", v: t.id })}
          />
        </span>
      ))}
    </div>
  );
}

/* ------------------------------ top right ------------------------------ */

export function TopRight() {
  const { s, d } = useWorkspace();
  const running = s.runtime.status === "running";
  return (
    <div className={cn(ISLAND, "absolute top-4 right-4 z-[200] h-[42px] gap-2 pr-1.5 pl-2.5")}>
      <div className="flex -space-x-1.5">
        {[
          { i: "SR", c: "#e5e9ed" },
          { i: "MO", c: "#e5e9ed" },
        ].map((a) => (
          <span
            key={a.i}
            className="grid h-[22px] w-[22px] place-items-center rounded-full border border-[#dce2e8] text-[9px] font-medium text-t2"
            style={{ background: a.c }}
          >
            {a.i}
          </span>
        ))}
      </div>
      <span className="mx-0.5 h-[22px] w-px bg-[#e5e9ed]" />
      <button
        onClick={() => d({ t: "palette", v: true })}
        className="flex h-[26px] items-center gap-2 rounded-[6px] border border-[#dce2e8] px-2 text-[11px] text-t3 transition-colors hover:border-[#c9d1d9] hover:text-t2"
      >
        <Icon name="search" size={12} />
        Search
        <Kbd>⌘K</Kbd>
      </button>
      <button
        onClick={() => (running ? d({ t: "resetRun" }) : d({ t: "run" }))}
        className={cn(
          "flex h-[28px] items-center gap-1.5 rounded-[6px] border px-2.5 text-[11.5px] font-medium transition-colors",
          running
            ? "border-[#dce2e8] bg-[#f4f6f8] text-t2 hover:text-t1"
            : "border-[#2196d2]/40 bg-[#2196d2]/[0.08] text-[#1686be] hover:bg-[#2196d2]/[0.12]",
        )}
      >
        <Icon name={running ? "power" : "play"} size={12} />
        {running ? "Stop" : "Run"}
      </button>
    </div>
  );
}

/* ------------------------------ zoom island ------------------------------ */

export function ZoomIsland() {
  const { s, d } = useWorkspace();
  const [zoomMenu, setZoomMenu] = useState(false);
  const [hist, setHist] = useState(false);
  return (
    <div className={cn(ISLAND, "absolute bottom-4 left-4 z-[200] h-[36px] gap-0.5 px-1")}>
      <IconBtn icon="zoomOut" size={26} iconSize={14} tip="Zoom out" keys="⌘−" side="top" onClick={() => d({ t: "zoomBy", d: -0.1 })} />
      <div className="relative">
        <button
          onClick={() => setZoomMenu((v) => !v)}
          className={cn(
            "tnum h-[26px] w-[50px] rounded-[6px] text-[11px] text-t2 transition-colors hover:bg-[#edf2f6] hover:text-t1",
            zoomMenu && "bg-[#edf2f6] text-t1",
          )}
        >
          {Math.round(s.zoom * 100)}%
        </button>
        {zoomMenu && (
          <Menu
            style={{ left: 0, bottom: 32 }}
            onClose={() => setZoomMenu(false)}
            items={[
              { label: "Zoom to fit", icon: "fit", keys: "⇧1", onClick: () => d({ t: "fit" }) },
              { label: "Zoom to selection", icon: "cursor", keys: "⇧2", onClick: () => d({ t: "setZoom", v: 1.25 }) },
              { sep: true },
              { label: "50%", onClick: () => d({ t: "setZoom", v: 0.5 }) },
              { label: "100%", onClick: () => d({ t: "setZoom", v: 1 }) },
              { label: "200%", onClick: () => d({ t: "setZoom", v: 2 }) },
            ]}
          />
        )}
      </div>
      <IconBtn icon="zoomIn" size={26} iconSize={14} tip="Zoom in" keys="⌘+" side="top" onClick={() => d({ t: "zoomBy", d: 0.1 })} />
      <span className="mx-0.5 h-[18px] w-px bg-[#e5e9ed]" />
      <IconBtn icon="fit" size={26} iconSize={14} tip="Fit view" keys="⇧1" side="top" onClick={() => d({ t: "fit" })} />
      <span className="mx-0.5 h-[18px] w-px bg-[#e5e9ed]" />
      <IconBtn icon="undo" size={26} iconSize={14} tip="Undo" keys="⌘Z" side="top" />
      <div className="relative">
        <IconBtn icon="history" size={26} iconSize={14} tip="History" side="top" active={hist} onClick={() => setHist((v) => !v)} />
        {hist && (
          <Menu
            style={{ left: -24, bottom: 32 }}
            onClose={() => setHist(false)}
            items={[
              { label: "Session history", disabled: true },
              ...s.history.slice(0, 5).map((h) => ({ label: h, icon: "clock" })),
            ]}
          />
        )}
      </div>
    </div>
  );
}

/* ------------------------------ surface dock ------------------------------ */

const SURFACE_META: Record<SurfaceId, { label: string; icon: string }> = {
  files: { label: "Files", icon: "folder" },
  inspector: { label: "Inspector", icon: "settings" },
  preview: { label: "Preview", icon: "table" },
  preview2: { label: "Preview 2", icon: "columns" },
  runtime: { label: "Runtime", icon: "clock" },
};

export function SurfaceDock() {
  const { s, d } = useWorkspace();
  const open = (Object.values(s.surfaces) as (typeof s.surfaces)[SurfaceId][]).filter((w) => w.open);
  if (!open.length) return null;
  return (
    <div className={cn(ISLAND, "absolute bottom-4 left-1/2 z-[200] h-[36px] -translate-x-1/2 gap-0.5 px-1")}>
      <span className="px-1.5 text-[9px] font-medium tracking-[0.14em] text-t4 uppercase">Surfaces</span>
      <span className="mr-0.5 h-[18px] w-px bg-[#26292f]" />
      {open.map((w) => {
        const meta = SURFACE_META[w.id];
        const minimized = w.mode === "minimized";
        const active = s.active === w.id && !minimized;
        return (
          <Tip key={w.id} label={minimized ? `Restore ${meta.label}` : meta.label} side="top">
            <button
              onClick={() => (minimized ? d({ t: "mode", id: w.id, mode: "floating" }) : d({ t: "focusSurface", id: w.id }))}
              className={cn(
                "flex h-[26px] items-center gap-1.5 rounded-[6px] px-2 text-[11px] transition-colors",
                active ? "bg-[#edf2f6] text-t1" : minimized ? "text-t4 hover:text-t2" : "text-t3 hover:bg-[#edf2f6] hover:text-t2",
              )}
            >
              <Icon name={meta.icon} size={12} />
              {meta.label}
              {w.badge === "error" && <span className="h-[5px] w-[5px] rounded-full bg-[#c95e62]" />}
              {w.badge === "activity" && <span className="h-[5px] w-[5px] rounded-full bg-[#2196d2]" />}
            </button>
          </Tip>
        );
      })}
    </div>
  );
}

/* ------------------------------ runtime status ------------------------------ */

export function StatusCluster() {
  const { s, d } = useWorkspace();
  const [notif, setNotif] = useState(false);
  const r = s.runtime;
  const label =
    r.status === "running"
      ? `Running · ${r.step + 1} / ${RUN_STEPS.length} · ${Math.round(((r.step + r.stepProgress / 100) / RUN_STEPS.length) * 100)}%`
      : r.status === "failed"
        ? "Failed · Final Export"
        : r.status === "complete"
          ? `Completed · ${RUN_STEPS.length} / ${RUN_STEPS.length}`
          : `Idle · Run #0${r.run}`;
  const dot = r.status === "running" ? "running" : r.status === "failed" ? "failed" : r.status === "complete" ? "complete" : "ready";

  return (
    <div className={cn(ISLAND, "absolute right-4 bottom-4 z-[200] h-[36px] gap-0.5 px-1")}>
      <button
        onClick={() => {
          d({ t: "runtimeExpanded", v: true });
          d({ t: "openSurface", id: "runtime" });
        }}
        className="flex h-[26px] items-center gap-2 rounded-[6px] px-2 text-[11px] text-t2 transition-colors hover:bg-[#edf2f6] hover:text-t1"
      >
        <StatusDot status={dot} pulse={r.status === "running"} />
        <span className="tnum">{label}</span>
        <Icon name="expand" size={11} className="text-t4" />
      </button>
      <span className="mx-0.5 h-[18px] w-px bg-[#e5e9ed]" />
      <div className="relative">
        <IconBtn icon="bell" size={26} iconSize={14} tip="Notifications" side="top" active={notif} onClick={() => setNotif((v) => !v)} />
        {!notif && r.status === "failed" && (
          <span className="pointer-events-none absolute top-[3px] right-[3px] h-[5px] w-[5px] rounded-full bg-[#c95e62]" />
        )}
        {notif && (
          <Menu
            style={{ right: -28, bottom: 32, width: 268 }}
            onClose={() => setNotif(false)}
            items={[
              { label: "Recent activity", disabled: true },
              { label: "Export schema mismatch · Run #042", icon: "alert", danger: true },
              { label: "Validation raised 3 warnings", icon: "validate" },
              { label: "Vector index rebuilt · 4m 47s", icon: "check" },
              { sep: true },
              { label: "Mark all as read", icon: "check" },
            ]}
          />
        )}
      </div>
      <IconBtn icon="help" size={26} iconSize={14} tip="Help & shortcuts" keys="?" side="top" />
    </div>
  );
}
