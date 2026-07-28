import { createContext, useContext, useMemo, useReducer, type Dispatch, type ReactNode } from "react";
import {
  INITIAL_EDGES,
  INITIAL_NODES,
  NODE_H,
  NODE_W,
  RUN_STEPS,
  type GEdge,
  type GNode,
  type NodeKind,
} from "./data";

export const CANVAS_W = 1576;
export const CANVAS_H = 976;

export type ToolId = "select" | "pan" | "add" | "connect" | "group" | "annotate" | "comment" | "layout";
export type SurfaceId = "files" | "inspector" | "preview" | "preview2" | "runtime";
export type SurfaceMode = "floating" | "docked" | "collapsed" | "minimized" | "maximized";
export type DockSide = "left" | "right" | "bottom" | "top";

export interface Rect {
  x: number;
  y: number;
  w: number;
  h: number;
}

export interface SurfaceWin extends Rect {
  id: SurfaceId;
  open: boolean;
  mode: SurfaceMode;
  dock: DockSide | null;
  z: number;
  last: number;
  restore: Rect | null;
  badge: "error" | "activity" | null;
}

export interface ContextMenuState {
  x: number;
  y: number;
  target: "canvas" | "node";
  nodeId?: string;
}

interface RuntimeState {
  status: "idle" | "running" | "complete" | "failed";
  step: number;
  stepProgress: number;
  run: number;
  acknowledged: boolean;
}

export interface WState {
  zoom: number;
  pan: { x: number; y: number };
  tool: ToolId;
  nodes: GNode[];
  edges: GEdge[];
  selected: string | null;
  hovered: string | null;
  surfaces: Record<SurfaceId, SurfaceWin>;
  active: SurfaceId | null;
  zTop: number;
  clock: number;
  runtime: RuntimeState;
  runtimeExpanded: boolean;
  palette: boolean;
  menu: ContextMenuState | null;
  previewNode: string;
  preview2Node: string;
  previewTab: "data" | "profile" | "chart";
  selectedRow: number;
  inspectorLocked: boolean;
  toast: string | null;
  history: string[];
  filesQuery: string;
}

const DEFAULT_RECTS: Record<SurfaceId, Rect> = {
  files: { x: 16, y: 318, w: 292, h: 470 },
  inspector: { x: 1236, y: 286, w: 324, h: 498 },
  preview: { x: 436, y: 548, w: 784, h: 356 },
  preview2: { x: 836, y: 548, w: 384, h: 356 },
  runtime: { x: 16, y: 436, w: 336, h: 352 },
};

const MIN_SIZE: Record<SurfaceId, { w: number; h: number }> = {
  files: { w: 280, h: 240 },
  inspector: { w: 300, h: 280 },
  preview: { w: 560, h: 240 },
  preview2: { w: 380, h: 240 },
  runtime: { w: 300, h: 220 },
};

function mkSurface(id: SurfaceId, open: boolean, z: number, last: number): SurfaceWin {
  return { id, ...DEFAULT_RECTS[id], open, mode: "floating", dock: null, z, last, restore: null, badge: null };
}

export const initialState: WState = {
  zoom: 0.88,
  pan: { x: 321, y: 152 },
  tool: "select",
  nodes: INITIAL_NODES.map((n) => ({ ...n })),
  edges: INITIAL_EDGES,
  selected: "n3",
  hovered: null,
  surfaces: {
    files: mkSurface("files", true, 1, 1),
    inspector: mkSurface("inspector", true, 3, 3),
    preview: mkSurface("preview", true, 2, 2),
    preview2: mkSurface("preview2", false, 0, 0),
    runtime: mkSurface("runtime", false, 0, 0),
  },
  active: "inspector",
  zTop: 4,
  clock: 4,
  runtime: { status: "idle", step: 0, stepProgress: 0, run: 41, acknowledged: true },
  runtimeExpanded: false,
  palette: false,
  menu: null,
  previewNode: "n1",
  preview2Node: "n3",
  previewTab: "data",
  selectedRow: 2,
  inspectorLocked: false,
  toast: null,
  history: ["Set overlap 120", "Connect Vector Index → Final Export", "Create Chunk Pipeline"],
  filesQuery: "",
};

export type Action =
  | { t: "zoomBy"; d: number; cx?: number; cy?: number }
  | { t: "setZoom"; v: number }
  | { t: "pan"; dx: number; dy: number }
  | { t: "fit" }
  | { t: "tool"; v: ToolId }
  | { t: "select"; id: string | null }
  | { t: "hover"; id: string | null }
  | { t: "moveNode"; id: string; dx: number; dy: number }
  | { t: "addNode"; kind: NodeKind; name: string; x: number; y: number; metric?: string; behavior?: string }
  | { t: "openSurface"; id: SurfaceId; rect?: Partial<Rect> }
  | { t: "closeSurface"; id: SurfaceId }
  | { t: "focusSurface"; id: SurfaceId }
  | { t: "moveSurface"; id: SurfaceId; x: number; y: number }
  | { t: "resizeSurface"; id: SurfaceId; w: number; h: number }
  | { t: "mode"; id: SurfaceId; mode: SurfaceMode; dock?: DockSide }
  | { t: "palette"; v: boolean }
  | { t: "menu"; v: ContextMenuState | null }
  | { t: "run" }
  | { t: "tick" }
  | { t: "resetRun" }
  | { t: "ack" }
  | { t: "runtimeExpanded"; v: boolean }
  | { t: "previewTarget"; id: string; secondary?: boolean }
  | { t: "previewTab"; v: "data" | "profile" | "chart" }
  | { t: "selectRow"; i: number }
  | { t: "toggleLock" }
  | { t: "toast"; v: string | null }
  | { t: "filesQuery"; v: string };

/** Overlap test with a tolerance so that a few pixels of contact are ignored. */
function intersects(a: Rect, b: Rect, tol = 24) {
  return a.x + tol < b.x + b.w && a.x + a.w - tol > b.x && a.y + tol < b.y + b.h && a.y + a.h - tol > b.y;
}

/** Bring surface forward + record activity clock. */
function focus(s: WState, id: SurfaceId): WState {
  const clock = s.clock + 1;
  const zTop = s.zTop + 1;
  return {
    ...s,
    clock,
    zTop,
    active: id,
    surfaces: { ...s.surfaces, [id]: { ...s.surfaces[id], z: zTop, last: clock, badge: null } },
  };
}

/**
 * Surface management rule: at most three expanded surfaces, and a newly opened
 * surface may not sit on top of an existing expanded surface. Least recently
 * used offenders are collapsed to their header.
 */
function makeRoom(s: WState, incoming: SurfaceId, rect: Rect): WState {
  const expanded = (Object.values(s.surfaces) as SurfaceWin[])
    .filter((w) => w.open && w.id !== incoming && (w.mode === "floating" || w.mode === "docked" || w.mode === "maximized"))
    .sort((a, b) => a.last - b.last);

  const next = { ...s.surfaces };
  let count = expanded.length + 1;
  for (const w of expanded) {
    const collides = intersects(rect, { x: w.x, y: w.y, w: w.w, h: w.h }) || w.mode === "maximized";
    if (collides || count > 3) {
      next[w.id] = { ...w, mode: "collapsed", restore: w.restore ?? { x: w.x, y: w.y, w: w.w, h: w.h } };
      count--;
    }
  }
  return { ...s, surfaces: next };
}

function statusesForRun(nodes: GNode[], step: number, progress: number, failed: boolean): GNode[] {
  return nodes.map((n) => {
    const idx = RUN_STEPS.findIndex((r) => r.id === n.id);
    if (idx === -1) return n;
    if (failed && idx === step) return { ...n, status: "failed", progress: undefined, behavior: "Schema mismatch · column `lang`" };
    if (idx < step) return { ...n, status: "complete", progress: undefined };
    if (idx === step) return { ...n, status: "running", progress };
    return { ...n, status: "waiting", progress: undefined };
  });
}

export function reducer(s: WState, a: Action): WState {
  switch (a.t) {
    case "zoomBy": {
      const z = Math.min(2.4, Math.max(0.24, +(s.zoom + a.d).toFixed(3)));
      const cx = a.cx ?? CANVAS_W / 2;
      const cy = a.cy ?? CANVAS_H / 2;
      const k = z / s.zoom;
      return { ...s, zoom: z, pan: { x: cx - (cx - s.pan.x) * k, y: cy - (cy - s.pan.y) * k } };
    }
    case "setZoom": {
      const z = Math.min(2.4, Math.max(0.24, a.v));
      const k = z / s.zoom;
      const cx = CANVAS_W / 2;
      const cy = CANVAS_H / 2;
      return { ...s, zoom: z, pan: { x: cx - (cx - s.pan.x) * k, y: cy - (cy - s.pan.y) * k } };
    }
    case "pan":
      return { ...s, pan: { x: s.pan.x + a.dx, y: s.pan.y + a.dy } };
    case "fit": {
      const xs = s.nodes.map((n) => n.x);
      const ys = s.nodes.map((n) => n.y);
      const minX = Math.min(...xs) - 40;
      const maxX = Math.max(...xs) + NODE_W + 40;
      const minY = Math.min(...ys) - 80;
      const maxY = Math.max(...ys) + NODE_H + 80;
      const pad = 96;
      const z = Math.min(1.4, Math.min((CANVAS_W - pad * 2) / (maxX - minX), (CANVAS_H - pad * 2) / (maxY - minY)));
      return {
        ...s,
        zoom: +z.toFixed(3),
        pan: { x: CANVAS_W / 2 - ((minX + maxX) / 2) * z, y: CANVAS_H / 2 - ((minY + maxY) / 2) * z },
      };
    }
    case "tool":
      return { ...s, tool: a.v, menu: null };
    case "select": {
      if (s.inspectorLocked && a.id && a.id !== s.selected) {
        return { ...s, selected: a.id, menu: null };
      }
      return { ...s, selected: a.id, menu: null };
    }
    case "hover":
      return s.hovered === a.id ? s : { ...s, hovered: a.id };
    case "moveNode":
      return {
        ...s,
        nodes: s.nodes.map((n) => (n.id === a.id ? { ...n, x: n.x + a.dx, y: n.y + a.dy } : n)),
      };
    case "addNode": {
      const seq = String(s.nodes.filter((n) => !n.aux).length + 1).padStart(2, "0");
      const id = `n${Date.now().toString(36).slice(-4)}`;
      const node: GNode = {
        id,
        seq,
        kind: a.kind,
        name: a.name,
        x: a.x,
        y: a.y,
        metric: a.metric ?? "pending scan",
        behavior: a.behavior ?? "no parameters set",
        status: "ready",
        objectId: `obj_${Math.random().toString(16).slice(2, 8)}`,
        duration: "—",
      };
      return {
        ...s,
        nodes: [...s.nodes, node],
        selected: id,
        history: [`Create ${a.name}`, ...s.history].slice(0, 12),
        toast: `${a.name} created on canvas`,
      };
    }
    case "openSurface": {
      const cur = s.surfaces[a.id];
      const rect: Rect = { ...DEFAULT_RECTS[a.id], ...(cur.restore ?? {}), ...(a.rect ?? {}) };
      let next = makeRoom(s, a.id, rect);
      next = {
        ...next,
        surfaces: {
          ...next.surfaces,
          [a.id]: { ...next.surfaces[a.id], ...rect, open: true, mode: "floating", dock: null, restore: null },
        },
      };
      return focus(next, a.id);
    }
    case "closeSurface": {
      const rest = { ...s.surfaces, [a.id]: { ...s.surfaces[a.id], open: false, mode: "floating" as SurfaceMode, badge: null } };
      const nextActive = (Object.values(rest) as SurfaceWin[])
        .filter((w) => w.open && w.mode !== "minimized")
        .sort((x, y) => y.last - x.last)[0];
      return { ...s, surfaces: rest, active: nextActive?.id ?? null, runtimeExpanded: a.id === "runtime" ? false : s.runtimeExpanded };
    }
    case "focusSurface":
      return focus(s, a.id);
    case "moveSurface": {
      const w = s.surfaces[a.id];
      const x = Math.min(CANVAS_W - 60, Math.max(-w.w + 120, a.x));
      const y = Math.min(CANVAS_H - 48, Math.max(8, a.y));
      return { ...s, surfaces: { ...s.surfaces, [a.id]: { ...w, x, y, mode: w.mode === "docked" ? "floating" : w.mode, dock: null } } };
    }
    case "resizeSurface": {
      const w = s.surfaces[a.id];
      const min = MIN_SIZE[a.id];
      return {
        ...s,
        surfaces: {
          ...s.surfaces,
          [a.id]: { ...w, w: Math.max(min.w, Math.min(CANVAS_W - w.x - 8, a.w)), h: Math.max(min.h, Math.min(CANVAS_H - w.y - 8, a.h)) },
        },
      };
    }
    case "mode": {
      const w = s.surfaces[a.id];
      const cur: Rect = { x: w.x, y: w.y, w: w.w, h: w.h };
      let patch: Partial<SurfaceWin> = { mode: a.mode, dock: a.dock ?? null };
      if (a.mode === "maximized" || a.mode === "docked") {
        patch.restore = w.restore ?? cur;
      }
      if (a.mode === "maximized") patch = { ...patch, x: 16, y: 68, w: CANVAS_W - 32, h: CANVAS_H - 128 };
      if (a.mode === "docked") {
        const d = a.dock ?? "bottom";
        if (d === "bottom") patch = { ...patch, x: 16, y: CANVAS_H - 16 - 344, w: CANVAS_W - 32, h: 344 };
        if (d === "left") patch = { ...patch, x: 16, y: 68, w: Math.max(280, w.w), h: CANVAS_H - 128 };
        if (d === "right") patch = { ...patch, x: CANVAS_W - 16 - Math.max(300, w.w), y: 68, w: Math.max(300, w.w), h: CANVAS_H - 128 };
      }
      if (a.mode === "floating" && w.restore) {
        patch = { ...patch, ...w.restore, restore: null, mode: "floating" };
      }
      let next: WState = { ...s, surfaces: { ...s.surfaces, [a.id]: { ...w, ...patch } } };
      if (a.mode === "maximized" || a.mode === "docked") {
        const r = next.surfaces[a.id];
        next = makeRoom(next, a.id, { x: r.x, y: r.y, w: r.w, h: r.h });
      }
      if (a.mode === "minimized") return { ...next, active: null };
      return focus(next, a.id);
    }
    case "palette":
      return { ...s, palette: a.v, menu: null };
    case "menu":
      return { ...s, menu: a.v };
    case "run": {
      const next: WState = {
        ...s,
        runtime: { status: "running", step: 0, stepProgress: 0, run: s.runtime.run + 1, acknowledged: false },
        nodes: statusesForRun(s.nodes, 0, 0, false),
        runtimeExpanded: false,
        toast: null,
        history: [`Run #0${s.runtime.run + 1} started`, ...s.history].slice(0, 12),
      };
      return next;
    }
    case "tick": {
      if (s.runtime.status !== "running") return s;
      let { step, stepProgress } = s.runtime;
      stepProgress += step === 2 ? 6 : step === 3 ? 7 : 13;
      // deterministic failure while exporting
      if (step === RUN_STEPS.length - 1 && stepProgress >= 62) {
        const rect = DEFAULT_RECTS.runtime;
        let next: WState = {
          ...s,
          runtime: { ...s.runtime, status: "failed", stepProgress: 62 },
          nodes: statusesForRun(s.nodes, step, 62, true),
          selected: "n6",
          runtimeExpanded: true,
          toast: null,
        };
        next = makeRoom(next, "runtime", rect);
        next = {
          ...next,
          surfaces: {
            ...next.surfaces,
            runtime: { ...next.surfaces.runtime, ...rect, open: true, mode: "floating", badge: "error" },
            inspector: { ...next.surfaces.inspector, open: true },
          },
        };
        return focus(next, "runtime");
      }
      if (stepProgress >= 100) {
        step += 1;
        stepProgress = 0;
        if (step >= RUN_STEPS.length) {
          return {
            ...s,
            runtime: { ...s.runtime, status: "complete", step: RUN_STEPS.length - 1, stepProgress: 100, acknowledged: true },
            nodes: statusesForRun(s.nodes, RUN_STEPS.length, 0, false),
            runtimeExpanded: false,
          };
        }
      }
      return { ...s, runtime: { ...s.runtime, step, stepProgress }, nodes: statusesForRun(s.nodes, step, stepProgress, false) };
    }
    case "resetRun":
      return {
        ...s,
        runtime: { status: "idle", step: 0, stepProgress: 0, run: s.runtime.run, acknowledged: true },
        nodes: INITIAL_NODES.map((n) => ({ ...n })),
        runtimeExpanded: false,
        surfaces: { ...s.surfaces, runtime: { ...s.surfaces.runtime, badge: null } },
      };
    case "ack":
      return { ...s, runtime: { ...s.runtime, acknowledged: true }, surfaces: { ...s.surfaces, runtime: { ...s.surfaces.runtime, badge: null } } };
    case "runtimeExpanded":
      return { ...s, runtimeExpanded: a.v };
    case "previewTarget":
      return a.secondary ? { ...s, preview2Node: a.id } : { ...s, previewNode: a.id, selectedRow: 2 };
    case "previewTab":
      return { ...s, previewTab: a.v };
    case "selectRow":
      return { ...s, selectedRow: a.i };
    case "toggleLock":
      return { ...s, inspectorLocked: !s.inspectorLocked };
    case "toast":
      return { ...s, toast: a.v };
    case "filesQuery":
      return { ...s, filesQuery: a.v };
    default:
      return s;
  }
}

const Ctx = createContext<{ s: WState; d: Dispatch<Action> }>({ s: initialState, d: () => {} });

export function WorkspaceProvider({ children }: { children: ReactNode }) {
  const [s, d] = useReducer(reducer, initialState);
  const value = useMemo(() => ({ s, d }), [s]);
  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useWorkspace() {
  return useContext(Ctx);
}

export function nodeById(s: WState, id: string | null) {
  return s.nodes.find((n) => n.id === id) ?? null;
}
