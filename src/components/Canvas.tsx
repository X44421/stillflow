import { useCallback, useRef, useState } from "react";
import {
  MousePointer2,
  Hand,
  Grid3X3,
  Minus,
  Plus,
  Maximize2,
  ChevronDown,
  FileText,
  Database,
  Sparkles,
  Filter,
  Replace,
  Group,
  Wand2,
  type LucideIcon,
} from "lucide-react";
import { cn } from "../utils/cn";
import { NODE_W, type PipelineNode } from "../data";

const kindIcon: Record<string, LucideIcon> = {
  source: FileText,
  transform: Wand2,
  filter: Filter,
  replace: Replace,
  ai: Sparkles,
  groupby: Group,
  output: Database,
};

const nodeHeight = (n: PipelineNode) => (n.rows ? 104 : 78);

function roundedPath(pts: { x: number; y: number }[], r = 10) {
  if (pts.length < 2) return "";
  let d = `M ${pts[0].x} ${pts[0].y}`;
  for (let i = 1; i < pts.length - 1; i++) {
    const p = pts[i];
    const prev = pts[i - 1];
    const next = pts[i + 1];
    const inLen = Math.hypot(p.x - prev.x, p.y - prev.y);
    const outLen = Math.hypot(next.x - p.x, next.y - p.y);
    const rr = Math.min(r, inLen / 2, outLen / 2);
    const inX = p.x - ((p.x - prev.x) / inLen) * rr;
    const inY = p.y - ((p.y - prev.y) / inLen) * rr;
    const outX = p.x + ((next.x - p.x) / outLen) * rr;
    const outY = p.y + ((next.y - p.y) / outLen) * rr;
    d += ` L ${inX} ${inY} Q ${p.x} ${p.y} ${outX} ${outY}`;
  }
  d += ` L ${pts[pts.length - 1].x} ${pts[pts.length - 1].y}`;
  return d;
}

interface CanvasProps {
  nodes: PipelineNode[];
  selectedId: string;
  onSelect: (id: string) => void;
  onMove: (id: string, x: number, y: number) => void;
}

export default function Canvas({ nodes, selectedId, onSelect, onMove }: CanvasProps) {
  const [zoom, setZoom] = useState(100);
  const [tool, setTool] = useState<"select" | "pan" | "grid">("select");
  const containerRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<{ id: string; dx: number; dy: number } | null>(null);

  const byId = (id: string) => nodes.find((n) => n.id === id)!;
  const scale = zoom / 100;

  const handlePointerDown = useCallback(
    (e: React.PointerEvent, node: PipelineNode) => {
      onSelect(node.id);
      dragRef.current = {
        id: node.id,
        dx: e.clientX / scale - node.x,
        dy: e.clientY / scale - node.y,
      };
      (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    },
    [onSelect, scale]
  );

  const handlePointerMove = useCallback(
    (e: React.PointerEvent) => {
      const d = dragRef.current;
      if (!d) return;
      onMove(d.id, Math.max(0, e.clientX / scale - d.dx), Math.max(0, e.clientY / scale - d.dy));
    },
    [onMove, scale]
  );

  const handlePointerUp = useCallback(() => {
    dragRef.current = null;
  }, []);

  // Straight-ish edges between adjacent nodes on same row
  const straightEdge = (aId: string, bId: string) => {
    const a = byId(aId);
    const b = byId(bId);
    const x1 = a.x + NODE_W;
    const y1 = a.y + nodeHeight(a) / 2;
    const x2 = b.x;
    const y2 = b.y + nodeHeight(b) / 2;
    const mx = (x1 + x2) / 2;
    return { d: `M ${x1} ${y1} C ${mx} ${y1}, ${mx} ${y2}, ${x2} ${y2}`, x1, y1, x2, y2 };
  };

  // Wrap-around edge from row 1 end to row 2 start
  const wrapEdge = (aId: string, bId: string) => {
    const a = byId(aId);
    const b = byId(bId);
    const x1 = a.x + NODE_W;
    const y1 = a.y + nodeHeight(a) / 2;
    const x2 = b.x;
    const y2 = b.y + nodeHeight(b) / 2;
    const gapY = (a.y + nodeHeight(a) + b.y) / 2;
    const pts = [
      { x: x1, y: y1 },
      { x: x1 + 48, y: y1 },
      { x: x1 + 48, y: gapY },
      { x: x2 - 56, y: gapY },
      { x: x2 - 56, y: y2 },
      { x: x2, y: y2 },
    ];
    return { d: roundedPath(pts, 12), x1, y1, x2, y2 };
  };

  const edges = [
    straightEdge("csv", "clean"),
    straightEdge("clean", "filter"),
    straightEdge("filter", "country"),
    wrapEdge("country", "ai"),
    straightEdge("ai", "agg"),
    straightEdge("agg", "out"),
  ];

  return (
    <div ref={containerRef} className="relative flex-1 overflow-hidden bg-zinc-50">
      {/* dot grid background */}
      <div
        className="absolute inset-0"
        style={{
          backgroundImage: "radial-gradient(circle, #d4d4d8 1px, transparent 1px)",
          backgroundSize: `${22 * scale}px ${22 * scale}px`,
          opacity: 0.55,
        }}
      />

      {/* Toolbar */}
      <div className="absolute left-4 top-4 z-10 flex items-center rounded-xl border border-zinc-200 bg-white p-1 shadow-sm">
        {(
          [
            { id: "select", icon: MousePointer2 },
            { id: "pan", icon: Hand },
            { id: "grid", icon: Grid3X3 },
          ] as const
        ).map(({ id, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setTool(id)}
            className={cn(
              "rounded-lg p-2 transition-colors",
              tool === id ? "bg-zinc-100 text-zinc-900" : "text-zinc-500 hover:bg-zinc-50"
            )}
          >
            <Icon className="h-4 w-4" />
          </button>
        ))}
        <div className="mx-1 h-5 w-px bg-zinc-200" />
        <button
          onClick={() => setZoom((z) => Math.max(25, z - 25))}
          className="rounded-lg p-2 text-zinc-500 hover:bg-zinc-50"
        >
          <Minus className="h-4 w-4" />
        </button>
        <span className="w-12 text-center text-[13px] text-zinc-700">{zoom}%</span>
        <button
          onClick={() => setZoom((z) => Math.min(200, z + 25))}
          className="rounded-lg p-2 text-zinc-500 hover:bg-zinc-50"
        >
          <Plus className="h-4 w-4" />
        </button>
        <div className="mx-1 h-5 w-px bg-zinc-200" />
        <button
          onClick={() => setZoom(100)}
          className="rounded-lg p-2 text-zinc-500 hover:bg-zinc-50"
        >
          <Maximize2 className="h-4 w-4" />
        </button>
      </div>

      {/* Auto Layout */}
      <div className="absolute right-4 top-4 z-10">
        <button className="flex items-center gap-2 rounded-xl border border-zinc-200 bg-white px-3.5 py-2 text-sm text-zinc-700 shadow-sm hover:bg-zinc-50">
          Auto Layout
          <ChevronDown className="h-4 w-4 text-zinc-400" />
        </button>
      </div>

      {/* Graph */}
      <div
        className="absolute inset-0"
        style={{ transform: `scale(${scale})`, transformOrigin: "0 0" }}
        onClick={() => onSelect("")}
      >
        <svg className="pointer-events-none absolute inset-0 h-[1400px] w-[1800px] overflow-visible">
          {edges.map((e, i) => (
            <g key={i}>
              <path d={e.d} fill="none" stroke="#a1a1aa" strokeWidth={1.4} />
              <circle cx={e.x1} cy={e.y1} r={3.2} fill="#fff" stroke="#a1a1aa" strokeWidth={1.4} />
              <circle cx={e.x2} cy={e.y2} r={3.2} fill="#fff" stroke="#a1a1aa" strokeWidth={1.4} />
              <path
                d={`M ${e.x2 - 7} ${e.y2 - 4} L ${e.x2 - 1.5} ${e.y2} L ${e.x2 - 7} ${e.y2 + 4}`}
                fill="none"
                stroke="#a1a1aa"
                strokeWidth={1.4}
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </g>
          ))}
        </svg>

        {nodes.map((node) => {
          const Icon = kindIcon[node.kind];
          const selected = node.id === selectedId;
          return (
            <div
              key={node.id}
              onPointerDown={(e) => {
                e.stopPropagation();
                handlePointerDown(e, node);
              }}
              onPointerMove={handlePointerMove}
              onPointerUp={handlePointerUp}
              onClick={(e) => e.stopPropagation()}
              style={{ left: node.x, top: node.y, width: NODE_W, height: nodeHeight(node) }}
              className={cn(
                "absolute cursor-grab select-none rounded-xl border bg-white px-3.5 py-3 shadow-sm transition-shadow active:cursor-grabbing",
                selected
                  ? "border-zinc-900 ring-1 ring-zinc-900"
                  : "border-zinc-200 hover:border-zinc-300 hover:shadow"
              )}
            >
              <div className="flex items-start gap-2">
                <Icon
                  className={cn(
                    "mt-0.5 h-4 w-4 shrink-0",
                    node.kind === "ai" ? "text-violet-500" : "text-zinc-500"
                  )}
                />
                <div className="min-w-0">
                  <p className="truncate text-[13px] font-semibold leading-tight text-zinc-900">
                    {node.title}
                  </p>
                  <p className="mt-1 text-xs text-zinc-500">{node.subtitle}</p>
                </div>
              </div>
              {node.rows && (
                <p className="mt-2.5 border-t border-zinc-100 pt-2 text-xs text-zinc-400">
                  {node.rows}
                </p>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
