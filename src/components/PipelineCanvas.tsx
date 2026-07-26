import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import {
  Plus,
  Play,
  Sparkles,
  LayoutGrid,
  Settings,
  Maximize2,
  Undo2,
  Redo2,
  Minus,
  ZoomIn,
  FileText,
  Filter,
  Copy,
  Type,
  Upload,
  CheckCircle2,
  Circle,
} from '../icons/hero';
import ObjectPalette from './ObjectPalette';
import { defaultConfig } from '../data';
import type { PipelineNode, NodeType, NodeStatus } from '../types';

interface PipelineCanvasProps {
  nodes: PipelineNode[];
  selectedNode: string;
  running?: boolean;
  onRunAll?: () => void;
  onSelectNode: (nodeId: string) => void;
  onAddNode: (node: PipelineNode) => void;
  onDeleteNode: (nodeId: string) => void;
}

interface NodePosition {
  x: number;
  y: number;
}

interface CanvasEdge {
  d: string;
  start: NodePosition;
  end: NodePosition;
}

const NODE_WIDTH = 260;
const NODE_HEIGHT = 78;
const NODE_WITH_ROWS_HEIGHT = 96;
const NODE_GAP = 40;
const CANVAS_PADDING = 160;
const DEFAULT_X = 420;
const DEFAULT_Y = 120;

function getNodeHeight(node: PipelineNode): number {
  return node.rows ? NODE_WITH_ROWS_HEIGHT : NODE_HEIGHT;
}

function buildDefaultPositions(
  nodes: PipelineNode[]
): Record<string, NodePosition> {
  const positions: Record<string, NodePosition> = {};
  let y = DEFAULT_Y;

  for (const node of nodes) {
    positions[node.id] = { x: DEFAULT_X, y };
    y += getNodeHeight(node) + NODE_GAP;
  }

  return positions;
}

function roundedPath(points: NodePosition[], radius = 10): string {
  if (points.length < 2) return '';
  let path = `M ${points[0].x} ${points[0].y}`;

  for (let index = 1; index < points.length - 1; index += 1) {
    const point = points[index];
    const previous = points[index - 1];
    const next = points[index + 1];
    const incomingLength = Math.hypot(
      point.x - previous.x,
      point.y - previous.y
    );
    const outgoingLength = Math.hypot(next.x - point.x, next.y - point.y);

    if (incomingLength === 0 || outgoingLength === 0) {
      path += ` L ${point.x} ${point.y}`;
      continue;
    }

    const adjustedRadius = Math.min(
      radius,
      incomingLength / 2,
      outgoingLength / 2
    );
    const incomingX =
      point.x - ((point.x - previous.x) / incomingLength) * adjustedRadius;
    const incomingY =
      point.y - ((point.y - previous.y) / incomingLength) * adjustedRadius;
    const outgoingX =
      point.x + ((next.x - point.x) / outgoingLength) * adjustedRadius;
    const outgoingY =
      point.y + ((next.y - point.y) / outgoingLength) * adjustedRadius;
    path +=
      ` L ${incomingX} ${incomingY}` +
      ` Q ${point.x} ${point.y} ${outgoingX} ${outgoingY}`;
  }

  const last = points[points.length - 1];
  return `${path} L ${last.x} ${last.y}`;
}

function buildEdge(
  source: PipelineNode,
  sourcePosition: NodePosition,
  targetPosition: NodePosition
): CanvasEdge {
  const start = {
    x: sourcePosition.x + NODE_WIDTH / 2,
    y: sourcePosition.y + getNodeHeight(source),
  };
  const end = {
    x: targetPosition.x + NODE_WIDTH / 2,
    y: targetPosition.y,
  };
  const aligned = Math.abs(start.x - end.x) < 8;

  if (aligned) {
    return {
      d: `M ${start.x} ${start.y} L ${end.x} ${end.y}`,
      start,
      end,
    };
  }

  const midpointY = start.y + (end.y - start.y) / 2;
  return {
    d: roundedPath(
      [
        start,
        { x: start.x, y: midpointY },
        { x: end.x, y: midpointY },
        end,
      ],
      12
    ),
    start,
    end,
  };
}

const NODE_ICON: Record<NodeType, React.ReactNode> = {
  source: (
    <div className="w-9 h-9 bg-green-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <FileText size={18} className="text-green-600" />
    </div>
  ),
  filter: (
    <div className="w-9 h-9 bg-purple-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Filter size={18} className="text-purple-600" />
    </div>
  ),
  deduplicate: (
    <div className="w-9 h-9 bg-teal-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Copy size={18} className="text-teal-600" />
    </div>
  ),
  normalize: (
    <div className="w-9 h-9 bg-orange-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Type size={18} className="text-orange-600" />
    </div>
  ),
  export: (
    <div className="w-9 h-9 bg-amber-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Upload size={18} className="text-amber-600" />
    </div>
  ),
};

const PipelineCanvas: React.FC<PipelineCanvasProps> = ({
  nodes,
  selectedNode,
  running = false,
  onRunAll,
  onSelectNode,
  onAddNode,
  onDeleteNode,
}) => {
  const [zoom, setZoom] = useState(100);
  const [showPalette, setShowPalette] = useState(false);
  const viewportRef = useRef<HTMLDivElement>(null);
  const dragRef = useRef<{ id: string; dx: number; dy: number } | null>(null);
  const scale = zoom / 100;

  const defaultPositions = useMemo(() => buildDefaultPositions(nodes), [nodes]);
  const nodeOrderKey = useMemo(
    () => nodes.map((node) => node.id).join('|'),
    [nodes]
  );
  const nodeOrderRef = useRef(nodeOrderKey);
  const [nodePositions, setNodePositions] = useState<Record<string, NodePosition>>(
    () => defaultPositions
  );

  useEffect(() => {
    setNodePositions((current) => {
      if (nodeOrderRef.current !== nodeOrderKey) {
        nodeOrderRef.current = nodeOrderKey;
        return defaultPositions;
      }

      const next: Record<string, NodePosition> = {};
      for (const node of nodes) {
        next[node.id] = current[node.id] ?? defaultPositions[node.id];
      }
      return next;
    });
  }, [defaultPositions, nodeOrderKey, nodes]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedNode) {
        const tag = (e.target as HTMLElement).tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || (e.target as HTMLElement).isContentEditable) return;
        e.preventDefault();
        onDeleteNode(selectedNode);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [selectedNode, onDeleteNode]);

  const getPointerPosition = useCallback(
    (event: React.PointerEvent): NodePosition => {
      const viewport = viewportRef.current;
      if (!viewport) return { x: 0, y: 0 };
      const bounds = viewport.getBoundingClientRect();
      return {
        x: (event.clientX - bounds.left + viewport.scrollLeft) / scale,
        y: (event.clientY - bounds.top + viewport.scrollTop) / scale,
      };
    },
    [scale]
  );

  const handleNodePointerDown = useCallback(
    (event: React.PointerEvent<HTMLDivElement>, node: PipelineNode) => {
      event.stopPropagation();
      onSelectNode(node.id);
      const pointer = getPointerPosition(event);
      const position = nodePositions[node.id] ?? defaultPositions[node.id];
      dragRef.current = {
        id: node.id,
        dx: pointer.x - position.x,
        dy: pointer.y - position.y,
      };
      event.currentTarget.setPointerCapture(event.pointerId);
    },
    [defaultPositions, getPointerPosition, nodePositions, onSelectNode]
  );

  const handleNodePointerMove = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      const dragging = dragRef.current;
      if (!dragging) return;
      const pointer = getPointerPosition(event);
      setNodePositions((current) => ({
        ...current,
        [dragging.id]: {
          x: Math.max(0, pointer.x - dragging.dx),
          y: Math.max(0, pointer.y - dragging.dy),
        },
      }));
    },
    [getPointerPosition]
  );

  const handleNodePointerUp = useCallback(
    (event: React.PointerEvent<HTMLDivElement>) => {
      dragRef.current = null;
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
    },
    []
  );

  const graphBounds = useMemo(() => {
    const positions = nodes.map(
      (node) => nodePositions[node.id] ?? defaultPositions[node.id]
    );
    const right =
      Math.max(...positions.map((position) => position.x + NODE_WIDTH), NODE_WIDTH) +
      CANVAS_PADDING;
    const bottom =
      Math.max(
        ...nodes.map((node) => {
          const position = nodePositions[node.id] ?? defaultPositions[node.id];
          return position.y + getNodeHeight(node);
        }),
        NODE_HEIGHT
      ) + CANVAS_PADDING;

    return {
      width: Math.max(900, right),
      height: Math.max(520, bottom),
    };
  }, [defaultPositions, nodePositions, nodes]);

  const edges = useMemo(
    () =>
      nodes.slice(0, -1).map((node, index) => {
        const next = nodes[index + 1];
        return buildEdge(
          node,
          nodePositions[node.id] ?? defaultPositions[node.id],
          nodePositions[next.id] ?? defaultPositions[next.id]
        );
      }),
    [defaultPositions, nodePositions, nodes]
  );

  const getStatusIcon = (status: NodeStatus) => {
    switch (status) {
      case 'completed':
        return <CheckCircle2 size={18} className="text-green-500" />;
      case 'running':
        return <div className="w-4 h-4 bg-gray-900 rounded-full animate-pulse-dot" />;
      case 'failed':
      case 'disabled':
      case 'pending':
        return <Circle size={18} className="text-gray-300" />;
    }
  };

  const handlePaletteAdd = (obj: {
    name: string;
    description: string;
    icon: string;
  }) => {
    let type: NodeType = 'filter';
    if (obj.icon === 'file-text' || obj.icon === 'cloud' || obj.icon === 'database') type = 'source';
    else if (obj.icon === 'filter') type = 'filter';
    else if (obj.icon === 'copy') type = 'deduplicate';
    else if (obj.icon === 'type') type = 'normalize';
    else if (obj.icon === 'upload') type = 'export';

    const id = `n${Date.now()}`;
    onAddNode({
      id,
      type,
      name: obj.name,
      description: obj.description,
      rows: '',
      status: 'pending',
      config: { ...defaultConfig },
    });
    setShowPalette(false);
  };

  return (
    <div className="min-h-0 flex-1 bg-gray-50 flex flex-col relative overflow-hidden">
      {/* Toolbar - top-left, vertical */}
      <div className="absolute top-4 left-4 z-20">
        <div className="flex flex-col bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-700 transition-colors"
            title="Add node"
            onClick={() => setShowPalette((p) => !p)}
          >
            <Plus size={18} strokeWidth={1.5} />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Run pipeline"
            onClick={onRunAll}
            disabled={running}
          >
            <Play size={18} strokeWidth={1.5} />
          </button>
          <div className="w-full h-px bg-gray-100 my-0.5" />
          {[Sparkles, LayoutGrid, Settings, Maximize2, Undo2, Redo2].map((Icon, i) => (
            <button
              key={i}
              className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            >
              <Icon size={18} strokeWidth={1.5} />
            </button>
          ))}
        </div>

        {/* Palette popover - opens to the right of toolbar */}
        {showPalette && (
          <>
            <div className="fixed inset-0 z-10" onClick={() => setShowPalette(false)} />
            <div className="absolute top-0 left-full ml-3 z-20">
              <ObjectPalette onAdd={handlePaletteAdd} />
            </div>
          </>
        )}
      </div>

      {/* Pipeline nodes - centered */}
      <div
        ref={viewportRef}
        className="flex-1 overflow-auto pt-24 pb-12"
        onClick={() => onSelectNode('')}
      >
        <div
          className="relative mx-auto flex-shrink-0"
          style={{
            width: graphBounds.width * scale,
            height: graphBounds.height * scale,
          }}
        >
          <div
            className="absolute left-0 top-0"
            style={{
              width: graphBounds.width,
              height: graphBounds.height,
              transform: `scale(${scale})`,
              transformOrigin: '0 0',
            }}
          >
            <svg
              className="pointer-events-none absolute inset-0 overflow-visible"
              width={graphBounds.width}
              height={graphBounds.height}
            >
              {edges.map((edge, index) => (
                <g key={`${index}-${edge.d}`}>
                  <path
                    d={edge.d}
                    fill="none"
                    stroke="#d1d5db"
                    strokeWidth={1.5}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <circle
                    cx={edge.start.x}
                    cy={edge.start.y}
                    r={3}
                    fill="#fff"
                    stroke="#d1d5db"
                    strokeWidth={1.5}
                  />
                  <path
                    d={
                      `M ${edge.end.x - 4.5} ${edge.end.y - 7}` +
                      ` L ${edge.end.x} ${edge.end.y - 1.5}` +
                      ` L ${edge.end.x + 4.5} ${edge.end.y - 7}`
                    }
                    fill="none"
                    stroke="#d1d5db"
                    strokeWidth={1.5}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </g>
              ))}
            </svg>

            {nodes.map((node) => {
              const position = nodePositions[node.id] ?? defaultPositions[node.id];
              return (
                <div
                  onPointerDown={(event) => handleNodePointerDown(event, node)}
                  onPointerMove={handleNodePointerMove}
                  onPointerUp={handleNodePointerUp}
                  onPointerCancel={handleNodePointerUp}
                  onClick={(event) => event.stopPropagation()}
                  style={{
                    left: position.x,
                    top: position.y,
                    width: NODE_WIDTH,
                    minHeight: getNodeHeight(node),
                  }}
                  className={`bg-white rounded-xl border px-4 py-3 flex items-center gap-3 min-w-[240px] max-w-[280px] cursor-pointer transition-all duration-150 ${
                    selectedNode === node.id
                      ? 'border-gray-900 shadow-lg ring-1 ring-gray-900'
                      : 'border-gray-200 shadow-sm hover:shadow-md hover:border-gray-300'
                  } absolute cursor-grab select-none active:cursor-grabbing`}
                >
                  {NODE_ICON[node.type]}
                  <div className="flex-1 min-w-0">
                    <div className="text-[13px] font-semibold text-gray-900">{node.name}</div>
                    <div className="text-[11px] text-gray-500">{node.description}</div>
                    {node.rows && (
                      <div className="text-[11px] text-gray-400 mt-0.5">
                        {node.rows} rows
                      </div>
                    )}
                  </div>
                  {getStatusIcon(node.status)}
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Zoom Controls - bottom-left */}
      <div className="absolute bottom-4 left-4 z-10 flex items-center bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
        <button
          onClick={() => setZoom((z) => Math.max(40, z - 10))}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
        >
          <Minus size={16} />
        </button>
        <span className="text-xs text-gray-600 font-medium px-2 min-w-[48px] text-center">{zoom}%</span>
        <button
          onClick={() => setZoom((z) => Math.min(200, z + 10))}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
        >
          <Plus size={16} />
        </button>
        <div className="w-px h-5 bg-gray-200 mx-0.5" />
        <button
          onClick={() => setZoom(100)}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          title="Reset zoom"
        >
          <ZoomIn size={16} />
        </button>
      </div>

      {/* Grid pattern background */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.03]"
        style={{
          backgroundImage: 'radial-gradient(circle, #000 1px, transparent 1px)',
          backgroundSize: `${24 * scale}px ${24 * scale}px`,
        }}
      />
    </div>
  );
};

export default PipelineCanvas;
