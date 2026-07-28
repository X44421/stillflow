import React, {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useState,
} from 'react';
import {
  Background,
  BackgroundVariant,
  BaseEdge,
  ConnectionLineType,
  Handle,
  Position,
  ReactFlow,
  getSmoothStepPath,
  type Connection,
  type ConnectionLineComponentProps,
  type EdgeProps,
  type NodeProps,
  type ReactFlowInstance,
} from '@xyflow/react';
import {
  Plus,
  Play,
  LayoutGrid,
  Maximize2,
  Undo2,
  Redo2,
  Minus,
  FileText,
  Filter,
  Copy,
  Type,
  Upload,
  Database,
} from '../icons/hero';
import ObjectPalette from './ObjectPalette';
import { defaultConfigFor } from '../data';
import type { PipelineNode, NodeType, NodeStatus } from '../types';
import {
  DEFAULT_VIEWPORT,
  EMPTY_FLOW_EDGES,
  EMPTY_FLOW_NODES,
  useCanvasStore,
} from '../features/canvas/canvasStore';
import { isValidDagConnection } from '../features/canvas/connectionRules';
import { layoutPipelineGraph } from '../features/canvas/elkLayout';
import {
  getNodeHeight,
  NODE_WIDTH,
  OUTPUT_ASSET_ID,
  type PipelineFlowEdge,
  type PipelineFlowNode,
} from '../features/canvas/graphAdapter';

interface PipelineCanvasProps {
  graphKey?: string;
  nodes: PipelineNode[];
  selectedNode: string;
  running?: boolean;
  onRunAll?: () => void;
  onSelectNode: (nodeId: string) => void;
  onNodeDoubleClick?: (nodeId: string) => void;
  onAddNode: (node: PipelineNode) => void;
  onDeleteNode: (nodeId: string) => void;
  /** Lifecycle actions rendered at the top-right (replaces the Run button). */
  topRightActions?: React.ReactNode;
}

const NODE_ICON: Record<NodeType, React.ReactNode> = {
  source: <FileText size={15} />,
  filter: <Filter size={15} />,
  deduplicate: <Copy size={15} />,
  normalize: <Type size={15} />,
  export: <Upload size={15} />,
};

const STATUS_DOT: Record<NodeStatus, string> = {
  completed: 'bg-[#4ba66a]',
  running: 'bg-[#2196d2] animate-pulse-dot',
  failed: 'bg-[#c95e62]',
  pending: 'bg-[#c9d1d9]',
  disabled: 'bg-[#c9d1d9]',
};

const STATUS_LABEL: Record<NodeStatus, string> = {
  completed: 'Completed',
  running: 'Running',
  failed: 'Failed',
  pending: 'Draft',
  disabled: 'Disabled',
};

/** Post-run impact summary shown on the node card: rows out and the delta. */
function impactLine(node: PipelineNode): string | null {
  const metrics = node.metrics;
  if (!metrics) return null;
  const { rowsIn, rowsOut } = metrics;
  if (rowsIn <= 0) return `${rowsOut.toLocaleString()} rows`;
  const delta = ((rowsOut - rowsIn) / rowsIn) * 100;
  if (Math.abs(delta) < 0.01) return `${rowsOut.toLocaleString()} rows · no change`;
  const sign = delta > 0 ? '+' : '−';
  const magnitude = Math.abs(delta);
  const pct = magnitude < 10 ? magnitude.toFixed(2) : magnitude.toFixed(1);
  return `${rowsOut.toLocaleString()} rows · ${sign}${pct}%`;
}

function PipelineFlowNodeView({
  data,
  selected,
}: NodeProps<PipelineFlowNode>) {
  const node = data.pipelineNode;
  const disabled = node.status === 'disabled';
  const isAsset = node.id === OUTPUT_ASSET_ID;
  const impact = impactLine(node);

  return (
    <>
      <Handle
        id="input"
        type="target"
        position={Position.Left}
        className="pipeline-node-handle"
      />
      <div
        style={{
          width: NODE_WIDTH,
          minHeight: getNodeHeight(node),
        }}
        className={`relative flex items-center gap-2.5 rounded-[7px] border px-3 py-2.5 cursor-grab select-none active:cursor-grabbing transition-colors duration-150 ${
          selected
            ? 'border-[#2196d2] bg-[#e8f4fa]'
            : isAsset
              ? 'border-dashed border-[#c9d1d9] bg-white hover:border-[#9099a4]'
              : 'border-[#dce2e8] bg-white hover:border-[#c9d1d9]'
        } ${disabled ? 'opacity-50' : ''}`}
      >
        <div className="grid h-7 w-7 shrink-0 place-items-center rounded-md bg-[#f4f6f8] text-[#5e6874]">
          {isAsset ? <Database size={15} /> : NODE_ICON[node.type]}
        </div>
        <div className="min-w-0 flex-1 pr-2">
          <div className="truncate text-[12.5px] leading-[16px] font-semibold text-[#171a1f]">
            {node.name}
          </div>
          <div className="truncate text-[11px] leading-[15px] text-[#5e6874]">
            {node.description}
          </div>
          {impact && (
            <div className="truncate text-[10.5px] leading-[14px] font-medium text-[#39434e] tabular">
              {impact}
            </div>
          )}
        </div>
        <span
          title={STATUS_LABEL[node.status]}
          className={`absolute top-[11px] right-3 h-[7px] w-[7px] rounded-full ${STATUS_DOT[node.status]}`}
        />
      </div>
      {!isAsset && (
        <Handle
          id="output"
          type="source"
          position={Position.Right}
          className="pipeline-node-handle"
        />
      )}
    </>
  );
}

function PipelineFlowEdgeView({
  id,
  sourceX,
  sourceY,
  sourcePosition,
  targetX,
  targetY,
  targetPosition,
  selected,
  data,
}: EdgeProps<PipelineFlowEdge>) {
  const [edgePath] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
    borderRadius: 12,
  });
  const tone = data?.tone;
  const stroke = selected || tone === 'active' ? '#2196d2' : '#c7d0d9';

  return (
    <g
      opacity={tone === 'dim' && !selected ? 0.35 : 1}
      style={{ transition: 'opacity 150ms ease' }}
    >
      <BaseEdge
        id={id}
        path={edgePath}
        interactionWidth={16}
        style={{
          stroke,
          strokeWidth: 1.25,
          strokeLinecap: 'round',
          strokeLinejoin: 'round',
        }}
      />
      <circle
        cx={sourceX}
        cy={sourceY}
        r={2.5}
        fill="#fff"
        stroke={stroke}
        strokeWidth={1.25}
      />
      <path
        d={
          `M ${targetX - 3} ${targetY - 6}` +
          ` L ${targetX} ${targetY - 1}` +
          ` L ${targetX + 3} ${targetY - 6}`
        }
        fill="none"
        stroke={stroke}
        strokeWidth={1.25}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </g>
  );
}

function PipelineConnectionLine({
  fromX,
  fromY,
  fromPosition,
  toX,
  toY,
  toPosition,
  connectionStatus,
}: ConnectionLineComponentProps<PipelineFlowNode>) {
  const [path] = getSmoothStepPath({
    sourceX: fromX,
    sourceY: fromY,
    sourcePosition: fromPosition,
    targetX: toX,
    targetY: toY,
    targetPosition: toPosition,
    borderRadius: 12,
  });
  const stroke = connectionStatus === 'invalid' ? '#c95e62' : '#2196d2';

  return (
    <>
      <path
        d={path}
        fill="none"
        stroke={stroke}
        strokeWidth={1.25}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle
        cx={fromX}
        cy={fromY}
        r={2.5}
        fill="#fff"
        stroke={stroke}
        strokeWidth={1.25}
      />
    </>
  );
}

const nodeTypes = {
  pipelineNode: PipelineFlowNodeView,
};

const edgeTypes = {
  pipelineEdge: PipelineFlowEdgeView,
};

const TOOLBAR_CLASS =
  'flex bg-white border border-[#dce2e8] rounded-[7px] shadow-[0_2px_6px_rgba(24,36,48,.06)] p-1 gap-px';
const TOOL_BUTTON_CLASS =
  'grid h-8 w-8 place-items-center rounded-md text-[#5e6874] transition-colors hover:bg-[#edf2f6] hover:text-[#171a1f] disabled:opacity-40 disabled:pointer-events-none';

const PipelineCanvas: React.FC<PipelineCanvasProps> = ({
  graphKey = 'default',
  nodes,
  selectedNode,
  running = false,
  onRunAll,
  onSelectNode,
  onNodeDoubleClick,
  onAddNode,
  onDeleteNode,
  topRightActions,
}) => {
  const [showPalette, setShowPalette] = useState(false);
  const [layoutRunning, setLayoutRunning] = useState(false);
  const [zoomPercent, setZoomPercent] = useState(100);
  const [flowInstance, setFlowInstance] =
    useState<ReactFlowInstance<PipelineFlowNode, PipelineFlowEdge> | null>(
      null
    );

  const flowNodes = useCanvasStore(
    useCallback(
      (state) => state.graphs[graphKey]?.nodes ?? EMPTY_FLOW_NODES,
      [graphKey]
    )
  );
  const flowEdges = useCanvasStore(
    useCallback(
      (state) => state.graphs[graphKey]?.edges ?? EMPTY_FLOW_EDGES,
      [graphKey]
    )
  );
  const viewport = useCanvasStore(
    useCallback(
      (state) => state.graphs[graphKey]?.viewport ?? DEFAULT_VIEWPORT,
      [graphKey]
    )
  );
  const syncGraph = useCanvasStore((state) => state.syncGraph);
  const applyGraphNodeChanges = useCanvasStore(
    (state) => state.applyGraphNodeChanges
  );
  const applyGraphEdgeChanges = useCanvasStore(
    (state) => state.applyGraphEdgeChanges
  );
  const connectGraph = useCanvasStore((state) => state.connectGraph);
  const reconnectGraph = useCanvasStore((state) => state.reconnectGraph);
  const setGraphViewport = useCanvasStore(
    (state) => state.setGraphViewport
  );
  const setGraphLayout = useCanvasStore((state) => state.setGraphLayout);
  const undo = useCanvasStore((state) => state.undo);
  const redo = useCanvasStore((state) => state.redo);

  /**
   * Selected node drives path emphasis: every edge connected to the
   * selection (upstream or downstream) is promoted to the accent color,
   * unrelated edges fall back to 35% opacity.
   */
  const displayEdges = useMemo<PipelineFlowEdge[]>(() => {
    if (!selectedNode) return flowEdges;
    const related = new Set<string>([selectedNode]);
    const adjacency = new Map<string, string[]>();
    const link = (from: string, to: string) => {
      const list = adjacency.get(from);
      if (list) list.push(to);
      else adjacency.set(from, [to]);
    };
    for (const edge of flowEdges) {
      link(edge.source, edge.target);
      link(edge.target, edge.source);
    }
    const queue = [selectedNode];
    while (queue.length > 0) {
      const current = queue.shift()!;
      for (const next of adjacency.get(current) ?? []) {
        if (!related.has(next)) {
          related.add(next);
          queue.push(next);
        }
      }
    }
    return flowEdges.map((edge) => ({
      ...edge,
      data: {
        tone:
          related.has(edge.source) && related.has(edge.target)
            ? ('active' as const)
            : ('dim' as const),
      },
    }));
  }, [flowEdges, selectedNode]);

  useLayoutEffect(() => {
    syncGraph(graphKey, nodes, selectedNode);
  }, [graphKey, nodes, selectedNode, syncGraph]);

  useEffect(() => {
    setZoomPercent(Math.round(viewport.zoom * 100));
  }, [graphKey, viewport.zoom]);

  const handlePaletteAdd = (obj: {
    name: string;
    description: string;
    icon: string;
  }) => {
    let type: NodeType = 'filter';
    if (
      obj.icon === 'file-text' ||
      obj.icon === 'cloud' ||
      obj.icon === 'database'
    ) {
      type = 'source';
    } else if (obj.icon === 'filter') {
      type = 'filter';
    } else if (obj.icon === 'copy') {
      type = 'deduplicate';
    } else if (obj.icon === 'type') {
      type = 'normalize';
    } else if (obj.icon === 'upload') {
      type = 'export';
    }

    const id = `n${Date.now()}`;
    onAddNode({
      id,
      type,
      name: obj.name,
      description: obj.description,
      rows: '',
      status: 'pending',
      config: defaultConfigFor(type),
    });
    setShowPalette(false);
  };

  const handleConnection = useCallback(
    (connection: Connection) => {
      connectGraph(graphKey, connection);
    },
    [connectGraph, graphKey]
  );

  const handleReconnect = useCallback(
    (edge: PipelineFlowEdge, connection: Connection) => {
      reconnectGraph(graphKey, edge, connection);
    },
    [graphKey, reconnectGraph]
  );

  const handleConnectionValidation = useCallback(
    (connection: Connection | PipelineFlowEdge) =>
      'source' in connection &&
      isValidDagConnection(
        {
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle ?? null,
          targetHandle: connection.targetHandle ?? null,
        },
        flowEdges
      ),
    [flowEdges]
  );

  const handleAutoLayout = useCallback(async () => {
    if (layoutRunning || flowNodes.length === 0) return;
    setLayoutRunning(true);
    try {
      const positions = await layoutPipelineGraph(flowNodes, flowEdges);
      setGraphLayout(graphKey, positions);
      requestAnimationFrame(() => {
        void flowInstance?.fitView({ padding: 0.2, duration: 240 });
      });
    } finally {
      setLayoutRunning(false);
    }
  }, [
    flowEdges,
    flowInstance,
    flowNodes,
    graphKey,
    layoutRunning,
    setGraphLayout,
  ]);

  const handleZoom = useCallback(
    (nextPercent: number) => {
      const zoom = Math.min(200, Math.max(40, nextPercent)) / 100;
      if (flowInstance) {
        const currentViewport = flowInstance.getViewport();
        void flowInstance.setViewport(
          { ...currentViewport, zoom },
          { duration: 120 }
        );
      } else {
        setGraphViewport(graphKey, { ...viewport, zoom });
      }
    },
    [flowInstance, graphKey, setGraphViewport, viewport]
  );

  return (
    <div className="min-h-0 flex-1 flex flex-col relative overflow-hidden">
      {/* Left toolbar — edit tools only */}
      <div className="absolute top-3 left-3 z-20">
        <div className={`${TOOLBAR_CLASS} flex-col`}>
          <button
            className={TOOL_BUTTON_CLASS}
            title="Add transform node"
            onClick={() => setShowPalette((open) => !open)}
          >
            <Plus size={16} strokeWidth={1.5} />
          </button>
          <div className="mx-1 my-0.5 h-px bg-[#edf2f6]" />
          <button
            className={TOOL_BUTTON_CLASS}
            title="Auto layout"
            onClick={() => void handleAutoLayout()}
            disabled={layoutRunning}
          >
            <LayoutGrid
              size={16}
              strokeWidth={1.5}
              className={layoutRunning ? 'animate-pulse' : undefined}
            />
          </button>
          <button
            className={TOOL_BUTTON_CLASS}
            title="Undo"
            onClick={() => undo(graphKey)}
          >
            <Undo2 size={16} strokeWidth={1.5} />
          </button>
          <button
            className={TOOL_BUTTON_CLASS}
            title="Redo"
            onClick={() => redo(graphKey)}
          >
            <Redo2 size={16} strokeWidth={1.5} />
          </button>
        </div>

        {showPalette && (
          <>
            <div
              className="fixed inset-0 z-10"
              onClick={() => setShowPalette(false)}
            />
            <div className="absolute top-0 left-full ml-2 z-20">
              <ObjectPalette onAdd={handlePaletteAdd} />
            </div>
          </>
        )}
      </div>

      {/* Top-right — pipeline lifecycle actions */}
      <div className="absolute top-3 right-3 z-20 flex items-center gap-1.5">
        {topRightActions ??
          (onRunAll && (
            <button
              onClick={onRunAll}
              disabled={running}
              title="Run the full pipeline end to end"
              className="flex h-8 items-center gap-1.5 rounded-[7px] border border-[#dce2e8] bg-white px-3 text-[12px] font-medium text-[#39434e] shadow-[0_2px_6px_rgba(24,36,48,.06)] transition-colors hover:bg-[#edf2f6] disabled:cursor-wait disabled:opacity-50"
            >
              <Play size={13} fill="currentColor" />
              {running ? 'Running…' : 'Run pipeline'}
            </button>
          ))}
      </div>

      <ReactFlow<PipelineFlowNode, PipelineFlowEdge>
        key={graphKey}
        className="bg-transparent"
        style={{ backgroundColor: 'transparent' }}
        nodes={flowNodes}
        edges={displayEdges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        defaultViewport={viewport}
        minZoom={0.4}
        maxZoom={2}
        defaultEdgeOptions={{
          type: 'pipelineEdge',
          reconnectable: true,
        }}
        connectionLineType={ConnectionLineType.SmoothStep}
        connectionLineComponent={PipelineConnectionLine}
        connectionLineStyle={{ stroke: '#2196d2', strokeWidth: 1.25 }}
        connectionRadius={28}
        reconnectRadius={24}
        connectionDragThreshold={3}
        deleteKeyCode={['Backspace', 'Delete']}
        multiSelectionKeyCode={null}
        zoomOnDoubleClick={false}
        onlyRenderVisibleElements
        proOptions={{ hideAttribution: true }}
        onInit={setFlowInstance}
        onNodesChange={(changes) =>
          applyGraphNodeChanges(graphKey, changes)
        }
        onEdgesChange={(changes) =>
          applyGraphEdgeChanges(graphKey, changes)
        }
        onConnect={handleConnection}
        onReconnect={handleReconnect}
        isValidConnection={handleConnectionValidation}
        onNodeClick={(_, node) => onSelectNode(node.id)}
        onNodeDoubleClick={(_, node) => onNodeDoubleClick?.(node.id)}
        onPaneClick={() => onSelectNode('')}
        onNodesDelete={(deletedNodes) => {
          for (const node of deletedNodes) onDeleteNode(node.id);
        }}
        onMoveEnd={(_, nextViewport) => {
          setZoomPercent(Math.round(nextViewport.zoom * 100));
          setGraphViewport(graphKey, nextViewport);
        }}
      >
        <Background
          id={`pipeline-grid-${graphKey}`}
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1}
          color="#000"
          className="opacity-[0.03]"
        />
      </ReactFlow>

      <div
        className={`${TOOLBAR_CLASS} absolute bottom-3 left-3 z-10 items-center`}
      >
        <button
          onClick={() => handleZoom(zoomPercent - 10)}
          className={TOOL_BUTTON_CLASS}
          title="Zoom out"
        >
          <Minus size={16} />
        </button>
        <span className="min-w-[44px] px-1 text-center text-[12px] font-medium text-[#5e6874] tabular">
          {zoomPercent}%
        </span>
        <button
          onClick={() => handleZoom(zoomPercent + 10)}
          className={TOOL_BUTTON_CLASS}
          title="Zoom in"
        >
          <Plus size={16} />
        </button>
        <div className="mx-1 h-4 w-px bg-[#edf2f6]" />
        <button
          className={TOOL_BUTTON_CLASS}
          title="Fit graph"
          onClick={() =>
            void flowInstance?.fitView({ padding: 0.2, duration: 240 })
          }
        >
          <Maximize2 size={16} strokeWidth={1.5} />
        </button>
      </div>
    </div>
  );
};

export default PipelineCanvas;