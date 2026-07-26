import React, {
  useCallback,
  useEffect,
  useLayoutEffect,
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
  onAddNode: (node: PipelineNode) => void;
  onDeleteNode: (nodeId: string) => void;
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

function getStatusIcon(status: NodeStatus): React.ReactNode {
  switch (status) {
    case 'completed':
      return <CheckCircle2 size={18} className="text-green-500" />;
    case 'running':
      return (
        <div className="w-4 h-4 bg-gray-900 rounded-full animate-pulse-dot" />
      );
    case 'failed':
    case 'disabled':
    case 'pending':
      return <Circle size={18} className="text-gray-300" />;
  }
}

function PipelineFlowNodeView({
  data,
  selected,
}: NodeProps<PipelineFlowNode>) {
  const node = data.pipelineNode;

  return (
    <>
      <Handle
        id="input"
        type="target"
        position={Position.Top}
        className="pipeline-node-handle"
      />
      <div
        style={{
          width: NODE_WIDTH,
          minHeight: getNodeHeight(node),
        }}
        className={`bg-white rounded-xl border px-4 py-3 flex items-center gap-3 min-w-[240px] max-w-[280px] cursor-grab select-none active:cursor-grabbing transition-all duration-150 ${
          selected
            ? 'border-gray-900 shadow-lg ring-1 ring-gray-900'
            : 'border-gray-200 shadow-sm hover:shadow-md hover:border-gray-300'
        }`}
      >
        {NODE_ICON[node.type]}
        <div className="flex-1 min-w-0">
          <div className="text-[13px] font-semibold text-gray-900">
            {node.name}
          </div>
          <div className="text-[11px] text-gray-500">{node.description}</div>
          {node.rows && (
            <div className="text-[11px] text-gray-400 mt-0.5">
              {node.rows} rows
            </div>
          )}
        </div>
        {getStatusIcon(node.status)}
      </div>
      <Handle
        id="output"
        type="source"
        position={Position.Bottom}
        className="pipeline-node-handle"
      />
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
  const stroke = selected ? '#111827' : '#d1d5db';

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        interactionWidth={16}
        style={{
          stroke,
          strokeWidth: 1.5,
          strokeLinecap: 'round',
          strokeLinejoin: 'round',
        }}
      />
      <circle
        cx={sourceX}
        cy={sourceY}
        r={3}
        fill="#fff"
        stroke={stroke}
        strokeWidth={1.5}
      />
      <path
        d={
          `M ${targetX - 4.5} ${targetY - 7}` +
          ` L ${targetX} ${targetY - 1.5}` +
          ` L ${targetX + 4.5} ${targetY - 7}`
        }
        fill="none"
        stroke={stroke}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </>
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
  const stroke = connectionStatus === 'invalid' ? '#ef4444' : '#9ca3af';

  return (
    <>
      <path
        d={path}
        fill="none"
        stroke={stroke}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle
        cx={fromX}
        cy={fromY}
        r={3}
        fill="#fff"
        stroke={stroke}
        strokeWidth={1.5}
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

const PipelineCanvas: React.FC<PipelineCanvasProps> = ({
  graphKey = 'default',
  nodes,
  selectedNode,
  running = false,
  onRunAll,
  onSelectNode,
  onAddNode,
  onDeleteNode,
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
      config: { ...defaultConfig },
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
    <div className="min-h-0 flex-1 bg-gray-50 flex flex-col relative overflow-hidden">
      <div className="absolute top-4 left-4 z-20">
        <div className="flex flex-col bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-700 transition-colors"
            title="Add node"
            onClick={() => setShowPalette((open) => !open)}
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
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="AI assist"
          >
            <Sparkles size={18} strokeWidth={1.5} />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Auto layout"
            onClick={() => void handleAutoLayout()}
            disabled={layoutRunning}
          >
            <LayoutGrid
              size={18}
              strokeWidth={1.5}
              className={layoutRunning ? 'animate-pulse' : undefined}
            />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Canvas settings"
          >
            <Settings size={18} strokeWidth={1.5} />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Fit graph"
            onClick={() =>
              void flowInstance?.fitView({ padding: 0.2, duration: 240 })
            }
          >
            <Maximize2 size={18} strokeWidth={1.5} />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Undo"
          >
            <Undo2 size={18} strokeWidth={1.5} />
          </button>
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
            title="Redo"
          >
            <Redo2 size={18} strokeWidth={1.5} />
          </button>
        </div>

        {showPalette && (
          <>
            <div
              className="fixed inset-0 z-10"
              onClick={() => setShowPalette(false)}
            />
            <div className="absolute top-0 left-full ml-3 z-20">
              <ObjectPalette onAdd={handlePaletteAdd} />
            </div>
          </>
        )}
      </div>

      <ReactFlow<PipelineFlowNode, PipelineFlowEdge>
        key={graphKey}
        className="bg-transparent"
        style={{ backgroundColor: 'transparent' }}
        nodes={flowNodes}
        edges={flowEdges}
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
        connectionLineStyle={{ stroke: '#d1d5db', strokeWidth: 1.5 }}
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

      <div className="absolute bottom-4 left-4 z-10 flex items-center bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
        <button
          onClick={() => handleZoom(zoomPercent - 10)}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          title="Zoom out"
        >
          <Minus size={16} />
        </button>
        <span className="text-xs text-gray-600 font-medium px-2 min-w-[48px] text-center">
          {zoomPercent}%
        </span>
        <button
          onClick={() => handleZoom(zoomPercent + 10)}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          title="Zoom in"
        >
          <Plus size={16} />
        </button>
        <div className="w-px h-5 bg-gray-200 mx-0.5" />
        <button
          onClick={() => handleZoom(100)}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          title="Reset zoom"
        >
          <ZoomIn size={16} />
        </button>
      </div>
    </div>
  );
};

export default PipelineCanvas;
