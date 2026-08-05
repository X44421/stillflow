import { useCallback, useEffect, useMemo } from 'react';
import { Breadcrumb, BreadcrumbItem, Label, Spinner } from '@patternfly/react-core';
import {
  AnchorEnd,
  ComponentFactory,
  DagreLayout,
  DefaultEdge,
  DefaultNode,
  EdgeAnimationSpeed,
  EdgeComponentProps,
  EdgeStyle,
  EdgeTerminalType,
  FIT_TO_SCREEN,
  GraphComponent,
  LayoutFactory,
  LEFT_TO_RIGHT,
  Model,
  ModelKind,
  NodeComponentProps,
  NodeShape,
  NodeStatus,
  RegisterComponentFactory,
  RegisterLayoutFactory,
  RESET_VIEW,
  TopologyControlBar,
  TopologyView,
  VisualizationProvider,
  VisualizationSurface,
  ZOOM_IN,
  ZOOM_OUT,
  createTopologyControlButtons,
  observer,
  useModel,
  useSvgAnchor,
  useVisualizationController,
  withPanZoom,
} from '@patternfly/react-topology';
import type { PipelineNode, RunStatus } from '../types';

const statusMap: Record<RunStatus, NodeStatus> = {
  ready: NodeStatus.success,
  running: NodeStatus.info,
  warning: NodeStatus.warning,
  error: NodeStatus.danger,
};

interface StillNodeData {
  title: string;
  subtitle: string;
  status: RunStatus;
  icon: string;
}

const StillNode = observer(({ element }: NodeComponentProps) => {
  const data = (element.getData() ?? {
    title: element.getLabel(),
    subtitle: '',
    status: 'ready',
    icon: '',
  }) as StillNodeData;
  const { width, height } = element.getDimensions();
  const targetRef = useSvgAnchor(AnchorEnd.target, 'edge');
  const sourceRef = useSvgAnchor(AnchorEnd.source, 'edge');

  return (
    <DefaultNode
      element={element}
      label={data.title}
      secondaryLabel={data.subtitle}
      nodeStatus={statusMap[data.status] ?? NodeStatus.default}
      showStatusBackground
      showStatusDecorator
      statusDecoratorTooltip={data.status}
      truncateLength={26}
    >
      <g className="still-node-ports">
        <circle ref={targetRef} className="still-node-port still-node-port--target" cx={0} cy={height / 2} r={5} />
        <circle ref={sourceRef} className="still-node-port still-node-port--source" cx={width} cy={height / 2} r={5} />
      </g>
    </DefaultNode>
  );
});

const StillEdge = observer(({ element }: EdgeComponentProps) => {
  const data = (element.getData() ?? {}) as { tag?: string };
  return (
    <DefaultEdge
      element={element}
      edgeStyle={EdgeStyle.solid}
      endTerminalType={EdgeTerminalType.directional}
      tag={data.tag}
      tagStatus={NodeStatus.info}
    />
  );
});

const componentFactory: ComponentFactory = (kind, _type) => {
  switch (kind) {
    case ModelKind.graph:
      return withPanZoom()(GraphComponent);
    case ModelKind.node:
      return StillNode;
    case ModelKind.edge:
      return StillEdge;
    default:
      return undefined;
  }
};

const layoutFactory: LayoutFactory = (type, graph) => {
  if (type === 'Dagre') {
    return new DagreLayout(graph, {
      rankdir: LEFT_TO_RIGHT,
      nodesep: 42,
      ranksep: 84,
      marginx: 24,
      marginy: 24,
    });
  }
  return undefined;
};

interface TopologyCanvasProps {
  nodes: PipelineNode[];
  edges: [string, string][];
  selectedNodeId: string | null;
  onSelectNode: (id: string) => void;
  onDeselectNode: () => void;
  nodeStatuses: Record<string, RunStatus>;
  isRunning: boolean;
  progress: number;
  breadcrumb: string;
}

export function TopologyCanvas({
  nodes,
  edges,
  selectedNodeId,
  onSelectNode,
  onDeselectNode,
  nodeStatuses,
  isRunning,
  progress,
  breadcrumb,
}: TopologyCanvasProps) {
  const model = useMemo<Model>(
    () => ({
      graph: { id: 'stillflow-graph', type: 'graph', layout: 'Dagre' },
      nodes: nodes.map((node) => {
        const status = nodeStatuses[node.id] ?? node.status;
        return {
          id: node.id,
          type: 'node',
          label: node.title,
          width: 220,
          height: 64,
          shape: NodeShape.rect,
          status: statusMap[status] ?? NodeStatus.success,
          data: { title: node.title, subtitle: node.subtitle, status, icon: node.icon },
        };
      }),
      edges: edges.map(([source, target]) => ({
        id: `edge-${source}-${target}`,
        type: 'edge',
        source,
        target,
        edgeStyle: EdgeStyle.solid,
        animationSpeed: isRunning ? EdgeAnimationSpeed.medium : EdgeAnimationSpeed.none,
        data: { tag: isRunning ? 'flowing' : 'linked' },
      })),
    }),
    [nodes, edges, nodeStatuses, isRunning]
  );

  return (
    <VisualizationProvider>
      <RegisterComponentFactory factory={componentFactory} />
      <RegisterLayoutFactory factory={layoutFactory} />
      <TopologyCanvasBody
        model={model}
        selectedNodeId={selectedNodeId}
        onSelectNode={onSelectNode}
        onDeselectNode={onDeselectNode}
        isRunning={isRunning}
        progress={progress}
        breadcrumb={breadcrumb}
      />
    </VisualizationProvider>
  );
}

interface TopologyCanvasBodyProps extends Omit<TopologyCanvasProps, 'nodes' | 'edges' | 'nodeStatuses'> {
  model: Model;
}

function TopologyCanvasBody({
  model,
  selectedNodeId,
  onSelectNode,
  onDeselectNode,
  isRunning,
  progress,
  breadcrumb,
}: TopologyCanvasBodyProps) {
  useModel(model);
  const controller = useVisualizationController();

  const controlButtons = useMemo(
    () =>
      createTopologyControlButtons({
        legend: false,
        zoomIn: true,
        zoomOut: true,
        fitToScreen: true,
        resetView: true,
      }),
    []
  );

  const handleControl = useCallback(
    (id: string) => {
      const graph = controller.getGraph();
      if (id === ZOOM_IN) {
        graph.scaleBy(1.25);
      } else if (id === ZOOM_OUT) {
        graph.scaleBy(0.8);
      } else if (id === FIT_TO_SCREEN) {
        graph.fit(24);
      } else if (id === RESET_VIEW) {
        graph.reset();
      }
    },
    [controller]
  );

  useEffect(() => {
    const listener = (ids: string[]) => {
      if (ids.length > 0) {
        onSelectNode(ids[0]);
      } else {
        onDeselectNode();
      }
    };
    controller.addEventListener('selection', listener);
    return () => controller.removeEventListener('selection', listener);
  }, [controller, onSelectNode, onDeselectNode]);

  return (
    <TopologyView
      className="still-topology-view"
      contextToolbar={
        <div className="still-topology-context">
          <Breadcrumb>
            <BreadcrumbItem>Workspace</BreadcrumbItem>
            <BreadcrumbItem isActive>{breadcrumb}</BreadcrumbItem>
          </Breadcrumb>
          <div className="still-topology-context__status">
            {isRunning ? (
              <>
                <Spinner size="sm" aria-label="Run in progress" />
                <Label color="blue">Running {Math.round(progress)}%</Label>
              </>
            ) : (
              <Label color="green">Ready</Label>
            )}
          </div>
        </div>
      }
      controlBar={<TopologyControlBar controlButtons={controlButtons} onButtonClick={handleControl} />}
    >
      <VisualizationSurface state={{ selectedIds: selectedNodeId ? [selectedNodeId] : [] }} />
    </TopologyView>
  );
}
