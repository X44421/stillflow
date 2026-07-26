import {
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type EdgeChange,
  type NodeChange,
  type Viewport,
  type XYPosition,
} from '@xyflow/react';
import { nanoid } from 'nanoid';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import type { PipelineNode } from '../../types';
import { isValidDagConnection } from './connectionRules';
import {
  defaultFlowEdges,
  toFlowNode,
  type PipelineFlowEdge,
  type PipelineFlowNode,
} from './graphAdapter';

interface CanvasGraphState {
  nodes: PipelineFlowNode[];
  edges: PipelineFlowEdge[];
  viewport: Viewport;
}

interface CanvasStore {
  graphs: Record<string, CanvasGraphState>;
  syncGraph: (
    graphKey: string,
    nodes: PipelineNode[],
    selectedNode: string
  ) => void;
  applyGraphNodeChanges: (
    graphKey: string,
    changes: NodeChange<PipelineFlowNode>[]
  ) => void;
  applyGraphEdgeChanges: (
    graphKey: string,
    changes: EdgeChange<PipelineFlowEdge>[]
  ) => void;
  connectGraph: (graphKey: string, connection: Connection) => void;
  setGraphViewport: (graphKey: string, viewport: Viewport) => void;
  setGraphLayout: (
    graphKey: string,
    positions: Record<string, XYPosition>
  ) => void;
}

export const DEFAULT_VIEWPORT: Viewport = { x: 0, y: 0, zoom: 1 };
export const EMPTY_FLOW_NODES: PipelineFlowNode[] = [];
export const EMPTY_FLOW_EDGES: PipelineFlowEdge[] = [];

export const useCanvasStore = create<CanvasStore>()(
  immer((set) => ({
    graphs: {},

    syncGraph: (graphKey, pipelineNodes, selectedNode) => {
      set((state) => {
        const current = state.graphs[graphKey];
        const existingById = new Map(
          (current?.nodes ?? []).map((node) => [node.id, node])
        );
        const previousIds = new Set(existingById.keys());
        const validIds = new Set(pipelineNodes.map((node) => node.id));

        const nextNodes = pipelineNodes.map((node, index) =>
          toFlowNode(
            node,
            index,
            existingById.get(node.id) as PipelineFlowNode | undefined,
            node.id === selectedNode
          )
        );

        const retainedEdges = (current?.edges ?? []).filter(
          (edge) => validIds.has(edge.source) && validIds.has(edge.target)
        ) as PipelineFlowEdge[];
        const defaultEdges = defaultFlowEdges(pipelineNodes);
        const edgesForNewNodes = current
          ? defaultEdges.filter(
              (edge) =>
                !previousIds.has(edge.source) || !previousIds.has(edge.target)
            )
          : defaultEdges;
        const existingPairs = new Set(
          retainedEdges.map((edge) => `${edge.source}->${edge.target}`)
        );

        state.graphs[graphKey] = {
          nodes: nextNodes,
          edges: [
            ...retainedEdges,
            ...edgesForNewNodes.filter(
              (edge) => !existingPairs.has(`${edge.source}->${edge.target}`)
            ),
          ],
          viewport: current?.viewport ?? { ...DEFAULT_VIEWPORT },
        };
      });
    },

    applyGraphNodeChanges: (graphKey, changes) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph) return;
        graph.nodes = applyNodeChanges(
          changes,
          graph.nodes as PipelineFlowNode[]
        );
      });
    },

    applyGraphEdgeChanges: (graphKey, changes) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph) return;
        graph.edges = applyEdgeChanges(
          changes,
          graph.edges as PipelineFlowEdge[]
        );
      });
    },

    connectGraph: (graphKey, connection) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph || !isValidDagConnection(connection, graph.edges)) return;
        if (!connection.source || !connection.target) return;

        graph.edges.push({
          id: `pipeline-edge-${nanoid(8)}`,
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle,
          targetHandle: connection.targetHandle,
          type: 'pipelineEdge',
        });
      });
    },

    setGraphViewport: (graphKey, viewport) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph) return;
        graph.viewport = viewport;
      });
    },

    setGraphLayout: (graphKey, positions) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph) return;
        for (const node of graph.nodes) {
          const position = positions[node.id];
          if (position) node.position = position;
        }
      });
    },
  }))
);
