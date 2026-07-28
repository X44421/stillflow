import {
  applyEdgeChanges,
  applyNodeChanges,
  reconnectEdge,
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
  toFlowNode,
  type PipelineFlowEdge,
  type PipelineFlowNode,
} from './graphAdapter';

interface CanvasGraphState {
  nodes: PipelineFlowNode[];
  edges: PipelineFlowEdge[];
  viewport: Viewport;
}

export type CanvasUndoRedo = Pick<
  CanvasStore,
  'undo' | 'redo' | 'canUndo' | 'canRedo'
>;

interface CanvasStore {
  graphs: Record<string, CanvasGraphState>;
  undoStack: Record<string, CanvasGraphState[]>;
  redoStack: Record<string, CanvasGraphState[]>;
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
  reconnectGraph: (
    graphKey: string,
    edge: PipelineFlowEdge,
    connection: Connection
  ) => void;
  setGraphViewport: (graphKey: string, viewport: Viewport) => void;
  setGraphLayout: (
    graphKey: string,
    positions: Record<string, XYPosition>
  ) => void;
  savePositionUndo: (graphKey: string) => void;
  undo: (graphKey: string) => boolean;
  redo: (graphKey: string) => boolean;
  canUndo: (graphKey: string) => boolean;
  canRedo: (graphKey: string) => boolean;
  persistGraph: (graphKey: string) => void;
}

export const DEFAULT_VIEWPORT: Viewport = { x: 0, y: 0, zoom: 1 };
export const EMPTY_FLOW_NODES: PipelineFlowNode[] = [];
export const EMPTY_FLOW_EDGES: PipelineFlowEdge[] = [];

const STORAGE_PREFIX = 'stillflow.canvas.';

function persistToStorage(graphKey: string, graph: CanvasGraphState): void {
  try {
    window.localStorage.setItem(
      `${STORAGE_PREFIX}${graphKey}`,
      JSON.stringify(graph)
    );
  } catch {
    // localStorage full or unavailable
  }
}

function restoreFromStorage(graphKey: string): CanvasGraphState | null {
  try {
    const raw = window.localStorage.getItem(`${STORAGE_PREFIX}${graphKey}`);
    if (!raw) return null;
    return JSON.parse(raw) as CanvasGraphState;
  } catch {
    return null;
  }
}

function cloneGraph(graph: CanvasGraphState): CanvasGraphState {
  return JSON.parse(JSON.stringify(graph));
}

export const useCanvasStore = create<CanvasStore>()(
  immer((set, get) => ({
    graphs: {},
    undoStack: {},
    redoStack: {},

    syncGraph: (graphKey, pipelineNodes, selectedNode) => {
      set((state) => {
        let current = state.graphs[graphKey];
        if (!current) {
          const saved = restoreFromStorage(graphKey);
          if (saved) current = saved;
        }
        if (current) state.graphs[graphKey] = current;

        const existingById = new Map(
          (current?.nodes ?? []).map((node) => [node.id, node])
        );
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
          (edge) =>
            validIds.has(edge.source) &&
            validIds.has(edge.target) &&
            edge.id !== `pipeline-edge-${edge.source}-${edge.target}`
        ) as PipelineFlowEdge[];

        /**
         * The pipeline order implies a connection between every consecutive
         * pair of nodes. These auto edges are regenerated on each sync (their
         * ids are excluded from retention above), and yield to any edge the
         * user drew or reconnected manually.
         */
        const autoEdges: PipelineFlowEdge[] = [];
        for (let index = 1; index < pipelineNodes.length; index++) {
          const source = pipelineNodes[index - 1].id;
          const target = pipelineNodes[index].id;
          const targetHasInput = retainedEdges.some(
            (edge) => edge.target === target
          );
          if (targetHasInput) continue;
          autoEdges.push({
            id: `pipeline-edge-${source}-${target}`,
            source,
            target,
            sourceHandle: 'output',
            targetHandle: 'input',
            type: 'pipelineEdge',
            reconnectable: true,
          });
        }

        state.graphs[graphKey] = {
          nodes: nextNodes,
          edges: [...autoEdges, ...retainedEdges],
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
      const hasRemove = changes.some((c) => c.type === 'remove');
      if (!hasRemove) {
        set((state) => {
          const graph = state.graphs[graphKey];
          if (!graph) return;
          graph.edges = applyEdgeChanges(
            changes,
            graph.edges as PipelineFlowEdge[]
          );
        });
        return;
      }
      const graph = get().graphs[graphKey];
      if (!graph) return;
      const prev = cloneGraph(graph);
      set((state) => {
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(prev);
        state.redoStack[graphKey] = [];
        const g = state.graphs[graphKey];
        if (!g) return;
        g.edges = applyEdgeChanges(
          changes,
          g.edges as PipelineFlowEdge[]
        );
      });
      persistToStorage(graphKey, get().graphs[graphKey]!);
    },

    connectGraph: (graphKey, connection) => {
      const graph = get().graphs[graphKey];
      if (!graph || !isValidDagConnection(connection, graph.edges)) return;
      if (!connection.source || !connection.target) return;
      const prev = cloneGraph(graph);
      set((state) => {
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(prev);
        state.redoStack[graphKey] = [];
        state.graphs[graphKey]!.edges.push({
          id: `pipeline-edge-${nanoid(8)}`,
          source: connection.source,
          target: connection.target,
          sourceHandle: connection.sourceHandle,
          targetHandle: connection.targetHandle,
          type: 'pipelineEdge',
          reconnectable: true,
        });
      });
      persistToStorage(graphKey, get().graphs[graphKey]!);
    },

    reconnectGraph: (graphKey, edge, connection) => {
      const graph = get().graphs[graphKey];
      if (!graph) return;
      const otherEdges = graph.edges.filter(
        (candidate) => candidate.id !== edge.id
      ) as PipelineFlowEdge[];
      if (!isValidDagConnection(connection, otherEdges)) return;
      const prev = cloneGraph(graph);
      set((state) => {
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(prev);
        state.redoStack[graphKey] = [];
        const g = state.graphs[graphKey];
        if (!g) return;
        g.edges = reconnectEdge(
          edge,
          connection,
          g.edges as PipelineFlowEdge[]
        );
      });
      persistToStorage(graphKey, get().graphs[graphKey]!);
    },

    setGraphViewport: (graphKey, viewport) => {
      set((state) => {
        const graph = state.graphs[graphKey];
        if (!graph) return;
        graph.viewport = viewport;
      });
      persistToStorage(graphKey, get().graphs[graphKey]!);
    },

    setGraphLayout: (graphKey, positions) => {
      const graph = get().graphs[graphKey];
      if (!graph) return;
      const prev = cloneGraph(graph);
      set((state) => {
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(prev);
        state.redoStack[graphKey] = [];
        const g = state.graphs[graphKey];
        if (!g) return;
        for (const node of g.nodes) {
          const position = positions[node.id];
          if (position) node.position = position;
        }
      });
      persistToStorage(graphKey, get().graphs[graphKey]!);
    },

    savePositionUndo: (graphKey) => {
      const graph = get().graphs[graphKey];
      if (!graph) return;
      const prev = cloneGraph(graph);
      set((state) => {
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(prev);
        state.redoStack[graphKey] = [];
      });
    },

    undo: (graphKey) => {
      let success = false;
      set((state) => {
        const stack = state.undoStack[graphKey];
        if (!stack || stack.length === 0) return;
        const snapshot = stack.pop()!;
        const redoSnapshot = cloneGraph(
          state.graphs[graphKey] as CanvasGraphState
        );
        if (!state.redoStack[graphKey]) state.redoStack[graphKey] = [];
        state.redoStack[graphKey].push(redoSnapshot);
        state.graphs[graphKey] = snapshot;
        success = true;
      });
      if (success) persistToStorage(graphKey, get().graphs[graphKey]!);
      return success;
    },

    redo: (graphKey) => {
      let success = false;
      set((state) => {
        const stack = state.redoStack[graphKey];
        if (!stack || stack.length === 0) return;
        const snapshot = stack.pop()!;
        const undoSonapshot = cloneGraph(
          state.graphs[graphKey] as CanvasGraphState
        );
        if (!state.undoStack[graphKey]) state.undoStack[graphKey] = [];
        state.undoStack[graphKey].push(undoSonapshot);
        state.graphs[graphKey] = snapshot;
        success = true;
      });
      if (success) persistToStorage(graphKey, get().graphs[graphKey]!);
      return success;
    },

    canUndo: (graphKey) => {
      const stack = get().undoStack[graphKey];
      return stack ? stack.length > 0 : false;
    },

    canRedo: (graphKey) => {
      const stack = get().redoStack[graphKey];
      return stack ? stack.length > 0 : false;
    },

    persistGraph: (graphKey) => {
      const graph = get().graphs[graphKey];
      if (graph) persistToStorage(graphKey, graph);
    },
  }))
);