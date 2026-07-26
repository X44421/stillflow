import type { Edge, Node, XYPosition } from '@xyflow/react';
import type { PipelineNode } from '../../types';

export const NODE_WIDTH = 260;
export const NODE_HEIGHT = 78;
export const NODE_WITH_ROWS_HEIGHT = 96;
export const NODE_GAP = 40;
export const DEFAULT_X = 420;
export const DEFAULT_Y = 120;

export type PipelineFlowNodeData = {
  pipelineNode: PipelineNode;
};

export type PipelineFlowNode = Node<
  PipelineFlowNodeData,
  'pipelineNode'
>;

export type PipelineFlowEdge = Edge<
  Record<string, never>,
  'pipelineEdge'
>;

export function getNodeHeight(node: PipelineNode): number {
  return node.rows ? NODE_WITH_ROWS_HEIGHT : NODE_HEIGHT;
}

export function defaultNodePosition(index: number): XYPosition {
  return {
    x: DEFAULT_X,
    y: DEFAULT_Y + index * (NODE_WITH_ROWS_HEIGHT + NODE_GAP),
  };
}

export function toFlowNode(
  pipelineNode: PipelineNode,
  index: number,
  existing?: PipelineFlowNode,
  selected = false
): PipelineFlowNode {
  return {
    ...existing,
    id: pipelineNode.id,
    type: 'pipelineNode',
    position: existing?.position ?? defaultNodePosition(index),
    data: { pipelineNode },
    selected,
    draggable: true,
    selectable: true,
    className: 'pipeline-flow-node',
    style: {
      ...existing?.style,
      width: NODE_WIDTH,
    },
  };
}

export function defaultFlowEdges(
  nodes: PipelineNode[]
): PipelineFlowEdge[] {
  return nodes.slice(0, -1).map((node, index) => ({
    id: `pipeline-edge-${node.id}-${nodes[index + 1].id}`,
    source: node.id,
    target: nodes[index + 1].id,
    type: 'pipelineEdge',
  }));
}
