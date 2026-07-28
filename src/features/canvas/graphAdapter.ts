import type { Edge, Node, XYPosition } from '@xyflow/react';
import type { PipelineNode } from '../../types';

export const NODE_WIDTH = 176;
export const NODE_HEIGHT = 58;
export const NODE_GAP = 72;
export const DEFAULT_X = 64;
export const DEFAULT_Y = 56;

/** Display-only terminal node representing the pipeline's output dataset. */
export const OUTPUT_ASSET_ID = 'output-asset';

export type PipelineFlowNodeData = {
  pipelineNode: PipelineNode;
};

export type PipelineFlowNode = Node<
  PipelineFlowNodeData,
  'pipelineNode'
>;

export type PipelineEdgeTone = 'active' | 'dim';

export type PipelineFlowEdge = Edge<
  { tone?: PipelineEdgeTone },
  'pipelineEdge'
>;

export function getNodeHeight(node: PipelineNode): number {
  // Nodes that produced metrics show an extra impact line.
  return NODE_HEIGHT + (node.metrics ? 14 : 0);
}

/**
 * Pipelines read left → right, anchored to the top-left work area instead of
 * the geometric center, so a growing chain expands naturally to the right.
 */
export function defaultNodePosition(index: number): XYPosition {
  return {
    x: DEFAULT_X + index * (NODE_WIDTH + NODE_GAP),
    y: DEFAULT_Y,
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