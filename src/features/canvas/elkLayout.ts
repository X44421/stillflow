import ELK from 'elkjs/lib/elk.bundled.js';
import type { XYPosition } from '@xyflow/react';
import {
  getNodeHeight,
  NODE_WIDTH,
  type PipelineFlowEdge,
  type PipelineFlowNode,
} from './graphAdapter';

const elk = new ELK();

export async function layoutPipelineGraph(
  nodes: PipelineFlowNode[],
  edges: PipelineFlowEdge[]
): Promise<Record<string, XYPosition>> {
  if (nodes.length === 0) return {};

  const graph = await elk.layout({
    id: 'pipeline',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'RIGHT',
      'elk.edgeRouting': 'ORTHOGONAL',
      'elk.spacing.nodeNode': '40',
      'elk.layered.spacing.nodeNodeBetweenLayers': '72',
      'elk.layered.nodePlacement.strategy': 'NETWORK_SIMPLEX',
      'elk.padding': '[top=40,left=40,bottom=40,right=40]',
    },
    children: nodes.map((node) => ({
      id: node.id,
      width: NODE_WIDTH,
      height: getNodeHeight(node.data.pipelineNode),
    })),
    edges: edges.map((edge) => ({
      id: edge.id,
      sources: [edge.source],
      targets: [edge.target],
    })),
  });

  return Object.fromEntries(
    (graph.children ?? []).map((node) => [
      node.id,
      {
        x: node.x ?? 0,
        y: node.y ?? 0,
      },
    ])
  );
}
