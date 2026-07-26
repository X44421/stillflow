import type { Connection } from '@xyflow/react';
import type { PipelineFlowEdge } from './graphAdapter';

export function isValidDagConnection(
  connection: Connection,
  edges: PipelineFlowEdge[]
): boolean {
  const { source, target } = connection;
  if (!source || !target || source === target) return false;

  const duplicate = edges.some(
    (edge) => edge.source === source && edge.target === target
  );
  if (duplicate) return false;

  const outgoing = new Map<string, string[]>();
  for (const edge of edges) {
    const targets = outgoing.get(edge.source) ?? [];
    targets.push(edge.target);
    outgoing.set(edge.source, targets);
  }

  const pending = [target];
  const visited = new Set<string>();
  while (pending.length > 0) {
    const nodeId = pending.pop();
    if (!nodeId || visited.has(nodeId)) continue;
    if (nodeId === source) return false;
    visited.add(nodeId);
    pending.push(...(outgoing.get(nodeId) ?? []));
  }

  return true;
}
