import type { NodeType, PipelineNodeConfig, TransformObject } from './types';

export const defaultConfig: PipelineNodeConfig = {
  column: '',
  strategy: 'Keep first',
  scope: 'Current dataset',
  nullHandling: 'Ignore',
};

/**
 * Rule defaults that match each node type's semantics — a Filter must never
 * inherit Deduplicate options like "Keep first" (and vice versa).
 */
export function defaultConfigFor(type: NodeType): PipelineNodeConfig {
  if (type === 'filter') {
    return {
      column: '',
      strategy: '',
      scope: 'Current dataset',
      nullHandling: 'Treat as non-match',
      mode: 'Keep matching rows',
      operator: 'is not empty',
      value: '',
    };
  }
  if (type === 'normalize') {
    return {
      column: '',
      strategy: '',
      scope: 'Current dataset',
      nullHandling: 'Ignore',
    };
  }
  return { ...defaultConfig };
}

export const transformObjects: TransformObject[] = [
  {
    id: 't4',
    name: 'Filter',
    description: 'Keep matched rows',
    category: 'transform',
    icon: 'filter',
    available: true,
  },
  {
    id: 't5',
    name: 'Deduplicate',
    description: 'Remove repeated records',
    category: 'transform',
    icon: 'copy',
    available: true,
  },
  {
    id: 't6',
    name: 'Normalize Text',
    description: 'Clean string values',
    category: 'transform',
    icon: 'type',
    available: true,
  },
  {
    id: 't7',
    name: 'Export CSV',
    description: 'Write cleaned data',
    category: 'output',
    icon: 'upload',
    available: true,
  },
];
