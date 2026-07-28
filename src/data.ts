import type { PipelineNodeConfig, TransformObject } from './types';

export const defaultConfig: PipelineNodeConfig = {
  column: '',
  strategy: 'Keep first',
  scope: 'Current dataset',
  nullHandling: 'Ignore',
};

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
