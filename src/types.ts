export type PreviewTab = 'data' | 'profile' | 'quality' | 'compare';
export type RunStatus = 'ready' | 'running' | 'warning' | 'error';

export interface TabItem {
  id: string;
  label: string;
  version?: string;
  unsaved?: boolean;
}

export interface PipelineNode {
  id: string;
  title: string;
  subtitle: string;
  status: RunStatus;
  icon: 'source' | 'transform' | 'dedup' | 'output';
}

export interface TableRow {
  id: string;
  name: string;
  email: string;
  phone: string;
  city: string;
  state: string;
  zip: string;
  status: 'active' | 'inactive';
  created_at: string;
  updated_at: string;
  score: number;
  emailModified?: boolean;
  emailNull?: boolean;
  phoneModified?: boolean;
  statusModified?: boolean;
  scoreInvalid?: boolean;
}

export interface CompareRow {
  name: string;
  email: string;
  phone: string;
  status: string;
  changed?: boolean;
}

export interface QualityRow {
  metric: string;
  result: string;
  status: RunStatus;
  statusLabel: string;
}

export interface QualityIssue {
  severity: 'danger' | 'warning' | 'info';
  title: string;
  detail: string;
  count: number;
}

export interface ColumnDef {
  key: keyof TableRow;
  label: string;
  sortable: boolean;
}
