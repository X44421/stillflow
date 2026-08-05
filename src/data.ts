import type { ColumnDef, CompareRow, PipelineNode, QualityIssue, QualityRow, TabItem, TableRow } from './types';

export const initialTabs: TabItem[] = [
  { id: 'dataset-customers', label: 'customers.csv', version: 'v3' },
  { id: 'session-cleanup', label: 'Customer Clean Session', version: 'draft' },
];

export const pipelineNodes: PipelineNode[] = [
  { id: '1', title: 'customers.csv', subtitle: 'Source - 80,000 rows', status: 'ready', icon: 'source' },
  { id: '2', title: 'Normalize whitespace', subtitle: 'name - Text transform', status: 'ready', icon: 'transform' },
  { id: '3', title: 'Remove duplicates', subtitle: 'Deduplication', status: 'ready', icon: 'dedup' },
  { id: '4', title: 'customer_clean.csv', subtitle: 'Output - ~78,500 rows', status: 'ready', icon: 'output' },
];

export const connections: [string, string][] = [
  ['1', '2'],
  ['2', '3'],
  ['3', '4'],
];

export const tableColumns: ColumnDef[] = [
  { key: 'id', label: 'ID', sortable: true },
  { key: 'name', label: 'Name', sortable: true },
  { key: 'email', label: 'Email', sortable: false },
  { key: 'phone', label: 'Phone', sortable: false },
  { key: 'city', label: 'City', sortable: true },
  { key: 'state', label: 'State', sortable: false },
  { key: 'zip', label: 'Zip', sortable: false },
  { key: 'status', label: 'Status', sortable: true },
  { key: 'created_at', label: 'Created', sortable: true },
  { key: 'updated_at', label: 'Updated', sortable: true },
  { key: 'score', label: 'Score', sortable: true },
];

const firstNames = [
  'Alice', 'Bob', 'Carol', 'Dan', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack', 'Kara', 'Leo', 'Mia',
  'Noah', 'Olive', 'Paul', 'Quinn', 'Rita', 'Sam', 'Tina', 'Uma', 'Victor', 'Wendy', 'Xavier', 'Yara', 'Zane',
];

const lastNames = [
  'Johnson', 'Smith', 'Davis', 'Wilson', 'Martinez', 'Lee', 'Kim', 'Brown', 'Taylor', 'Anderson', 'Thomas',
  'Moore', 'Jackson', 'White', 'Harris', 'Martin', 'Thompson', 'Garcia', 'Robinson', 'Clark', 'Rodriguez',
  'Lewis', 'Walker', 'Hall', 'Young', 'King',
];

const cities = [
  'Portland', 'Seattle', 'Boise', 'Denver', 'Austin', 'Miami', 'Chicago', 'New York', 'Atlanta', 'Phoenix',
  'San Diego', 'Boston',
];

const states = ['OR', 'WA', 'ID', 'CO', 'TX', 'FL', 'IL', 'NY', 'GA', 'AZ', 'CA', 'MA'];
const domains = ['email.com', 'example.com', 'mailbox.org', 'post.io'];

export function makeTableRows(count = 120): TableRow[] {
  return Array.from({ length: count }, (_, index) => buildRow(index));
}

function buildRow(index: number): TableRow {
  const firstName = firstNames[index % firstNames.length];
  const lastName = lastNames[(index * 7) % lastNames.length];
  const name = `${firstName} ${lastName}`;
  const email =
    index % 11 === 0
      ? 'NULL'
      : `${firstName.toLowerCase()}.${lastName.toLowerCase()}@${domains[index % domains.length]}`;
  const month = String((index % 12) + 1).padStart(2, '0');
  const day = String((index % 27) + 1).padStart(2, '0');

  return {
    id: String(1001 + index),
    name,
    email,
    phone: `555-01${String((index % 90) + 10).padStart(2, '0')}`,
    city: cities[index % cities.length],
    state: states[index % states.length],
    zip: String(90000 + ((index * 173) % 10000)),
    status: index % 9 === 3 ? 'inactive' : 'active',
    created_at: `2024-${month}-${day}`,
    updated_at: `2025-${month}-${day}`,
    score: 55 + ((index * 13) % 45),
    emailModified: index % 7 === 3,
    emailNull: index % 11 === 0,
    phoneModified: index % 13 === 5,
    statusModified: index % 17 === 9,
    scoreInvalid: index % 19 === 11,
  };
}

export const qualityRows: QualityRow[] = [
  { metric: 'Schema validity', result: 'Valid', status: 'ready', statusLabel: 'Pass' },
  { metric: 'Completeness', result: '97.1%', status: 'ready', statusLabel: 'Pass' },
  { metric: 'Duplicate score', result: '2.0%', status: 'warning', statusLabel: 'Warning' },
  { metric: 'Text validity (email)', result: '99.4%', status: 'ready', statusLabel: 'Pass' },
  { metric: 'Privacy risk', result: 'Medium', status: 'warning', statusLabel: 'Review' },
  { metric: 'Token health', result: 'Good', status: 'ready', statusLabel: 'Pass' },
  { metric: 'Label balance', result: '78/22', status: 'ready', statusLabel: 'Pass' },
];

export const qualityIssues: QualityIssue[] = [
  {
    severity: 'warning',
    title: 'Duplicate rows detected',
    detail: '1,588 rows share identical name and email values.',
    count: 1588,
  },
  {
    severity: 'warning',
    title: 'Medium privacy risk',
    detail: 'Email addresses appear in 2.4% of unverified records.',
    count: 121,
  },
  {
    severity: 'info',
    title: 'Null email values',
    detail: '47 records have a NULL email after normalization.',
    count: 47,
  },
  {
    severity: 'info',
    title: 'Type mismatches',
    detail: 'Score column contains non-numeric values in 12 rows.',
    count: 12,
  },
];

export const compareBeforeRows: CompareRow[] = [
  { name: 'Alice Johnson', email: 'alice@email.com', phone: '555-0101', status: 'active' },
  { name: 'Bob Smith', email: 'bob@oldmail.com', phone: '555-0102', status: 'active', changed: true },
  { name: 'Carol Davis', email: 'NULL', phone: '555-0103', status: 'inactive', changed: true },
  { name: 'Dan Wilson', email: 'dan@email.com', phone: '555-0104', status: 'active' },
];

export const compareAfterRows: CompareRow[] = [
  { name: 'Alice Johnson', email: 'alice@email.com', phone: '555-0101', status: 'active' },
  { name: 'Bob Smith', email: 'bob.smith@email.com', phone: '555-0102', status: 'active', changed: true },
  { name: 'Carol Davis', email: 'carol.d@email.com', phone: '555-0103', status: 'inactive', changed: true },
  { name: 'Dan Wilson', email: 'dan@email.com', phone: '555-0104', status: 'active' },
];
