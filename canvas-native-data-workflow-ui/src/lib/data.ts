export type NodeStatus = "ready" | "running" | "warning" | "failed" | "waiting" | "complete";

export type NodeKind = "DATASET" | "FILTER" | "PIPE" | "EMBED" | "INDEX" | "VALIDATE" | "EXPORT";

export interface GNode {
  id: string;
  seq: string;
  kind: NodeKind;
  name: string;
  x: number;
  y: number;
  metric: string;
  behavior: string;
  status: NodeStatus;
  progress?: number;
  aux?: boolean;
  objectId: string;
  duration: string;
}

export interface GEdge {
  id: string;
  from: string;
  to: string;
  kind: "flow" | "aux";
  fromPort: "right" | "bottom";
  toPort: "left" | "top";
  label?: string;
}

export const NODE_W = 192;
export const NODE_H = 112;

export const ICON_BY_KIND: Record<NodeKind, string> = {
  DATASET: "database",
  FILTER: "filter",
  PIPE: "pipe",
  EMBED: "embed",
  INDEX: "index",
  VALIDATE: "validate",
  EXPORT: "exportIcon",
};

export const INITIAL_NODES: GNode[] = [
  {
    id: "n1",
    seq: "01",
    kind: "DATASET",
    name: "Customer Dataset",
    x: 0,
    y: 0,
    metric: "1.2M rows · 24 columns",
    behavior: "2.4% missing",
    status: "ready",
    objectId: "obj_9f21ac",
    duration: "—",
  },
  {
    id: "n2",
    seq: "02",
    kind: "FILTER",
    name: "Remove Invalid Rows",
    x: 240,
    y: 0,
    metric: "1.2M → 982K rows",
    behavior: "quality_score > 0.8",
    status: "ready",
    objectId: "obj_4c07de",
    duration: "8.2s",
  },
  {
    id: "n3",
    seq: "03",
    kind: "PIPE",
    name: "Chunk Pipeline",
    x: 480,
    y: 0,
    metric: "982K → 3.4M chunks",
    behavior: "size 800 · overlap 120",
    status: "ready",
    objectId: "obj_71b3e8",
    duration: "3m 12s",
  },
  {
    id: "n4",
    seq: "04",
    kind: "EMBED",
    name: "Text Embedding",
    x: 720,
    y: 0,
    metric: "1024 dimensions",
    behavior: "model: text-embed-3",
    status: "ready",
    objectId: "obj_2a55f0",
    duration: "11m 04s",
  },
  {
    id: "n5",
    seq: "05",
    kind: "INDEX",
    name: "Vector Index",
    x: 960,
    y: 0,
    metric: "3.4M vectors · HNSW",
    behavior: "recall@10 · 0.964",
    status: "ready",
    objectId: "obj_c81d47",
    duration: "4m 47s",
  },
  {
    id: "n6",
    seq: "06",
    kind: "EXPORT",
    name: "Final Export",
    x: 1200,
    y: 0,
    metric: "312 MB · Parquet",
    behavior: "Schema mismatch",
    status: "warning",
    objectId: "obj_e30ba9",
    duration: "56s",
  },
  {
    id: "n7",
    seq: "A1",
    kind: "VALIDATE",
    name: "Customer Quality",
    x: 480,
    y: 210,
    metric: "97.2% valid",
    behavior: "12 rules · 3 warnings",
    status: "warning",
    aux: true,
    objectId: "obj_55ff12",
    duration: "42s",
  },
];

export const INITIAL_EDGES: GEdge[] = [
  { id: "e1", from: "n1", to: "n2", kind: "flow", fromPort: "right", toPort: "left", label: "1.2M rows" },
  { id: "e2", from: "n2", to: "n3", kind: "flow", fromPort: "right", toPort: "left", label: "982K rows" },
  { id: "e3", from: "n3", to: "n4", kind: "flow", fromPort: "right", toPort: "left", label: "3.4M chunks" },
  { id: "e4", from: "n4", to: "n5", kind: "flow", fromPort: "right", toPort: "left", label: "3.4M vectors" },
  { id: "e5", from: "n5", to: "n6", kind: "flow", fromPort: "right", toPort: "left", label: "index snapshot" },
  { id: "e6", from: "n3", to: "n7", kind: "aux", fromPort: "bottom", toPort: "top", label: "async sample" },
];

export const GROUP_FRAME = {
  x: 700,
  y: -46,
  w: 472,
  h: 176,
  label: "Vectorization Stage",
  meta: "2 objects",
};

export const ANNOTATION = {
  x: 762,
  y: 226,
  w: 214,
  title: "Validation note",
  body: "Quality checks run async on a 5% sample. Warnings do not block export, but a failed rule freezes the vector index.",
  author: "M. Okafor",
};

export const COMMENT_PIN = { x: 880, y: -22, initials: "SR", count: 2 };

/* ---------------- Files surface ---------------- */

export interface FileRow {
  id: string;
  name: string;
  meta: string;
  ext: string;
  kind: NodeKind;
}

export const RECENT_FILES: FileRow[] = [
  { id: "f1", name: "customer.csv", meta: "412 MB · 2h ago", ext: "csv", kind: "DATASET" },
  { id: "f2", name: "orders.parquet", meta: "1.1 GB · yesterday", ext: "parquet", kind: "DATASET" },
  { id: "f3", name: "raw_logs.json", meta: "84 MB · 3d ago", ext: "json", kind: "DATASET" },
];

export interface ObjectGroup {
  id: string;
  label: string;
  count: number;
  icon: string;
  children: { id: string; name: string; meta: string; status?: NodeStatus }[];
}

export const OBJECT_GROUPS: ObjectGroup[] = [
  {
    id: "g1",
    label: "Datasets",
    count: 12,
    icon: "database",
    children: [
      { id: "o1", name: "Customer Dataset", meta: "1.2M rows", status: "ready" },
      { id: "o2", name: "Chunk Dataset", meta: "3.4M rows", status: "ready" },
      { id: "o3", name: "Support Threads", meta: "218K rows", status: "warning" },
    ],
  },
  {
    id: "g2",
    label: "Pipelines",
    count: 6,
    icon: "pipe",
    children: [
      { id: "o4", name: "Chunk Pipeline", meta: "v14 · active", status: "ready" },
      { id: "o5", name: "Dedup + Normalize", meta: "v3 · draft" },
    ],
  },
  { id: "g3", label: "Models", count: 4, icon: "embed", children: [{ id: "o6", name: "text-embed-3", meta: "1024 dim" }] },
  {
    id: "g4",
    label: "Vector Stores",
    count: 3,
    icon: "index",
    children: [{ id: "o7", name: "kb-prod-hnsw", meta: "3.4M vectors", status: "ready" }],
  },
  {
    id: "g5",
    label: "Evaluations",
    count: 8,
    icon: "validate",
    children: [{ id: "o8", name: "Customer Quality", meta: "97.2% valid", status: "warning" }],
  },
];

/* ---------------- Preview surface ---------------- */

export interface PreviewRow {
  source: string;
  file_name: string;
  page: number;
  title: string;
  content: string;
}

export const PREVIEW_ROWS: PreviewRow[] = [
  {
    source: "zendesk",
    file_name: "customer.csv",
    page: 1,
    title: "Refund window policy",
    content: "Refunds are processed within 14 business days of the return being received at the regional…",
  },
  {
    source: "zendesk",
    file_name: "customer.csv",
    page: 1,
    title: "Refund window policy",
    content: "…warehouse. Customers in the EU may extend the window to 30 days under the distance selling…",
  },
  {
    source: "confluence",
    file_name: "onboarding.md",
    page: 4,
    title: "Account provisioning",
    content: "New workspace owners receive a provisioning token valid for 72 hours. Tokens are single-use…",
  },
  {
    source: "confluence",
    file_name: "onboarding.md",
    page: 5,
    title: "Account provisioning",
    content: "…and must be redeemed from the same network region to satisfy residency requirements.",
  },
  {
    source: "notion",
    file_name: "pricing_faq.md",
    page: 2,
    title: "Volume discount tiers",
    content: "Tier 2 begins at 5M processed rows per month and applies a 12% reduction to compute credits…",
  },
  {
    source: "zendesk",
    file_name: "customer.csv",
    page: 9,
    title: "Data retention",
    content: "Deleted objects remain recoverable for 30 days, after which vector shards are purged on the…",
  },
  {
    source: "s3://kb-raw",
    file_name: "raw_logs.json",
    page: 12,
    title: "Ingestion failures",
    content: "Rows failing schema coercion are quarantined with an error code and the original payload hash…",
  },
  {
    source: "notion",
    file_name: "pricing_faq.md",
    page: 3,
    title: "Overage handling",
    content: "Overages are billed at list price unless a committed-use agreement is attached to the org…",
  },
  {
    source: "confluence",
    file_name: "runbook.md",
    page: 1,
    title: "Reindex procedure",
    content: "Take the index offline, snapshot the HNSW graph, then rebuild from the chunk dataset partition…",
  },
  {
    source: "zendesk",
    file_name: "customer.csv",
    page: 22,
    title: "SLA definitions",
    content: "Priority 1 incidents acknowledge within 15 minutes and update every hour until mitigation…",
  },
  {
    source: "s3://kb-raw",
    file_name: "raw_logs.json",
    page: 41,
    title: "Rate limits",
    content: "Embedding requests are capped at 3,000 RPM per project with burst allowance of 20 seconds…",
  },
  {
    source: "notion",
    file_name: "glossary.md",
    page: 1,
    title: "Chunk overlap",
    content: "Overlap preserves sentence continuity across boundaries and is measured in tokens, not chars…",
  },
];

export interface ProfileCol {
  name: string;
  type: string;
  missing: number;
  unique: number;
  valid: number;
  hist: number[];
}

export const PROFILE_COLS: ProfileCol[] = [
  { name: "source", type: "string", missing: 0, unique: 4, valid: 100, hist: [42, 88, 30, 62, 18, 9, 5, 3] },
  { name: "file_name", type: "string", missing: 0, unique: 612, valid: 100, hist: [70, 52, 44, 38, 30, 24, 16, 10] },
  { name: "page", type: "int32", missing: 0.4, unique: 348, valid: 99.6, hist: [12, 34, 58, 82, 66, 40, 22, 11] },
  { name: "title", type: "string", missing: 1.8, unique: 9411, valid: 98.2, hist: [22, 44, 61, 74, 55, 33, 20, 8] },
  { name: "content", type: "string", missing: 0.1, unique: 3411982, valid: 99.9, hist: [8, 26, 55, 90, 71, 46, 24, 12] },
  { name: "token_count", type: "int32", missing: 0, unique: 794, valid: 100, hist: [5, 18, 46, 88, 92, 51, 27, 14] },
  { name: "lang", type: "string", missing: 3.1, unique: 11, valid: 96.9, hist: [96, 22, 14, 9, 6, 4, 3, 2] },
];

/* ---------------- Inspector ---------------- */

export const PARAMS = [
  { key: "Chunk Size", value: "800", unit: "tokens" },
  { key: "Overlap", value: "120", unit: "tokens" },
  { key: "Strategy", value: "Sliding Window", unit: "" },
  { key: "Encoding", value: "cl100k_base", unit: "" },
  { key: "Min Length", value: "48", unit: "tokens" },
];

export const EVENTS = [
  { t: "12:04:18", label: "Run #042 started", tone: "t2" },
  { t: "12:04:26", label: "Input schema resolved · 24 cols", tone: "t3" },
  { t: "12:07:38", label: "3,412,000 chunks written", tone: "t3" },
  { t: "12:07:41", label: "Sample handed to validation", tone: "t3" },
];

/* ---------------- Runtime ---------------- */

export interface RunStep {
  id: string;
  label: string;
}

export const RUN_STEPS: RunStep[] = [
  { id: "n1", label: "Customer Dataset" },
  { id: "n2", label: "Remove Invalid Rows" },
  { id: "n3", label: "Chunk Pipeline" },
  { id: "n4", label: "Text Embedding" },
  { id: "n5", label: "Vector Index" },
  { id: "n6", label: "Final Export" },
];

export const STATUS_COLOR: Record<string, string> = {
  ready: "#4ba66a",
  complete: "#4ba66a",
  running: "#2196d2",
  warning: "#c58b32",
  failed: "#c95e62",
  waiting: "#b0b8c1",
};
