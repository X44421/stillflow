export type NodeKind = "source" | "transform" | "filter" | "replace" | "ai" | "groupby" | "output";

export interface PipelineNode {
  id: string;
  title: string;
  subtitle: string;
  kind: NodeKind;
  rows?: string;
  x: number;
  y: number;
  nodeId: string;
  description: string;
  status: string;
  created: string;
  updated: string;
}

export const NODE_W = 176;

export const nodes: PipelineNode[] = [
  {
    id: "csv",
    title: "Sales_2024.csv",
    subtitle: "CSV · 1.2M rows",
    kind: "source",
    x: 16,
    y: 152,
    nodeId: "node_a91c04",
    description: "Source file uploaded from local disk.",
    status: "Ready",
    created: "2024-07-16 09:58",
    updated: "2024-07-16 09:58",
  },
  {
    id: "clean",
    title: "Clean Column Names",
    subtitle: "Transform",
    kind: "transform",
    x: 240,
    y: 152,
    nodeId: "node_b22f81",
    description: "Normalize column names to snake_case.",
    status: "Ready",
    created: "2024-07-16 10:02",
    updated: "2024-07-16 10:02",
  },
  {
    id: "filter",
    title: "Filter Invalid Rows",
    subtitle: "Filter",
    kind: "filter",
    rows: "1.1M rows",
    x: 470,
    y: 152,
    nodeId: "node_c73d19",
    description: "Drop rows with null order_id or negative sales.",
    status: "Ready",
    created: "2024-07-16 10:05",
    updated: "2024-07-16 10:05",
  },
  {
    id: "country",
    title: "Standardize Country",
    subtitle: "Replace",
    kind: "replace",
    rows: "1.1M rows",
    x: 700,
    y: 152,
    nodeId: "node_d18e55",
    description: "Map country codes to full country names.",
    status: "Ready",
    created: "2024-07-16 10:11",
    updated: "2024-07-16 10:11",
  },
  {
    id: "ai",
    title: "AI Product Category",
    subtitle: "AI Classify",
    kind: "ai",
    rows: "1.1M rows",
    x: 240,
    y: 396,
    nodeId: "node_7f3e2a",
    description: "Use AI to classify product category based on product name and description.",
    status: "Ready",
    created: "2024-07-16 10:24",
    updated: "2024-07-16 10:24",
  },
  {
    id: "agg",
    title: "Aggregate Sales",
    subtitle: "Group By",
    kind: "groupby",
    rows: "12.6K rows",
    x: 470,
    y: 396,
    nodeId: "node_e91b32",
    description: "Group by country and product_category, sum sales.",
    status: "Ready",
    created: "2024-07-16 10:29",
    updated: "2024-07-16 10:29",
  },
  {
    id: "out",
    title: "Output: Clean Sales",
    subtitle: "Parquet",
    kind: "output",
    rows: "12.6K rows",
    x: 700,
    y: 396,
    nodeId: "node_f04a77",
    description: "Write cleaned dataset as Parquet to workspace storage.",
    status: "Ready",
    created: "2024-07-16 10:31",
    updated: "2024-07-16 10:31",
  },
];

export interface PreviewRow {
  orderId: string;
  orderDate: string;
  customerId: string;
  country: string;
  category: string;
  amount: string;
}

const countries = ["United States", "Canada", "United Kingdom", "Germany", "France", "Japan", "Australia", "Brazil"];
const categories = ["Electronics", "Home Appliances", "Furniture", "Sports", "Toys", "Office Supplies"];
const amounts = ["299.99", "159.00", "499.00", "89.90", "120.00", "45.50", "780.25", "230.10", "64.99", "349.00"];

export const previewRows: PreviewRow[] = Array.from({ length: 50 }, (_, i) => {
  const day = 1 + Math.floor(i / 4);
  return {
    orderId: String(1000001 + i),
    orderDate: `2024-01-${String(day).padStart(2, "0")}`,
    customerId: `CUST_${String(((i * 547) % 2000) + 1).padStart(5, "0")}`,
    country: countries[(i * 3) % countries.length],
    category: categories[(i * 5) % categories.length],
    amount: amounts[(i * 7) % amounts.length],
  };
});
