/* ------------------------------------------------------------------ *
 *  Tiny CSV parser + column profiler (the engine behind the
 *  Kaggle-style "Detail" preview table with per-column summaries).
 * ------------------------------------------------------------------ */

export type Row = Record<string, string>;

export type ColumnType = "integer" | "decimal" | "date" | "boolean" | "string";

export interface Bucket {
  label: string;
  x0: number;
  x1: number;
  count: number;
}

export interface CategorySlice {
  label: string;
  count: number;
  pct: number;
  other?: boolean;
}

export interface ColumnStats {
  name: string;
  index: number;
  type: ColumnType;
  total: number;
  valid: number;
  mismatched: number;
  missing: number;
  unique: number;
  /** numeric / date */
  min?: number;
  max?: number;
  mean?: number;
  std?: number;
  buckets: Bucket[];
  /** categorical */
  categories: CategorySlice[];
  mostCommon?: CategorySlice;
  /** string lengths */
  minLen?: number;
  maxLen?: number;
  sample: string[];
}

/* ----------------------------- parsing ---------------------------- */

export function parseCSV(text: string): { columns: string[]; rows: Row[] } {
  const table: string[][] = [];
  let field = "";
  let record: string[] = [];
  let inQuotes = false;

  const src = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  for (let i = 0; i < src.length; i++) {
    const c = src[i];
    if (inQuotes) {
      if (c === '"') {
        if (src[i + 1] === '"') {
          field += '"';
          i++;
        } else inQuotes = false;
      } else field += c;
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      record.push(field);
      field = "";
    } else if (c === "\n") {
      record.push(field);
      table.push(record);
      record = [];
      field = "";
    } else field += c;
  }
  if (field.length || record.length) {
    record.push(field);
    table.push(record);
  }

  const header = (table.shift() ?? []).map((h, i) => h.trim() || `column_${i + 1}`);
  const rows = table
    .filter((r) => r.some((v) => v !== ""))
    .map((r) => {
      const o: Row = {};
      header.forEach((h, i) => (o[h] = (r[i] ?? "").trim()));
      return o;
    });

  return { columns: header, rows };
}

export function toCSV(columns: string[], rows: Row[]): string {
  const esc = (v: string) => (/[",\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v);
  return [columns.join(","), ...rows.map((r) => columns.map((c) => esc(r[c] ?? "")).join(","))].join("\n");
}

/* --------------------------- type helpers -------------------------- */

const INT_RE = /^-?\d{1,15}$/;
const DEC_RE = /^-?\d*\.\d+$/;
const DATE_RE = /^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$/;
const BOOL_RE = /^(true|false)$/i;

export function isMissing(v: string | undefined) {
  return v === undefined || v === "" || v.toLowerCase() === "nan" || v.toLowerCase() === "null";
}

function detectType(values: string[]): ColumnType {
  let int = 0;
  let dec = 0;
  let date = 0;
  let bool = 0;
  let n = 0;
  for (const v of values) {
    if (isMissing(v)) continue;
    n++;
    if (DATE_RE.test(v)) date++;
    else if (INT_RE.test(v)) int++;
    else if (DEC_RE.test(v)) dec++;
    else if (BOOL_RE.test(v)) bool++;
  }
  if (!n) return "string";
  if (date / n > 0.8) return "date";
  if ((int + dec) / n > 0.85) return dec > n * 0.15 ? "decimal" : "integer";
  if (bool / n > 0.85) return "boolean";
  return "string";
}

function matchesType(v: string, t: ColumnType) {
  switch (t) {
    case "integer":
      return INT_RE.test(v);
    case "decimal":
      return INT_RE.test(v) || DEC_RE.test(v);
    case "date":
      return DATE_RE.test(v);
    case "boolean":
      return BOOL_RE.test(v);
    default:
      return true;
  }
}

export function numberOf(v: string, t: ColumnType): number {
  if (t === "date") return new Date(v.replace(" ", "T")).getTime();
  return Number(v);
}

/* --------------------------- formatting ---------------------------- */

export function compact(n: number): string {
  const a = Math.abs(n);
  if (a >= 1e9) return `${trim(n / 1e9)}b`;
  if (a >= 1e6) return `${trim(n / 1e6)}m`;
  if (a >= 1e4) return `${trim(n / 1e3)}k`;
  if (a >= 1e3) return n.toLocaleString("en-US");
  return trim(n);
}

function trim(n: number) {
  const r = Math.round(n * 100) / 100;
  return String(r);
}

export function pctLabel(p: number) {
  if (p === 0) return "0%";
  if (p > 0 && p < 1) return "<1%";
  if (p > 99 && p < 100) return ">99%";
  return `${Math.round(p)}%`;
}

export function axisLabel(v: number, t: ColumnType) {
  if (t === "date") {
    const d = new Date(v);
    return d.toLocaleDateString("en-US", { month: "short", year: "numeric" });
  }
  return compact(v);
}

/* ---------------------------- profiling ---------------------------- */

export function profileColumn(name: string, index: number, rows: Row[]): ColumnStats {
  const raw = rows.map((r) => r[name] ?? "");
  const type = detectType(raw);

  let missing = 0;
  let mismatched = 0;
  const present: string[] = [];
  for (const v of raw) {
    if (isMissing(v)) missing++;
    else if (!matchesType(v, type)) mismatched++;
    else present.push(v);
  }

  const freq = new Map<string, number>();
  for (const v of present) freq.set(v, (freq.get(v) ?? 0) + 1);
  const unique = freq.size;

  const stats: ColumnStats = {
    name,
    index,
    type,
    total: rows.length,
    valid: present.length,
    mismatched,
    missing,
    unique,
    buckets: [],
    categories: [],
    sample: present.slice(0, 6),
  };

  if (type === "integer" || type === "decimal" || type === "date") {
    const nums = present.map((v) => numberOf(v, type)).filter((n) => Number.isFinite(n));
    if (nums.length) {
      const min = Math.min(...nums);
      const max = Math.max(...nums);
      const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
      const std = Math.sqrt(nums.reduce((a, b) => a + (b - mean) ** 2, 0) / nums.length);
      stats.min = min;
      stats.max = max;
      stats.mean = mean;
      stats.std = std;

      const n = 20;
      const span = max - min || 1;
      const buckets: Bucket[] = Array.from({ length: n }, (_, i) => ({
        label: "",
        x0: min + (span * i) / n,
        x1: min + (span * (i + 1)) / n,
        count: 0,
      }));
      for (const v of nums) {
        let i = Math.floor(((v - min) / span) * n);
        if (i >= n) i = n - 1;
        if (i < 0) i = 0;
        buckets[i].count++;
      }
      buckets.forEach((b) => {
        b.label =
          type === "date"
            ? `${axisLabel(b.x0, type)}`
            : `${compact(Math.round(b.x0 * 100) / 100)} – ${compact(Math.round(b.x1 * 100) / 100)}`;
      });
      stats.buckets = buckets;
    }
  } else {
    const lens = present.map((v) => v.length);
    stats.minLen = lens.length ? Math.min(...lens) : 0;
    stats.maxLen = lens.length ? Math.max(...lens) : 0;
    const sorted = [...freq.entries()].sort((a, b) => b[1] - a[1]);
    const top = sorted.slice(0, 2).filter(([, c]) => c > 1 || sorted.length <= 3);
    const covered = top.reduce((a, [, c]) => a + c, 0);
    const rest = present.length - covered;
    const cats: CategorySlice[] = top.map(([label, count]) => ({
      label,
      count,
      pct: (count / rows.length) * 100,
    }));
    if (rest > 0)
      cats.push({
        label: `Other (${rest.toLocaleString("en-US")})`,
        count: rest,
        pct: (rest / rows.length) * 100,
        other: true,
      });
    stats.categories = cats;
    stats.mostCommon = cats[0];
  }

  return stats;
}

export function profileAll(columns: string[], rows: Row[]) {
  return columns.map((c, i) => profileColumn(c, i, rows));
}

export function bytes(n: number) {
  if (n > 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GB`;
  if (n > 1024 ** 2) return `${(n / 1024 ** 2).toFixed(2)} MB`;
  if (n > 1024) return `${(n / 1024).toFixed(2)} kB`;
  return `${n} B`;
}
