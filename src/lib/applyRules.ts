/* ------------------------------------------------------------------ *
 *  Client-side rule engine — mirrors backend/src/pipeline.rs semantics
 *  so the preview can show truthful stage data, cell changes and
 *  rejected rows for the displayed sample WITHOUT a backend run.
 * ------------------------------------------------------------------ */

import { isMissing, type Row } from "./csv";
import type { PipelineNode } from "../types";

export interface CellChange {
  rowIndex: number;
  column: string;
  from: string;
  to: string;
}

export interface RejectedRow {
  rowIndex: number;
  row: Row;
  reason: string;
}

export interface NodeImpact {
  nodeId: string;
  nodeName: string;
  nodeType: PipelineNode["type"];
  rowsIn: number;
  rowsOut: number;
  rejected: RejectedRow[];
  changes: CellChange[];
}

export interface StageResult {
  rows: Row[];
  impacts: NodeImpact[];
}

const MAX_TRACKED = 500;

/* ------------------------------ filter ----------------------------- */

function filterMatches(
  raw: string,
  operator: string,
  value: string,
  nullHandling: string
): { matched: boolean; reason: string | null } {
  const cell = raw.trim();
  if (cell === "" || isMissing(cell)) {
    if (operator === "is empty") return { matched: true, reason: null };
    if (operator === "is not empty" || operator === "") {
      return { matched: false, reason: isMissing(cell) && cell !== "" ? "null value" : "empty value" };
    }
    const matched = nullHandling === "Treat as match";
    return { matched, reason: matched ? null : "empty value" };
  }
  const v = value.trim();
  switch (operator) {
    case "is empty":
      return { matched: false, reason: "not empty" };
    case "equals":
      return cell.toLowerCase() === v.toLowerCase()
        ? { matched: true, reason: null }
        : { matched: false, reason: "not equal" };
    case "not equals":
      return cell.toLowerCase() !== v.toLowerCase()
        ? { matched: true, reason: null }
        : { matched: false, reason: "equal" };
    case "contains":
      return cell.toLowerCase().includes(v.toLowerCase())
        ? { matched: true, reason: null }
        : { matched: false, reason: "missing substring" };
    case "not contains":
      return !cell.toLowerCase().includes(v.toLowerCase())
        ? { matched: true, reason: null }
        : { matched: false, reason: "contains substring" };
    case "greater than":
    case "less than": {
      const a = Number(cell);
      const b = Number(v);
      if (!Number.isFinite(a) || !Number.isFinite(b)) {
        return { matched: false, reason: "not a number" };
      }
      const ok = operator === "greater than" ? a > b : a < b;
      return ok
        ? { matched: true, reason: null }
        : { matched: false, reason: operator === "greater than" ? "below threshold" : "above threshold" };
    }
    default:
      return { matched: true, reason: null };
  }
}

function applyFilter(rows: Row[], node: PipelineNode, impact: NodeImpact): Row[] {
  const { column, nullHandling } = node.config;
  const operator = node.config.operator ?? "is not empty";
  const value = node.config.value ?? "";
  const removeMatching = node.config.mode === "Remove matching rows";
  const out: Row[] = [];
  rows.forEach((row, rowIndex) => {
    const { matched, reason } = filterMatches(row[column] ?? "", operator, value, nullHandling);
    const keep = removeMatching ? !matched : matched;
    if (keep) out.push(row);
    else if (impact.rejected.length < MAX_TRACKED) {
      impact.rejected.push({
        rowIndex,
        row,
        reason: reason ? `${reason} in ${column}` : `matched rule on ${column}`,
      });
    }
  });
  return out;
}

/* ---------------------------- deduplicate -------------------------- */

function applyDedup(rows: Row[], node: PipelineNode, impact: NodeImpact): Row[] {
  const { column, strategy, nullHandling } = node.config;
  const keepLast = strategy === "Keep last";
  const removeNulls = nullHandling === "Remove null rows";

  const keyOf = (row: Row) =>
    column.trim() === "" ? JSON.stringify(row) : (row[column] ?? "").trim();

  const working = keepLast ? [...rows].reverse() : rows;
  const seen = new Map<string, number>();
  const keptReversed: { row: Row; rowIndex: number }[] = [];

  working.forEach((row, i) => {
    const rowIndex = keepLast ? rows.length - 1 - i : i;
    const key = keyOf(row);
    if (removeNulls && column.trim() !== "" && key === "") {
      if (impact.rejected.length < MAX_TRACKED) {
        impact.rejected.push({ rowIndex, row, reason: `null value in ${column}` });
      }
      return;
    }
    const firstIndex = seen.get(key);
    if (firstIndex === undefined) {
      seen.set(key, rowIndex);
      keptReversed.push({ row, rowIndex });
    } else if (impact.rejected.length < MAX_TRACKED) {
      impact.rejected.push({
        rowIndex,
        row,
        reason: column.trim() === "" ? "identical row" : `duplicate of row ${firstIndex + 1}`,
      });
    }
  });

  const kept = keepLast ? keptReversed.reverse() : keptReversed;
  return kept.map((entry) => entry.row);
}

/* ----------------------------- normalize --------------------------- */

function applyNormalize(rows: Row[], node: PipelineNode, impact: NodeImpact): Row[] {
  const target = node.config.column.trim();
  return rows.map((row, rowIndex) => {
    let next: Row | null = null;
    for (const column of Object.keys(row)) {
      if (target !== "" && column !== target) continue;
      const raw = row[column] ?? "";
      let cleaned = raw.trim();
      if (cleaned.includes("@")) cleaned = cleaned.toLowerCase();
      if (cleaned !== raw) {
        next ??= { ...row };
        next[column] = cleaned;
        if (impact.changes.length < MAX_TRACKED) {
          impact.changes.push({ rowIndex, column, from: raw, to: cleaned });
        }
      }
    }
    return next ?? row;
  });
}

/* ------------------------------ chain ------------------------------ */

function emptyImpact(node: PipelineNode): NodeImpact {
  return {
    nodeId: node.id,
    nodeName: node.name,
    nodeType: node.type,
    rowsIn: 0,
    rowsOut: 0,
    rejected: [],
    changes: [],
  };
}

function applyOne(rows: Row[], node: PipelineNode, impact: NodeImpact): Row[] {
  if (node.status === "disabled") return rows;
  switch (node.type) {
    case "filter":
      return applyFilter(rows, node, impact);
    case "deduplicate":
      return applyDedup(rows, node, impact);
    case "normalize":
      return applyNormalize(rows, node, impact);
    default:
      return rows;
  }
}

/**
 * Applies the pipeline chain up to and including `throughNodeId`
 * (or the whole chain when omitted) against the given rows.
 */
export function applyChain(
  rows: Row[],
  nodes: PipelineNode[],
  throughNodeId?: string
): StageResult {
  const impacts: NodeImpact[] = [];
  let current = rows;
  for (const node of nodes) {
    const impact = emptyImpact(node);
    impact.rowsIn = current.length;
    current = applyOne(current, node, impact);
    impact.rowsOut = current.length;
    impacts.push(impact);
    if (throughNodeId && node.id === throughNodeId) break;
  }
  return { rows: current, impacts };
}

/** Rows removed by a node, grouped by reason — the Rejected summary. */
export function rejectSummary(
  impact: { rejected: RejectedRow[] } | null
): [string, number][] {
  if (!impact) return [];
  const groups = new Map<string, number>();
  for (const rejected of impact.rejected) {
    groups.set(rejected.reason, (groups.get(rejected.reason) ?? 0) + 1);
  }
  return [...groups.entries()].sort((a, b) => b[1] - a[1]);
}
