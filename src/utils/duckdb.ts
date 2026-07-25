import {
  getJsDelivrBundles,
  AsyncDuckDB,
  AsyncDuckDBConnection,
  ConsoleLogger,
} from '@duckdb/duckdb-wasm';
import { CUSTOMERS_CSV } from './sample-customers';
import type {
  DataPreviewResult,
  PipelineMetrics,
  PipelineNodeConfig,
  PreviewColumn,
} from '../types';

let db: AsyncDuckDB | null = null;
let conn: AsyncDuckDBConnection | null = null;
let stageSequence = 0;
const runtimeTables = new Set<string>();

const SOURCE_TABLE = 'raw_customers';

function quoteIdentifier(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

function toNumber(value: unknown): number {
  if (typeof value === 'bigint') return Number(value);
  if (typeof value === 'number') return value;
  return Number(value ?? 0);
}

function toSerializable(value: unknown): unknown {
  if (typeof value === 'bigint') return Number(value);
  if (value instanceof Date) return value.toISOString();
  return value;
}

function normalizeRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, toSerializable(value)])
  );
}

function safeFileName(name: string): string {
  const normalized = name.replace(/[^a-zA-Z0-9._-]/g, '_');
  return normalized.toLowerCase().endsWith('.csv') ? normalized : `${normalized}.csv`;
}

async function getConnection(): Promise<AsyncDuckDBConnection> {
  return initDuckDB();
}

export async function initDuckDB(): Promise<AsyncDuckDBConnection> {
  if (conn) return conn;

  const bundles = getJsDelivrBundles();
  const bundle = bundles.mvp;
  const logger = new ConsoleLogger();
  const worker = new Worker(bundle.mainWorker);

  db = new AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule);
  conn = await db.connect();
  return conn;
}

export async function loadCsvData(name: string, contents: string): Promise<DataPreviewResult> {
  const c = await getConnection();
  if (!db) throw new Error('DuckDB did not initialize.');

  const registeredName = `${Date.now()}-${safeFileName(name)}`;
  await db.registerFileText(registeredName, contents);
  await c.query(
    `CREATE OR REPLACE TABLE ${quoteIdentifier(SOURCE_TABLE)} AS
     SELECT * FROM read_csv_auto('${registeredName.replace(/'/g, "''")}', header = true)`
  );

  return getTablePreview(SOURCE_TABLE);
}

export async function loadSampleData(): Promise<DataPreviewResult> {
  return loadCsvData('sample-customers.csv', CUSTOMERS_CSV);
}

export async function resetRuntimeTables(): Promise<void> {
  if (runtimeTables.size === 0) {
    stageSequence = 0;
    return;
  }

  const c = await getConnection();
  for (const tableName of runtimeTables) {
    await c.query(`DROP TABLE IF EXISTS ${quoteIdentifier(tableName)}`);
  }
  runtimeTables.clear();
  stageSequence = 0;
}

async function getColumns(tableName: string): Promise<{ name: string; type: string }[]> {
  const c = await getConnection();
  const result = await c.query(`DESCRIBE SELECT * FROM ${quoteIdentifier(tableName)}`);
  return result.toArray().map((row) => ({
    name: String(row.column_name),
    type: String(row.column_type),
  }));
}

export async function getTablePreview(
  tableName: string,
  limit = 100
): Promise<DataPreviewResult> {
  const c = await getConnection();
  const safeLimit = Math.max(1, Math.min(500, Math.floor(limit)));
  const table = quoteIdentifier(tableName);
  const schema = await getColumns(tableName);
  const totalRowsResult = await c.query(`SELECT count(*) AS count FROM ${table}`);
  const totalRows = toNumber(totalRowsResult.toArray()[0]?.count);
  const rowsResult = await c.query(`SELECT * FROM ${table} LIMIT ${safeLimit}`);
  const rows = rowsResult.toArray().map((row) => normalizeRow(row));

  const columns: PreviewColumn[] = [];
  for (const column of schema) {
    const identifier = quoteIdentifier(column.name);
    const profile = await c.query(
      `SELECT
        count(*) - count(${identifier}) AS null_count,
        count(DISTINCT ${identifier}) AS distinct_count
       FROM ${table}`
    );
    const values = profile.toArray()[0];
    columns.push({
      ...column,
      nullCount: toNumber(values?.null_count),
      distinctCount: toNumber(values?.distinct_count),
    });
  }

  return { tableName, columns, rows, totalRows };
}

export interface NodeExecution {
  nodeType: string;
  tableName: string;
  sql: string;
  metrics: PipelineMetrics;
}

export async function runPipelineNode(
  nodeType: string,
  prevTable: string,
  config?: PipelineNodeConfig
): Promise<NodeExecution> {
  const c = await getConnection();
  const startTime = performance.now();
  const inputTable = quoteIdentifier(prevTable);
  const tableName = `stage_${String(++stageSequence).padStart(4, '0')}_${nodeType}`;
  const outputTable = quoteIdentifier(tableName);
  const availableColumns = await getColumns(prevTable);
  const fallbackColumn = availableColumns[0]?.name;
  const requestedColumn = config?.column || fallbackColumn;

  if (!requestedColumn || !availableColumns.some((column) => column.name === requestedColumn)) {
    throw new Error(
      `Column "${requestedColumn || 'unknown'}" does not exist in ${prevTable}.`
    );
  }

  const column = quoteIdentifier(requestedColumn);
  let sql = '';

  switch (nodeType) {
    case 'source':
      sql = `CREATE OR REPLACE TABLE ${outputTable} AS SELECT * FROM ${inputTable}`;
      break;
    case 'filter':
      sql = `CREATE OR REPLACE TABLE ${outputTable} AS
        SELECT * FROM ${inputTable}
        WHERE ${column} IS NOT NULL AND trim(CAST(${column} AS VARCHAR)) != ''`;
      break;
    case 'deduplicate': {
      const strategy = config?.strategy || 'Keep first';
      const nullHandling = config?.nullHandling || 'Ignore';
      const order = strategy === 'Keep last' ? 'DESC' : 'ASC';
      const nullFilter =
        nullHandling === 'Remove null rows'
          ? `WHERE ${column} IS NOT NULL AND trim(CAST(${column} AS VARCHAR)) != ''`
          : '';
      sql = `CREATE OR REPLACE TABLE ${outputTable} AS
        SELECT * EXCLUDE (_stillflow_row_number)
        FROM (
          SELECT *,
            row_number() OVER (
              PARTITION BY ${column}
              ORDER BY rowid ${order}
            ) AS _stillflow_row_number
          FROM ${inputTable}
          ${nullFilter}
        )
        WHERE _stillflow_row_number = 1`;
      break;
    }
    case 'normalize':
      sql = `CREATE OR REPLACE TABLE ${outputTable} AS
        SELECT * REPLACE (
          CASE
            WHEN ${column} IS NULL THEN ${column}
            ELSE trim(CAST(${column} AS VARCHAR))
          END AS ${column}
        )
        FROM ${inputTable}`;
      break;
    case 'export':
      sql = `CREATE OR REPLACE TABLE ${outputTable} AS SELECT * FROM ${inputTable}`;
      break;
    default:
      throw new Error(`Unknown node type: ${nodeType}`);
  }

  await c.query(sql);
  runtimeTables.add(tableName);

  const rowsInResult = await c.query(`SELECT count(*) AS count FROM ${inputTable}`);
  const rowsOutResult = await c.query(`SELECT count(*) AS count FROM ${outputTable}`);
  const rowsIn = toNumber(rowsInResult.toArray()[0]?.count);
  const rowsOut = toNumber(rowsOutResult.toArray()[0]?.count);
  const outputColumns = await getColumns(tableName);
  const nullTerms = outputColumns.map(
    ({ name }) => `sum(CASE WHEN ${quoteIdentifier(name)} IS NULL THEN 1 ELSE 0 END)`
  );
  const nullResult = await c.query(
    `SELECT ${nullTerms.length ? nullTerms.join(' + ') : '0'} AS count FROM ${outputTable}`
  );
  const nullCount = toNumber(nullResult.toArray()[0]?.count);
  const possibleValues = Math.max(1, rowsOut * Math.max(1, outputColumns.length));
  const missing = Math.round((nullCount / possibleValues) * 1000) / 10;
  const qualityScore = Math.max(0, Math.round(100 - (nullCount / possibleValues) * 100));
  const duplicates =
    nodeType === 'deduplicate' && rowsIn > 0
      ? Math.round(((rowsIn - rowsOut) / rowsIn) * 1000) / 10
      : 0;

  return {
    nodeType,
    tableName,
    sql,
    metrics: {
      rowsIn,
      rowsOut,
      duplicates,
      missing,
      nullColumns: nullCount,
      qualityScore,
      duration: Math.round((performance.now() - startTime) * 10) / 10,
      memory: Math.round((32 + (rowsOut * Math.max(1, outputColumns.length) * 16) / 1_048_576) * 10) / 10,
    },
  };
}

export function formatRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export interface FullPipelineResult {
  executions: {
    nodeId: string;
    nodeType: string;
    metrics: PipelineMetrics;
    tableName: string;
  }[];
  totalDuration: number;
  outputTable: string;
}

export async function runFullPipeline(
  nodes: { id: string; type: string; config?: PipelineNodeConfig }[],
  options?: {
    prevTable?: string;
    onStageStart?: (nodeId: string, index: number) => void;
    onStageComplete?: (nodeId: string, index: number, metrics: PipelineMetrics) => void;
  }
): Promise<FullPipelineResult> {
  await resetRuntimeTables();
  let prevTable = options?.prevTable ?? SOURCE_TABLE;
  const executions: FullPipelineResult['executions'] = [];
  const startedAt = performance.now();

  for (const [index, node] of nodes.entries()) {
    options?.onStageStart?.(node.id, index);
    const result = await runPipelineNode(node.type, prevTable, node.config);
    executions.push({
      nodeId: node.id,
      nodeType: node.type,
      metrics: result.metrics,
      tableName: result.tableName,
    });
    prevTable = result.tableName;
    options?.onStageComplete?.(node.id, index, result.metrics);
  }

  return {
    executions,
    totalDuration: Math.round(performance.now() - startedAt),
    outputTable: prevTable,
  };
}
