import {
  getJsDelivrBundles,
  selectBundle,
  AsyncDuckDB,
  AsyncDuckDBConnection,
  ConsoleLogger,
} from '@duckdb/duckdb-wasm';
import { CUSTOMERS_CSV } from './sample-customers';

let db: AsyncDuckDB | null = null;
let conn: AsyncDuckDBConnection | null = null;
let initPromise: Promise<AsyncDuckDBConnection> | null = null;
let sampleDataPromise: Promise<void> | null = null;
let stageSequence = 0;
const SAMPLE_FILE_NAME = 'sample-customers.csv';

export interface PipelineMetrics {
  rowsIn: number;
  rowsOut: number;
  duplicates: number;
  missing: number;
  nullColumns: number;
  qualityScore: number;
  duration: number;
  memory: number;
}

export async function initDuckDB(): Promise<AsyncDuckDBConnection> {
  if (conn) return conn;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    const bundle = await selectBundle(getJsDelivrBundles());
    if (!bundle.mainWorker) {
      throw new Error('DuckDB worker is unavailable for this browser');
    }

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts(${JSON.stringify(bundle.mainWorker)});`], {
        type: 'text/javascript',
      })
    );
    const worker = new Worker(workerUrl);
    const database = new AsyncDuckDB(new ConsoleLogger(), worker);

    try {
      await database.instantiate(bundle.mainModule, bundle.pthreadWorker);
      const connection = await database.connect();
      db = database;
      conn = connection;
      return connection;
    } catch (error) {
      worker.terminate();
      throw error;
    } finally {
      URL.revokeObjectURL(workerUrl);
    }
  })();

  try {
    return await initPromise;
  } catch (error) {
    initPromise = null;
    throw error;
  }
}

export async function loadSampleData(): Promise<void> {
  if (sampleDataPromise) return sampleDataPromise;

  sampleDataPromise = (async () => {
    const c = await initDuckDB();
    if (!db) throw new Error('DuckDB did not finish initializing');

    await db.registerFileText(SAMPLE_FILE_NAME, CUSTOMERS_CSV);
    await c.query(
      `CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM read_csv_auto('${SAMPLE_FILE_NAME}')`
    );
  })();

  try {
    await sampleDataPromise;
  } catch (error) {
    sampleDataPromise = null;
    throw error;
  }
}

function toNumber(value: unknown): number {
  if (typeof value === 'bigint') return Number(value);
  const converted = Number(value ?? 0);
  return Number.isFinite(converted) ? converted : 0;
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
  config?: Record<string, string>
): Promise<NodeExecution> {
  const c = await initDuckDB();
  const startTime = performance.now();

  const tableName = `stg_${nodeType}_${Date.now()}_${++stageSequence}`;
  let sql = '';

  switch (nodeType) {
    case 'source': {
      sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM raw_customers`;
      break;
    }
    case 'filter': {
      const col = config?.column || 'status';
      sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM ${prevTable} WHERE ${col} IS NOT NULL AND ${col} != ''`;
      break;
    }
    case 'deduplicate': {
      const col = config?.column || 'customer_id';
      const strat = config?.strategy || 'Keep first';
      const nullHandling = config?.nullHandling || 'Ignore';
      let baseWhere = '';
      if (nullHandling === 'Remove null rows') {
        baseWhere = ` WHERE ${col} IS NOT NULL AND ${col} != '' `;
      } else if (nullHandling === 'Treat as duplicate') {
        baseWhere = '';
      }
      if (strat === 'Merge records') {
        sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT
          ${col},
          string_agg(DISTINCT name, ' | ') AS name,
          string_agg(DISTINCT email, ' | ') AS email,
          max(amount) AS amount,
          max(category) AS category,
          max(status) AS status,
          min(created_at) AS created_at,
          max(margin_pct) AS margin_pct
        FROM (SELECT * FROM ${prevTable}${baseWhere}) t
        GROUP BY ${col}`;
      } else {
        const order = strat === 'Keep last' ? 'DESC' : 'ASC';
        sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT
          customer_id, name, email, amount, category, status, created_at, margin_pct
        FROM (
          SELECT *, row_number() OVER (PARTITION BY ${col} ORDER BY created_at ${order}) AS _rn
          FROM (SELECT * FROM ${prevTable}${baseWhere}) src
        ) WHERE _rn = 1`;
      }
      break;
    }
    case 'normalize': {
      const nullHandling = config?.nullHandling || 'Ignore';
      sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT
        customer_id,
        ${nullHandling === 'Remove null rows'
          ? `CASE WHEN name IS NULL OR trim(name) = '' THEN 'Unknown' ELSE trim(name) END`
          : `CASE WHEN trim(coalesce(name, '')) = '' THEN name ELSE trim(name) END`} AS name,
        CASE WHEN email IS NULL OR trim(email) = '' THEN email ELSE lower(trim(email)) END AS email,
        amount, category, status, created_at, margin_pct
      FROM ${prevTable}`;
      break;
    }
    case 'export': {
      sql = `CREATE OR REPLACE TABLE ${tableName} AS SELECT * FROM ${prevTable}`;
      break;
    }
    default:
      throw new Error(`Unknown node type: ${nodeType}`);
  }

  await c.query(sql);

  const rowsIn = toNumber(
    (await c.query(`SELECT count(*) AS cnt FROM ${prevTable}`)).toArray()[0]?.cnt
  );
  const rowsOut = toNumber(
    (await c.query(`SELECT count(*) AS cnt FROM ${tableName}`)).toArray()[0]?.cnt
  );
  const hasNull = toNumber(
    (
      await c.query(`
        SELECT count(*) AS cnt FROM ${tableName}
        WHERE name IS NULL OR email IS NULL OR amount IS NULL OR status IS NULL
      `)
    ).toArray()[0]?.cnt
  );
  const totalRows = rowsOut > 0 ? rowsOut : 1;
  const missing = Math.round((hasNull / totalRows) * 1000) / 10;
  const nullCols = (
    await c.query(`SELECT
      sum(CASE WHEN name IS NULL OR trim(name)='' THEN 1 ELSE 0 END) AS nn,
      sum(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS ne,
      sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS na,
      sum(CASE WHEN status IS NULL OR trim(status)='' THEN 1 ELSE 0 END) AS ns
    FROM ${tableName}`)
  ).toArray()[0];
  const nullCount =
    toNumber(nullCols?.nn) +
    toNumber(nullCols?.ne) +
    toNumber(nullCols?.na) +
    toNumber(nullCols?.ns);
  const qualityScore = Math.max(0, Math.min(100, Math.round(100 - (nullCount / (totalRows * 4)) * 100)));

  const endTime = performance.now();
  const duplicates = nodeType === 'deduplicate' && rowsIn > 0
    ? Math.round(((rowsIn - rowsOut) / rowsIn) * 1000) / 10
    : (rowsIn > 0 ? Math.round(((rowsIn - rowsOut) / rowsIn) * 1000) / 10 : 0);

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
      duration: Math.round((endTime - startTime) * 10) / 10,
      // Deterministic memory estimate (avoid Math.random) — scales with rowsOut
      memory: Math.round(96 + (rowsOut / 100) * 12),
    },
  };
}

export function formatRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return String(n);
}

export interface FullPipelineResult {
  executions: { nodeId: string; nodeType: string; metrics: PipelineMetrics; tableName: string }[];
  totalDuration: number;
}

/**
 * Run every node in a pipeline sequentially and return metrics for EVERY stage.
 * nodes: ordered list, latest config per node is sourced from `node.config` (caller controls ordering).
 */
export async function runFullPipeline(
  nodes: { id: string; type: string; config?: Record<string, string> }[],
  prevTableFallback = 'raw_customers',
  options?: {
    onStageStart?: (nodeId: string, index: number) => void;
    onStageComplete?: (nodeId: string, index: number, metrics: PipelineMetrics) => void;
  }
): Promise<FullPipelineResult> {
  let prevTable = prevTableFallback;
  const executions: FullPipelineResult['executions'] = [];
  const t0 = performance.now();
  for (const [index, n] of nodes.entries()) {
    options?.onStageStart?.(n.id, index);
    const result = await runPipelineNode(n.type, prevTable, n.config);
    executions.push({
      nodeId: n.id,
      nodeType: n.type,
      metrics: result.metrics,
      tableName: result.tableName,
    });
    options?.onStageComplete?.(n.id, index, result.metrics);
    prevTable = result.tableName;
  }
  return { executions, totalDuration: Math.round(performance.now() - t0) };
}
