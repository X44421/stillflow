import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Header from './components/Header';
import IconSidebar from './components/IconSidebar';
import DatasetPanel from './components/DatasetPanel';
import PipelineCanvas from './components/PipelineCanvas';
import DataPreview from './components/DataPreview';
import ActivityPanel from './components/ActivityPanel';
import DetailPanel from './components/DetailPanel';
import { datasets as initialDatasets, initialPipelineNodes } from './data';
import {
  formatRows,
  getTablePreview,
  loadCsvData,
  loadSampleData,
  runFullPipeline,
} from './utils/duckdb';
import type {
  DataPreviewResult,
  Dataset,
  PipelineNode,
  PreviewLimit,
  WorkspaceEvent,
  WorkspaceView,
} from './types';

const WORKSPACE_STORAGE_KEY = 'stillflow.workspace.v1';

interface WorkspaceSnapshot {
  nodes: PipelineNode[];
  datasetItems: Dataset[];
  selectedDatasetId: string | null;
  activeSourceId: string;
  selectedNode: string;
  assetsVisible: boolean;
  events: WorkspaceEvent[];
  previewLimit: PreviewLimit;
}

function readWorkspaceSnapshot(): Partial<WorkspaceSnapshot> | null {
  if (typeof window === 'undefined') return null;

  try {
    const raw = window.localStorage.getItem(WORKSPACE_STORAGE_KEY);
    return raw ? JSON.parse(raw) as Partial<WorkspaceSnapshot> : null;
  } catch {
    return null;
  }
}

function inferEventSequence(events: WorkspaceEvent[]): number {
  return events.reduce((max, event) => {
    const value = Number(event.id.replace(/^evt-/, ''));
    return Number.isFinite(value) ? Math.max(max, value) : max;
  }, 0);
}

function resetRuntimeState(node: PipelineNode): PipelineNode {
  return {
    ...node,
    status: node.status === 'disabled' ? 'disabled' : 'pending',
    rows: node.type === 'source' ? node.rows : '',
    metrics: undefined,
    error: undefined,
  };
}

const App: React.FC = () => {
  const snapshot = useRef(readWorkspaceSnapshot()).current;
  const initialNodes = snapshot?.nodes?.length ? snapshot.nodes : initialPipelineNodes;
  const [nodes, setNodes] = useState<PipelineNode[]>(initialNodes);
  const [datasetItems, setDatasetItems] = useState<Dataset[]>(
    snapshot?.datasetItems?.length ? snapshot.datasetItems : initialDatasets
  );
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(
    snapshot?.selectedDatasetId ?? null
  );
  const [activeSourceId, setActiveSourceId] = useState<string>(
    snapshot?.activeSourceId ?? 'sample-customers'
  );
  const [selectedNode, setSelectedNode] = useState(
    initialNodes.some((node) => node.id === snapshot?.selectedNode)
      ? snapshot?.selectedNode ?? 'n3'
      : initialNodes[0]?.id ?? ''
  );
  const [showDetail, setShowDetail] = useState(true);
  const [assetsVisible, setAssetsVisible] = useState(snapshot?.assetsVisible ?? true);
  const [activeView, setActiveView] = useState<WorkspaceView>('graph');
  const [preview, setPreview] = useState<DataPreviewResult | null>(null);
  const [previewLabel, setPreviewLabel] = useState('Data result');
  const [previewLimit, setPreviewLimit] = useState<PreviewLimit>(
    snapshot?.previewLimit === 500 ? 500 : 100
  );
  const [events, setEvents] = useState<WorkspaceEvent[]>(snapshot?.events ?? []);
  const [nodeTables, setNodeTables] = useState<Record<string, string>>({});
  const [globalRunning, setGlobalRunning] = useState(false);
  const [globalProgress, setGlobalProgress] = useState(0);
  const [loadingData, setLoadingData] = useState(false);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileContents = useRef<Record<string, string>>({});
  const eventSequence = useRef(inferEventSequence(snapshot?.events ?? []));

  const appendEvent = useCallback(
    (
      objectId: string,
      objectName: string,
      action: string,
      detail: string,
      actor: WorkspaceEvent['actor'],
      level: WorkspaceEvent['level'] = 'info'
    ) => {
      const timestamp = new Intl.DateTimeFormat('en', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      }).format(new Date());
      setEvents((current) =>
        [
          {
            id: `evt-${++eventSequence.current}`,
            objectId,
            objectName,
            action,
            detail,
            actor,
            level,
            timestamp,
          },
          ...current,
        ].slice(0, 100)
      );
    },
    []
  );

  useEffect(() => {
    const snapshotToSave: WorkspaceSnapshot = {
      nodes,
      datasetItems,
      selectedDatasetId,
      activeSourceId,
      selectedNode,
      assetsVisible,
      events,
      previewLimit,
    };

    const timeout = window.setTimeout(() => {
      try {
        window.localStorage.setItem(WORKSPACE_STORAGE_KEY, JSON.stringify(snapshotToSave));
      } catch {
        // Persistence is helpful, but the workspace must keep running if storage is unavailable.
      }
    }, 150);

    return () => window.clearTimeout(timeout);
  }, [
    activeSourceId,
    assetsVisible,
    datasetItems,
    events,
    nodes,
    previewLimit,
    selectedDatasetId,
    selectedNode,
  ]);

  const clearArtifactsForNodes = useCallback(
    (nodeIds: string[]) => {
      if (nodeIds.length === 0) return;

      const idSet = new Set(nodeIds);
      const staleTables = nodeIds
        .map((nodeId) => nodeTables[nodeId])
        .filter((tableName): tableName is string => Boolean(tableName));
      const outputInvalidated = nodes.some(
        (node) => idSet.has(node.id) && node.type === 'export'
      );

      setNodeTables((current) =>
        Object.fromEntries(Object.entries(current).filter(([nodeId]) => !idSet.has(nodeId)))
      );

      if (outputInvalidated) {
        setDatasetItems((current) => current.filter((dataset) => dataset.id !== 'latest-output'));
        if (selectedDatasetId === 'latest-output') {
          setSelectedDatasetId(null);
        }
      }

      if (preview && staleTables.includes(preview.tableName)) {
        setPreview(null);
        setPreviewLabel('Data result');
      }
    },
    [nodeTables, nodes, preview, selectedDatasetId]
  );

  const loadSourceDataset = useCallback(
    async (datasetId: string, limit: PreviewLimit = previewLimit): Promise<DataPreviewResult> => {
      const dataset = datasetItems.find((item) => item.id === datasetId);
      const loaded =
        !dataset || dataset.source === 'sample'
          ? await loadSampleData()
          : await (async () => {
              const contents = fileContents.current[datasetId];
              if (!contents) throw new Error(`The local contents for ${dataset.name} are unavailable.`);
              return loadCsvData(dataset.name, contents);
            })();
      return limit === 100 ? loaded : getTablePreview(loaded.tableName, limit);
    },
    [datasetItems, previewLimit]
  );

  const handleSelectDataset = useCallback(
    async (dataset: Dataset) => {
      setSelectedDatasetId(dataset.id);
      setPreviewLabel(dataset.name);
      setActiveView('data');
      setLoadingData(true);
      setError(null);
      try {
        let nextPreview: DataPreviewResult;
        if (dataset.category === 'source') {
          nextPreview = await loadSourceDataset(dataset.id, previewLimit);
          setActiveSourceId(dataset.id);
          const firstColumn = nextPreview.columns[0]?.name ?? 'customer_id';
          setNodes((current) =>
            current.map((node) => ({
              ...node,
              ...(node.type === 'source'
                ? {
                    name: dataset.name,
                    description: `${dataset.type.toUpperCase()} source object`,
                    rows: formatRows(nextPreview.totalRows),
                    status: 'completed' as const,
                  }
                : {
                    status: node.status === 'disabled' ? node.status : ('pending' as const),
                    metrics: undefined,
                    error: undefined,
                  }),
              config: { ...node.config, column: firstColumn },
            }))
          );
        } else if (dataset.tableName) {
          nextPreview = await getTablePreview(dataset.tableName, previewLimit);
        } else {
          throw new Error(`${dataset.name} has no materialized runtime table.`);
        }
        setPreview(nextPreview);
        appendEvent(
          dataset.id,
          dataset.name,
          'Opened',
          `${nextPreview.totalRows.toLocaleString()} rows inspected`,
          'User'
        );
      } catch (caught) {
        const message = caught instanceof Error ? caught.message : 'Unable to open this dataset.';
        setError(message);
        appendEvent(dataset.id, dataset.name, 'Open failed', message, 'Engine', 'error');
      } finally {
        setLoadingData(false);
      }
    },
    [appendEvent, loadSourceDataset]
  );

  const handleImportCsv = useCallback(
    async (file: File) => {
      if (!file.name.toLowerCase().endsWith('.csv')) {
        setError('StillFlow currently accepts CSV files for local deterministic execution.');
        return;
      }
      if (file.size > 50 * 1024 * 1024) {
        setError('This browser milestone accepts CSV files up to 50 MB.');
        return;
      }

      setImporting(true);
      setLoadingData(true);
      setError(null);
      try {
        const contents = await file.text();
        const loadedPreview = await loadCsvData(file.name, contents);
        const nextPreview =
          previewLimit === 100
            ? loadedPreview
            : await getTablePreview(loadedPreview.tableName, previewLimit);
        const id = `local-${Date.now()}`;
        const dataset: Dataset = {
          id,
          name: file.name,
          type: 'csv',
          category: 'source',
          size: `${nextPreview.totalRows.toLocaleString()} rows`,
          source: 'local',
          tableName: nextPreview.tableName,
        };
        fileContents.current[id] = contents;
        setDatasetItems((current) => [
          dataset,
          ...current.filter((item) => item.name !== file.name),
        ]);
        setSelectedDatasetId(id);
        setActiveSourceId(id);
        setPreview(nextPreview);
        setPreviewLabel(file.name);
        setActiveView('data');

        const firstColumn = nextPreview.columns[0]?.name ?? 'customer_id';
        setNodes((current) =>
          current.map((node) => ({
            ...node,
            ...(node.type === 'source'
              ? {
                  name: file.name,
                  description: 'Local CSV source object',
                  rows: formatRows(nextPreview.totalRows),
                  status: 'completed' as const,
                }
              : {
                  status: node.status === 'disabled' ? node.status : ('pending' as const),
                  metrics: undefined,
                  error: undefined,
                }),
            config: { ...node.config, column: firstColumn },
          }))
        );
        appendEvent(
          id,
          file.name,
          'Imported',
          `${nextPreview.totalRows.toLocaleString()} rows registered in DuckDB`,
          'User',
          'success'
        );
      } catch (caught) {
        const message = caught instanceof Error ? caught.message : 'CSV import failed.';
        setError(message);
        appendEvent('workspace', file.name, 'Import failed', message, 'Engine', 'error');
      } finally {
        setImporting(false);
        setLoadingData(false);
      }
    },
    [appendEvent, previewLimit]
  );

  const runNodes = useCallback(
    async (targetNodeId?: string) => {
      if (globalRunning) return;
      const targetIndex = targetNodeId
        ? nodes.findIndex((node) => node.id === targetNodeId)
        : nodes.length - 1;
      if (targetIndex < 0) return;

      const chain = nodes.slice(0, targetIndex + 1);
      const executable = chain.filter((node) => node.status !== 'disabled');
      if (executable.length === 0) {
        setError('There are no enabled objects to run.');
        return;
      }

      let runningNodeId = executable[0].id;
      setGlobalRunning(true);
      setGlobalProgress(2);
      setError(null);
      setNodes((current) =>
        current.map((node) =>
          executable.some((item) => item.id === node.id)
            ? { ...node, status: 'pending', error: undefined }
            : node
        )
      );
      setNodeTables({});
      setDatasetItems((current) => current.filter((dataset) => dataset.id !== 'latest-output'));

      try {
        await loadSourceDataset(activeSourceId, previewLimit);
        setGlobalProgress(10);
        const result = await runFullPipeline(
          executable.map((node) => ({
            id: node.id,
            type: node.type,
            config: node.config,
          })),
          {
            onStageStart: (nodeId, index) => {
              runningNodeId = nodeId;
              setGlobalProgress(10 + Math.round((index / executable.length) * 75));
              setNodes((current) =>
                current.map((node) =>
                  node.id === nodeId ? { ...node, status: 'running', error: undefined } : node
                )
              );
            },
            onStageComplete: (nodeId, index, metrics) => {
              const object = executable.find((node) => node.id === nodeId);
              setGlobalProgress(10 + Math.round(((index + 1) / executable.length) * 75));
              setNodes((current) =>
                current.map((node) =>
                  node.id === nodeId
                    ? {
                        ...node,
                        status: 'completed',
                        rows: formatRows(metrics.rowsOut),
                        metrics,
                        error: undefined,
                      }
                    : node
                )
              );
              appendEvent(
                nodeId,
                object?.name ?? nodeId,
                'Executed',
                `${metrics.rowsIn.toLocaleString()} → ${metrics.rowsOut.toLocaleString()} rows`,
                'Engine',
                'success'
              );
            },
          }
        );

        const tables = Object.fromEntries(
          result.executions.map((execution) => [execution.nodeId, execution.tableName])
        );
        setNodeTables((current) => ({ ...current, ...tables }));
        const nextPreview = await getTablePreview(result.outputTable, previewLimit);
        setPreview(nextPreview);
        setPreviewLabel(`${executable.at(-1)?.name ?? 'Result'} output`);
        setGlobalProgress(100);
        setActiveView('data');

        const outputDataset: Dataset = {
          id: 'latest-output',
          name: 'clean_customers',
          type: 'table',
          category: 'output',
          size: `${nextPreview.totalRows.toLocaleString()} rows`,
          source: 'connected',
          tableName: result.outputTable,
        };
        setDatasetItems((current) => [
          ...current.filter((dataset) => dataset.id !== outputDataset.id),
          outputDataset,
        ]);
        setSelectedDatasetId(outputDataset.id);
        appendEvent(
          'latest-output',
          outputDataset.name,
          'Materialized',
          `${nextPreview.totalRows.toLocaleString()} rows in ${result.totalDuration} ms`,
          'Engine',
          'success'
        );
      } catch (caught) {
        const message = caught instanceof Error ? caught.message : 'Pipeline execution failed.';
        setError(message);
        const failedNode = nodes.find((node) => node.id === runningNodeId);
        setNodes((current) =>
          current.map((node) =>
            node.id === runningNodeId ? { ...node, status: 'failed', error: message } : node
          )
        );
        setSelectedNode(runningNodeId);
        setShowDetail(true);
        setActiveView('graph');
        appendEvent(
          runningNodeId,
          failedNode?.name ?? runningNodeId,
          'Execution failed',
          message,
          'Engine',
          'error'
        );
      } finally {
        setGlobalRunning(false);
        window.setTimeout(() => setGlobalProgress(0), 700);
      }
    },
    [activeSourceId, appendEvent, globalRunning, loadSourceDataset, nodes]
  );

  const handleUpdateNode = useCallback(
    (nodeId: string, patch: Partial<PipelineNode>) => {
      const index = nodes.findIndex((item) => item.id === nodeId);
      const node = nodes.find((item) => item.id === nodeId);
      const shouldInvalidate = Boolean(patch.config || patch.status);
      const affectedIds = shouldInvalidate && index >= 0
        ? nodes.slice(index).map((item) => item.id)
        : [nodeId];
      setNodes((current) =>
        current.map((item) => {
          if (item.id === nodeId) {
            return shouldInvalidate ? { ...resetRuntimeState(item), ...patch } : { ...item, ...patch };
          }
          if (shouldInvalidate && affectedIds.includes(item.id)) return resetRuntimeState(item);
          return item;
        })
      );
      if (shouldInvalidate) {
        clearArtifactsForNodes(affectedIds);
      }
      appendEvent(
        nodeId,
        node?.name ?? nodeId,
        patch.status === 'disabled' ? 'Disabled' : patch.status === 'pending' ? 'Configured' : 'Updated',
        patch.config ? `Column set to ${patch.config.column}` : 'Object state changed',
        'User'
      );
    },
    [appendEvent, clearArtifactsForNodes, nodes]
  );

  const handleDeleteNode = useCallback(
    (nodeId: string) => {
      const index = nodes.findIndex((node) => node.id === nodeId);
      const node = nodes[index];
      if (!node || node.type === 'source') {
        setError('The active source object is required by this mission.');
        return;
      }
      const nextSelection = nodes[index - 1]?.id ?? nodes[index + 1]?.id ?? '';
      const downstreamIds = nodes.slice(index).map((item) => item.id);
      const remainingDownstreamIds = nodes.slice(index + 1).map((item) => item.id);
      setNodes((current) =>
        current
          .filter((item) => item.id !== nodeId)
          .map((item) =>
            remainingDownstreamIds.includes(item.id) ? resetRuntimeState(item) : item
          )
      );
      setSelectedNode(nextSelection);
      clearArtifactsForNodes(downstreamIds);
      appendEvent(nodeId, node.name, 'Deleted', 'Removed from mission context', 'User');
    },
    [appendEvent, clearArtifactsForNodes, nodes]
  );

  const handleDuplicateNode = useCallback(
    (nodeId: string) => {
      const index = nodes.findIndex((node) => node.id === nodeId);
      const node = nodes[index];
      if (!node) return;
      if (node.type === 'source') {
        setError('The active source object is unique in this mission.');
        return;
      }
      const duplicate: PipelineNode = {
        ...node,
        id: `n${Date.now()}`,
        name: `${node.name} copy`,
        status: 'pending',
        metrics: undefined,
        error: undefined,
        config: { ...node.config },
      };
      const downstreamIds = nodes.slice(index + 1).map((item) => item.id);
      setNodes((current) => [
        ...current.slice(0, index + 1),
        duplicate,
        ...current.slice(index + 1).map(resetRuntimeState),
      ]);
      clearArtifactsForNodes(downstreamIds);
      setSelectedNode(duplicate.id);
      appendEvent(
        duplicate.id,
        duplicate.name,
        'Created',
        `Duplicated from ${node.name}`,
        'User'
      );
    },
    [appendEvent, clearArtifactsForNodes, nodes]
  );

  const handleAddNode = useCallback(
    (node: PipelineNode) => {
      const selectedIndex = nodes.findIndex((item) => item.id === selectedNode);
      const insertIndex = selectedIndex >= 0 ? selectedIndex + 1 : nodes.length;
      const downstreamIds = nodes.slice(insertIndex).map((item) => item.id);
      setNodes((current) => [
        ...current.slice(0, insertIndex),
        node,
        ...current.slice(insertIndex).map(resetRuntimeState),
      ]);
      clearArtifactsForNodes(downstreamIds);
      setSelectedNode(node.id);
      setShowDetail(true);
      appendEvent(node.id, node.name, 'Created', 'Added from Capability Library', 'User');
    },
    [appendEvent, clearArtifactsForNodes, nodes, selectedNode]
  );

  const handlePreviewNode = useCallback(async () => {
    const tableName = nodeTables[selectedNode];
    const node = nodes.find((item) => item.id === selectedNode);
    if (!tableName || !node) return;
    setLoadingData(true);
    try {
      setPreview(await getTablePreview(tableName, previewLimit));
      setPreviewLabel(`${node.name} output`);
      setActiveView('data');
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : 'Unable to preview this object.');
    } finally {
      setLoadingData(false);
    }
  }, [nodeTables, nodes, previewLimit, selectedNode]);

  const handlePreviewLimitChange = useCallback(
    async (limit: PreviewLimit) => {
      setPreviewLimit(limit);
      if (!preview) return;

      setLoadingData(true);
      try {
        setPreview(await getTablePreview(preview.tableName, limit));
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : 'Unable to reload this preview.');
      } finally {
        setLoadingData(false);
      }
    },
    [preview]
  );

  const handleSearch = useCallback(
    (query: string) => {
      const normalized = query.trim().toLowerCase();
      if (!normalized) return;
      const match = nodes.find(
        (node) =>
          node.name.toLowerCase().includes(normalized) ||
          node.description.toLowerCase().includes(normalized)
      );
      if (match) {
        setSelectedNode(match.id);
        setShowDetail(true);
        setActiveView('graph');
      }
    },
    [nodes]
  );

  const selected = nodes.find((node) => node.id === selectedNode) ?? null;
  const availableColumns = useMemo(
    () => preview?.columns.map((column) => column.name) ?? [],
    [preview]
  );
  const statusLabel = error
    ? 'Needs attention'
    : globalRunning
      ? 'Running'
      : nodes.every((node) => node.status === 'completed' || node.status === 'disabled')
        ? 'Ready'
        : 'Configured';

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-white">
      <Header
        running={globalRunning}
        progress={globalProgress}
        onRunAll={() => void runNodes()}
        onSearch={handleSearch}
        savedLabel={`${events.length} events recorded`}
        statusLabel={statusLabel}
        error={error}
      />
      <div className="flex flex-1 min-h-0 overflow-hidden">
        <IconSidebar
          activeView={activeView}
          assetsVisible={assetsVisible}
          onViewChange={setActiveView}
          onToggleAssets={() => setAssetsVisible((visible) => !visible)}
        />
        {assetsVisible && (
          <DatasetPanel
            datasets={datasetItems}
            selectedId={selectedDatasetId}
            importing={importing}
            onSelectDataset={(dataset) => void handleSelectDataset(dataset)}
            onImportCsv={handleImportCsv}
          />
        )}
        <main className="flex-1 min-w-0 flex flex-col bg-white">
          <div className="h-9 flex-shrink-0 px-3 border-b border-gray-200 flex items-center justify-between bg-white">
            <div className="flex items-center gap-1">
              {(['graph', 'data'] as const).map((view) => (
                <button
                  key={view}
                  onClick={() => setActiveView(view)}
                  className={`h-7 px-2.5 rounded text-[11px] font-medium capitalize ${
                    activeView === view
                      ? 'bg-gray-900 text-white'
                      : 'text-gray-500 hover:bg-gray-100'
                  }`}
                >
                  {view === 'graph' ? 'Object Graph' : 'Data Preview'}
                </button>
              ))}
            </div>
            <div className="min-w-0 truncate text-[10px] text-gray-400">
              {activeView === 'graph' ? 'Current mission context' : previewLabel}
            </div>
          </div>
          <div className="flex-1 min-h-0 flex flex-col">
            {activeView === 'graph' ? (
              <PipelineCanvas
                nodes={nodes}
                selectedNode={selectedNode}
                running={globalRunning}
                onRunAll={() => void runNodes()}
                onSelectNode={(nodeId) => {
                  setSelectedNode(nodeId);
                  setShowDetail(true);
                }}
                onAddNode={handleAddNode}
                onDeleteNode={handleDeleteNode}
              />
            ) : (
              <DataPreview
                preview={preview}
                loading={loadingData}
                label={previewLabel}
                limit={previewLimit}
                onLimitChange={handlePreviewLimitChange}
              />
            )}
            <ActivityPanel events={events} />
          </div>
        </main>
        {showDetail && selected && (
          <DetailPanel
            node={selected}
            nodes={nodes}
            events={events}
            availableColumns={availableColumns}
            onClose={() => setShowDetail(false)}
            onDelete={handleDeleteNode}
            onDuplicate={handleDuplicateNode}
            onRun={runNodes}
            onPreview={() => void handlePreviewNode()}
            onUpdate={handleUpdateNode}
          />
        )}
      </div>
    </div>
  );
};

export default App;
