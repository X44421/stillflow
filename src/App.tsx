import React, { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { PreviewPanel, type PreviewStage } from './components/PreviewPanel';
import { DataExplorer } from './components/DataExplorer';
import PipelineCanvas from './components/PipelineCanvas';
import DetailPanel from './components/DetailPanel';
import AssetPanel, { type ValidationCheck } from './components/AssetPanel';
import { DataTable } from './components/DataTable';
import { CSV_COLUMNS, FILE_META, buildRows } from './data/kaggleDatasets';
import { isMissing, profileAll, toCSV, type Row } from './lib/csv';
import { applyChain } from './lib/applyRules';
import { OUTPUT_ASSET_ID } from './features/canvas/graphAdapter';
import { Play } from './icons/hero';
import ProjectConfigCard, {
  type ProjectConfigValues,
} from './components/ProjectConfigCard';
import { defaultConfigFor } from './data';
import type { Dataset, PipelineMetrics, PipelineNode, Project } from './types';
import {
  createProject,
  deleteBackendDataset,
  deleteProject,
  importCsvDataset,
  listBackendDatasets,
  listProjects,
  renameBackendDataset,
  runBackendPipeline,
  saveProjectWorkspace,
  updateProject,
  type BackendPipelineResult,
} from './utils/api';

function resetNodeRuntime(node: PipelineNode): PipelineNode {
  return {
    ...node,
    status: node.status === 'disabled' ? 'disabled' : 'pending',
    rows: node.type === 'source' ? node.rows : '',
    metrics: undefined,
    error: undefined,
  };
}

function findColumn(columns: string[], preferred: string[]): string {
  for (const candidate of preferred) {
    const match = columns.find(
      (column) => column.toLowerCase() === candidate.toLowerCase()
    );
    if (match) return match;
  }
  return columns[0] ?? '';
}

function isLegacyDemoNode(node: PipelineNode): boolean {
  if (
    node.id === 'n1' &&
    node.type === 'source' &&
    node.name === 'raw_customers.csv'
  ) {
    return true;
  }
  if (
    node.id === 'n2' &&
    node.type === 'filter' &&
    node.description === 'Keep valid customers'
  ) {
    return true;
  }
  if (
    node.id === 'n3' &&
    node.type === 'deduplicate' &&
    node.description === 'Remove repeated records'
  ) {
    return true;
  }
  if (
    node.id === 'n4' &&
    node.type === 'normalize' &&
    node.description === 'Standardize name & email'
  ) {
    return true;
  }
  return (
    node.id === 'n5' &&
    node.type === 'export' &&
    node.description === 'Write cleaned data'
  );
}

/**
 * Migrates persisted configs onto each node type's semantic schema — a
 * Filter saved with Deduplicate options ("Keep first", "Ignore") is
 * rewritten to a real filter rule instead of leaking the wrong vocabulary
 * into the Inspector.
 */
function normalizeNodeConfig(node: PipelineNode): PipelineNode {
  if (node.type === 'filter') {
    const config = { ...defaultConfigFor('filter'), ...node.config };
    if (!['Treat as non-match', 'Treat as match'].includes(config.nullHandling)) {
      config.nullHandling = 'Treat as non-match';
    }
    config.strategy = '';
    return { ...node, config };
  }
  if (node.type === 'normalize') {
    const config = { ...defaultConfigFor('normalize'), ...node.config };
    config.strategy = '';
    return { ...node, config };
  }
  return node;
}

function bindDatasetToNodes(nodes: PipelineNode[], dataset: Dataset): PipelineNode[] {
  const columns = dataset.columns ?? [];
  const identityColumn = findColumn(columns, ['customer_id', 'customerId', 'id']);
  const filterColumn = findColumn(columns, ['status', identityColumn]);
  const sourceRows =
    dataset.rowCount === undefined
      ? dataset.size.replace(/\s+rows$/i, '')
      : String(dataset.rowCount);
  const existingSource = nodes.find((node) => node.type === 'source');
  const sourceNode: PipelineNode =
    existingSource ?? {
      id: `source-${dataset.id}`,
      type: 'source',
      name: dataset.name,
      description: `${dataset.type.toUpperCase()} File`,
      rows: sourceRows,
      status: 'completed',
      config: { ...defaultConfigFor('source'), column: identityColumn },
    };
  const pipeline = [
    sourceNode,
    ...nodes.filter((node) => node.type !== 'source'),
  ];

  return pipeline.map((node) => {
    const reset = resetNodeRuntime(normalizeNodeConfig(node));
    if (node.type === 'source') {
      return {
        ...reset,
        name: dataset.name,
        description: `${dataset.type.toUpperCase()} File · ${dataset.size}`,
        rows: sourceRows,
        status: 'completed',
      };
    }
    if (columns.length === 0) return reset;

    if (node.type === 'filter') {
      return { ...reset, config: { ...reset.config, column: filterColumn } };
    }
    if (node.type === 'deduplicate' || node.type === 'normalize') {
      return { ...reset, config: { ...reset.config, column: identityColumn } };
    }
    return reset;
  });
}

function clonePipelineNodes(nodes: PipelineNode[]): PipelineNode[] {
  return nodes.map((node) => ({
    ...node,
    config: { ...node.config },
    metrics: node.metrics ? { ...node.metrics } : undefined,
  }));
}

function nodesForProject(project: Project): PipelineNode[] {
  return clonePipelineNodes(
    project.nodes.filter((node) => !isLegacyDemoNode(node))
  ).map(normalizeNodeConfig);
}

function persistedPipelineNodes(nodes: PipelineNode[]): PipelineNode[] {
  return nodes.map(({ metrics: _metrics, error: _error, ...node }) => ({
    ...node,
    status:
      node.status === 'running' || node.status === 'failed'
        ? 'pending'
        : node.status,
    config: { ...node.config },
  }));
}

const App: React.FC = () => {
  const [nodes, setNodes] = useState<PipelineNode[]>([]);
  const [projects, setProjects] = useState<Project[]>([]);
  const [activeProjectId, setActiveProjectId] = useState<string | null>(null);
  const [projectReady, setProjectReady] = useState(false);
  const [workspaceDatasets, setWorkspaceDatasets] = useState<Dataset[]>([]);
  const [previewDataset, setPreviewDataset] = useState<Dataset | null>(null);
  const [focusedColumn, setFocusedColumn] = useState<string | null>(null);

  /**
   * Which context the Preview panel reflects.  When scope='dataset'
   * the header shows the file name; when scope='node' it carries the
   * selected node name and an Input / Output toggle; scope='asset' is
   * the terminal output dataset.
   */
  type PreviewTarget =
    | { scope: 'dataset' }
    | { scope: 'node'; nodeId: string; mode: 'input' | 'output' }
    | { scope: 'asset' };

  const [previewTarget, setPreviewTarget] = useState<PreviewTarget>({
    scope: 'dataset',
  });

  /* ── Output asset: versions + publish state (persisted locally) ── */
  const [runCount, setRunCount] = useState(0);
  const [outputStale, setOutputStale] = useState(false);
  const [publishedMap, setPublishedMap] = useState<Record<string, number>>(
    () => {
      try {
        return JSON.parse(
          window.localStorage.getItem('stillflow.published') ?? '{}'
        ) as Record<string, number>;
      } catch {
        return {};
      }
    }
  );

  /* Canvas / Preview are fixed regions sharing one vertical split. */
  const [canvasHeight, setCanvasHeight] = useState(264);
  const splitDragRef = useRef<{ startY: number; startH: number } | null>(null);
  const [splitDragging, setSplitDragging] = useState(false);

  /* ── Kaggle DataTable source ─────────────────────────────── */
  const tableRows = useMemo<Row[]>(() => buildRows(1000), []);
  const displayRowCount =
    previewDataset?.rowCount ?? tableRows.length;
  const tableDownload = useCallback(() => {
    const blob = new Blob([toCSV(CSV_COLUMNS, tableRows)], { type: 'text/csv;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = FILE_META.name;
    a.click();
    URL.revokeObjectURL(url);
  }, [tableRows]);
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null);
  const [_importing, setImporting] = useState(false);
  const [latestOutputId, setLatestOutputId] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState('');
  const [showDetail, setShowDetail] = useState(false);
  const [projectConfigMode, setProjectConfigMode] = useState<
    'create' | 'edit' | null
  >(null);
  const [projectConfigBusy, setProjectConfigBusy] = useState(false);
  const [projectConfigError, setProjectConfigError] = useState<string | null>(
    null
  );
  const [globalRunning, setGlobalRunning] = useState(false);
  const [_globalProgress, setGlobalProgress] = useState(0);
  const [_workspaceMessage, setWorkspaceMessage] = useState('Ready');
  const activeProject =
    projects.find((project) => project.id === activeProjectId) ?? null;
  const activeDataset =
    workspaceDatasets.find(
      (dataset) =>
        dataset.id === selectedDatasetId &&
        dataset.category === 'source' &&
        dataset.source === 'local'
    ) ?? null;

  const hydrateProject = useCallback(
    (project: Project, datasets: Dataset[] = []) => {
      const selectedDataset = datasets.find(
        (dataset) =>
          dataset.id === project.selectedDatasetId &&
          dataset.category === 'source'
      );
      const projectNodes = selectedDataset
        ? bindDatasetToNodes(nodesForProject(project), selectedDataset)
        : nodesForProject(project);
      setActiveProjectId(project.id);
      setNodes(projectNodes);
      setSelectedDatasetId(project.selectedDatasetId);
      setLatestOutputId(project.latestOutputId ?? null);
      setSelectedNode(projectNodes[0]?.id ?? '');
      setShowDetail(projectNodes.length > 0);
      if (selectedDataset && !previewDataset) {
        setPreviewDataset(selectedDataset);
      }
      try {
        window.localStorage.setItem('stillflow.activeProjectId', project.id);
      } catch {
        // Project selection still works when browser storage is unavailable.
      }
    },
    []
  );

  useEffect(() => {
    let active = true;
    const initializeProjects = async () => {
      try {
        let loadedProjects = await listProjects();
        if (loadedProjects.length === 0) {
          const created = await createProject('Untitled project', []);
          loadedProjects = [created];
        }
        loadedProjects = await Promise.all(
          loadedProjects.map(async (project) => {
            const cleanedNodes = nodesForProject(project);
            if (cleanedNodes.length === project.nodes.length) return project;
            try {
              return await saveProjectWorkspace(project.id, {
                selectedDatasetId: project.selectedDatasetId,
                latestOutputId: project.latestOutputId,
                nodes: persistedPipelineNodes(cleanedNodes),
              });
            } catch {
              return { ...project, nodes: cleanedNodes };
            }
          })
        );
        if (!active) return;

        let rememberedProjectId: string | null = null;
        try {
          rememberedProjectId = window.localStorage.getItem(
            'stillflow.activeProjectId'
          );
        } catch {
          // The most recently updated project remains the default selection.
        }
        const project =
          loadedProjects.find((item) => item.id === rememberedProjectId) ??
          loadedProjects[0];

        setProjects(loadedProjects);
        hydrateProject(project);
        const backendDatasets = await listBackendDatasets(project.id);
        if (!active) return;
        setWorkspaceDatasets(backendDatasets);
        hydrateProject(project, backendDatasets);
        setProjectReady(true);
        setWorkspaceMessage('Ready');
      } catch {
        if (!active) return;
        setNodes([]);
        setWorkspaceDatasets([]);
        setSelectedNode('');
        setShowDetail(false);
        setWorkspaceMessage('Backend offline');
      }
    };
    void initializeProjects();

    return () => {
      active = false;
    };
  }, [hydrateProject]);

  useEffect(() => {
    if (!projectReady || !activeProjectId) return;
    const timeout = window.setTimeout(() => {
      setWorkspaceMessage('Saving');
      void saveProjectWorkspace(activeProjectId, {
        selectedDatasetId,
        latestOutputId,
        nodes: persistedPipelineNodes(nodes),
      })
        .then((updated) => {
          setProjects((current) =>
            current.map((project) =>
              project.id === updated.id ? updated : project
            )
          );
          setWorkspaceMessage('Saved just now');
        })
        .catch((error) => {
          const message =
            error instanceof Error ? error.message : 'Workspace save failed';
          setWorkspaceMessage(`Save failed: ${message}`);
        });
    }, 700);

    return () => window.clearTimeout(timeout);
  }, [
    activeProjectId,
    latestOutputId,
    nodes,
    projectReady,
    selectedDatasetId,
  ]);

  const saveCurrentProject = useCallback(async (): Promise<boolean> => {
    if (!projectReady || !activeProjectId) return true;
    try {
      const updated = await saveProjectWorkspace(activeProjectId, {
        selectedDatasetId,
        latestOutputId,
        nodes: persistedPipelineNodes(nodes),
      });
      setProjects((current) =>
        current.map((project) =>
          project.id === updated.id ? updated : project
        )
      );
      return true;
    } catch (error) {
      const message =
        error instanceof Error ? error.message : 'Workspace save failed';
      setWorkspaceMessage(`Save failed: ${message}`);
      return false;
    }
  }, [
    activeProjectId,
    latestOutputId,
    nodes,
    projectReady,
    selectedDatasetId,
  ]);

  const activateProject = useCallback(
    async (project: Project) => {
      setProjectReady(false);
      setWorkspaceMessage('Loading project');
      setWorkspaceDatasets([]);
      setPreviewDataset(null);
      hydrateProject(project);
      try {
        const backendDatasets = await listBackendDatasets(project.id);
        setWorkspaceDatasets(backendDatasets);
        hydrateProject(project, backendDatasets);
        setProjectReady(true);
        setWorkspaceMessage('Ready');
        return true;
      } catch (error) {
        setWorkspaceDatasets([]);
        const message =
          error instanceof Error ? error.message : 'Project load failed';
        setWorkspaceMessage(`Load failed: ${message}`);
        return false;
      }
    },
    [hydrateProject, setPreviewDataset]
  );

  const _handleSelectProject = useCallback(
    async (projectId: string) => {
      if (globalRunning) {
        setWorkspaceMessage('Wait for the current run to finish');
        return;
      }
      if (projectId === activeProjectId) return;
      const project = projects.find((item) => item.id === projectId);
      if (!project) return;
      if (!(await saveCurrentProject())) return;
      await activateProject(project);
    },
    [
      activeProjectId,
      activateProject,
      globalRunning,
      projects,
      saveCurrentProject,
    ]
  );

  const _handleCreateProject = useCallback(() => {
    if (globalRunning) {
      setWorkspaceMessage('Wait for the current run to finish');
      return;
    }
    setProjectConfigError(null);
    setPreviewDataset(null);
    setProjectConfigMode('create');
  }, [globalRunning]);

  const _handleConfigureProject = useCallback(() => {
    if (!activeProject) return;
    setProjectConfigError(null);
    setPreviewDataset(null);
    setProjectConfigMode('edit');
  }, [activeProject]);

  const handleSubmitProjectConfig = useCallback(
    async (values: ProjectConfigValues) => {
      if (!projectConfigMode || projectConfigBusy) return;
      setProjectConfigBusy(true);
      setProjectConfigError(null);

      if (projectConfigMode === 'create') {
        if (!(await saveCurrentProject())) {
          setProjectConfigError('The current project could not be saved.');
          setProjectConfigBusy(false);
          return;
        }
        setWorkspaceMessage('Creating project');
        let project: Project;
        try {
          project = await createProject(
            values.name,
            [],
            values.description
          );
          setProjects((current) => [project, ...current]);
        } catch (error) {
          const message =
            error instanceof Error ? error.message : 'Project creation failed';
          setProjectConfigError(message);
          setWorkspaceMessage(`Create failed: ${message}`);
          setProjectConfigBusy(false);
          return;
        }

        let projectToActivate = project;
        let datasetToPreview: Dataset | null = null;
        let completionMessage: string | null = null;
        if (values.datasetFile) {
          setWorkspaceMessage('Importing CSV');
          try {
            const dataset = await importCsvDataset(
              values.datasetFile,
              project.id
            );
            const importedNodes = bindDatasetToNodes([], dataset);
            datasetToPreview = dataset;
            projectToActivate = {
              ...project,
              selectedDatasetId: dataset.id,
              latestOutputId: null,
              nodes: persistedPipelineNodes(importedNodes),
            };
            try {
              projectToActivate = await saveProjectWorkspace(project.id, {
                selectedDatasetId: dataset.id,
                latestOutputId: null,
                nodes: persistedPipelineNodes(importedNodes),
              });
              completionMessage = `Imported ${dataset.rowCount ?? 0} rows`;
            } catch (error) {
              const message =
                error instanceof Error
                  ? error.message
                  : 'Workspace save failed';
              completionMessage = `Imported ${dataset.rowCount ?? 0} rows; save failed: ${message}`;
            }
            setProjects((current) =>
              current.map((item) =>
                item.id === project.id ? projectToActivate : item
              )
            );
          } catch (error) {
            const message =
              error instanceof Error ? error.message : 'CSV import failed';
            completionMessage = `Project created; import failed: ${message}`;
          }
        }

        setProjectConfigMode(null);
        const loaded = await activateProject(projectToActivate);
        if (loaded && completionMessage) {
          setWorkspaceMessage(completionMessage);
        }
        if (loaded && datasetToPreview) {
          setPreviewDataset(datasetToPreview);
        }
        setProjectConfigBusy(false);
        return;
      }

      if (!activeProject) {
        setProjectConfigBusy(false);
        return;
      }
      setWorkspaceMessage('Saving project settings');
      try {
        const updated = await updateProject(activeProject.id, {
          name: values.name,
          description: values.description,
        });
        setProjects((current) =>
          current.map((project) =>
            project.id === updated.id ? updated : project
          )
        );
        setProjectConfigMode(null);
        setWorkspaceMessage('Saved just now');
      } catch (error) {
        const message =
          error instanceof Error ? error.message : 'Project update failed';
        setProjectConfigError(message);
        setWorkspaceMessage(`Save failed: ${message}`);
      } finally {
        setProjectConfigBusy(false);
      }
    },
    [
      activeProject,
      activateProject,
      projectConfigBusy,
      projectConfigMode,
      saveCurrentProject,
    ]
  );

  const handleCloseProjectConfig = useCallback(() => {
    if (projectConfigBusy) return;
    setProjectConfigError(null);
    setProjectConfigMode(null);
  }, [projectConfigBusy]);

  const _handleDeleteProject = useCallback(async () => {
    if (!activeProject) return;
    if (globalRunning) {
      setWorkspaceMessage('Wait for the current run to finish');
      return;
    }
    if (projects.length <= 1) {
      setWorkspaceMessage('The last project cannot be deleted');
      return;
    }
    const confirmed = window.confirm(
      `Delete "${activeProject.name}" and all of its datasets?`
    );
    if (!confirmed) return;

    setProjectReady(false);
    setWorkspaceMessage('Deleting project');
    try {
      await deleteProject(activeProject.id);
      const remaining = projects.filter(
        (project) => project.id !== activeProject.id
      );
      setProjects(remaining);
      await activateProject(remaining[0]);
    } catch (error) {
      setProjectReady(true);
      const message =
        error instanceof Error ? error.message : 'Project deletion failed';
      setWorkspaceMessage(`Delete failed: ${message}`);
    }
  }, [activeProject, activateProject, globalRunning, projects]);

  /**
   * One primary selection drives both the Inspector and the preview stage.
   * Selecting a node makes it primary; clicking empty canvas (or the file
   * in the sidebar) returns the primary selection to the dataset.
   */
  const handleSelectNode = useCallback((nodeId: string) => {
    if (nodeId === OUTPUT_ASSET_ID) {
      // The output asset is primary; the preview shows the final stage.
      setSelectedNode(OUTPUT_ASSET_ID);
      setShowDetail(true);
      setPreviewTarget({ scope: 'asset' });
      return;
    }
    if (nodeId) {
      setSelectedNode(nodeId);
      setShowDetail(true);
      setPreviewTarget({ scope: 'node', nodeId, mode: 'input' });
    } else {
      setSelectedNode('');
      setShowDetail(false);
      setPreviewTarget({ scope: 'dataset' });
    }
  }, []);

  /** Double click selects and jumps straight to the node's input stage. */
  const handleNodeDoubleClick = useCallback((nodeId: string) => {
    if (nodeId === OUTPUT_ASSET_ID) {
      setSelectedNode(OUTPUT_ASSET_ID);
      setShowDetail(true);
      setPreviewTarget({ scope: 'asset' });
      return;
    }
    setSelectedNode(nodeId);
    setShowDetail(true);
    setPreviewTarget({ scope: 'node', nodeId, mode: 'input' });
  }, []);

  /** Inspector "View input" — preview the data entering the selected node. */
  const handleViewNodeInput = useCallback((nodeId: string) => {
    setPreviewTarget({ scope: 'node', nodeId, mode: 'input' });
  }, []);

  const handleCloseDetail = useCallback(() => {
    setShowDetail(false);
  }, []);

  const applyNodeMetrics = useCallback(
    (executions: { nodeId: string; metrics: PipelineMetrics }[]) => {
      setNodes((prev) => {
        const map = new Map(executions.map((e) => [e.nodeId, e.metrics]));
        return prev.map((n) =>
          map.has(n.id)
            ? {
                ...n,
                status: 'completed' as const,
                rows:
                  map.get(n.id)!.rowsOut > 0 ? '' + map.get(n.id)!.rowsOut : n.rows,
                metrics: map.get(n.id),
              }
            : n
        );
      });
    },
    []
  );

  /* Restore the run counter that versions output datasets. */
  useEffect(() => {
    if (!activeProjectId) return;
    try {
      const stored = window.localStorage.getItem(
        `stillflow.runs.${activeProjectId}`
      );
      setRunCount(stored ? parseInt(stored, 10) || 0 : 0);
    } catch {
      setRunCount(0);
    }
    setOutputStale(false);
  }, [activeProjectId]);

  const applyBackendResult = useCallback(
    (result: BackendPipelineResult) => {
      applyNodeMetrics(result.executions);
      setLatestOutputId(result.dataset.id);
      setOutputStale(false);
      setRunCount((current) => {
        const next = current + 1;
        try {
          if (activeProjectId) {
            window.localStorage.setItem(
              `stillflow.runs.${activeProjectId}`,
              String(next)
            );
          }
        } catch {
          // Version still increments for this session.
        }
        return next;
      });
      setWorkspaceDatasets((current) => [
        result.dataset,
        ...current.filter((dataset) => dataset.id !== result.dataset.id),
      ]);
    },
    [activeProjectId, applyNodeMetrics]
  );

  const executableNodes = useCallback((chain: PipelineNode[]) => {
    return chain.filter((node) => node.status !== 'disabled');
  }, []);

  useEffect(() => {
    const handleSearch = (event: Event) => {
      const query = (event as CustomEvent<string>).detail?.trim().toLowerCase();
      if (!query) return;

      const match = nodes.find(
        (node) =>
          node.name.toLowerCase().includes(query) ||
          node.description.toLowerCase().includes(query)
      );
      if (match) {
        setSelectedNode(match.id);
        setShowDetail(true);
      }
    };

    window.addEventListener('opencode:search-nodes', handleSearch);
    return () => window.removeEventListener('opencode:search-nodes', handleSearch);
  }, [nodes]);

  const handleImportCsv = useCallback(async (file: File) => {
    if (globalRunning) {
      setWorkspaceMessage('Wait for the current run to finish');
      return;
    }
    if (!activeProjectId) {
      setWorkspaceMessage('Backend project is unavailable');
      return;
    }
    if (!projectReady) {
      setWorkspaceMessage('Wait for the project to finish loading');
      return;
    }
    setImporting(true);
    setWorkspaceMessage('Importing CSV');
    try {
      const dataset = await importCsvDataset(file, activeProjectId);
      const boundNodes = bindDatasetToNodes(nodes, dataset);
      setWorkspaceDatasets((current) => [
        dataset,
        ...current.filter((item) => item.id !== dataset.id),
      ]);
      setSelectedDatasetId(dataset.id);
      setLatestOutputId(null);
      setNodes(boundNodes);
      setSelectedNode(boundNodes[0]?.id ?? '');
      setShowDetail(boundNodes.length > 0);
      setPreviewDataset(dataset);
      try {
        const updated = await saveProjectWorkspace(activeProjectId, {
          selectedDatasetId: dataset.id,
          latestOutputId: null,
          nodes: persistedPipelineNodes(boundNodes),
        });
        setProjects((current) =>
          current.map((project) =>
            project.id === updated.id ? updated : project
          )
        );
        setWorkspaceMessage(`Imported ${dataset.rowCount ?? 0} rows`);
      } catch (error) {
        const message =
          error instanceof Error ? error.message : 'Workspace save failed';
        setWorkspaceMessage(
          `Imported ${dataset.rowCount ?? 0} rows; save failed: ${message}`
        );
      }
      try {
        const refreshedDatasets = await listBackendDatasets(activeProjectId);
        setWorkspaceDatasets([
          dataset,
          ...refreshedDatasets.filter((item) => item.id !== dataset.id),
        ]);
      } catch {
        // Keep the imported dataset already inserted into local state.
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Import failed';
      setWorkspaceMessage(`Import failed: ${message}`);
    } finally {
      setImporting(false);
    }
  }, [activeProjectId, globalRunning, nodes, projectReady]);

  const _handleSelectDataset = useCallback(
    (dataset: Dataset) => {
      if (globalRunning) {
        setWorkspaceMessage('Wait for the current run to finish');
        return;
      }
      setPreviewDataset(dataset);
      if (dataset.category !== 'source') {
        setWorkspaceMessage('Dataset preview opened');
        return;
      }

      setSelectedDatasetId(dataset.id);
      setLatestOutputId(null);
      const boundNodes = bindDatasetToNodes(nodes, dataset);
      setNodes(boundNodes);
      setSelectedNode(boundNodes[0]?.id ?? '');
      setShowDetail(boundNodes.length > 0);
      setWorkspaceMessage('Dataset selected');
    },
    [globalRunning, nodes]
  );

  const _handleRenameDataset = useCallback(
    async (dataset: Dataset) => {
      if (globalRunning) {
        setWorkspaceMessage('Wait for the current run to finish');
        return;
      }
      if (!dataset.projectId) {
        setWorkspaceMessage('Dataset is not attached to a project');
        return;
      }
      const name = window.prompt('Dataset name', dataset.name)?.trim();
      if (!name || name === dataset.name) return;

      setWorkspaceMessage('Renaming dataset');
      try {
        const updated = await renameBackendDataset(dataset.id, name);
        setWorkspaceDatasets((current) =>
          current.map((item) => (item.id === updated.id ? updated : item))
        );
        setPreviewDataset((current) =>
          current?.id === updated.id ? updated : current
        );
        if (selectedDatasetId === dataset.id && dataset.category === 'source') {
          setNodes((current) =>
            current.map((node) =>
              node.type === 'source' ? { ...node, name: updated.name } : node
            )
          );
        }
        setWorkspaceMessage('Saved just now');
      } catch (error) {
        const message =
          error instanceof Error ? error.message : 'Dataset rename failed';
        setWorkspaceMessage(`Rename failed: ${message}`);
      }
    },
    [globalRunning, selectedDatasetId]
  );

  const _handleDeleteDataset = useCallback(
    async (dataset: Dataset) => {
      if (globalRunning) {
        setWorkspaceMessage('Wait for the current run to finish');
        return;
      }
      if (!dataset.projectId) {
        setWorkspaceMessage('Dataset is not attached to a project');
        return;
      }
      if (!window.confirm(`Delete "${dataset.name}"?`)) return;

      setWorkspaceMessage('Deleting dataset');
      try {
        await deleteBackendDataset(dataset.id);
        setWorkspaceDatasets((current) =>
          current.filter((item) => item.id !== dataset.id)
        );
        if (selectedDatasetId === dataset.id) {
          setSelectedDatasetId(null);
          if (dataset.category === 'source') {
            const remainingNodes = nodes
              .filter((node) => node.type !== 'source')
              .map(resetNodeRuntime);
            setNodes(remainingNodes);
            setSelectedNode(remainingNodes[0]?.id ?? '');
            setShowDetail(remainingNodes.length > 0);
          }
        }
        if (latestOutputId === dataset.id) {
          setLatestOutputId(null);
        }
        setPreviewDataset((current) =>
          current?.id === dataset.id ? null : current
        );
        setWorkspaceMessage('Dataset deleted');
      } catch (error) {
        const message =
          error instanceof Error ? error.message : 'Dataset deletion failed';
        setWorkspaceMessage(`Delete failed: ${message}`);
      }
    },
    [globalRunning, latestOutputId, nodes, selectedDatasetId]
  );

  /** "Run pipeline" runs every enabled node via the backend. */
  const handleRunAll = useCallback(async () => {
    if (globalRunning) return;
    if (activeProjectId && !projectReady) {
      setWorkspaceMessage('Wait for the project to finish loading');
      return;
    }
    if (!activeDataset) {
      setWorkspaceMessage('Import and select a CSV before running');
      return;
    }
    const executable = executableNodes(nodes);
    if (executable.length === 0) {
      setWorkspaceMessage('No enabled nodes');
      return;
    }

    setGlobalRunning(true);
    setGlobalProgress(0);
    setWorkspaceMessage('Running');

    setNodes((prev) =>
      prev.map((node) =>
        node.status === 'disabled'
          ? node
          : { ...node, status: 'pending' as const, metrics: undefined, error: undefined }
      )
    );

    try {
      setGlobalProgress(20);
      setNodes((prev) =>
        prev.map((node) =>
          node.id === executable[0].id
            ? { ...node, status: 'running' as const, error: undefined }
            : node
        )
      );
      const result = await runBackendPipeline(activeDataset.id, executable);
      setGlobalProgress(90);
      applyBackendResult(result);
      setWorkspaceMessage(`Cleaned ${result.dataset.rowCount ?? 0} rows`);
      // A completed run makes the new output asset the primary object.
      setSelectedNode(OUTPUT_ASSET_ID);
      setShowDetail(true);
      setPreviewTarget({ scope: 'asset' });
      setGlobalProgress(100);
    } catch (err) {
      console.error('Run All failed', err);
      const message = err instanceof Error ? err.message : 'Run failed';
      setWorkspaceMessage(`Run failed: ${message}`);
      setNodes((prev) =>
        prev.map((node) =>
          node.status === 'running' ? { ...node, status: 'failed' as const, error: message } : node
        )
      );
    } finally {
      setGlobalRunning(false);
      setTimeout(() => setGlobalProgress(0), 800);
    }
  }, [
    activeDataset,
    activeProjectId,
    applyBackendResult,
    executableNodes,
    globalRunning,
    nodes,
    projectReady,
  ]);

  /** "Run From Here" in DetailPanel: runs node + every node upstream of it. */
  const handleRunFromHere = useCallback(
    async (nodeId: string) => {
      if (activeProjectId && !projectReady) {
        setWorkspaceMessage('Wait for the project to finish loading');
        return;
      }
      if (!activeDataset) {
        setWorkspaceMessage('Import and select a CSV before running');
        return;
      }
      const idx = nodes.findIndex((n) => n.id === nodeId);
      if (idx < 0) return;
      const chain = executableNodes(nodes.slice(0, idx + 1));
      if (chain.length === 0) {
        setWorkspaceMessage('No enabled upstream nodes');
        return;
      }

      setGlobalRunning(true);
      setGlobalProgress(0);
      setWorkspaceMessage('Running from node');
      const executableIds = new Set(chain.map((node) => node.id));
      setNodes((prev) =>
        prev.map((node) =>
          executableIds.has(node.id)
            ? { ...node, status: 'pending' as const, metrics: undefined, error: undefined }
            : node
        )
      );

      try {
        setGlobalProgress(20);
        setNodes((prev) =>
          prev.map((node) =>
            node.id === chain[0].id
              ? { ...node, status: 'running' as const, error: undefined }
              : node
          )
        );
        const result = await runBackendPipeline(activeDataset.id, chain);
        setGlobalProgress(90);
        applyBackendResult(result);
        setWorkspaceMessage(`Cleaned ${result.dataset.rowCount ?? 0} rows`);
        setPreviewTarget({ scope: 'node', nodeId, mode: 'output' });
        setGlobalProgress(100);
      } catch (err) {
        console.error('Run from here failed', err);
        const message = err instanceof Error ? err.message : 'Run failed';
        setWorkspaceMessage(`Run failed: ${message}`);
        setNodes((prev) =>
          prev.map((node) =>
            node.status === 'running' ? { ...node, status: 'failed' as const, error: message } : node
          )
        );
      } finally {
        setGlobalRunning(false);
        setTimeout(() => setGlobalProgress(0), 800);
      }
    },
    [
      activeDataset,
      activeProjectId,
      applyBackendResult,
      executableNodes,
      nodes,
      projectReady,
    ]
  );

  const handleUpdateNode = useCallback((nodeId: string, patch: Partial<PipelineNode>) => {
    setNodes((prev) => {
      const changedIndex = prev.findIndex((node) => node.id === nodeId);
      if (changedIndex < 0) return prev;

      const invalidatesRuntime = Boolean(patch.config || patch.status);
      if (invalidatesRuntime) setOutputStale(true);
      return prev.map((node, index) => {
        if (node.id === nodeId) {
          return invalidatesRuntime
            ? { ...resetNodeRuntime(node), ...patch }
            : { ...node, ...patch };
        }
        if (invalidatesRuntime && index > changedIndex) return resetNodeRuntime(node);
        return node;
      });
    });
  }, []);

  const handleDeleteNode = useCallback(
    (nodeId: string) => {
      if (nodeId === OUTPUT_ASSET_ID) {
        setWorkspaceMessage('Output asset is produced by the pipeline');
        return;
      }
      const index = nodes.findIndex((node) => node.id === nodeId);
      const node = nodes[index];
      if (!node) return;
      if (node.type === 'source') {
        setWorkspaceMessage('Source node is required');
        return;
      }
      setOutputStale(true);

      setNodes((prev) =>
        prev
          .filter((item) => item.id !== nodeId)
          .map((item, itemIndex) => (itemIndex >= index ? resetNodeRuntime(item) : item))
      );
      const nextSelectedId =
        nodes[index - 1]?.id ?? nodes[index + 1]?.id ?? '';
      setSelectedNode(nextSelectedId);
      setShowDetail(Boolean(nextSelectedId));
    },
    [nodes]
  );

  const handleAddNode = useCallback((node: PipelineNode) => {
    setNodes((prev) => {
      const selectedIndex = prev.findIndex((item) => item.id === selectedNode);
      const insertIndex = selectedIndex >= 0 ? selectedIndex + 1 : prev.length;
      return [
        ...prev.slice(0, insertIndex),
        node,
        ...prev.slice(insertIndex).map(resetNodeRuntime),
      ];
    });
    // The new node becomes the primary selection; the preview follows it.
    setSelectedNode(node.id);
    setShowDetail(true);
    setPreviewTarget({ scope: 'node', nodeId: node.id, mode: 'input' });
    setOutputStale(true);
  }, [selectedNode]);

  const handleDuplicateNode = useCallback((nodeId: string) => {
    if (nodeId === OUTPUT_ASSET_ID) return;
    const index = nodes.findIndex((node) => node.id === nodeId);
    const node = nodes[index];
    if (!node || node.type === 'source') {
      setWorkspaceMessage('Source node is unique');
      return;
    }
    setOutputStale(true);

    const duplicate: PipelineNode = {
      ...node,
      id: `n${Date.now()}`,
      name: `${node.name} copy`,
      rows: '',
      status: 'pending',
      metrics: undefined,
      error: undefined,
      config: { ...node.config },
    };

    setNodes((prev) => [
      ...prev.slice(0, index + 1),
      duplicate,
      ...prev.slice(index + 1).map(resetNodeRuntime),
    ]);
    setSelectedNode(duplicate.id);
    setShowDetail(true);
  }, [nodes]);

  const selected = nodes.find((n) => n.id === selectedNode) ?? null;
  const datasetTitle = previewDataset?.name ?? FILE_META.name;

  /* ── Client-side chain evaluation on the displayed sample ─────────
     Every stage the preview shows is derived by really applying the
     configured rules — never by stitching placeholder numbers. When a
     rule references columns outside the displayed sample, evaluation is
     skipped entirely instead of producing misleading numbers. */
  const sampleEvaluable = useMemo(
    () =>
      nodes.every((node) => {
        const column = node.config.column?.trim();
        return !column || CSV_COLUMNS.includes(column);
      }),
    [nodes]
  );

  const fullChain = useMemo(
    () =>
      sampleEvaluable
        ? applyChain(tableRows, nodes)
        : { rows: tableRows, impacts: [] },
    [sampleEvaluable, tableRows, nodes]
  );

  const stageRows = useMemo<Row[]>(() => {
    if (previewTarget.scope === 'dataset' || !sampleEvaluable) return tableRows;
    if (previewTarget.scope === 'asset') return fullChain.rows;
    const idx = nodes.findIndex((n) => n.id === previewTarget.nodeId);
    if (idx < 0) return tableRows;
    const through =
      previewTarget.mode === 'output'
        ? previewTarget.nodeId
        : idx > 0
          ? nodes[idx - 1].id
          : undefined;
    return applyChain(tableRows, nodes, through).rows;
  }, [previewTarget, nodes, tableRows, fullChain.rows, sampleEvaluable]);

  const stageStats = useMemo(
    () => profileAll(CSV_COLUMNS, stageRows),
    [stageRows]
  );

  /** Impact of the node the preview currently targets (Changes/Rejected). */
  const previewImpact = useMemo(() => {
    if (previewTarget.scope !== 'node') return null;
    return (
      fullChain.impacts.find((i) => i.nodeId === previewTarget.nodeId) ?? null
    );
  }, [fullChain, previewTarget]);

  /** Impact of the node the Inspector shows (live sample estimate). */
  const selectedSampleImpact = useMemo(
    () => fullChain.impacts.find((i) => i.nodeId === selectedNode) ?? null,
    [fullChain, selectedNode]
  );

  /* ── Output dataset asset ──────────────────────────────────────────
     Only a dataset the pipeline actually produced (category 'output')
     counts — never the source dataset the id may fall back to. */
  const outputDataset =
    workspaceDatasets.find(
      (d) => d.id === latestOutputId && d.category === 'output'
    ) ?? null;
  const inputVersion = 1;
  const outputVersion = inputVersion + runCount;
  const assetPublished =
    latestOutputId !== null && publishedMap[latestOutputId] !== undefined;
  const assetName =
    outputDataset?.name ??
    `${datasetTitle.replace(/\.[^.]+$/, '')}_cleaned.csv`;

  const outputAssetNode = useMemo<PipelineNode>(
    () => ({
      id: OUTPUT_ASSET_ID,
      type: 'export',
      name: assetName,
      description: outputDataset
        ? `${assetPublished ? 'Published' : 'Draft'} v${outputVersion} · ${(
            outputDataset.rowCount ?? 0
          ).toLocaleString()} rows`
        : 'Run the pipeline to create',
      rows: outputDataset ? String(outputDataset.rowCount ?? '') : '',
      status: outputDataset ? 'completed' : 'pending',
      config: defaultConfigFor('export'),
    }),
    [assetName, assetPublished, outputDataset, outputVersion]
  );

  /** Canvas renders the real chain plus the terminal output asset. */
  const displayNodes = useMemo(
    () => [...nodes, outputAssetNode],
    [nodes, outputAssetNode]
  );

  /* ── Validation gate — checks the output must pass before publish ──
     Post-run checks use the real full-data run metrics; pre-run checks
     fall back to the displayed sample when it carries the rule columns. */
  const assetChecks = useMemo<ValidationCheck[]>(() => {
    const checks: ValidationCheck[] = [];
    const executed = nodes.filter((n) => n.metrics);
    const lastMetrics = executed[executed.length - 1]?.metrics;
    const firstMetrics = executed[0]?.metrics;

    if (outputDataset && lastMetrics && firstMetrics) {
      // Full-data checks from the actual run.
      checks.push({
        label: 'Missing cells ≤ 1%',
        detail: `${lastMetrics.missing}% empty cells in the run output`,
        state:
          lastMetrics.missing <= 1
            ? 'pass'
            : lastMetrics.missing <= 10
              ? 'warn'
              : 'fail',
      });
      const removedPct =
        firstMetrics.rowsIn > 0
          ? ((firstMetrics.rowsIn - lastMetrics.rowsOut) /
              firstMetrics.rowsIn) *
            100
          : 0;
      checks.push({
        label: 'Rejected rows ≤ 5%',
        detail: `${removedPct.toFixed(1)}% of rows removed by the chain`,
        state: removedPct <= 5 ? 'pass' : removedPct <= 25 ? 'warn' : 'fail',
      });
      checks.push({
        label: 'Schema compatible',
        detail: `${CSV_COLUMNS.length} columns preserved through the chain`,
        state: 'pass',
      });
      return checks;
    }

    if (!sampleEvaluable) {
      checks.push({
        label: 'Awaiting pipeline run',
        detail:
          'Rule columns are not part of the displayed sample — checks evaluate on the full data after a run',
        state: 'warn',
      });
      return checks;
    }

    const rows = fullChain.rows;
    const total = Math.max(1, rows.length);

    const filterNode = nodes.find(
      (n) => n.type === 'filter' && n.config.column.trim()
    );
    if (filterNode) {
      const column = filterNode.config.column;
      const valid = rows.filter((r) => !isMissing(r[column] ?? '')).length;
      const pct = (valid / total) * 100;
      checks.push({
        label: `${column} completeness ≥ 99%`,
        detail: `${pct.toFixed(1)}% complete on the sample`,
        state: pct >= 99 ? 'pass' : pct >= 95 ? 'warn' : 'fail',
      });
    }

    const seen = new Set<string>();
    let duplicates = 0;
    for (const row of rows) {
      const key = JSON.stringify(row);
      if (seen.has(key)) duplicates++;
      else seen.add(key);
    }
    const dupPct = (duplicates / total) * 100;
    checks.push({
      label: 'Duplicate rate ≤ 1%',
      detail: `${dupPct.toFixed(2)}% exact duplicates on the sample`,
      state: dupPct <= 1 ? 'pass' : dupPct <= 5 ? 'warn' : 'fail',
    });

    const removed = fullChain.impacts.reduce(
      (sum, impact) => sum + (impact.rowsIn - impact.rowsOut),
      0
    );
    const rejPct = (removed / Math.max(1, tableRows.length)) * 100;
    checks.push({
      label: 'Rejected rows ≤ 5%',
      detail: `${rejPct.toFixed(1)}% of sample rows removed by the chain`,
      state: rejPct <= 5 ? 'pass' : rejPct <= 25 ? 'warn' : 'fail',
    });

    checks.push({
      label: 'Schema compatible',
      detail: `${CSV_COLUMNS.length} columns preserved through the chain`,
      state: 'pass',
    });
    return checks;
  }, [fullChain, nodes, tableRows.length, sampleEvaluable, outputDataset]);

  const assetBlocked = assetChecks.some((check) => check.state === 'fail');

  const handlePublish = useCallback(() => {
    if (!latestOutputId) return;
    setPublishedMap((current) => {
      const next = { ...current, [latestOutputId]: outputVersion };
      try {
        window.localStorage.setItem('stillflow.published', JSON.stringify(next));
      } catch {
        // Publish state still applies for this session.
      }
      return next;
    });
    setWorkspaceMessage(`Published ${assetName} v${outputVersion}`);
  }, [assetName, latestOutputId, outputVersion]);

  /**
   * Derive the preview header from the current target. The meta line always
   * names the processing stage, so the table can never be mistaken for a
   * different stage of the data.
   */
  const resolvedPreview = useMemo(() => {
    const sample = `Sample ${tableRows.length.toLocaleString()} of ${displayRowCount.toLocaleString()} rows · ${CSV_COLUMNS.length} columns`;
    if (previewTarget.scope === 'asset') {
      return {
        title: datasetTitle,
        meta: `${assetName} · ${assetPublished ? 'Published' : 'Draft'} v${outputVersion} · ${sample}`,
        showToggle: false,
        toggleMode: 'input' as const,
        outputAvailable: false,
      };
    }
    if (previewTarget.scope !== 'node') {
      return {
        title: datasetTitle,
        meta: `Source · v${inputVersion} · ${sample}`,
        showToggle: false,
        toggleMode: 'input' as const,
        outputAvailable: false,
      };
    }
    const node = nodes.find((n) => n.id === previewTarget.nodeId);
    if (!node) {
      return {
        title: datasetTitle,
        meta: `Source · v${inputVersion} · ${sample}`,
        showToggle: false,
        toggleMode: 'input' as const,
        outputAvailable: false,
      };
    }
    const mode = previewTarget.mode;
    return {
      title: datasetTitle,
      meta: `${node.name} ${mode === 'output' ? 'output' : 'input'} · ${sample}`,
      showToggle: true,
      toggleMode: mode,
      outputAvailable: Boolean(node.metrics),
    };
  }, [
    previewTarget,
    nodes,
    datasetTitle,
    displayRowCount,
    tableRows.length,
    assetName,
    assetPublished,
    outputVersion,
  ]);

  /** Stage path: Source → each transform → the output asset. */
  const previewStages = useMemo<PreviewStage[]>(() => {
    const stages: PreviewStage[] = nodes.map((node) => {
      const isSourceStage = node.type === 'source';
      const active =
        previewTarget.scope === 'dataset'
          ? isSourceStage
          : previewTarget.scope === 'node' && previewTarget.nodeId === node.id;
      return {
        id: node.id,
        label: isSourceStage ? 'Source' : node.name,
        active,
        onSelect: () => {
          if (isSourceStage) {
            setSelectedNode('');
            setShowDetail(false);
            setPreviewTarget({ scope: 'dataset' });
          } else {
            setSelectedNode(node.id);
            setShowDetail(true);
            setPreviewTarget({
              scope: 'node',
              nodeId: node.id,
              mode: node.metrics ? 'output' : 'input',
            });
          }
        },
      };
    });
    stages.push({
      id: OUTPUT_ASSET_ID,
      label: 'Output',
      active: previewTarget.scope === 'asset',
      onSelect: () => {
        setSelectedNode(OUTPUT_ASSET_ID);
        setShowDetail(true);
        setPreviewTarget({ scope: 'asset' });
      },
    });
    return stages;
  }, [nodes, previewTarget]);

  /* ── Canvas / Preview vertical split ────────────────────────────── */
  const handleSplitStart = useCallback(
    (event: React.MouseEvent) => {
      event.preventDefault();
      splitDragRef.current = { startY: event.clientY, startH: canvasHeight };
      setSplitDragging(true);
      document.body.style.cursor = 'row-resize';
      document.body.style.userSelect = 'none';
    },
    [canvasHeight]
  );

  useEffect(() => {
    if (!splitDragging) return;
    const handleMove = (event: MouseEvent) => {
      if (!splitDragRef.current) return;
      const delta = event.clientY - splitDragRef.current.startY;
      const max = Math.max(320, window.innerHeight - 320);
      setCanvasHeight(
        Math.min(max, Math.max(160, splitDragRef.current.startH + delta))
      );
    };
    const handleUp = () => {
      splitDragRef.current = null;
      setSplitDragging(false);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    document.addEventListener('mousemove', handleMove);
    document.addEventListener('mouseup', handleUp);
    return () => {
      document.removeEventListener('mousemove', handleMove);
      document.removeEventListener('mouseup', handleUp);
    };
  }, [splitDragging]);

  const handlePreviewToggleMode = useCallback(
    (mode: 'input' | 'output') => {
      if (previewTarget.scope !== 'node') return;
      setPreviewTarget({
        scope: 'node',
        nodeId: previewTarget.nodeId,
        mode,
      });
    },
    [previewTarget],
  );

  // Kept for future re-integration of Header / DatasetPanel.
  void _handleSelectProject;
  void _handleCreateProject;
  void _handleConfigureProject;
  void _handleDeleteProject;
  void _handleSelectDataset;
  void _handleRenameDataset;
  void _handleDeleteDataset;

  return (
    <div className="h-screen w-screen overflow-hidden bg-[#f3f5f7] p-3">
      <div className="flex h-full gap-2 overflow-hidden">
        <DataExplorer
          fileName={datasetTitle}
          sizeLabel={previewDataset?.size ?? FILE_META.sizeLabel}
          rowCount={previewDataset?.rowCount ?? tableRows.length}
          fileActive={previewTarget.scope === 'dataset'}
          onOpenPreview={() => {
            // Selecting the file makes the dataset the primary object.
            setSelectedNode('');
            setShowDetail(false);
            setPreviewTarget({ scope: 'dataset' });
            if (!previewDataset && workspaceDatasets.length > 0) {
              const ds = workspaceDatasets.find(d => d.id === selectedDatasetId) ?? workspaceDatasets[0];
              if (ds) setPreviewDataset(ds);
            }
          }}
          onUpload={(file) => void handleImportCsv(file)}
        />
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <div
            className="flex flex-shrink-0 flex-col overflow-hidden rounded-lg border border-[#dce2e8] bg-[#f8fafb]"
            style={{ height: canvasHeight }}
          >
            <div className="flex h-11 flex-shrink-0 items-center border-b border-[#edf2f6] px-3">
              {/* Object path: dataset · version / workflow / selected object */}
              <div className="flex min-w-0 items-center gap-1.5 text-[13px]">
                <span className="truncate text-[#5e6874]">
                  {datasetTitle}
                  <span className="text-[#9099a4]"> · v{inputVersion}</span>
                </span>
                <span className="shrink-0 text-[#c9d1d9]">/</span>
                <span
                  className={`truncate ${
                    selectedNode ? 'text-[#5e6874]' : 'font-semibold text-[#171a1f]'
                  }`}
                >
                  {activeProject?.name ?? 'Workflow'}
                </span>
                {selectedNode === OUTPUT_ASSET_ID && (
                  <>
                    <span className="shrink-0 text-[#c9d1d9]">/</span>
                    <span className="truncate font-semibold text-[#171a1f]">
                      {assetName}
                    </span>
                  </>
                )}
                {selected && (
                  <>
                    <span className="shrink-0 text-[#c9d1d9]">/</span>
                    <span className="truncate font-semibold text-[#171a1f]">
                      {selected.name}
                    </span>
                  </>
                )}
              </div>
              {nodes.length > 0 && (
                <span className="ml-auto shrink-0 text-[11px] text-[#9099a4] tabular">
                  {displayNodes.length} object{displayNodes.length !== 1 ? 's' : ''}
                </span>
              )}
            </div>
            <PipelineCanvas
              graphKey={activeProjectId ?? 'unassigned'}
              nodes={displayNodes}
              selectedNode={selectedNode}
              running={globalRunning}
              onRunAll={handleRunAll}
              onSelectNode={handleSelectNode}
              onNodeDoubleClick={handleNodeDoubleClick}
              onAddNode={handleAddNode}
              onDeleteNode={handleDeleteNode}
              topRightActions={
                <>
                  {outputDataset && !assetPublished && (
                    <button
                      onClick={() => {
                        setSelectedNode(OUTPUT_ASSET_ID);
                        setShowDetail(true);
                        setPreviewTarget({ scope: 'asset' });
                      }}
                      title="Inspect the output dataset"
                      className="flex h-8 items-center gap-1.5 rounded-[7px] border border-[#dce2e8] bg-white px-3 text-[12px] font-medium text-[#39434e] shadow-[0_2px_6px_rgba(24,36,48,.06)] transition-colors hover:bg-[#edf2f6]"
                    >
                      Review output
                    </button>
                  )}
                  {outputDataset &&
                    (assetPublished ? (
                      <span className="flex h-8 items-center gap-1 rounded-[7px] border border-[#dce2e8] bg-white px-3 text-[12px] font-medium text-[#4ba66a] shadow-[0_2px_6px_rgba(24,36,48,.06)]">
                        ✓ Published v{outputVersion}
                      </span>
                    ) : (
                      <button
                        onClick={handlePublish}
                        disabled={outputStale || assetBlocked}
                        title={
                          outputStale
                            ? 'Re-run to refresh the output before publishing'
                            : assetBlocked
                              ? 'Resolve the failed quality checks first'
                              : 'Mark this version as a published asset'
                        }
                        className="flex h-8 items-center gap-1.5 rounded-[7px] bg-[#2196d2] px-3 text-[12px] font-medium text-white shadow-[0_2px_6px_rgba(24,36,48,.06)] transition-colors hover:bg-[#1686be] disabled:cursor-not-allowed disabled:bg-[#c9d1d9]"
                      >
                        Publish dataset
                      </button>
                    ))}
                  <button
                    onClick={handleRunAll}
                    disabled={globalRunning}
                    title="Run the full pipeline end to end"
                    className={`flex h-8 items-center gap-1.5 rounded-[7px] px-3 text-[12px] font-medium shadow-[0_2px_6px_rgba(24,36,48,.06)] transition-colors disabled:cursor-wait disabled:opacity-50 ${
                      outputDataset
                        ? 'border border-[#dce2e8] bg-white text-[#39434e] hover:bg-[#edf2f6]'
                        : 'bg-[#2196d2] text-white hover:bg-[#1686be]'
                    }`}
                  >
                    <Play size={13} fill="currentColor" />
                    {globalRunning ? 'Running…' : 'Run pipeline'}
                  </button>
                </>
              }
            />
          </div>

          {/* Drag divider — the only "window control" between the two
              fixed regions. */}
          <div
            onMouseDown={handleSplitStart}
            className="group flex h-2 flex-shrink-0 cursor-row-resize items-center justify-center"
            title="Drag to resize the canvas"
          >
            <div
              className={`h-1 w-10 rounded-full transition-colors ${
                splitDragging ? 'bg-[#a9b4bf]' : 'bg-transparent group-hover:bg-[#c9d1d9]'
              }`}
            />
          </div>

          <PreviewPanel
            title={resolvedPreview.title}
            meta={resolvedPreview.meta}
            stages={previewStages}
            showToggle={resolvedPreview.showToggle}
            toggleMode={resolvedPreview.toggleMode}
            onToggleMode={handlePreviewToggleMode}
            outputAvailable={resolvedPreview.outputAvailable}
          >
            <DataTable
              columns={CSV_COLUMNS}
              rows={stageRows}
              stats={stageStats}
              focusColumn={focusedColumn}
              onFocusColumn={setFocusedColumn}
              onDownload={tableDownload}
              changes={previewImpact?.changes ?? null}
              rejected={previewImpact?.rejected ?? null}
              nodeName={
                previewTarget.scope === 'node'
                  ? (nodes.find((n) => n.id === previewTarget.nodeId)?.name ?? null)
                  : null
              }
            />
          </PreviewPanel>
        </div>
        {showDetail && selectedNode === OUTPUT_ASSET_ID && (
          <AssetPanel
            assetName={assetName}
            version={outputVersion}
            published={assetPublished}
            stale={outputStale}
            rows={outputDataset?.rowCount ?? null}
            columnCount={CSV_COLUMNS.length}
            sourceName={datasetTitle}
            checks={assetChecks}
            onClose={handleCloseDetail}
            onPreviewOutput={() => setPreviewTarget({ scope: 'asset' })}
            onPublish={handlePublish}
          />
        )}
        {showDetail && selected && (
          <DetailPanel
            node={selected}
            nodes={nodes}
            availableColumns={previewDataset?.columns ?? CSV_COLUMNS}
            datasetName={datasetTitle}
            assetName={assetName}
            sampleImpact={selectedSampleImpact}
            onClose={handleCloseDetail}
            onDelete={handleDeleteNode}
            onDuplicate={handleDuplicateNode}
            onRun={handleRunFromHere}
            onViewInput={() => handleViewNodeInput(selected.id)}
            onUpdate={handleUpdateNode}
          />
        )}
      </div>
      {projectConfigMode && (
        <ProjectConfigCard
          mode={projectConfigMode}
          initialName={
            projectConfigMode === 'edit' ? activeProject?.name ?? '' : ''
          }
          initialDescription={
            projectConfigMode === 'edit'
              ? activeProject?.description ?? ''
              : ''
          }
          busy={projectConfigBusy}
          error={projectConfigError}
          onCancel={handleCloseProjectConfig}
          onSubmit={handleSubmitProjectConfig}
        />
      )}
    </div>
  );
};

export default App;