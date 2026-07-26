import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { DataExplorer } from './components/DataExplorer';
import PipelineCanvas from './components/PipelineCanvas';
import DetailPanel from './components/DetailPanel';
import { DataTable } from './components/DataTable';
import { CSV_COLUMNS, FILE_META, buildRows } from './data/kaggleDatasets';
import { profileAll, toCSV, type Row } from './lib/csv';
import ProjectConfigCard, {
  type ProjectConfigValues,
} from './components/ProjectConfigCard';
import { defaultConfig } from './data';
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
      config: { ...defaultConfig, column: identityColumn },
    };
  const pipeline = [
    sourceNode,
    ...nodes.filter((node) => node.type !== 'source'),
  ];

  return pipeline.map((node) => {
    const reset = resetNodeRuntime(node);
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
  );
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

  /* ── Kaggle DataTable source ─────────────────────────────── */
  const tableRows = useMemo<Row[]>(() => buildRows(1000), []);
  const tableStats = useMemo(() => profileAll(CSV_COLUMNS, tableRows), [tableRows]);
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
  const [importing, setImporting] = useState(false);
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
  const [globalProgress, setGlobalProgress] = useState(0);
  const [workspaceMessage, setWorkspaceMessage] = useState('Ready');
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
      setLatestOutputId(project.latestOutputId);
      setSelectedNode(projectNodes[0]?.id ?? '');
      setShowDetail(projectNodes.length > 0);
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
    [hydrateProject]
  );

  const handleSelectProject = useCallback(
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

  const handleCreateProject = useCallback(() => {
    if (globalRunning) {
      setWorkspaceMessage('Wait for the current run to finish');
      return;
    }
    setProjectConfigError(null);
    setPreviewDataset(null);
    setProjectConfigMode('create');
  }, [globalRunning]);

  const handleConfigureProject = useCallback(() => {
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

  const handleDeleteProject = useCallback(async () => {
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

  const handleSelectNode = useCallback((nodeId: string) => {
    setSelectedNode(nodeId);
    setShowDetail(true);
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

  const applyBackendResult = useCallback(
    (result: BackendPipelineResult) => {
      applyNodeMetrics(result.executions);
      setLatestOutputId(result.dataset.id);
      setWorkspaceDatasets((current) => [
        result.dataset,
        ...current.filter((dataset) => dataset.id !== result.dataset.id),
      ]);
    },
    [applyNodeMetrics]
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

  const handleSelectDataset = useCallback(
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

  const handleRenameDataset = useCallback(
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

  const handleDeleteDataset = useCallback(
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

  const handlePreviewResult = useCallback(() => {
    if (!latestOutputId) {
      setWorkspaceMessage('Run the pipeline to create a result');
      return;
    }
    const output = workspaceDatasets.find(
      (dataset) => dataset.id === latestOutputId
    );
    if (!output) {
      setWorkspaceMessage('The latest output is unavailable');
      return;
    }
    setPreviewDataset(output);
    setWorkspaceMessage('Dataset preview opened');
  }, [latestOutputId, workspaceDatasets]);

  /** Header "Run All" runs every node, updating status + progress along the way. */
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
      const index = nodes.findIndex((node) => node.id === nodeId);
      const node = nodes[index];
      if (!node) return;
      if (node.type === 'source') {
        setWorkspaceMessage('Source node is required');
        return;
      }

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
    setSelectedNode(node.id);
    setShowDetail(true);
  }, [selectedNode]);

  const handleDuplicateNode = useCallback((nodeId: string) => {
    const index = nodes.findIndex((node) => node.id === nodeId);
    const node = nodes[index];
    if (!node || node.type === 'source') {
      setWorkspaceMessage('Source node is unique');
      return;
    }

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

  return (
    <div className="h-screen w-screen overflow-hidden bg-[#f5f7f8]">
      <div className="flex h-full overflow-hidden">
        <DataExplorer
          fileName={previewDataset?.name ?? FILE_META.name}
          sizeLabel={previewDataset?.size ?? FILE_META.sizeLabel}
          rowCount={previewDataset?.rowCount ?? tableRows.length}
          stats={tableStats}
          selected={focusedColumn}
          onSelect={setFocusedColumn}
          onUpload={(file) => void handleImportCsv(file)}
          onReset={() => setFocusedColumn(null)}
          custom={Boolean(previewDataset)}
        />
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <PipelineCanvas
            graphKey={activeProjectId ?? 'unassigned'}
            nodes={nodes}
            selectedNode={selectedNode}
            running={globalRunning}
            onRunAll={handleRunAll}
            onSelectNode={handleSelectNode}
            onAddNode={handleAddNode}
            onDeleteNode={handleDeleteNode}
          />
          {previewDataset && (
            <div className="flex-shrink-0 overflow-hidden rounded-t-xl border border-[#e3e6e8] bg-white shadow-[0_-4px_16px_rgba(0,0,0,0.04)]">
              <DataTable
                columns={CSV_COLUMNS}
                rows={tableRows}
                stats={tableStats}
                fileName={previewDataset?.name ?? FILE_META.name}
                sizeLabel={previewDataset?.size ?? FILE_META.sizeLabel}
                focusColumn={focusedColumn}
                onDownload={tableDownload}
              />
            </div>
          )}
        </div>
        {showDetail && selected && (
          <DetailPanel
            node={selected}
            nodes={nodes}
            onClose={handleCloseDetail}
            onDelete={handleDeleteNode}
            onDuplicate={handleDuplicateNode}
            onRun={handleRunFromHere}
            onPreview={handlePreviewResult}
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
