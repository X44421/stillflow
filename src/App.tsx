import React, { useState, useCallback, useEffect } from 'react';
import Header from './components/Header';
import IconSidebar from './components/IconSidebar';
import DatasetPanel from './components/DatasetPanel';
import PipelineCanvas from './components/PipelineCanvas';
import DetailPanel from './components/DetailPanel';
import { initialPipelineNodes } from './data';
import type { PipelineNode } from './types';
import { initDuckDB, loadSampleData, runFullPipeline, type PipelineMetrics } from './utils/duckdb';

function resetNodeRuntime(node: PipelineNode): PipelineNode {
  return {
    ...node,
    status: node.status === 'disabled' ? 'disabled' : 'pending',
    rows: node.type === 'source' ? node.rows : '',
    metrics: undefined,
    error: undefined,
  };
}

const App: React.FC = () => {
  const [activeIcon, setActiveIcon] = useState(0);
  const [nodes, setNodes] = useState<PipelineNode[]>(initialPipelineNodes);
  const [selectedNode, setSelectedNode] = useState('n3');
  const [showDetail, setShowDetail] = useState(true);
  const [globalRunning, setGlobalRunning] = useState(false);
  const [globalProgress, setGlobalProgress] = useState(0);
  const [workspaceMessage, setWorkspaceMessage] = useState('Ready');

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

  const invalidateFromIndex = useCallback((index: number) => {
    setNodes((prev) =>
      prev.map((node, nodeIndex) => (nodeIndex >= index ? resetNodeRuntime(node) : node))
    );
  }, []);

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

  /** Header "Run All" runs every node, updating status + progress along the way. */
  const handleRunAll = useCallback(async () => {
    if (globalRunning) return;
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
      await initDuckDB();
      setGlobalProgress(10);
      await loadSampleData();
      setGlobalProgress(25);

      const result = await runFullPipeline(
        executable.map((n) => ({
          id: n.id,
          type: n.type,
          config: {
            column: n.config.column,
            strategy: n.config.strategy,
            scope: n.config.scope,
            nullHandling: n.config.nullHandling,
          },
        })),
        'raw_customers',
        {
          onStageStart: (nodeId, index) => {
            setGlobalProgress(25 + Math.round((index / executable.length) * 65));
            setNodes((prev) =>
              prev.map((node) =>
                node.id === nodeId ? { ...node, status: 'running' as const, error: undefined } : node
              )
            );
          },
          onStageComplete: (nodeId, index, metrics) => {
            setGlobalProgress(25 + Math.round(((index + 1) / executable.length) * 65));
            setNodes((prev) =>
              prev.map((node) =>
                node.id === nodeId
                  ? {
                      ...node,
                      status: 'completed' as const,
                      rows: metrics.rowsOut > 0 ? String(metrics.rowsOut) : node.rows,
                      metrics,
                      error: undefined,
                    }
                  : node
              )
            );
          },
        }
      );

      applyNodeMetrics(result.executions);
      setGlobalProgress(100);
      setWorkspaceMessage('Run completed');
    } catch (err) {
      console.error('Run All failed', err);
      const message = err instanceof Error ? err.message : 'Run failed';
      setWorkspaceMessage('Run failed');
      setNodes((prev) =>
        prev.map((node) =>
          node.status === 'running' ? { ...node, status: 'failed' as const, error: message } : node
        )
      );
    } finally {
      setGlobalRunning(false);
      setTimeout(() => setGlobalProgress(0), 800);
    }
  }, [applyNodeMetrics, executableNodes, globalRunning, nodes]);

  /** "Run From Here" in DetailPanel: runs node + every node upstream of it. */
  const handleRunFromHere = useCallback(
    async (nodeId: string) => {
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
        await initDuckDB();
        await loadSampleData();
        const result = await runFullPipeline(
          chain.map((n) => ({
            id: n.id,
            type: n.type,
            config: {
              column: n.config.column,
              strategy: n.config.strategy,
              scope: n.config.scope,
              nullHandling: n.config.nullHandling,
            },
          })),
          'raw_customers',
          {
            onStageStart: (stageNodeId, index) => {
              setGlobalProgress(Math.round((index / chain.length) * 90));
              setNodes((prev) =>
                prev.map((node) =>
                  node.id === stageNodeId ? { ...node, status: 'running' as const } : node
                )
              );
            },
            onStageComplete: (stageNodeId, index, metrics) => {
              setGlobalProgress(Math.round(((index + 1) / chain.length) * 90));
              setNodes((prev) =>
                prev.map((node) =>
                  node.id === stageNodeId
                    ? {
                        ...node,
                        status: 'completed' as const,
                        rows: metrics.rowsOut > 0 ? String(metrics.rowsOut) : node.rows,
                        metrics,
                        error: undefined,
                      }
                    : node
                )
              );
            },
          }
        );
        applyNodeMetrics(result.executions);
        setGlobalProgress(100);
        setWorkspaceMessage('Run completed');
      } catch (err) {
        console.error('Run from here failed', err);
        const message = err instanceof Error ? err.message : 'Run failed';
        setWorkspaceMessage('Run failed');
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
    [applyNodeMetrics, executableNodes, nodes]
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
      setSelectedNode(nodes[index - 1]?.id ?? nodes[index + 1]?.id ?? 'n1');
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
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-white">
      <Header
        running={globalRunning}
        progress={globalProgress}
        onRunAll={handleRunAll}
        savedLabel={workspaceMessage}
        statusLabel={globalRunning ? 'Running' : 'Published'}
      />
      <div className="flex flex-1 overflow-hidden">
        <IconSidebar activeIcon={activeIcon} onIconClick={setActiveIcon} />
        <DatasetPanel
          onSelectDataset={(dataset) => {
            const datasetName = typeof dataset === 'string' ? dataset : dataset.name;
            handleUpdateNode('n1', { name: datasetName, description: 'Selected dataset', rows: '' });
            invalidateFromIndex(0);
          }}
        />
        <PipelineCanvas
          nodes={nodes}
          selectedNode={selectedNode}
          running={globalRunning}
          onRunAll={handleRunAll}
          onSelectNode={handleSelectNode}
          onAddNode={handleAddNode}
          onDeleteNode={handleDeleteNode}
        />
        {showDetail && selected && (
          <DetailPanel
            node={selected}
            nodes={nodes}
            onClose={handleCloseDetail}
            onDelete={handleDeleteNode}
            onDuplicate={handleDuplicateNode}
            onRun={handleRunFromHere}
            onUpdate={handleUpdateNode}
          />
        )}
      </div>
    </div>
  );
};

export default App;
