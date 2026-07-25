import React, { useState, useCallback } from 'react';
import Header from './components/Header';
import IconSidebar from './components/IconSidebar';
import DatasetPanel from './components/DatasetPanel';
import PipelineCanvas from './components/PipelineCanvas';
import DetailPanel from './components/DetailPanel';
import { initialPipelineNodes } from './data';
import type { PipelineNode } from './types';
import { initDuckDB, loadSampleData, runFullPipeline, type PipelineMetrics } from './utils/duckdb';

const App: React.FC = () => {
  const [activeIcon, setActiveIcon] = useState(0);
  const [nodes, setNodes] = useState<PipelineNode[]>(initialPipelineNodes);
  const [selectedNode, setSelectedNode] = useState('n3');
  const [showDetail, setShowDetail] = useState(true);
  const [globalRunning, setGlobalRunning] = useState(false);
  const [globalProgress, setGlobalProgress] = useState(0);

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

  /** Header "Run All" runs every node, updating status + progress along the way. */
  const handleRunAll = useCallback(async () => {
    if (globalRunning) return;
    setGlobalRunning(true);
    setGlobalProgress(0);

    // Mark all as pending, then run sequentially
    setNodes((prev) => prev.map((n) => ({ ...n, status: 'pending' as const })));

    try {
      await initDuckDB();
      setGlobalProgress(10);
      await loadSampleData();
      setGlobalProgress(25);

      const result = await runFullPipeline(
        nodes.map((n) => ({
          id: n.id,
          type: n.type,
          config: {
            column: n.config.column,
            strategy: n.config.strategy,
            scope: n.config.scope,
            nullHandling: n.config.nullHandling,
          },
        }))
      );

      result.executions.forEach((_, i) => {
        const pct = 25 + Math.round(((i + 1) / result.executions.length) * 70);
        setGlobalProgress(pct);
      });

      applyNodeMetrics(result.executions);
      setGlobalProgress(100);
    } catch (err) {
      console.error('Run All failed', err);
    } finally {
      setGlobalRunning(false);
      setTimeout(() => setGlobalProgress(0), 800);
    }
  }, [globalRunning, nodes, applyNodeMetrics]);

  /** "Run From Here" in DetailPanel: runs node + every node upstream of it. */
  const handleRunFromHere = useCallback(
    async (nodeId: string) => {
      const idx = nodes.findIndex((n) => n.id === nodeId);
      if (idx < 0) return;
      const chain = nodes.slice(0, idx + 1);

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
          }))
        );
        applyNodeMetrics(result.executions);
      } catch (err) {
        console.error('Run from here failed', err);
      }
    },
    [nodes, applyNodeMetrics]
  );

  const handleUpdateNode = useCallback((nodeId: string, patch: Partial<PipelineNode>) => {
    setNodes((prev) => prev.map((n) => (n.id === nodeId ? { ...n, ...patch } : n)));
  }, []);

  const handleDeleteNode = useCallback(
    (nodeId: string) => {
      setNodes((prev) => prev.filter((n) => n.id !== nodeId));
      setSelectedNode('n1');
    },
    []
  );

  const handleAddNode = useCallback((node: PipelineNode) => {
    setNodes((prev) => [...prev, node]);
  }, []);

  const selected = nodes.find((n) => n.id === selectedNode) ?? null;

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-white">
      <Header running={globalRunning} progress={globalProgress} onRunAll={handleRunAll} />
      <div className="flex flex-1 overflow-hidden">
        <IconSidebar activeIcon={activeIcon} onIconClick={setActiveIcon} />
        <DatasetPanel
          onSelectDataset={(name) => {
            // Set first source node's "rows" to reflect chosen dataset (demo wiring)
            handleUpdateNode('n1', { description: name, rows: '' });
          }}
        />
        <PipelineCanvas
          nodes={nodes}
          selectedNode={selectedNode}
          onSelectNode={handleSelectNode}
          onAddNode={handleAddNode}
        />
        {showDetail && selected && (
          <DetailPanel
            node={selected}
            nodes={nodes}
            onClose={handleCloseDetail}
            onDelete={handleDeleteNode}
            onRun={handleRunFromHere}
            onUpdate={handleUpdateNode}
          />
        )}
      </div>
    </div>
  );
};

export default App;