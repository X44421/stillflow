import React, { useState, useEffect } from 'react';
import {
  Plus,
  Play,
  LayoutGrid,
  Maximize2,
  Undo2,
  Redo2,
  Minus,
  ZoomIn,
  FileText,
  Filter,
  Copy,
  Type,
  Upload,
  CheckCircle2,
  Circle,
} from '../icons/hero';
import ObjectPalette from './ObjectPalette';
import { defaultConfigFor } from '../data';
import type { PipelineNode, NodeType } from '../types';

interface PipelineCanvasProps {
  nodes: PipelineNode[];
  selectedNode: string;
  running?: boolean;
  onRunAll?: () => void;
  onSelectNode: (nodeId: string) => void;
  onAddNode: (node: PipelineNode) => void;
  onDeleteNode: (nodeId: string) => void;
}

const NODE_ICON: Record<NodeType, React.ReactNode> = {
  source: (
    <div className="w-9 h-9 bg-green-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <FileText size={18} className="text-green-600" />
    </div>
  ),
  filter: (
    <div className="w-9 h-9 bg-purple-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Filter size={18} className="text-purple-600" />
    </div>
  ),
  deduplicate: (
    <div className="w-9 h-9 bg-teal-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Copy size={18} className="text-teal-600" />
    </div>
  ),
  normalize: (
    <div className="w-9 h-9 bg-orange-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Type size={18} className="text-orange-600" />
    </div>
  ),
  export: (
    <div className="w-9 h-9 bg-amber-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Upload size={18} className="text-amber-600" />
    </div>
  ),
};

const PipelineCanvas: React.FC<PipelineCanvasProps> = ({
  nodes,
  selectedNode,
  running = false,
  onRunAll,
  onSelectNode,
  onAddNode,
  onDeleteNode,
}) => {
  const [zoom, setZoom] = useState(100);
  const [showPalette, setShowPalette] = useState(false);
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.key === 'Delete' || e.key === 'Backspace') && selectedNode) {
        const tag = (e.target as HTMLElement).tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || (e.target as HTMLElement).isContentEditable) return;
        e.preventDefault();
        onDeleteNode(selectedNode);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [selectedNode, onDeleteNode]);

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle2 size={18} className="text-green-500" />;
      case 'running':
        return <div className="w-4 h-4 bg-gray-900 rounded-full animate-pulse-dot" />;
      case 'pending':
        return <Circle size={18} className="text-gray-300" />;
    }
  };

  const handlePaletteAdd = (obj: {
    name: string;
    description: string;
    icon: string;
  }) => {
    let type: NodeType = 'filter';
    if (obj.icon === 'file-text' || obj.icon === 'cloud' || obj.icon === 'database') type = 'source';
    else if (obj.icon === 'filter') type = 'filter';
    else if (obj.icon === 'copy') type = 'deduplicate';
    else if (obj.icon === 'type') type = 'normalize';
    else if (obj.icon === 'upload') type = 'export';

    const id = `n${Date.now()}`;
    onAddNode({
      id,
      type,
      name: obj.name,
      description: obj.description,
      rows: '',
      status: 'pending',
      config: defaultConfigFor(type),
    });
    setShowPalette(false);
  };

  return (
    <div className="flex-1 bg-gray-50 flex flex-col relative overflow-hidden">
      {/* Toolbar — top-left: edit tools */}
      <div className="absolute top-4 left-4 z-20">
        <div className="flex flex-col bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
          <button
            className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-700 transition-colors"
            title="Add transform node"
            onClick={() => setShowPalette((p) => !p)}
          >
            <Plus size={18} strokeWidth={1.5} />
          </button>
          <div className="w-full h-px bg-gray-100 my-0.5" />
          {[LayoutGrid, Undo2, Redo2].map((Icon, i) => (
            <button
              key={i}
              className="w-9 h-9 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
              title={['Auto layout', 'Undo', 'Redo'][i]}
            >
              <Icon size={18} strokeWidth={1.5} />
            </button>
          ))}
        </div>

        {/* Palette popover */}
        {showPalette && (
          <>
            <div className="fixed inset-0 z-10" onClick={() => setShowPalette(false)} />
            <div className="absolute top-0 left-full ml-3 z-20">
              <ObjectPalette onAdd={handlePaletteAdd} />
            </div>
          </>
        )}
      </div>

      {/* Top-right: execution */}
      {onRunAll && (
        <div className="absolute top-4 right-4 z-20">
          <button
            onClick={onRunAll}
            disabled={running}
            title="Run the full pipeline end to end"
            className="flex h-9 items-center gap-1.5 rounded-xl bg-white border border-gray-200 px-3.5 text-[13px] font-medium text-gray-700 shadow-sm hover:bg-gray-50 transition-colors disabled:opacity-55 disabled:cursor-wait"
          >
            <Play size={14} fill="currentColor" />
            {running ? 'Running…' : 'Run pipeline'}
          </button>
        </div>
      )}

      {/* Pipeline nodes - centered */}
      <div
        className="flex-1 flex items-center justify-center overflow-auto pt-24 pb-12"
        style={{ transform: `scale(${zoom / 100})`, transformOrigin: 'center center' }}
      >
        <div className="flex flex-col items-center flex-shrink-0">
          {nodes.map((node, index) => (
            <React.Fragment key={node.id}>
              <div
                onClick={() => onSelectNode(node.id)}
                className={`bg-white rounded-xl border px-4 py-3 flex items-center gap-3 min-w-[240px] max-w-[280px] cursor-pointer transition-all duration-150 ${
                  selectedNode === node.id
                    ? 'border-gray-900 shadow-lg ring-1 ring-gray-900'
                    : 'border-gray-200 shadow-sm hover:shadow-md hover:border-gray-300'
                }`}
              >
                {NODE_ICON[node.type]}
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] font-semibold text-gray-900">{node.name}</div>
                  <div className="text-[11px] text-gray-500">{node.description}</div>
                  {node.rows && (
                    <div className="text-[11px] text-gray-400 mt-0.5">{node.rows} rows</div>
                  )}
                </div>
                {getStatusIcon(node.status)}
              </div>
              {index < nodes.length - 1 && (
                <div className="flex flex-col items-center">
                  <div className="w-0.5 h-8 bg-gray-300" />
                  <svg width="10" height="8" viewBox="0 0 10 8" className="text-gray-300 -mt-px">
                    <path d="M5 8L0 0h10z" fill="currentColor" />
                  </svg>
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
      </div>

      {/* Zoom Controls - bottom-left */}
      <div className="absolute bottom-4 left-4 z-10 flex items-center bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
        <button
          onClick={() => setZoom((z) => Math.max(40, z - 10))}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
        >
          <Minus size={16} />
        </button>
        <span className="text-xs text-gray-600 font-medium px-2 min-w-[48px] text-center">{zoom}%</span>
        <button
          onClick={() => setZoom((z) => Math.min(200, z + 10))}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
        >
          <Plus size={16} />
        </button>
        <div className="w-px h-5 bg-gray-200 mx-0.5" />
        <button
          onClick={() => setZoom(100)}
          className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors"
          title="Reset zoom"
        >
          <ZoomIn size={16} />
        </button>
      </div>

      {/* Grid pattern background */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.03]"
        style={{
          backgroundImage: 'radial-gradient(circle, #000 1px, transparent 1px)',
          backgroundSize: '24px 24px',
        }}
      />
    </div>
  );
};

export default PipelineCanvas;
