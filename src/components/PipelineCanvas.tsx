import React, { useState } from 'react';
import {
  Plus,
  Play,
  Sparkles,
  LayoutGrid,
  Settings,
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
  Table,
} from 'lucide-react';
import ObjectPalette from './ObjectPalette';
import CSVPreview from './CSVPreview';

interface PipelineCanvasProps {
  selectedNode: string;
  onSelectNode: (nodeId: string) => void;
}

const PipelineCanvas: React.FC<PipelineCanvasProps> = ({ selectedNode, onSelectNode }) => {
  const [showCSV, setShowCSV] = useState(false);
  const nodes = [
    {
      id: 'n1',
      icon: (
        <div className="w-9 h-9 bg-green-50 rounded-lg flex items-center justify-center flex-shrink-0">
          <FileText size={18} className="text-green-600" />
        </div>
      ),
      name: 'raw_customers.csv',
      description: 'CSV File · 2.4M rows',
      status: 'completed' as const,
    },
    {
      id: 'n2',
      icon: (
        <div className="w-9 h-9 bg-purple-50 rounded-lg flex items-center justify-center flex-shrink-0">
          <Filter size={18} className="text-purple-600" />
        </div>
      ),
      name: 'Filter',
      description: 'Keep valid customers',
      rows: '1.8M rows',
      status: 'completed' as const,
    },
    {
      id: 'n3',
      icon: (
        <div className="w-9 h-9 bg-teal-50 rounded-lg flex items-center justify-center flex-shrink-0">
          <Copy size={18} className="text-teal-600" />
        </div>
      ),
      name: 'Deduplicate',
      description: 'Remove repeated records',
      rows: '1.2M rows',
      status: 'running' as const,
    },
    {
      id: 'n4',
      icon: (
        <div className="w-9 h-9 bg-orange-50 rounded-lg flex items-center justify-center flex-shrink-0">
          <Type size={18} className="text-orange-600" />
        </div>
      ),
      name: 'Normalize Text',
      description: 'Standardize name & email',
      rows: '1.2M rows',
      status: 'pending' as const,
    },
    {
      id: 'n5',
      icon: (
        <div className="w-9 h-9 bg-amber-50 rounded-lg flex items-center justify-center flex-shrink-0">
          <Upload size={18} className="text-amber-600" />
        </div>
      ),
      name: 'Export CSV',
      description: 'Write cleaned data',
      rows: '1.2M rows',
      status: 'pending' as const,
    },
  ];

  const getStatusIcon = (status: 'completed' | 'running' | 'pending') => {
    switch (status) {
      case 'completed':
        return <CheckCircle2 size={18} className="text-green-500" />;
      case 'running':
        return (
          <div className="w-4 h-4 bg-gray-900 rounded-full animate-pulse-dot" />
        );
      case 'pending':
        return <Circle size={18} className="text-gray-300" />;
    }
  };

  return (
    <div className="flex-1 bg-gray-50 flex flex-col relative overflow-hidden">
      {/* Toolbar */}
      <div className="absolute top-4 left-1/2 -translate-x-1/2 z-10 flex items-center bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
        {[Plus, Play, Sparkles, LayoutGrid, Settings, Maximize2, Undo2, Redo2].map((Icon, i) => (
          <button
            key={i}
            className={`w-9 h-9 flex items-center justify-center rounded-lg transition-colors ${
              i === 0 ? 'hover:bg-gray-100 text-gray-700' : 'hover:bg-gray-100 text-gray-500'
            }`}
          >
            <Icon size={18} strokeWidth={1.5} />
          </button>
        ))}
        <div className="w-px h-5 bg-gray-200 mx-0.5" />
        <button
          onClick={() => setShowCSV(!showCSV)}
          className={`w-9 h-9 flex items-center justify-center rounded-lg transition-colors ${
            showCSV ? 'bg-gray-900 text-white' : 'hover:bg-gray-100 text-gray-500'
          }`}
          title="CSV Preview"
        >
          <Table size={18} strokeWidth={1.5} />
        </button>
      </div>

      {/* CSV Preview Overlay */}
      {showCSV && (
        <div className="absolute inset-0 z-20 bg-white/95 backdrop-blur-sm flex items-center justify-center p-8">
          <div className="w-full max-w-4xl h-full max-h-[600px] border border-gray-200 rounded-2xl shadow-xl flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-4 py-2 border-b border-gray-200 flex-shrink-0">
              <span className="text-xs font-semibold text-gray-900">CSV Preview</span>
              <button
                onClick={() => setShowCSV(false)}
                className="w-6 h-6 flex items-center justify-center rounded hover:bg-gray-100 transition-colors"
              >
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><path d="M1 1l12 12M13 1L1 13" /></svg>
              </button>
            </div>
            <div className="flex-1 overflow-y-auto">
              <CSVPreview />
            </div>
          </div>
        </div>
      )}

      {/* Canvas with pipeline and object palette side by side */}
      <div className="flex-1 flex items-start justify-center pt-20 pb-16 overflow-auto">
        {/* Object Palette */}
        <div className="mt-4 mr-6 flex-shrink-0">
          <ObjectPalette />
        </div>

        {/* Pipeline Nodes */}
        <div className="flex flex-col items-center flex-shrink-0 mt-4">
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
                {node.icon}
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] font-semibold text-gray-900">{node.name}</div>
                  <div className="text-[11px] text-gray-500">{node.description}</div>
                  {node.rows && (
                    <div className="text-[11px] text-gray-400 mt-0.5">{node.rows}</div>
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

      {/* Zoom Controls */}
      <div className="absolute bottom-4 left-1/2 -translate-x-1/2 flex items-center bg-white border border-gray-200 rounded-xl shadow-sm px-1 py-1 gap-0.5">
        <button className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors">
          <Minus size={16} />
        </button>
        <span className="text-xs text-gray-600 font-medium px-2 min-w-[48px] text-center">100%</span>
        <button className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors">
          <Plus size={16} />
        </button>
        <div className="w-px h-5 bg-gray-200 mx-0.5" />
        <button className="w-8 h-8 flex items-center justify-center rounded-lg hover:bg-gray-100 text-gray-500 transition-colors">
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
