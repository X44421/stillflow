import React, { useState, useMemo, useRef } from 'react';
import { Search, MoreHorizontal, ChevronDown, ChevronRight, FileText, Database, HardDrive } from '../icons/hero';
import type { Dataset } from '../types';
import { datasets as fallbackDatasets } from '../data';

const typeIconMap: Record<string, React.ReactNode> = {
  csv: (
    <div className="w-8 h-8 bg-green-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <FileText size={16} className="text-green-600" />
    </div>
  ),
  parquet: (
    <div className="w-8 h-8 bg-blue-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Database size={16} className="text-blue-600" />
    </div>
  ),
  excel: (
    <div className="w-8 h-8 bg-emerald-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <FileText size={16} className="text-emerald-600" />
    </div>
  ),
  s3: (
    <div className="w-8 h-8 bg-red-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <HardDrive size={16} className="text-red-500" />
    </div>
  ),
  table: (
    <div className="w-8 h-8 bg-purple-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Database size={16} className="text-purple-600" />
    </div>
  ),
};

const typeLabel: Record<string, string> = {
  csv: 'CSV',
  parquet: 'Parquet',
  excel: 'Excel',
  s3: 'S3 Folder',
  table: 'Table',
};

interface DatasetPanelProps {
  datasets?: Dataset[];
  selectedId?: string | null;
  importing?: boolean;
  onSelectDataset?: (dataset: Dataset) => void;
  onImportCsv?: (file: File) => Promise<void>;
  onRenameDataset?: (dataset: Dataset) => void | Promise<void>;
  onDeleteDataset?: (dataset: Dataset) => void | Promise<void>;
}

const DatasetPanel: React.FC<DatasetPanelProps> = ({
  datasets: externalDatasets,
  selectedId: controlledSelectedId,
  importing = false,
  onSelectDataset,
  onImportCsv,
  onRenameDataset,
  onDeleteDataset,
}) => {
  const datasets = externalDatasets ?? fallbackDatasets;
  const [activeTab, setActiveTab] = useState<'all' | 'source' | 'interim' | 'output'>('all');
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedSections, setExpandedSections] = useState({
    source: true,
    interim: true,
    output: true,
  });
  const [localSelectedId, setLocalSelectedId] = useState<string | null>(null);
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const selectedId =
    controlledSelectedId === undefined ? localSelectedId : controlledSelectedId;

  const tabs = [
    { key: 'all' as const, label: 'All' },
    { key: 'source' as const, label: 'Source' },
    { key: 'interim' as const, label: 'Interim' },
    { key: 'output' as const, label: 'Output' },
  ];

  const toggleSection = (section: 'source' | 'interim' | 'output') => {
    setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }));
  };

  const filteredDatasets = useMemo(() => {
    const inTab = activeTab === 'all' ? datasets : datasets.filter(d => d.category === activeTab);
    if (searchQuery.trim() === '') return inTab;
    const q = searchQuery.toLowerCase();
    return inTab.filter(
      d =>
        d.name.toLowerCase().includes(q) ||
        d.size.toLowerCase().includes(q) ||
        d.type.toLowerCase().includes(q)
    );
  }, [activeTab, datasets, searchQuery]);

  const sourceDatasets = filteredDatasets.filter(d => d.category === 'source');
  const interimDatasets = filteredDatasets.filter(d => d.category === 'interim');
  const outputDatasets = filteredDatasets.filter(d => d.category === 'output');

  const renderSection = (
    title: string,
    items: typeof datasets,
    key: 'source' | 'interim' | 'output'
  ) => {
    if (items.length === 0) return null;
    const isExpanded = expandedSections[key];
    return (
      <div className="mb-1">
        <button
          onClick={() => toggleSection(key)}
          className="flex items-center justify-between w-full px-3 py-2 text-xs font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded-md transition-colors"
        >
          <div className="flex items-center gap-1.5">
            {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            <span className="uppercase tracking-wider">{title}</span>
          </div>
          <span className="text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded text-[11px]">{items.length}</span>
        </button>
        {isExpanded && (
          <div className="mt-0.5">
            {items.map(dataset => (
              <div
                key={dataset.id}
                onClick={() => {
                  setLocalSelectedId(dataset.id);
                  onSelectDataset?.(dataset);
                }}
                className={`group relative flex items-center gap-2.5 px-3 py-2 rounded-lg cursor-pointer transition-colors mx-1 ${
                  selectedId === dataset.id
                    ? 'bg-gray-100 ring-1 ring-gray-300'
                    : 'hover:bg-gray-50'
                }`}
              >
                {typeIconMap[dataset.type]}
                <div className="flex-1 min-w-0">
                  <div className="text-[13px] font-medium text-gray-900 truncate">{dataset.name}</div>
                  <div className="text-[11px] text-gray-500">
                    {typeLabel[dataset.type]} · {dataset.size}
                  </div>
                </div>
                <button
                  type="button"
                  className="opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-200 rounded transition-all"
                  aria-label={`Manage ${dataset.name}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    if (!dataset.projectId) return;
                    setOpenMenuId((current) =>
                      current === dataset.id ? null : dataset.id
                    );
                  }}
                >
                  <MoreHorizontal size={14} className="text-gray-500" />
                </button>
                {openMenuId === dataset.id && (
                  <>
                    <button
                      type="button"
                      className="fixed inset-0 z-20 cursor-default"
                      aria-label="Close dataset menu"
                      onClick={(event) => {
                        event.stopPropagation();
                        setOpenMenuId(null);
                      }}
                    />
                    <div className="absolute right-1 top-9 z-30 w-28 rounded-lg border border-gray-200 bg-white p-1 shadow-lg">
                      <button
                        type="button"
                        className="w-full rounded-md px-2 py-1.5 text-left text-[12px] text-gray-700 hover:bg-gray-50"
                        onClick={(event) => {
                          event.stopPropagation();
                          setOpenMenuId(null);
                          void onRenameDataset?.(dataset);
                        }}
                      >
                        Rename
                      </button>
                      <button
                        type="button"
                        className="w-full rounded-md px-2 py-1.5 text-left text-[12px] text-red-600 hover:bg-red-50"
                        onClick={(event) => {
                          event.stopPropagation();
                          setOpenMenuId(null);
                          void onDeleteDataset?.(dataset);
                        }}
                      >
                        Delete
                      </button>
                    </div>
                  </>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className="w-[272px] bg-white border-r border-gray-200 flex flex-col flex-shrink-0 overflow-hidden">
      <div className="p-3 pb-2">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-[15px] font-semibold text-gray-900">Datasets</h2>
          <div className="flex items-center gap-1">
            <button
              className="w-7 h-7 flex items-center justify-center text-gray-500 hover:bg-gray-100 rounded-md transition-colors text-lg font-light"
              title={importing ? 'Importing CSV' : 'Import CSV'}
              disabled={importing}
              onClick={() => {
                if (onImportCsv) {
                  fileInputRef.current?.click();
                } else {
                  setSearchQuery('');
                }
              }}
            >
              +
            </button>
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,text/csv"
              className="hidden"
              onChange={(event) => {
                const file = event.target.files?.[0];
                event.target.value = '';
                if (file && onImportCsv) void onImportCsv(file);
              }}
            />
            <button className="w-7 h-7 flex items-center justify-center text-gray-500 hover:bg-gray-100 rounded-md transition-colors" title="Grid view">
              <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5">
                <rect x="1" y="1" width="5" height="5" rx="1" />
                <rect x="8" y="1" width="5" height="5" rx="1" />
                <rect x="1" y="8" width="5" height="5" rx="1" />
                <rect x="8" y="8" width="5" height="5" rx="1" />
              </svg>
            </button>
          </div>
        </div>
        <div className="relative mb-3">
          <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search datasets"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full h-8 pl-8 pr-10 text-[13px] border border-gray-200 rounded-lg bg-gray-50 focus:bg-white focus:border-gray-300 focus:outline-none transition-colors placeholder:text-gray-400"
          />
          <span className="absolute right-2.5 top-1/2 -translate-y-1/2 text-[11px] text-gray-400 bg-gray-100 px-1.5 py-0.5 rounded border border-gray-200">
            ⌘K
          </span>
        </div>
        <div className="flex gap-0.5 bg-gray-100 p-0.5 rounded-lg">
          {tabs.map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`flex-1 text-[12px] font-medium py-1.5 rounded-md transition-all ${
                activeTab === tab.key
                  ? 'bg-white text-gray-900 shadow-sm'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-1 pb-3">
        {filteredDatasets.length === 0 ? (
          <div className="px-3 py-6 text-center text-[12px] text-gray-400">
            {searchQuery.trim()
              ? `No datasets match "${searchQuery}".`
              : 'No datasets yet.'}
          </div>
        ) : (
          <>
            {renderSection('Source', sourceDatasets, 'source')}
            {renderSection('Interim', interimDatasets, 'interim')}
            {renderSection('Output', outputDatasets, 'output')}
          </>
        )}
      </div>
    </div>
  );
};

export default DatasetPanel;
