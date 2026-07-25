import React, { useState } from 'react';
import {
  Search,
  FileText,
  HardDrive,
  Database,
  Filter,
  Copy,
  Type,
  Upload,
  Sparkles,
  Plus,
} from '../icons/hero';
import { transformObjects } from '../data';

const iconMap: Record<string, React.ReactNode> = {
  'file-text': (
    <div className="w-9 h-9 bg-green-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <FileText size={18} className="text-green-600" />
    </div>
  ),
  'cloud': (
    <div className="w-9 h-9 bg-red-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <HardDrive size={18} className="text-red-500" />
    </div>
  ),
  'database': (
    <div className="w-9 h-9 bg-blue-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Database size={18} className="text-blue-600" />
    </div>
  ),
  'filter': (
    <div className="w-9 h-9 bg-purple-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Filter size={18} className="text-purple-600" />
    </div>
  ),
  'copy': (
    <div className="w-9 h-9 bg-teal-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Copy size={18} className="text-teal-600" />
    </div>
  ),
  'type': (
    <div className="w-9 h-9 bg-orange-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Type size={18} className="text-orange-600" />
    </div>
  ),
  'upload': (
    <div className="w-9 h-9 bg-amber-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Upload size={18} className="text-amber-600" />
    </div>
  ),
  'sparkles': (
    <div className="w-9 h-9 bg-violet-50 rounded-lg flex items-center justify-center flex-shrink-0">
      <Sparkles size={18} className="text-violet-600" />
    </div>
  ),
};

interface ObjectPaletteProps {
  onAdd?: (obj: { id: string; name: string; description: string; category: string; icon: string }) => void;
}

const ObjectPalette: React.FC<ObjectPaletteProps> = ({ onAdd }) => {
  const [activeTab, setActiveTab] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState('');

  const tabs = [
    { key: 'all', label: 'All' },
    { key: 'source', label: 'Source' },
    { key: 'transform', label: 'Transform' },
    { key: 'output', label: 'Output' },
    { key: 'ai', label: 'AI' },
  ];

  const filteredObjects = transformObjects.filter(obj => {
    const matchesTab = activeTab === 'all' || obj.category === activeTab;
    const matchesSearch = searchQuery === '' ||
      obj.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      obj.description.toLowerCase().includes(searchQuery.toLowerCase());
    return matchesTab && matchesSearch;
  });

  return (
    <div className="w-[220px] bg-white border border-gray-200 rounded-xl shadow-sm flex flex-col max-h-[520px] overflow-hidden">
      <div className="p-3 pb-2">
        <div className="relative mb-3">
          <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search object"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full h-8 pl-8 pr-3 text-[13px] border border-gray-200 rounded-lg bg-gray-50 focus:bg-white focus:border-gray-300 focus:outline-none transition-colors placeholder:text-gray-400"
          />
        </div>
        <div className="flex gap-1 flex-wrap">
          {tabs.map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`text-[12px] font-medium px-2.5 py-1 rounded-full transition-all ${
                activeTab === tab.key
                  ? 'bg-gray-900 text-white'
                  : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-2 pb-2">
        {filteredObjects.map(obj => (
          <div
            key={obj.id}
            onClick={() => onAdd?.(obj)}
            className="flex items-center gap-2.5 px-2 py-2.5 hover:bg-gray-100 rounded-lg cursor-pointer transition-colors group"
            title={`Add ${obj.name} to pipeline`}
          >
            {iconMap[obj.icon] ?? (
              <div className="w-9 h-9 bg-gray-50 rounded-lg flex items-center justify-center flex-shrink-0">
                <FileText size={18} className="text-gray-400" />
              </div>
            )}
            <div className="min-w-0 flex-1">
              <div className="text-[13px] font-medium text-gray-900">{obj.name}</div>
              <div className="text-[11px] text-gray-500">{obj.description}</div>
            </div>
            <Plus
              size={14}
              className="opacity-0 group-hover:opacity-100 text-gray-400 transition-opacity mr-1"
            />
          </div>
        ))}
      </div>
    </div>
  );
};

export default ObjectPalette;
