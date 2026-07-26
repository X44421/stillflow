import React, { useState } from 'react';
import {
  Search,
  FileText,
  Filter,
  Copy,
  Type,
  Upload,
  Plus,
} from '../icons/hero';
import { transformObjects } from '../data';

const iconMap: Record<string, React.ReactNode> = {
  'filter': (
    <div className="w-9 h-9 bg-[#e8f7fe] rounded-lg flex items-center justify-center flex-shrink-0">
      <Filter size={18} className="text-[#0b6c96]" />
    </div>
  ),
  'copy': (
    <div className="w-9 h-9 bg-[#e8f7fe] rounded-lg flex items-center justify-center flex-shrink-0">
      <Copy size={18} className="text-[#0b6c96]" />
    </div>
  ),
  'type': (
    <div className="w-9 h-9 bg-[#e8f7fe] rounded-lg flex items-center justify-center flex-shrink-0">
      <Type size={18} className="text-[#0b6c96]" />
    </div>
  ),
  'upload': (
    <div className="w-9 h-9 bg-[#e8f7fe] rounded-lg flex items-center justify-center flex-shrink-0">
      <Upload size={18} className="text-[#0b6c96]" />
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
    { key: 'transform', label: 'Transform' },
    { key: 'output', label: 'Output' },
  ];

  const filteredObjects = transformObjects.filter(obj => {
    const matchesTab = activeTab === 'all' || obj.category === activeTab;
    const matchesSearch = searchQuery === '' ||
      obj.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      obj.description.toLowerCase().includes(searchQuery.toLowerCase());
    return matchesTab && matchesSearch;
  });

  return (
    <div className="w-[280px] bg-white border border-[#e3e6e8] rounded-xl shadow-[0_8px_24px_rgba(32,33,36,.16)] flex flex-col max-h-[480px] overflow-hidden">
      <div className="p-3 pb-2">
        <div className="relative">
          <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search nodes…"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full h-8 pl-8 pr-3 text-[13px] border border-[#dadce0] rounded-full bg-[#f1f3f4] focus:bg-white focus:border-[#20beff] focus:ring-1 focus:ring-[#20beff] focus:outline-none transition-colors placeholder:text-[#80868b]"
          />
        </div>
        <div className="flex gap-1 mt-2.5 flex-wrap">
          {tabs.map(tab => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`text-[11px] font-medium px-2.5 py-1 rounded-full transition-all ${
                activeTab === tab.key
                  ? 'bg-[#e8f7fe] text-[#0b6c96]'
                  : 'bg-[#f1f3f4] text-[#5f6368] hover:bg-[#e3e6e8]'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto px-1.5 pb-2 space-y-0.5">
        {filteredObjects.map(obj => (
          <div
            key={obj.id}
            onClick={() => onAdd?.(obj)}
            className="flex items-center gap-2.5 px-2.5 py-2.5 hover:bg-[#f1f3f4] rounded-lg cursor-pointer transition-colors group"
            title={`Add ${obj.name}`}
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
            <Plus size={14} className="opacity-0 group-hover:opacity-100 text-gray-400 transition-opacity mr-1 flex-shrink-0" />
          </div>
        ))}
      </div>
    </div>
  );
};

export default ObjectPalette;
