import React from 'react';
import {
  Layers,
  Play,
  Sparkles,
  LayoutGrid,
  Settings,
  Clock,
  Grid3X3,
} from '../icons/hero';
import { CollapseButton } from '../icons';

interface IconSidebarProps {
  activeIcon?: number;
  onIconClick?: (index: number) => void;
}

const icons = [
  { icon: Layers, label: 'Layers' },
  { icon: Play, label: 'Run' },
  { icon: Sparkles, label: 'AI' },
  { icon: LayoutGrid, label: 'Grid' },
  { icon: Settings, label: 'Settings' },
  { icon: Clock, label: 'History' },
  { icon: Grid3X3, label: 'Components' },
];

const IconSidebar: React.FC<IconSidebarProps> = ({ activeIcon = 0, onIconClick = () => {} }) => {
  return (
    <div className="w-12 bg-white border-r border-gray-200 flex flex-col items-center py-2 flex-shrink-0">
      {icons.map((item, index) => {
        const Icon = item.icon;
        const isActive = activeIcon === index;
        return (
          <button
            key={index}
            onClick={() => onIconClick(index)}
            className={`w-9 h-9 flex items-center justify-center rounded-lg mb-0.5 transition-colors ${
              isActive
                ? 'bg-gray-100 text-gray-900'
                : 'text-gray-500 hover:bg-gray-50 hover:text-gray-700'
            }`}
            title={item.label}
          >
            <Icon size={18} strokeWidth={isActive ? 2 : 1.5} />
          </button>
        );
      })}
      <div className="flex-1" />
      <button className="w-9 h-9 flex items-center justify-center rounded-lg text-gray-500 hover:bg-gray-50 hover:text-gray-700 transition-colors mb-1" title="Collapse sidebar">
        <CollapseButton size={18} />
      </button>
      <div className="w-7 h-7 bg-gray-900 rounded-full flex items-center justify-center text-white text-xs font-semibold cursor-pointer">
        D
      </div>
    </div>
  );
};

export default IconSidebar;
