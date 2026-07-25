import React from 'react';
import type { WorkspaceEvent } from '../types';

interface ActivityPanelProps {
  events: WorkspaceEvent[];
}

const ActivityPanel: React.FC<ActivityPanelProps> = ({ events }) => {
  if (events.length === 0) return null;
  return (
    <div className="border-t border-gray-200 bg-white px-3 py-2 text-[11px] text-gray-500">
      {events.length} event{events.length !== 1 ? 's' : ''}
    </div>
  );
};

export default ActivityPanel;
