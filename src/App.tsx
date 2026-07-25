import React, { useState } from 'react';
import Header from './components/Header';
import IconSidebar from './components/IconSidebar';
import DatasetPanel from './components/DatasetPanel';
import PipelineCanvas from './components/PipelineCanvas';
import DetailPanel from './components/DetailPanel';

const App: React.FC = () => {
  const [activeIcon, setActiveIcon] = useState(0);
  const [selectedNode, setSelectedNode] = useState('n3');
  const [showDetail, setShowDetail] = useState(true);

  const handleSelectNode = (nodeId: string) => {
    setSelectedNode(nodeId);
    setShowDetail(true);
  };

  return (
    <div className="h-screen w-screen flex flex-col overflow-hidden bg-white">
      <Header />
      <div className="flex flex-1 overflow-hidden">
        <IconSidebar activeIcon={activeIcon} onIconClick={setActiveIcon} />
        <DatasetPanel />
        <PipelineCanvas
          selectedNode={selectedNode}
          onSelectNode={handleSelectNode}
        />
        {showDetail && selectedNode === 'n3' && (
          <DetailPanel onClose={() => setShowDetail(false)} />
        )}
      </div>
    </div>
  );
};

export default App;
