import { useCallback, useEffect, useRef, useState } from 'react';
import {
  Drawer,
  DrawerColorVariant,
  DrawerContent,
  DrawerPanelContent,
  Page,
  PageSection,
} from '@patternfly/react-core';
import { ObjectTabBar } from './components/ObjectTabBar';
import { WorkspaceSidebar } from './components/WorkspaceSidebar';
import { TopologyCanvas } from './components/TopologyCanvas';
import { PreviewWorkspace } from './components/PreviewWorkspace';
import { Inspector } from './components/Inspector';
import { useMediaQuery } from './hooks/useMediaQuery';
import type { PreviewTab, RunStatus, TabItem } from './types';
import { connections, initialTabs, pipelineNodes } from './data';

export default function App() {
  const isNarrow = useMediaQuery('(max-width: 1024px)');

  const [tabs, setTabs] = useState<TabItem[]>(initialTabs);
  const [activeTabId, setActiveTabId] = useState('dataset-customers');
  const [activeNavId, setActiveNavId] = useState('dataset-customers');
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [previewTab, setPreviewTab] = useState<PreviewTab>('data');
  const [previewOpen, setPreviewOpen] = useState(true);
  const [inspectorOpen, setInspectorOpen] = useState(true);

  const [isRunning, setIsRunning] = useState(false);
  const [progress, setProgress] = useState(0);
  const [error, setError] = useState(false);
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, RunStatus>>({});
  const runInterval = useRef<ReturnType<typeof setInterval> | null>(null);

  const [breadcrumb, setBreadcrumb] = useState('Customer cleanup');
  const [previewTitle, setPreviewTitle] = useState('customers.csv');
  const [previewMeta, setPreviewMeta] = useState('80,000 rows | 13 columns');
  const [inspectorObjTitle, setInspectorObjTitle] = useState('Customer cleanup');
  const [inspectorObjType, setInspectorObjType] = useState('Pipeline');

  const selectNode = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
    const node = pipelineNodes.find((candidate) => candidate.id === nodeId);
    if (node) {
      setBreadcrumb(node.title);
      setInspectorObjTitle(node.title);
      setInspectorObjType(node.subtitle);
      setPreviewTitle(`${node.title} output`);
      setPreviewMeta('12,418 rows changed | 0 rejected');
    }
  }, []);

  const deselectAllNodes = useCallback(() => {
    setSelectedNodeId(null);
    setBreadcrumb('Customer cleanup');
    setInspectorObjTitle('Customer cleanup');
    setInspectorObjType('Pipeline');
    setPreviewTitle('customers.csv');
    setPreviewMeta('80,000 rows | 13 columns');
  }, []);

  const finishRun = useCallback((success: boolean, targetId: string) => {
    if (runInterval.current) {
      window.clearInterval(runInterval.current);
      runInterval.current = null;
    }
    setIsRunning(false);
    setError(!success);
    setNodeStatuses((previous) => ({
      ...previous,
      [targetId]: success ? 'ready' : 'error',
    }));
    if (success) {
      window.setTimeout(() => setProgress(0), 800);
    }
  }, []);

  const startRun = useCallback(
    (targetId: string) => {
      if (isRunning) {
        return;
      }
      setIsRunning(true);
      setError(false);
      setProgress(0);
      setNodeStatuses((previous) => ({ ...previous, [targetId]: 'running' }));
      let value = 0;
      runInterval.current = window.setInterval(() => {
        value += Math.random() * 7 + 3;
        if (value >= 100) {
          value = 100;
          setProgress(100);
          finishRun(true, targetId);
          return;
        }
        setProgress(value);
      }, 250);
    },
    [isRunning, finishRun]
  );

  useEffect(() => {
    return () => {
      if (runInterval.current) {
        window.clearInterval(runInterval.current);
      }
    };
  }, []);

  const handleRun = useCallback(() => {
    const targetId = selectedNodeId ?? '2';
    if (!selectedNodeId) {
      selectNode(targetId);
    }
    startRun(targetId);
  }, [selectedNodeId, selectNode, startRun]);

  const handleCancelRun = useCallback(() => {
    finishRun(false, selectedNodeId ?? '2');
  }, [selectedNodeId, finishRun]);

  const handleTabSelect = useCallback((id: string) => {
    setActiveTabId(id);
    if (id === 'dataset-customers') {
      setBreadcrumb('customers.csv');
      setInspectorObjTitle('customers.csv');
      setInspectorObjType('Dataset');
      setPreviewTitle('customers.csv');
      setPreviewMeta('80,000 rows | 13 columns');
      setActiveNavId('dataset-customers');
    } else if (id === 'session-cleanup') {
      setBreadcrumb('Customer cleanup');
      setInspectorObjTitle('Customer cleanup');
      setInspectorObjType('Pipeline');
      setPreviewTitle('customers.csv');
      setPreviewMeta('80,000 rows | 13 columns');
      setActiveNavId('session-cleanup');
    }
  }, []);

  const handleTabClose = useCallback(
    (id: string) => {
      setTabs((previous) => previous.filter((tab) => tab.id !== id));
      if (activeTabId === id) {
        const remaining = tabs.find((tab) => tab.id !== id);
        if (remaining) {
          handleTabSelect(remaining.id);
        }
      }
    },
    [activeTabId, tabs, handleTabSelect]
  );

  const handleAddTab = useCallback(() => {
    const newTab: TabItem = {
      id: `tab-${Date.now()}`,
      label: 'new_session',
      unsaved: true,
    };
    setTabs((previous) => [...previous, newTab]);
    setActiveTabId(newTab.id);
    setBreadcrumb('New session');
    setInspectorObjTitle('New session');
    setInspectorObjType('Pipeline');
    setPreviewTitle('new_session.csv');
    setPreviewMeta('No rows loaded');
  }, []);

  const handleNavSelect = useCallback((id: string) => {
    setActiveNavId(id);
    if (id === 'pipeline-cleanup') {
      setBreadcrumb('Customer cleanup');
      setInspectorObjTitle('Customer cleanup');
      setInspectorObjType('Pipeline');
      setPreviewTitle('customers.csv');
      setPreviewMeta('80,000 rows | 13 columns');
      setActiveTabId('session-cleanup');
    } else if (id === 'dataset-customers') {
      setBreadcrumb('customers.csv');
      setInspectorObjTitle('customers.csv');
      setInspectorObjType('Dataset');
      setPreviewTitle('customers.csv');
      setPreviewMeta('80,000 rows | 13 columns');
      setActiveTabId('dataset-customers');
    } else if (id === 'dataset-clean') {
      setBreadcrumb('customer_clean.csv');
      setInspectorObjTitle('customer_clean.csv');
      setInspectorObjType('Dataset');
      setPreviewTitle('customer_clean.csv');
      setPreviewMeta('78,412 rows | 13 columns');
    }
  }, []);

  const previewPanel = (
    <DrawerPanelContent
      isResizable
      defaultSize={isNarrow ? '45%' : '42%'}
      minSize={isNarrow ? '280px' : '480px'}
      maxSize={isNarrow ? '70%' : '75%'}
      colorVariant={DrawerColorVariant.default}
      resizeAriaLabel="Resize preview panel"
    >
      <div className="still-preview-panel">
        <PreviewWorkspace
          activeTab={previewTab}
          onTabChange={setPreviewTab}
          title={previewTitle}
          meta={previewMeta}
          isRunning={isRunning}
          progress={progress}
        />
      </div>
    </DrawerPanelContent>
  );

  const inspectorPanel = (
    <DrawerPanelContent
      isResizable
      defaultSize={isNarrow ? '300px' : '336px'}
      minSize="280px"
      maxSize="440px"
      colorVariant={DrawerColorVariant.secondary}
      resizeAriaLabel="Resize inspector panel"
    >
      <Inspector
        objectTitle={inspectorObjTitle}
        objectType={inspectorObjType}
        isRunning={isRunning}
        progress={progress}
        error={error}
        onRunNode={handleRun}
        onCancelRun={handleCancelRun}
        onValidate={() => setError(false)}
        onClose={() => setInspectorOpen(false)}
      />
    </DrawerPanelContent>
  );

  return (
    <Page
      className="still-page"
      masthead={
        <ObjectTabBar
          tabs={tabs}
          activeTabId={activeTabId}
          isRunning={isRunning}
          previewOpen={previewOpen}
          inspectorOpen={inspectorOpen}
          onSelect={handleTabSelect}
          onClose={handleTabClose}
          onAdd={handleAddTab}
          onRun={handleRun}
          onCancel={handleCancelRun}
          onTogglePreview={() => setPreviewOpen((open) => !open)}
          onToggleInspector={() => setInspectorOpen((open) => !open)}
        />
      }
      sidebar={<WorkspaceSidebar activeNavId={activeNavId} onSelect={handleNavSelect} />}
      isManagedSidebar
      isContentFilled
    >
      <PageSection
        hasBodyWrapper={false}
        isFilled
        padding={{ default: 'noPadding' }}
        className="still-workspace-section"
        aria-label="StillFlow workspace"
      >
        <Drawer isExpanded={inspectorOpen} isInline position="end" className="still-workspace-drawer">
          <DrawerContent panelContent={inspectorPanel}>
            <Drawer
              isExpanded={previewOpen}
              isInline
              position={isNarrow ? 'bottom' : 'end'}
              className="still-preview-drawer"
            >
              <DrawerContent panelContent={previewPanel}>
                <TopologyCanvas
                  nodes={pipelineNodes}
                  edges={connections}
                  selectedNodeId={selectedNodeId}
                  onSelectNode={selectNode}
                  onDeselectNode={deselectAllNodes}
                  nodeStatuses={nodeStatuses}
                  isRunning={isRunning}
                  progress={progress}
                  breadcrumb={breadcrumb}
                />
              </DrawerContent>
            </Drawer>
          </DrawerContent>
        </Drawer>
      </PageSection>
    </Page>
  );
}
