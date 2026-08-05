import { useState } from 'react';
import {
  Button,
  Masthead,
  MastheadContent,
  MastheadToggle,
  PageToggleButton,
  SearchInput,
} from '@patternfly/react-core';
import {
  BarsIcon,
  CompressIcon,
  ExpandArrowsAltIcon,
  EyeIcon,
  EyeSlashIcon,
  PlayIcon,
  StopIcon,
  PlusIcon,
  TimesIcon,
} from '@patternfly/react-icons';
import type { TabItem } from '../types';

interface ObjectTabBarProps {
  tabs: TabItem[];
  activeTabId: string;
  isRunning: boolean;
  previewOpen: boolean;
  inspectorOpen: boolean;
  onSelect: (id: string) => void;
  onClose: (id: string) => void;
  onAdd: () => void;
  onRun: () => void;
  onCancel: () => void;
  onTogglePreview: () => void;
  onToggleInspector: () => void;
}

export function ObjectTabBar({
  tabs,
  activeTabId,
  isRunning,
  previewOpen,
  inspectorOpen,
  onSelect,
  onClose,
  onAdd,
  onRun,
  onCancel,
  onTogglePreview,
  onToggleInspector,
}: ObjectTabBarProps) {
  const [search, setSearch] = useState('');

  return (
    <Masthead className="still-masthead-shell">
      <MastheadToggle>
        <PageToggleButton variant="plain" aria-label="Toggle navigation">
          <BarsIcon />
        </PageToggleButton>
      </MastheadToggle>
      <MastheadContent>
        <div className="still-masthead">
          <div className="still-object-tabs" role="tablist" aria-label="Open object tabs">
            {tabs.map((tab) => (
              <div key={tab.id} className={`still-object-tab${tab.id === activeTabId ? ' is-active' : ''}${tab.unsaved ? ' is-unsaved' : ''}`}>
                <Button variant="plain" className="still-object-tab-label" onClick={() => onSelect(tab.id)} aria-selected={tab.id === activeTabId} role="tab">
                  <span className="still-object-tab__name">{tab.label}</span>
                  <span className="still-object-tab__version">{tab.version ?? 'draft'}</span>
                  {tab.unsaved && <span className="still-object-tab__dot" aria-hidden="true" />}
                </Button>
                <Button variant="plain" className="still-object-tab-close" onClick={() => onClose(tab.id)} aria-label={`Close ${tab.label}`}>
                  <TimesIcon />
                </Button>
              </div>
            ))}
            <Button variant="plain" className="still-object-tab-add" onClick={onAdd} aria-label="Add tab">
              <PlusIcon />
            </Button>
          </div>
          <SearchInput
            aria-label="Global search"
            placeholder="Search objects"
            value={search}
            onChange={(_event, value) => setSearch(value)}
            onClear={() => setSearch('')}
            className="still-global-search"
          />
          <Button variant="plain" className="qf-icon-button" icon={previewOpen ? <EyeIcon /> : <EyeSlashIcon />} onClick={onTogglePreview} aria-label="Toggle preview panel" />
          <Button
            variant="plain"
            className="qf-icon-button"
            icon={inspectorOpen ? <CompressIcon /> : <ExpandArrowsAltIcon />}
            onClick={onToggleInspector}
            aria-label="Toggle inspector panel"
          />
          {isRunning ? (
            <Button variant="danger" className="qf-stop-button" icon={<StopIcon />} onClick={onCancel}>
              Stop
            </Button>
          ) : (
            <Button variant="primary" className="qf-run-button" icon={<PlayIcon />} onClick={onRun}>
              Run
            </Button>
          )}
        </div>
      </MastheadContent>
    </Masthead>
  );
}
