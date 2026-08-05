import { useState } from 'react';
import {
  Button,
  Dropdown,
  DropdownItem,
  DropdownList,
  Label,
  MenuToggle,
  SearchInput,
  Select,
  SelectList,
  SelectOption,
  Tab,
  Tabs,
  TabTitleText,
  Title,
  Toolbar,
  ToolbarContent,
  ToolbarFilter,
  ToolbarGroup,
  ToolbarItem,
} from '@patternfly/react-core';
import { ColumnsIcon, ExportIcon, SyncIcon } from '@patternfly/react-icons';
import type { PreviewTab } from '../types';
import { tableColumns } from '../data';
import { CompareView } from './CompareView';
import { DataView } from './DataView';
import { ProfileView } from './ProfileView';
import { QualityView } from './QualityView';

interface PreviewWorkspaceProps {
  activeTab: PreviewTab;
  onTabChange: (tab: PreviewTab) => void;
  title: string;
  meta: string;
  isRunning: boolean;
  progress: number;
}

export function PreviewWorkspace({ activeTab, onTabChange, title, meta, isRunning, progress }: PreviewWorkspaceProps) {
  const [searchText, setSearchText] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [visibleColumns, setVisibleColumns] = useState<string[]>(tableColumns.map((column) => column.key));
  const [perPage, setPerPage] = useState(50);
  const [page, setPage] = useState(1);
  const [filterOpen, setFilterOpen] = useState(false);
  const [rowsOpen, setRowsOpen] = useState(false);
  const [columnsOpen, setColumnsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const toggleColumn = (key: string) => {
    setVisibleColumns((previous) => {
      if (previous.includes(key)) {
        return previous.length === 1 ? previous : previous.filter((column) => column !== key);
      }
      return [...previous, key];
    });
  };

  const refresh = () => {
    setIsLoading(true);
    window.setTimeout(() => setIsLoading(false), 1400);
  };

  const handlePerPage = (nextPerPage: number) => {
    setPerPage(nextPerPage);
    setPage(1);
  };

  return (
    <div className="still-preview-workspace">
      <Tabs
        activeKey={activeTab}
        onSelect={(_event, key) => onTabChange(key as PreviewTab)}
        variant="secondary"
        aria-label="Preview views"
        className="still-preview-tabs"
      >
        <Tab eventKey="data" title={<TabTitleText>Data</TabTitleText>} />
        <Tab eventKey="profile" title={<TabTitleText>Profile</TabTitleText>} />
        <Tab eventKey="quality" title={<TabTitleText>Quality</TabTitleText>} />
        <Tab eventKey="compare" title={<TabTitleText>Compare</TabTitleText>} />
      </Tabs>

      {activeTab === 'data' ? (
        <Toolbar
          className="still-preview-toolbar"
          clearAllFilters={() => {
            setSearchText('');
            setStatusFilter('all');
          }}
          clearFiltersButtonText="Clear all filters"
        >
          <ToolbarContent>
            <ToolbarItem>
              <SearchInput
                aria-label="Search dataset rows"
                placeholder="Search name, email, city"
                value={searchText}
                onChange={(_event, value) => setSearchText(value)}
                onClear={() => setSearchText('')}
              />
            </ToolbarItem>
            <ToolbarFilter
              categoryName="Status"
              labels={statusFilter === 'all' ? [] : [statusFilter]}
              deleteLabel={() => setStatusFilter('all')}
              deleteLabelGroup={() => setStatusFilter('all')}
              showToolbarItem
            >
              <Select
                isOpen={filterOpen}
                onOpenChange={(open) => setFilterOpen(open)}
                selected={statusFilter}
                onSelect={(_event, value) => {
                  setStatusFilter(String(value));
                  setFilterOpen(false);
                }}
                toggle={(toggleRef) => (
                  <MenuToggle ref={toggleRef} onClick={() => setFilterOpen((open) => !open)} isExpanded={filterOpen}>
                    Status: {statusFilter}
                  </MenuToggle>
                )}
              >
                <SelectList>
                  <SelectOption value="all">All statuses</SelectOption>
                  <SelectOption value="active">Active</SelectOption>
                  <SelectOption value="inactive">Inactive</SelectOption>
                </SelectList>
              </Select>
            </ToolbarFilter>
            <ToolbarItem>
              <Dropdown
                isOpen={columnsOpen}
                onOpenChange={(open) => setColumnsOpen(open)}
                onSelect={(_event, value) => {
                  if (value !== undefined) {
                    toggleColumn(String(value));
                  }
                }}
                toggle={(toggleRef) => (
                  <MenuToggle
                    ref={toggleRef}
                    onClick={() => setColumnsOpen((open) => !open)}
                    isExpanded={columnsOpen}
                    icon={<ColumnsIcon />}
                  >
                    Columns
                  </MenuToggle>
                )}
              >
                <DropdownList>
                  {tableColumns.map((column) => (
                    <DropdownItem
                      key={column.key}
                      value={column.key}
                      hasCheckbox
                      isSelected={visibleColumns.includes(column.key)}
                    >
                      {column.label}
                    </DropdownItem>
                  ))}
                </DropdownList>
              </Dropdown>
            </ToolbarItem>
            <ToolbarItem>
              <Select
                isOpen={rowsOpen}
                onOpenChange={(open) => setRowsOpen(open)}
                selected={perPage}
                onSelect={(_event, value) => {
                  handlePerPage(Number(value));
                  setRowsOpen(false);
                }}
                toggle={(toggleRef) => (
                  <MenuToggle ref={toggleRef} onClick={() => setRowsOpen((open) => !open)} isExpanded={rowsOpen}>
                    Rows: {perPage}
                  </MenuToggle>
                )}
              >
                <SelectList>
                  <SelectOption value={50}>50 rows</SelectOption>
                  <SelectOption value={100}>100 rows</SelectOption>
                  <SelectOption value={200}>200 rows</SelectOption>
                </SelectList>
              </Select>
            </ToolbarItem>
            <ToolbarItem>
              <Button variant="plain" icon={<SyncIcon />} onClick={refresh} aria-label="Refresh preview" />
            </ToolbarItem>
            <ToolbarItem align={{ default: 'alignRight' }}>
              <Label>{meta}</Label>
            </ToolbarItem>
          </ToolbarContent>
        </Toolbar>
      ) : (
        <Toolbar className="still-preview-toolbar">
          <ToolbarContent>
            <ToolbarGroup>
              <ToolbarItem>
                <Title headingLevel="h3" size="md">
                  {title}
                </Title>
              </ToolbarItem>
              <ToolbarItem>
                <Label>{meta}</Label>
              </ToolbarItem>
            </ToolbarGroup>
            <ToolbarItem align={{ default: 'alignRight' }}>
              <Button variant="secondary" icon={<ExportIcon />}>
                Export report
              </Button>
            </ToolbarItem>
          </ToolbarContent>
        </Toolbar>
      )}

      <div className="still-preview-content">
        {activeTab === 'data' && (
          <DataView
            searchText={searchText}
            statusFilter={statusFilter}
            visibleColumns={visibleColumns}
            page={page}
            perPage={perPage}
            isLoading={isLoading}
            onSetPage={setPage}
            onPerPageSelect={handlePerPage}
          />
        )}
        {activeTab === 'profile' && <ProfileView />}
        {activeTab === 'quality' && <QualityView isRunning={isRunning} progress={progress} />}
        {activeTab === 'compare' && <CompareView />}
      </div>
    </div>
  );
}
