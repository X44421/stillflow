import {
  Nav,
  NavExpandable,
  NavGroup,
  NavItem,
  PageSidebar,
  PageSidebarBody,
} from '@patternfly/react-core';
import {
  ChartLineIcon,
  CubesIcon,
  DatabaseIcon,
  ExportIcon,
  FolderOpenIcon,
  HistoryIcon,
  ProjectDiagramIcon,
  StreamIcon,
} from '@patternfly/react-icons';

interface WorkspaceSidebarProps {
  activeNavId: string;
  onSelect: (id: string) => void;
}

export function WorkspaceSidebar({ activeNavId, onSelect }: WorkspaceSidebarProps) {
  return (
    <PageSidebar className="still-sidebar">
      <PageSidebarBody>
        <Nav aria-label="Quirefold navigation" onSelect={(_event, itemId) => onSelect(String(itemId))}>
          <NavGroup title="Recent">
            <NavItem itemId="dataset-customers" isActive={activeNavId === 'dataset-customers'} icon={<DatabaseIcon />}>
              customers.csv
            </NavItem>
            <NavItem itemId="session-cleanup" isActive={activeNavId === 'session-cleanup'} icon={<StreamIcon />}>
              Customer Clean Session
            </NavItem>
          </NavGroup>
          <NavExpandable title="Projects" groupId="projects" isExpanded icon={<ProjectDiagramIcon />}>
            <NavItem itemId="pipeline-cleanup" isActive={activeNavId === 'pipeline-cleanup'}>
              Customer cleanup
            </NavItem>
          </NavExpandable>
          <NavExpandable title="Datasets" groupId="datasets" isExpanded icon={<DatabaseIcon />}>
            <NavItem itemId="dataset-customers" isActive={activeNavId === 'dataset-customers'}>
              customers.csv
            </NavItem>
            <NavItem itemId="dataset-clean" isActive={activeNavId === 'dataset-clean'}>
              customer_clean.csv
            </NavItem>
          </NavExpandable>
          <NavExpandable title="Runs" groupId="runs" isExpanded icon={<HistoryIcon />}>
            <NavItem itemId="run-latest">Latest run</NavItem>
            <NavItem itemId="run-history">Run history</NavItem>
          </NavExpandable>
          <NavExpandable title="Reports" groupId="reports" icon={<ChartLineIcon />}>
            <NavItem itemId="report-quality">Quality report</NavItem>
            <NavItem itemId="report-profile">Profile report</NavItem>
          </NavExpandable>
          <NavExpandable title="Exports" groupId="exports" icon={<ExportIcon />}>
            <NavItem itemId="export-csv">CSV export</NavItem>
            <NavItem itemId="export-parquet">Parquet export</NavItem>
          </NavExpandable>
          <NavGroup title="Workspace">
            <NavItem itemId="browse" icon={<FolderOpenIcon />}>
              Browse objects
            </NavItem>
            <NavItem itemId="templates" icon={<CubesIcon />}>
              Templates
            </NavItem>
          </NavGroup>
        </Nav>
      </PageSidebarBody>
    </PageSidebar>
  );
}
