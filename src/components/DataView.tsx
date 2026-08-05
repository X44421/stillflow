import { useMemo, useState } from 'react';
import type { MouseEvent } from 'react';
import {
  Button,
  EmptyState,
  EmptyStateBody,
  EmptyStateFooter,
  Label,
  Pagination,
  Skeleton,
} from '@patternfly/react-core';
import {
  InnerScrollContainer,
  OuterScrollContainer,
  Table,
  TableVariant,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from '@patternfly/react-table';
import type { ISortBy } from '@patternfly/react-table';
import { PencilAltIcon, SearchIcon } from '@patternfly/react-icons';
import type { TableRow } from '../types';
import { makeTableRows, tableColumns } from '../data';

interface DataViewProps {
  searchText: string;
  statusFilter: string;
  visibleColumns: string[];
  page: number;
  perPage: number;
  isLoading: boolean;
  onSetPage: (page: number) => void;
  onPerPageSelect: (perPage: number) => void;
}

export function DataView({
  searchText,
  statusFilter,
  visibleColumns,
  page,
  perPage,
  isLoading,
  onSetPage,
  onPerPageSelect,
}: DataViewProps) {
  const rows = useMemo(() => makeTableRows(), []);
  const [selectedRows, setSelectedRows] = useState<string[]>([]);
  const [sortBy, setSortBy] = useState<ISortBy>({});

  const filteredRows = useMemo(() => {
    const query = searchText.trim().toLowerCase();
    return rows.filter((row) => {
      const matchesQuery =
        !query ||
        [row.id, row.name, row.email, row.city, row.state, row.status].some((value) =>
          String(value).toLowerCase().includes(query)
        );
      const matchesStatus = statusFilter === 'all' || row.status === statusFilter;
      return matchesQuery && matchesStatus;
    });
  }, [rows, searchText, statusFilter]);

  const visibleColumnDefs = useMemo(
    () => tableColumns.filter((column) => visibleColumns.includes(column.key)),
    [visibleColumns]
  );

  const sortedRows = useMemo(() => {
    if (sortBy.index === undefined || !sortBy.direction) {
      return filteredRows;
    }
    const column = visibleColumnDefs[sortBy.index];
    if (!column) {
      return filteredRows;
    }
    const direction = sortBy.direction === 'asc' ? 1 : -1;
    return [...filteredRows].sort((left, right) => {
      const leftValue = String(left[column.key]);
      const rightValue = String(right[column.key]);
      return leftValue.localeCompare(rightValue, undefined, { numeric: true }) * direction;
    });
  }, [filteredRows, sortBy, visibleColumnDefs]);

  const pageCount = Math.max(1, Math.ceil(sortedRows.length / perPage));
  const safePage = Math.min(page, pageCount);
  const startIndex = (safePage - 1) * perPage;
  const pageRows = sortedRows.slice(startIndex, startIndex + perPage);
  const pageRowIds = pageRows.map((row) => row.id);
  const allSelected = pageRowIds.length > 0 && pageRowIds.every((id) => selectedRows.includes(id));
  const someSelected = pageRowIds.some((id) => selectedRows.includes(id));

  const onSort = (_event: MouseEvent, columnIndex: number, direction: 'asc' | 'desc') => {
    setSortBy({ index: columnIndex, direction });
  };

  const renderValue = (row: TableRow, key: keyof TableRow) => {
    if (key === 'status') {
      return <Label color={row.status === 'active' ? 'green' : 'grey'} isCompact>{row.status}</Label>;
    }
    if (key === 'email' && row.emailNull) {
      return <Label color="red" isCompact>NULL</Label>;
    }
    if (key === 'score' && row.scoreInvalid) {
      return <Label color="orange" isCompact>{row.score}</Label>;
    }
    if ((key === 'email' && row.emailModified) || (key === 'phone' && row.phoneModified)) {
      return (
        <Label color="blue" isCompact icon={<PencilAltIcon />}>
          {String(row[key])}
        </Label>
      );
    }
    return String(row[key]);
  };

  return (
    <div className="still-data-view">
      {isLoading ? (
        <div className="still-loading-rows">
          {Array.from({ length: 8 }, (_, index) => (
            <Skeleton key={index} width="100%" height="24px" screenreaderText="Loading rows" />
          ))}
        </div>
      ) : pageRows.length === 0 ? (
        <EmptyState variant="sm" titleText="No results found" icon={SearchIcon} className="still-empty-state">
          <EmptyStateBody>No rows match the current search and filter settings.</EmptyStateBody>
          <EmptyStateFooter>
            <Button variant="link" onClick={() => onSetPage(1)}>
              Clear filters
            </Button>
          </EmptyStateFooter>
        </EmptyState>
      ) : (
        <OuterScrollContainer className="still-table-scroll" style={{ height: '100%' }}>
          <InnerScrollContainer>
            <Table variant={TableVariant.compact} isStickyHeader aria-label="Dataset preview">
              <Thead>
                <Tr>
                  <Th
                    select={{
                      isSelected: allSelected,
                      isIndeterminate: someSelected,
                      onSelect: (_event, isSelected) => {
                        setSelectedRows((previous) => {
                          const next = new Set(previous);
                          pageRowIds.forEach((id) => (isSelected ? next.add(id) : next.delete(id)));
                          return [...next];
                        });
                      },
                    }}
                    isStickyColumn
                    stickyMinWidth="48px"
                    hasRightBorder
                    screenReaderText="Select all rows"
                  />
                  {visibleColumnDefs.map((column, index) => (
                    <Th
                      key={column.key}
                      sort={column.sortable ? { sortBy, onSort, columnIndex: index } : undefined}
                      isStickyColumn={column.key === 'id'}
                      stickyLeftOffset={column.key === 'id' ? '48px' : undefined}
                      stickyMinWidth={column.key === 'id' ? '90px' : undefined}
                      hasRightBorder={column.key === 'id'}
                      modifier="nowrap"
                    >
                      {column.label}
                    </Th>
                  ))}
                </Tr>
              </Thead>
              <Tbody>
                {pageRows.map((row, rowIndex) => (
                  <Tr key={row.id}>
                    <Td
                      select={{
                        rowIndex: startIndex + rowIndex,
                        isSelected: selectedRows.includes(row.id),
                        onSelect: (_event, isSelected, _rowIndex, rowData) => {
                          const id = String(rowData.id);
                          setSelectedRows((previous) =>
                            isSelected ? [...previous, id] : previous.filter((existing) => existing !== id)
                          );
                        },
                      }}
                      isStickyColumn
                      stickyMinWidth="48px"
                      hasRightBorder
                    />
                    {visibleColumnDefs.map((column) => (
                      <Td
                        key={column.key}
                        dataLabel={column.label}
                        modifier="nowrap"
                        isStickyColumn={column.key === 'id'}
                        stickyLeftOffset={column.key === 'id' ? '48px' : undefined}
                        stickyMinWidth={column.key === 'id' ? '90px' : undefined}
                        hasRightBorder={column.key === 'id'}
                      >
                        {renderValue(row, column.key)}
                      </Td>
                    ))}
                  </Tr>
                ))}
              </Tbody>
            </Table>
          </InnerScrollContainer>
        </OuterScrollContainer>
      )}

      {!isLoading && pageRows.length > 0 && (
        <div className="still-table-footer">
          {selectedRows.length > 0 && (
            <Label color="blue" isCompact>
              {selectedRows.length} selected
            </Label>
          )}
          <Pagination
            itemCount={sortedRows.length}
            page={safePage}
            perPage={perPage}
            perPageOptions={[
              { title: '50', value: 50 },
              { title: '100', value: 100 },
              { title: '200', value: 200 },
            ]}
            onSetPage={(_event, newPage) => onSetPage(newPage)}
            onPerPageSelect={(_event, newPerPage) => onPerPageSelect(newPerPage)}
            variant="bottom"
            isCompact
          />
        </div>
      )}
    </div>
  );
}
