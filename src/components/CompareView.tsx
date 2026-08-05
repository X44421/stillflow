import {
  Card,
  CardBody,
  CardTitle,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  Label,
} from '@patternfly/react-core';
import {
  Table,
  TableVariant,
  Tbody,
  Td,
  Th,
  Thead,
  Tr,
} from '@patternfly/react-table';
import {
  Chart,
  ChartAxis,
  ChartBar,
  ChartBoxPlot,
  ChartGroup,
  ChartThemeColor,
} from '@patternfly/react-charts/victory';
import { compareAfterRows, compareBeforeRows } from '../data';
import type { CompareRow } from '../types';

const beforeData = [
  { name: 'Rows', value: 80000 },
  { name: 'Duplicates', value: 1588 },
  { name: 'Null emails', value: 47 },
  { name: 'Bad scores', value: 12 },
  { name: 'Inactive', value: 1220 },
];

const afterData = [
  { name: 'Rows', value: 78412 },
  { name: 'Duplicates', value: 0 },
  { name: 'Null emails', value: 47 },
  { name: 'Bad scores', value: 0 },
  { name: 'Inactive', value: 1214 },
];

const scoreBoxData = [
  { x: 'Before', min: 22, q1: 48, median: 71, q3: 86, max: 99 },
  { x: 'After', min: 58, q1: 74, median: 86, q3: 94, max: 100 },
];

function CompareTable({ title, rows }: { title: string; rows: CompareRow[] }) {
  return (
    <Card isCompact isFullHeight>
      <CardTitle>{title}</CardTitle>
      <CardBody className="still-card-table">
        <Table variant={TableVariant.compact} aria-label={title}>
          <Thead>
            <Tr>
              <Th>Name</Th>
              <Th>Email</Th>
              <Th>Phone</Th>
              <Th>Status</Th>
            </Tr>
          </Thead>
          <Tbody>
            {rows.map((row) => (
              <Tr key={`${title}-${row.name}`}>
                <Td dataLabel="Name">
                  {row.name}
                  {row.changed && (
                    <Label color="blue" isCompact className="pf-v6-u-ml-sm">
                      changed
                    </Label>
                  )}
                </Td>
                <Td dataLabel="Email">{row.email}</Td>
                <Td dataLabel="Phone">{row.phone}</Td>
                <Td dataLabel="Status">{row.status}</Td>
              </Tr>
            ))}
          </Tbody>
        </Table>
      </CardBody>
    </Card>
  );
}

export function CompareView() {
  return (
    <div className="still-compare-grid">
      <CompareTable title="Before" rows={compareBeforeRows} />
      <CompareTable title="After" rows={compareAfterRows} />

      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Before and after metrics</CardTitle>
        <CardBody>
          <Chart
            height={240}
            width={720}
            padding={{ top: 16, bottom: 55, left: 60, right: 24 }}
            domainPadding={{ x: 40 }}
            themeColor={ChartThemeColor.multi}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartGroup>
              <ChartBar data={beforeData} x="name" y="value" barRatio={0.8} />
              <ChartBar data={afterData} x="name" y="value" barRatio={0.8} />
            </ChartGroup>
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight>
        <CardTitle>Score distribution</CardTitle>
        <CardBody>
          <Chart
            height={220}
            width={420}
            padding={{ top: 16, bottom: 55, left: 45, right: 24 }}
            domainPadding={{ x: 40 }}
            themeColor={ChartThemeColor.blue}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartBoxPlot data={scoreBoxData} boxWidth={36} whiskerWidth={24} />
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight>
        <CardTitle>Run summary</CardTitle>
        <CardBody>
          <DescriptionList isHorizontal isCompact>
            <DescriptionListGroup>
              <DescriptionListTerm>Rows changed</DescriptionListTerm>
              <DescriptionListDescription>1,588</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Rows rejected</DescriptionListTerm>
              <DescriptionListDescription>0</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Duration</DescriptionListTerm>
              <DescriptionListDescription>4m 12s</DescriptionListDescription>
            </DescriptionListGroup>
            <DescriptionListGroup>
              <DescriptionListTerm>Output</DescriptionListTerm>
              <DescriptionListDescription>customer_clean.csv</DescriptionListDescription>
            </DescriptionListGroup>
          </DescriptionList>
        </CardBody>
      </Card>
    </div>
  );
}
