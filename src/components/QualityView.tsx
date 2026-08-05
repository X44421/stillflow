import { Alert, Card, CardBody, CardTitle, Label, Progress } from '@patternfly/react-core';
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
  ChartBullet,
  ChartStack,
  ChartThemeColor,
} from '@patternfly/react-charts/victory';
import { qualityIssues, qualityRows } from '../data';

interface QualityViewProps {
  isRunning: boolean;
  progress: number;
}

const metricData = [
  { name: 'Schema', value: 92 },
  { name: 'Complete', value: 97 },
  { name: 'Duplicates', value: 98 },
  { name: 'Email', value: 99 },
  { name: 'Privacy', value: 82 },
  { name: 'Tokens', value: 94 },
  { name: 'Labels', value: 90 },
];

const passData = metricData.map((item) => ({ name: item.name, value: Math.round(item.value * 0.82) }));
const reviewData = metricData.map((item, index) => ({ name: item.name, value: item.value - passData[index].value }));

const severityColor = (severity: string) => {
  if (severity === 'danger') {
    return 'red';
  }
  if (severity === 'warning') {
    return 'orange';
  }
  return 'blue';
};

export function QualityView({ isRunning, progress }: QualityViewProps) {
  return (
    <div className="still-charts-grid">
      {isRunning && (
        <Card isCompact className="still-card-wide">
          <CardBody>
            <Alert
              variant="warning"
              isInline
              title="Quality checks are still running"
              className="still-quality-alert"
            >
              {Math.round(progress)}% of checks have completed. Results shown below may update as the run finishes.
            </Alert>
          </CardBody>
        </Card>
      )}

      <Card isCompact isFullHeight>
        <CardTitle>Overall quality</CardTitle>
        <CardBody>
          <Progress
            value={92}
            title="Quality score"
            variant="success"
            measureLocation="outside"
            aria-label="Overall quality score"
          />
          <div className="still-view-meta">
            <Label color="green">Pass</Label>
            <Label color="orange">2 warnings</Label>
            <Label color="blue">4 checks</Label>
          </div>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight>
        <CardTitle>Metric health</CardTitle>
        <CardBody>
          <Chart
            height={220}
            width={420}
            padding={{ top: 16, bottom: 55, left: 55, right: 24 }}
            domainPadding={{ x: 24 }}
            themeColor={ChartThemeColor.multi}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartBar data={metricData} x="name" y="value" barRatio={0.68} />
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight>
        <CardTitle>Schema validity</CardTitle>
        <CardBody>
          <ChartBullet
            ariaDesc="Schema validity score"
            ariaTitle="Schema validity"
            height={220}
            width={420}
            title="Valid records"
            subTitle="92 of 100"
            domain={{ x: [0, 2], y: [0, 100] }}
            qualitativeRangeData={[{ y: 100 }]}
            primarySegmentedMeasureData={[{ y: 92 }]}
            comparativeWarningMeasureData={[{ y: 95 }]}
            comparativeErrorMeasureData={[{ y: 98 }]}
            qualitativeRangeLegendData={[{ name: 'Range' }]}
            primarySegmentedMeasureLegendData={[{ name: 'Valid' }]}
            comparativeWarningMeasureLegendData={[{ name: 'Warning' }]}
            comparativeErrorMeasureLegendData={[{ name: 'Error' }]}
            themeColor={ChartThemeColor.green}
            legendPosition="bottom"
          />
        </CardBody>
      </Card>

      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Check stack</CardTitle>
        <CardBody>
          <Chart
            height={210}
            width={650}
            padding={{ top: 16, bottom: 55, left: 55, right: 24 }}
            domainPadding={{ x: 32 }}
            themeColor={ChartThemeColor.multiOrdered}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartStack>
              <ChartBar data={passData} x="name" y="value" barRatio={0.7} />
              <ChartBar data={reviewData} x="name" y="value" barRatio={0.7} />
            </ChartStack>
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Quality issues</CardTitle>
        <CardBody className="still-card-table">
          <Table variant={TableVariant.compact} aria-label="Quality issues">
            <Thead>
              <Tr>
                <Th>Severity</Th>
                <Th>Issue</Th>
                <Th>Details</Th>
                <Th modifier="nowrap">Records</Th>
              </Tr>
            </Thead>
            <Tbody>
              {qualityIssues.map((issue) => (
                <Tr key={issue.title}>
                  <Td dataLabel="Severity">
                    <Label color={severityColor(issue.severity)} isCompact>
                      {issue.severity}
                    </Label>
                  </Td>
                  <Td dataLabel="Issue">{issue.title}</Td>
                  <Td dataLabel="Details">{issue.detail}</Td>
                  <Td dataLabel="Records" modifier="nowrap">
                    {issue.count.toLocaleString()}
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Rule summary</CardTitle>
        <CardBody>
          <Table variant={TableVariant.compact} aria-label="Quality rule summary">
            <Thead>
              <Tr>
                <Th>Metric</Th>
                <Th>Result</Th>
                <Th>Status</Th>
              </Tr>
            </Thead>
            <Tbody>
              {qualityRows.map((row) => (
                <Tr key={row.metric}>
                  <Td dataLabel="Metric">{row.metric}</Td>
                  <Td dataLabel="Result">{row.result}</Td>
                  <Td dataLabel="Status">
                    <Label
                      color={row.status === 'warning' ? 'orange' : row.status === 'error' ? 'red' : 'green'}
                      isCompact
                    >
                      {row.statusLabel}
                    </Label>
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </CardBody>
      </Card>
    </div>
  );
}
