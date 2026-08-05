import { Card, CardBody, CardTitle } from '@patternfly/react-core';
import {
  Chart,
  ChartAxis,
  ChartBar,
  ChartBoxPlot,
  ChartLine,
  ChartThemeColor,
} from '@patternfly/react-charts/victory';

const distributionData = [
  { name: 'ID', value: 80000 },
  { name: 'Name', value: 79612 },
  { name: 'Email', value: 79320 },
  { name: 'Phone', value: 77455 },
  { name: 'City', value: 79140 },
  { name: 'State', value: 79502 },
  { name: 'Zip', value: 78900 },
  { name: 'Status', value: 79648 },
];

const scoreBoxData = [
  { x: 'Before', min: 22, q1: 48, median: 71, q3: 86, max: 99 },
  { x: 'After', min: 58, q1: 74, median: 86, q3: 94, max: 100 },
];

const topValueData = [
  { name: 'Portland', value: 12480 },
  { name: 'Seattle', value: 11034 },
  { name: 'Austin', value: 9862 },
  { name: 'Chicago', value: 8734 },
  { name: 'Boston', value: 7206 },
];

const tokenTrendData = [
  { day: 'Mon', tokens: 4200 },
  { day: 'Tue', tokens: 4750 },
  { day: 'Wed', tokens: 4380 },
  { day: 'Thu', tokens: 5120 },
  { day: 'Fri', tokens: 4960 },
  { day: 'Sat', tokens: 3860 },
  { day: 'Sun', tokens: 4120 },
];

export function ProfileView() {
  return (
    <div className="still-charts-grid">
      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Column completeness</CardTitle>
        <CardBody>
          <Chart
            height={220}
            width={650}
            padding={{ top: 16, bottom: 55, left: 55, right: 24 }}
            domainPadding={{ x: 24 }}
            themeColor={ChartThemeColor.multi}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartBar data={distributionData} x="name" y="value" barRatio={0.65} />
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
            <ChartBoxPlot
              data={scoreBoxData}
              boxWidth={36}
              whiskerWidth={24}
              medianLabels={({ datum }) => Math.round(datum.median)}
            />
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight>
        <CardTitle>Top city values</CardTitle>
        <CardBody>
          <Chart
            height={220}
            width={420}
            padding={{ top: 16, bottom: 50, left: 90, right: 24 }}
            domainPadding={{ x: 24 }}
            themeColor={ChartThemeColor.green}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartBar data={topValueData} x="name" y="value" horizontal barRatio={0.7} />
          </Chart>
        </CardBody>
      </Card>

      <Card isCompact isFullHeight className="still-card-wide">
        <CardTitle>Token length trend</CardTitle>
        <CardBody>
          <Chart
            height={210}
            width={650}
            padding={{ top: 16, bottom: 50, left: 55, right: 24 }}
            domainPadding={{ x: 24 }}
            themeColor={ChartThemeColor.multiUnordered}
          >
            <ChartAxis />
            <ChartAxis dependentAxis />
            <ChartLine data={tokenTrendData} x="day" y="tokens" />
          </Chart>
        </CardBody>
      </Card>
    </div>
  );
}
