import { chromium } from 'playwright';

const BASE_URL = 'http://localhost:8080/preview-workspace.html';
const ENDPOINT_BASE = 'https://mcp.figma.com/mcp/capture';

const captures = [
  { name: 'record-view-table', format: 'record', tab: 'data', view: 'table', captureId: 'bf086314-d963-4bf4-bfcf-e2d97c25e7af' },
  { name: 'record-view-record', format: 'record', tab: 'data', view: 'record', captureId: '61613213-026f-4d69-8db6-4f57a6edaad9' },
  { name: 'record-view-raw', format: 'record', tab: 'data', view: 'raw', captureId: '2426bea0-d5f4-4783-a63d-cd95635ebc29' },
  { name: 'text-view-document', format: 'text', tab: 'data', view: 'document', captureId: 'ad35a860-c53f-4053-ad91-b49d19ba3ba8' },
  { name: 'text-view-chunk', format: 'text', tab: 'data', view: 'chunk', captureId: 'b02ae5f4-9297-4258-b53d-c00137103d87' },
  { name: 'text-view-raw', format: 'text', tab: 'data', view: 'raw', captureId: '6a3a7da8-c1fc-4e60-aa3d-0988f77a1905' },
  { name: 'text-view-metrics', format: 'text', tab: 'data', view: 'document', metrics: 'open', captureId: 'c0217231-190f-4284-9c50-699e89c2a4c5' },
  { name: 'conversation-view-records', format: 'conversation', tab: 'data', view: 'records', captureId: '57f87ce7-3b9f-499f-b662-265b4f75ce09' },
  { name: 'conversation-view-conversation', format: 'conversation', tab: 'data', view: 'conversation', captureId: '29eaa762-cc44-498b-b188-a9fc80242e16' },
  { name: 'conversation-view-raw', format: 'conversation', tab: 'data', view: 'raw', captureId: '4a7b999e-a8e0-4403-910e-af9c7acd4e83' },
  { name: 'event-view-events', format: 'event', tab: 'data', view: 'events', captureId: '19be6a9c-5075-462f-a9bc-d6edb4146844' },
  { name: 'event-view-raw', format: 'event', tab: 'data', view: 'raw', captureId: 'c0dc2993-90b5-4f84-8d56-0ab2758f47f2' },
];

function buildUrl(cap) {
  const endpoint = encodeURIComponent(
    `${ENDPOINT_BASE}/${cap.captureId}/submit?bindVariables=true`,
  );
  let url = `${BASE_URL}?format=${cap.format}&tab=${cap.tab}&view=${cap.view}`;
  if (cap.metrics) url += `&metrics=${cap.metrics}`;
  return `${url}#figmacapture=${cap.captureId}&figmaendpoint=${endpoint}&figmadelay=2000`;
}

const browser = await chromium.launch({ headless: true, channel: 'chrome' });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const results = [];

try {
  for (const cap of captures) {
    console.log(`Capturing ${cap.name}...`);
    try {
      await page.goto(buildUrl(cap), { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.waitForTimeout(5000);
      results.push({ name: cap.name, captureId: cap.captureId, ok: true });
    } catch (err) {
      results.push({ name: cap.name, captureId: cap.captureId, ok: false, error: String(err) });
    }
  }
} finally {
  await browser.close();
}

console.log(JSON.stringify(results, null, 2));
