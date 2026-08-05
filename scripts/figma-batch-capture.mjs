import { chromium } from 'playwright';

const BASE_URL = 'http://localhost:8080/preview-workspace.html';
const ENDPOINT_BASE = 'https://mcp.figma.com/mcp/capture';

const captures = [
  { name: 'tabular-data', format: 'tabular', tab: 'data', captureId: '1034095a-0d10-44c7-9d12-14ae19f33824' },
  { name: 'tabular-profile', format: 'tabular', tab: 'profile', captureId: 'bc41cd66-4160-440f-bedb-337c2aee5c9f' },
  { name: 'tabular-quality', format: 'tabular', tab: 'quality', captureId: 'e39adaf9-7536-415a-82b1-8bbbf433cb82' },
  { name: 'tabular-compare', format: 'tabular', tab: 'compare', captureId: '722cce06-042d-46f3-87d1-2748911bfe99' },
  { name: 'record-data', format: 'record', tab: 'data', captureId: 'add06f2b-fb51-4482-9b45-cdece203d000' },
  { name: 'record-profile', format: 'record', tab: 'profile', captureId: 'cd967cbc-702f-4a79-be5c-5e6e36d605f3' },
  { name: 'record-quality', format: 'record', tab: 'quality', captureId: '4766f30a-735c-46b2-86f4-b6812a6334a9' },
  { name: 'record-compare', format: 'record', tab: 'compare', captureId: '7d79325a-901b-4560-bdb7-2e3145946e71' },
  { name: 'text-data', format: 'text', tab: 'data', captureId: 'e135e425-a6b6-4674-99c2-821bb340c6b7' },
  { name: 'text-profile', format: 'text', tab: 'profile', captureId: '7b7758a3-46c7-41e9-b312-abed3a8ab753' },
  { name: 'text-quality', format: 'text', tab: 'quality', captureId: '68bc5155-4c9d-4bce-859c-35967350deba' },
  { name: 'text-compare', format: 'text', tab: 'compare', captureId: '687a5c37-67b9-44bf-ab23-89cb831fa3f0' },
  { name: 'conversation-data', format: 'conversation', tab: 'data', captureId: '99ecca72-5b50-4889-b158-68e7b8f874e3' },
  { name: 'conversation-profile', format: 'conversation', tab: 'profile', captureId: '4e335bdd-9d25-41e3-b3f8-033307c44d49' },
  { name: 'conversation-quality', format: 'conversation', tab: 'quality', captureId: 'b200f864-7055-43b5-85e4-c9b276afb9ed' },
  { name: 'conversation-compare', format: 'conversation', tab: 'compare', captureId: 'd25989ad-8f36-4e83-a081-4efde012c9bc' },
  { name: 'event-data', format: 'event', tab: 'data', captureId: '82de8840-08d4-4943-bd21-6207c749d968' },
  { name: 'event-profile', format: 'event', tab: 'profile', captureId: 'dbcf18c1-0a5c-4fa0-9694-1ef8201bc72e' },
  { name: 'event-quality', format: 'event', tab: 'quality', captureId: '5017699b-5f4f-4705-919b-d81ef67d0532' },
  { name: 'event-compare', format: 'event', tab: 'compare', captureId: 'f6a1e3cf-38cc-4e68-8c4d-a53415e75294' },
];

function buildUrl(cap) {
  const endpoint = encodeURIComponent(
    `${ENDPOINT_BASE}/${cap.captureId}/submit?bindVariables=true`,
  );
  return `${BASE_URL}?format=${cap.format}&tab=${cap.tab}#figmacapture=${cap.captureId}&figmaendpoint=${endpoint}&figmadelay=2000`;
}

const browser = await chromium.launch({ headless: true, channel: 'chrome' });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
const results = [];

try {
  for (const cap of captures) {
    const url = buildUrl(cap);
    console.log(`Capturing ${cap.name}...`);
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      // Wait for auto-capture to fire and submit
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
