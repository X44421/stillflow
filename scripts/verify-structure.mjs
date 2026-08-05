import { chromium } from 'playwright';

const baseUrl = process.env.BASE_URL ?? 'http://127.0.0.1:5173/';
const chromePath = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const results = [];
const consoleErrors = [];

function ok(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
  results.push(`PASS ${message}`);
}

async function waitForText(locator, expected, timeout = 8000) {
  await locator.waitFor({ state: 'visible', timeout });
  await locator.filter({ hasText: expected }).waitFor({ state: 'visible', timeout });
}

const browser = await chromium.launch({
  executablePath: chromePath,
  headless: true,
  args: ['--no-sandbox', '--disable-gpu'],
});

try {
  const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => consoleErrors.push(`pageerror: ${error.message}`));
  page.on('response', (response) => {
    if (response.status() >= 400) {
      console.log('HTTP error:', response.status(), response.url());
    }
  });

  await page.goto(baseUrl, { waitUntil: 'networkidle' });
  await page.locator('.still-data-pane').waitFor({ state: 'visible', timeout: 15000 });
  await page.locator('.still-workflow-pane').waitFor({ state: 'visible', timeout: 15000 });
  await page.locator('.still-inspector-pane').waitFor({ state: 'visible', timeout: 15000 });
  await page.waitForTimeout(1200);

  const titlebar = page.locator('.still-desktop-titlebar');
  const sidebar = page.locator('.still-sidebar');
  const dataPane = page.locator('.still-data-pane');
  const divider = page.locator('.still-pane-divider');
  const workflowPane = page.locator('.still-workflow-pane');
  const inspector = page.locator('.still-inspector-pane');

  const titlebarBox = await titlebar.boundingBox();
  const sidebarBox = await sidebar.boundingBox();
  const dataBox = await dataPane.boundingBox();
  const dividerBox = await divider.boundingBox();
  const workflowBox = await workflowPane.boundingBox();
  const inspectorBox = await inspector.boundingBox();

  ok(Math.round(titlebarBox.height) === 36, `1920 titlebar is 36px (${titlebarBox.height})`);
  ok(Math.round(sidebarBox.width) === 240, `1920 sidebar is 240px (${sidebarBox.width})`);
  ok(Math.round(dataBox.width) === 580, `1920 data pane is 580px (${dataBox.width})`);
  ok(Math.round(dividerBox.width) === 8, `1920 divider is 8px (${dividerBox.width})`);
  ok(Math.round(inspectorBox.width) === 360, `1920 inspector is 360px (${inspectorBox.width})`);
  ok(Math.round(workflowBox.width) === 732, `1920 workflow pane is 732px (${workflowBox.width})`);

  await waitForText(page.locator('.still-topology-context'), 'Customer cleanup');
  await waitForText(page.locator('.still-sidebar'), 'customers.csv');
  await waitForText(page.locator('.still-inspector-pane'), 'Object');
  await waitForText(page.locator('.still-data-pane'), '80,000 rows | 13 columns');
  ok(true, 'original content is present in sidebar, data pane, workflow, and inspector');

  await page.screenshot({ path: '.codex-structure-1920.png', fullPage: true });
  results.push('PASS screenshot .codex-structure-1920.png captured');

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.waitForTimeout(600);

  const compactTitlebar = await titlebar.boundingBox();
  const compactSidebar = await sidebar.boundingBox();
  const compactData = await dataPane.boundingBox();
  const compactDivider = await divider.boundingBox();
  const compactWorkflow = await workflowPane.boundingBox();
  const compactInspector = await inspector.boundingBox();

  ok(Math.round(compactTitlebar.height) === 36, `1440 titlebar is 36px (${compactTitlebar.height})`);
  ok(Math.round(compactSidebar.width) === 240, `1440 sidebar is 240px (${compactSidebar.width})`);
  ok(Math.round(compactData.width) === 580, `1440 data pane is 580px (${compactData.width})`);
  ok(Math.round(compactDivider.width) === 8, `1440 divider is 8px (${compactDivider.width})`);
  ok(Math.round(compactInspector.width) === 360, `1440 inspector is 360px (${compactInspector.width})`);
  ok(compactWorkflow.width >= 240, `1440 workflow pane remains visible (${compactWorkflow.width})`);

  const overflow = await page.evaluate(() => ({
    doc: document.documentElement.scrollWidth,
    body: document.body.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  ok(overflow.doc <= overflow.client + 1 && overflow.body <= overflow.client + 1, `no horizontal overflow at 1440px (${JSON.stringify(overflow)})`);
  await page.screenshot({ path: '.codex-structure-1440.png', fullPage: true });
  results.push('PASS screenshot .codex-structure-1440.png captured');

  const blockingErrors = consoleErrors.filter(
    (message) => !message.includes('Received an empty string for a boolean attribute') || !message.includes('inert')
  );
  if (blockingErrors.length > 0) {
    console.log('Console errors:', blockingErrors.join('\n'));
  }
  ok(blockingErrors.length === 0, `no blocking console errors (${blockingErrors.length})`);
} finally {
  await browser.close();
}

console.log(`\n${results.length} checks passed`);
console.log(results.join('\n'));
