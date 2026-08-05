import { chromium } from 'playwright';
import fs from 'node:fs';

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

  await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.locator('.still-workspace-main').waitFor({ state: 'visible', timeout: 15000 });
  await page.waitForTimeout(1200);

  const sidebar = page.locator('.still-sidebar');
  const dataPane = page.locator('.still-data-pane');
  const workflowPane = page.locator('.still-workflow-pane');
  const inspector = page.locator('.still-inspector');
  await sidebar.waitFor({ state: 'visible' });
  await dataPane.waitFor({ state: 'visible' });
  await workflowPane.waitFor({ state: 'visible' });
  await inspector.waitFor({ state: 'visible' });

  const sidebarBox = await sidebar.boundingBox();
  const dataBox = await dataPane.boundingBox();
  const workflowBox = await workflowPane.boundingBox();
  const inspectorBox = await inspector.boundingBox();
  ok(Math.round(sidebarBox.width) === 240, `1920 sidebar is 240px (${sidebarBox.width})`);
  ok(Math.round(dataBox.width) === 580, `1920 data pane is 580px (${dataBox.width})`);
  ok(Math.round(inspectorBox.width) === 360, `1920 inspector is 360px (${inspectorBox.width})`);
  ok(Math.round(workflowBox.width) === 732, `1920 workflow pane is 732px (${workflowBox.width})`);

  await waitForText(page.locator('.still-preview-title'), 'customers.csv');
  await waitForText(page.locator('.still-preview-meta'), 'Sampled 50');
  await waitForText(page.locator('.still-table'), 'Alice Johnson');
  await waitForText(page.locator('.still-table'), 'invalid-email');
  await waitForText(page.locator('.still-table'), 'Missing');
  await waitForText(page.locator('.still-workflow-svg'), 'Customer normalization');
  await waitForText(page.locator('.still-inspector'), '3 issues need review');
  ok(true, 'design copy is present in data pane, workflow, and inspector');

  const selectedDataset = page.locator('.still-sidebar__item--dataset.is-selected');
  ok((await selectedDataset.count()) === 1, 'customers.csv is selected in the sidebar');

  await page.screenshot({ path: '.codex-desktop-1920.png', fullPage: true });
  results.push('PASS screenshot .codex-desktop-1920.png captured');

  await page.getByRole('tab', { name: 'Profile', exact: true }).click();
  await page.locator('.still-charts-grid').waitFor({ state: 'visible', timeout: 4000 });
  await waitForText(page.locator('.still-preview-content'), 'Column completeness');
  ok(true, 'Profile tab renders chart content');

  await page.getByRole('tab', { name: 'Compare', exact: true }).click();
  await waitForText(page.locator('.still-preview-content'), 'Before');
  ok(true, 'Compare tab renders comparison content');

  await page.getByRole('tab', { name: 'Data', exact: true }).click();
  await page.locator('.still-table').waitFor({ state: 'visible', timeout: 4000 });
  await page.screenshot({ path: '.codex-tabs-1920.png', fullPage: true });
  results.push('PASS screenshot .codex-tabs-1920.png captured');

  // 1440: compact widths but every pane remains visible and usable.
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.waitForTimeout(600);
  const compactSidebar = await sidebar.boundingBox();
  const compactData = await dataPane.boundingBox();
  const compactWorkflow = await workflowPane.boundingBox();
  const compactInspector = await inspector.boundingBox();
  ok(Math.round(compactSidebar.width) === 208, `1440 sidebar is 208px (${compactSidebar.width})`);
  ok(Math.round(compactData.width) === 520, `1440 data pane is 520px (${compactData.width})`);
  ok(Math.round(compactInspector.width) === 320, `1440 inspector is 320px (${compactInspector.width})`);
  ok(compactWorkflow.width > 300, `1440 workflow pane remains usable (${compactWorkflow.width})`);
  const overflow = await page.evaluate(() => ({
    doc: document.documentElement.scrollWidth,
    body: document.body.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  ok(overflow.doc <= overflow.client + 1 && overflow.body <= overflow.client + 1, `no horizontal overflow at 1440px (${JSON.stringify(overflow)})`);
  await page.screenshot({ path: '.codex-desktop-1440.png', fullPage: true });
  results.push('PASS screenshot .codex-desktop-1440.png captured');

  if (consoleErrors.length > 0) {
    console.log('Console errors:', consoleErrors.join('\n'));
  }
  ok(consoleErrors.length === 0, `no console errors (${consoleErrors.length})`);
} finally {
  await browser.close();
}

console.log(`\n${results.length} checks passed`);
console.log(results.join('\n'));
