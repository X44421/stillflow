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
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => consoleErrors.push(`pageerror: ${error.message}`));

  await page.goto(baseUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.locator('.still-workspace-main').waitFor({ state: 'visible', timeout: 15000 });
  await page.waitForTimeout(1200);

  // Initial split layout: both panes visible and equally sized.
  const canvasPane = page.locator('[aria-label="Pipeline canvas"]');
  const previewPane = page.locator('[aria-label="Data preview"]');
  await canvasPane.waitFor({ state: 'visible' });
  await previewPane.waitFor({ state: 'visible' });
  const canvasBox = await canvasPane.boundingBox();
  const previewBox = await previewPane.boundingBox();
  ok(Boolean(canvasBox) && Boolean(previewBox), 'split shows canvas and preview panes');
  if (canvasBox && previewBox) {
    const ratio = canvasBox.width / previewBox.width;
    ok(canvasBox.width > previewBox.width && ratio > 1.05 && ratio < 1.25, `canvas pane is wider than preview per design (${canvasBox.width} vs ${previewBox.width})`);
  }

  const previewTitle = page.locator('.still-preview-header__title');
  const inspectorTitle = page.locator('.still-inspector-title-row h2');
  await waitForText(previewTitle, 'customers.csv');
  await waitForText(inspectorTitle, 'customers.csv');
  ok(true, 'initial tab shows customers.csv in preview and inspector');

  // Select a pipeline node; inspector follows, preview stays on active tab.
  await page.getByRole('button', { name: 'Normalize & Validate', exact: true }).click();
  await waitForText(inspectorTitle, 'Normalize & Validate');
  await waitForText(previewTitle, 'customers.csv');
  ok(true, 'node selection updates inspector and keeps preview on active tab');
  ok(await page.locator('.still-node.is-selected').count() > 0, 'selected node has visual selected state');

  // Switch object tab; selection clears and both inspector and preview follow.
  await page.getByRole('tab', { name: /Customer Clean Session/ }).click();
  await waitForText(inspectorTitle, 'Customer Clean Session');
  await waitForText(previewTitle, 'Customer Clean Session');
  await page.waitForTimeout(250);
  ok((await page.locator('.still-node.is-selected').count()) === 0, 'tab switch clears node selection');
  ok(true, 'tab switch updates inspector and preview together');

  // Run starts, shows progress, then completes.
  await page.locator('.still-object-toolbar__actions').getByRole('button', { name: 'Run', exact: true }).click();
  await page.locator('.still-object-toolbar__status').filter({ hasText: 'Running' }).waitFor({ state: 'visible', timeout: 4000 });
  await page.getByRole('button', { name: 'Cancel run' }).waitFor({ state: 'visible', timeout: 4000 });
  ok(true, 'run starts and surfaces progress UI');
  await page.locator('.still-object-toolbar__actions').getByRole('button', { name: 'Run', exact: true }).waitFor({ state: 'visible', timeout: 20000 });
  await page.locator('.still-object-toolbar__status').filter({ hasText: 'Ready' }).waitFor({ state: 'visible', timeout: 4000 });
  ok(true, 'run completes and returns toolbar to ready state');

  // Validate shows a transient valid state.
  await page.locator('.still-object-toolbar__actions').getByRole('button', { name: 'Validate', exact: true }).click();
  await page.locator('.still-object-toolbar__status').filter({ hasText: 'Valid' }).waitFor({ state: 'visible', timeout: 4000 });
  ok(true, 'validate action reports a valid configuration');

  await page.screenshot({ path: '.codex-verify-1440.png', fullPage: true });
  results.push('PASS screenshot .codex-verify-1440.png captured');

  // Narrow viewport: no horizontal overflow, split resolves to canvas, sidebar can open.
  await page.setViewportSize({ width: 900, height: 800 });
  await page.waitForTimeout(500);
  const viewportClass = await page.locator('.still-viewport').getAttribute('class');
  ok(viewportClass?.includes('is-canvas') ?? false, `narrow split resolves to canvas (${viewportClass})`);
  const overflow = await page.evaluate(() => ({
    doc: document.documentElement.scrollWidth,
    body: document.body.scrollWidth,
    client: document.documentElement.clientWidth,
  }));
  ok(overflow.doc <= overflow.client + 1 && overflow.body <= overflow.client + 1, `no horizontal overflow at 900px (${JSON.stringify(overflow)})`);

  await page.getByRole('button', { name: 'Toggle navigation' }).click();
  await page.locator('.pf-v6-c-nav__link').filter({ hasText: 'CSV export' }).waitFor({ state: 'visible', timeout: 4000 });
  ok(true, 'collapsed sidebar can be opened and navigation remains reachable at 900px');

  await page.screenshot({ path: '.codex-verify-900.png', fullPage: true });
  results.push('PASS screenshot .codex-verify-900.png captured');

  ok(consoleErrors.length === 0, `no console errors (${consoleErrors.length})`);
  if (consoleErrors.length > 0) {
    console.log('Console errors:', consoleErrors.join('\n'));
  }
} finally {
  await browser.close();
}

console.log(`\n${results.length} checks passed`);
console.log(results.join('\n'));
