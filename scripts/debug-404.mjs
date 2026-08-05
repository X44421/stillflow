import { chromium } from 'playwright';

const chromePath = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe';
const browser = await chromium.launch({
  executablePath: chromePath,
  headless: true,
  args: ['--no-sandbox', '--disable-gpu'],
});
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
page.on('console', (message) => console.log('console:', message.type(), message.text()));
page.on('console', (message) => {
  if (message.type() === 'error') console.log('console location:', message.location());
});
page.on('pageerror', (error) => console.log('pageerror:', error.message));
page.on('response', (response) => {
  if (response.url().includes('RedHat')) {
    console.log('font response:', response.status(), response.url());
    console.log('font headers:', response.headers());
  }
  if (response.status() >= 400) {
    console.log('http:', response.status(), response.url());
  }
});
page.on('requestfailed', (request) => console.log('requestfailed:', request.url(), request.failure()?.errorText));
page.on('request', (request) => console.log('request:', request.method(), request.url()));
await page.goto('http://127.0.0.1:5173/', { waitUntil: 'domcontentloaded', timeout: 30000 });
await page.waitForTimeout(3000);
const resources = await page.evaluate(() => performance.getEntriesByType('resource').map((entry) => ({ name: entry.name, status: entry.responseStatus, duration: entry.duration })));
console.log('bad resources:', JSON.stringify(resources.filter((entry) => entry.status >= 400 || entry.duration > 10000), null, 2));
console.log('font entries:', JSON.stringify(resources.filter((entry) => entry.name.includes('RedHat')), null, 2));
await browser.close();
