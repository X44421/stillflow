import { chromium } from 'playwright';

const browser = await chromium.launch({
  executablePath: 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  headless: true,
  args: ['--no-sandbox', '--disable-gpu'],
});

try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  await page.goto('http://127.0.0.1:5173/', { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.locator('.still-workspace-main').waitFor({ state: 'visible', timeout: 15000 });
  await page.waitForTimeout(1000);

  await page.getByRole('button', { name: 'Normalize & Validate', exact: true }).click();
  await page.waitForTimeout(300);

  const tab = page.getByRole('tab', { name: /Customer Clean Session/ });
  const box = await tab.boundingBox();
  console.log('tab box', box);
  console.log('tab visible', await tab.isVisible());
  console.log('search box', await page.locator('.still-global-search').boundingBox());
  console.log('elements at tab center', await page.evaluate(({ x, y }) => {
    return document.elementsFromPoint(x, y).slice(0, 8).map((el) => `${el.tagName}.${typeof el.className === 'string' ? el.className : ''}`);
  }, { x: box.x + box.width / 2, y: box.y + box.height / 2 }));

  await page.mouse.click(box.x + box.width / 2, box.y + box.height / 2);
  await page.waitForTimeout(400);
  console.log('active tab after mouse click', await page.locator('.still-object-tab.is-active .still-object-tab__name').allTextContents());
  console.log('preview title after mouse click', await page.locator('.still-preview-header__title').textContent());
  console.log('inspector title after mouse click', await page.locator('.still-inspector-title-row h2').textContent());

  await page.screenshot({ path: '.codex-debug-tabs.png' });
} finally {
  await browser.close();
}
