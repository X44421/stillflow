import { chromium } from 'playwright';

const captureId = '5125cefa-57bc-4cf4-b7cc-0087dd1b8c22';
const endpoint =
  'https://mcp.figma.com/mcp/capture/5125cefa-57bc-4cf4-b7cc-0087dd1b8c22/submit?bindVariables=true';
const targetUrl =
  'http://localhost:8080/preview-workspace.html#figmacapture=5125cefa-57bc-4cf4-b7cc-0087dd1b8c22&figmaendpoint=https%3A%2F%2Fmcp.figma.com%2Fmcp%2Fcapture%2F5125cefa-57bc-4cf4-b7cc-0087dd1b8c22%2Fsubmit%3FbindVariables%3Dtrue&figmadelay=2500';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

try {
  await page.goto(targetUrl, { waitUntil: 'networkidle', timeout: 90000 });
  await page.waitForTimeout(4000);

  const result = await page.evaluate(async () => {
    if (window.figma?.captureForDesign) {
      return { status: 'auto-captured' };
    }
    return { status: 'waiting-for-auto-capture' };
  });

  console.log(JSON.stringify(result, null, 2));
} finally {
  await browser.close();
}
