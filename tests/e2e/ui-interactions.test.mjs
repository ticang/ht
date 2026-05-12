import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createRequire } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const require = createRequire(import.meta.url);
const root = path.resolve('.');

async function loadPlaywright() {
  try {
    return await import('playwright');
  } catch {
    const bundled = path.join(
      os.homedir(),
      '.cache',
      'codex-runtimes',
      'codex-primary-runtime',
      'dependencies',
      'node',
      'node_modules',
      'playwright'
    );
    return require(bundled);
  }
}

function edgeExecutablePath() {
  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE) {
    return process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE;
  }

  if (process.platform === 'win32') {
    return 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe';
  }

  return undefined;
}

function pageUrl(fileName) {
  return pathToFileURL(path.join(root, 'ui', fileName)).href;
}

async function openPage(browser, fileName) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const consoleErrors = [];
  const pageErrors = [];

  page.on('console', (message) => {
    if (message.type() === 'error') {
      consoleErrors.push(message.text());
    }
  });
  page.on('pageerror', (error) => pageErrors.push(error.message));

  await page.goto(pageUrl(fileName), { waitUntil: 'load' });
  await page.waitForTimeout(100);

  return { page, consoleErrors, pageErrors };
}

async function visibleTableRows(page) {
  return page.locator('tbody tr').evaluateAll((rows) =>
    rows
      .filter((row) => row.offsetParent !== null && getComputedStyle(row).display !== 'none')
      .map((row) => row.innerText.replace(/\s+/g, ' ').trim())
  );
}

test('UI prototype interactions', async (t) => {
  const { chromium } = await loadPlaywright();
  const browser = await chromium.launch({
    headless: true,
    executablePath: edgeExecutablePath(),
  });

  t.after(async () => {
    await browser.close();
  });

  await t.test('contracts page filters rows by search text and status pill', async () => {
    const { page, consoleErrors, pageErrors } = await openPage(browser, 'contracts.html');

    await page.getByPlaceholder('搜索合同编号、名称...').fill('租赁');
    let rows = await visibleTableRows(page);
    assert.equal(rows.length, 1);
    assert.match(rows[0], /HT-2026-003/);
    assert.match(rows[0], /办公场地租赁合同/);

    await page.getByPlaceholder('搜索合同编号、名称...').fill('');
    await page.locator('.pill').filter({ hasText: /^签署/ }).click();
    rows = await visibleTableRows(page);
    assert.equal(rows.length, 1);
    assert.match(rows[0], /HT-2026-002/);
    assert.match(rows[0], /签署/);

    assert.deepEqual(consoleErrors, []);
    assert.deepEqual(pageErrors, []);
    await page.close();
  });

  await t.test('regulations page filters by query, category, and toggles search mode', async () => {
    const { page, consoleErrors, pageErrors } = await openPage(browser, 'regulations.html');

    await page.getByPlaceholder('搜索法规名称、内容或关键词...').fill('民法典');
    let rows = await visibleTableRows(page);
    assert.equal(rows.length, 1);
    assert.match(rows[0], /中华人民共和国民法典/);

    await page.getByPlaceholder('搜索法规名称、内容或关键词...').fill('');
    await page.getByRole('button', { name: /部门规章/ }).click();
    rows = await visibleTableRows(page);
    assert.equal(rows.length, 1);
    assert.match(rows[0], /合同违法行为监督处理办法/);

    await page.getByRole('button', { name: '关键词' }).click();
    await assertActiveText(page, '.segmented-btn.active', '关键词');

    assert.deepEqual(consoleErrors, []);
    assert.deepEqual(pageErrors, []);
    await page.close();
  });

  await t.test('compliance page keeps batch selection count in the action button', async () => {
    const { page, consoleErrors, pageErrors } = await openPage(browser, 'compliance.html');

    await assertText(page, 'a.btn-primary[href="compliance-report.html"] >> text=批量校验', '批量校验 (2)');
    await page.getByLabel('ZZ集团租赁合同').check();
    await assertText(page, 'a.btn-primary[href="compliance-report.html"] >> text=批量校验', '批量校验 (3)');
    await page.getByLabel('YY科技技术服务协议').uncheck();
    await assertText(page, 'a.btn-primary[href="compliance-report.html"] >> text=批量校验', '批量校验 (2)');

    assert.deepEqual(consoleErrors, []);
    assert.deepEqual(pageErrors, []);
    await page.close();
  });

  await t.test('dashboard sends a chat message and clears the input', async () => {
    const { page, consoleErrors, pageErrors } = await openPage(browser, 'dashboard.html');
    const input = page.getByPlaceholder('输入消息，或上传合同文件...');

    await input.fill('查询民法典下采购合同的付款风险');
    await page.locator('.send-btn').click();

    const latestUserMessage = page.locator('.msg-row.user .user-bubble').last();
    await assertText(latestUserMessage, undefined, '查询民法典下采购合同的付款风险');
    await assertContains(page.locator('.msg-row .agent-bubble').last(), undefined, '已收到您的问题');
    assert.equal(await input.inputValue(), '');

    assert.deepEqual(consoleErrors, []);
    assert.deepEqual(pageErrors, []);
    await page.close();
  });
});

async function assertText(pageOrLocator, selector, expected) {
  const locator = selector ? pageOrLocator.locator(selector).first() : pageOrLocator;
  await locator.waitFor({ state: 'visible' });
  const text = (await locator.innerText()).replace(/\s+/g, ' ').trim();
  assert.equal(text, expected);
}

async function assertActiveText(page, selector, expected) {
  const text = (await page.locator(selector).innerText()).replace(/\s+/g, ' ').trim();
  assert.equal(text, expected);
}

async function assertContains(pageOrLocator, selector, expected) {
  const locator = selector ? pageOrLocator.locator(selector).first() : pageOrLocator;
  await locator.waitFor({ state: 'visible' });
  const text = (await locator.innerText()).replace(/\s+/g, ' ').trim();
  assert.match(text, new RegExp(expected));
}
