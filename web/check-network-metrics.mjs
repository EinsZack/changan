// Read-only browser/protocol regression against an actual completed K3s task.
import {chromium, expect} from '@playwright/test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {taskMeasurements} from './src/taskMetrics.js';
const base = process.env.CHANGAN_BROWSER_BASE || 'http://127.0.0.1:8410';
const id = process.env.CHANGAN_NETWORK_TASK_ID;
assert.ok(id, 'CHANGAN_NETWORK_TASK_ID is required; this check creates no tasks');
const response = await fetch(base + '/api/tasks/' + id);
assert.equal(response.status, 200);
const task = await response.json();
assert.equal(task.status, 'SUCCEEDED');
const metrics = taskMeasurements(task);
assert.equal(metrics.transferCount, task.total);
assert.ok(metrics.receiveMbps > 0 && Number.isFinite(metrics.receiveMbps));
const measured = task.attempts.find(a => a.decision.candidates.some(c => c.node_id === a.node_id && c.transfer_seconds != null));
if (process.env.CHANGAN_NETWORK_EXPECT_FEEDBACK === '1') assert.ok(measured, 'a later dispatch must consume observed transfer rate');
const output = process.env.CHANGAN_NETWORK_EVIDENCE || '../docs/20261108_network_metrics';
fs.mkdirSync(output, {recursive: true});
const browser = await chromium.launch({headless: true});
const errors = [];
try {
    const page = await browser.newPage({viewport: {width: 1440, height: 1000}});
    page.on('pageerror', error => errors.push(error.message));
    await page.goto(base);
    await page.getByRole('button', {name: '任务中心', exact: false}).first().click();
    await page.getByText(task.name, {exact: true}).first().click();
    await page.getByRole('button', {name: '传输与吞吐', exact: true}).click();
    await expect(page.locator('.measurement-cards')).toContainText(metrics.receiveMbps.toFixed(2) + ' Mbit/s');
    await expect(page.getByRole('region', {name: '片段执行实测明细'}).locator('tbody tr')).toHaveCount(task.total);
    await page.screenshot({path: output + '/transfers.png', fullPage: true});
    if (measured) {
        await page.getByRole('button', {name: '调度决策', exact: true}).click();
        await page.locator('.panel-title select').selectOption(measured.id);
        await expect(page.locator('tr.highlight')).toContainText('HTTP 下载实测');
        await expect(page.locator('tr.highlight')).not.toContainText('暂无有效传输观测');
        await page.screenshot({path: output + '/decision.png', fullPage: true});
    }
    assert.deepEqual(errors, []);
    const report = {task_id: task.id, total: task.total, transfer_count: metrics.transferCount,
        receive_mbps: metrics.receiveMbps, saved_network_decisions: metrics.snapshots.filter(s => s.transferSeconds !== null).length,
        page_errors: errors, environment: 'same server K3s logical nodes; no THOR or vehicle network', at: new Date().toISOString()};
    fs.writeFileSync(output + '/browser-check.json', JSON.stringify(report, null, 2) + '\n');
    console.log(JSON.stringify(report));
} finally {
    await browser.close();
}
