import {test, expect} from '@playwright/test';
import fs from 'node:fs';

function fixture() {
    const created = 1793440000;
    const segments = [0, 1].map(ordinal => ({
        id: 'segment-' + ordinal, ordinal, start: ordinal * 4, end: ordinal * 4 + 4,
        status: 'SUCCEEDED', active_attempt: 'try-' + ordinal,
        result: {attempt_id: 'try-' + ordinal, actual_node: 'lab-' + (ordinal + 1), kind: 'server_process',
            input_bytes: ordinal ? 3000000 : 1000000, input_transfer_seconds: ordinal ? 3 : 1,
            sampled_frames: ordinal ? 60 : 20, elapsed_seconds: ordinal ? 3 : 2, measured_fps: ordinal ? 20 : 10},
    }));
    const attempts = segments.map((segment, ordinal) => ({
        id: segment.active_attempt, segment_id: segment.id, node_id: 'lab-' + (ordinal + 1),
        actual_node: 'lab-' + (ordinal + 1), status: 'SUCCEEDED', created, started: created + 1,
        updated: created + 4, finished: created + 4,
        decision: {duration_ms: 0.1, candidates: [{
            node_id: 'lab-' + (ordinal + 1), eligible: true, reasons: [], compute_seconds: 2,
            transfer_seconds: 0.4, estimate_source: '实测推理吞吐', snapshot_at: created - 5,
            network_source: '历史片段HTTP观测（浏览器测试夹具）',
            snapshot: {observed_http_rtt_ms: 12.5, bandwidth_mbps: 20},
        }]},
    }));
    return {id: 'task_metrics_fixture', name: '统计展示测试（固定夹具）', created, started: created, finished: created + 5,
        status: 'SUCCEEDED', total: 2, completed: 2, params: {algorithm: 'resource_aware'},
        trigger_type: 'service', result: null, segments, attempts};
}

async function openFixture(page, getTask) {
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('**/api/**', async route => {
        const task = getTask();
        const path = new URL(route.request().url()).pathname;
        let body;
        if (path === '/api/overview') body = {totals: {[task.status]: 1}, nodes: []};
        else if (path === '/api/tasks') body = [task];
        else if (path === '/api/tasks/' + task.id) body = task;
        else if (path === '/api/alerts') body = {summary: {unread: 0, open: 0, acknowledged: 0}, items: [], total: 0};
        else if (path === '/api/strategies' || path.endsWith('/events')) body = [];
        else throw new Error('Unexpected API request: ' + path);
        await route.fulfill({json: body});
    });
    await page.goto('/');
    await page.getByText(getTask().name, {exact: true}).click();
    await page.getByRole('button', {name: '传输与吞吐', exact: true}).click();
    await expect(page.getByRole('heading', {name: '片段执行实测'})).toBeVisible();
    return errors;
}

test('shows weighted task measurements and separate historical network observations', async ({page}) => {
    const task = fixture();
    const errors = await openFixture(page, () => task);
    const cards = page.locator('.measurement-cards');
    await expect(cards).toContainText('3.81 MiB');
    await expect(cards).toContainText('8.00 Mbit/s');
    await expect(cards).toContainText('16.00 帧/s');
    await expect(cards).toContainText('2 / 2');
    const execution = page.getByRole('region', {name: '片段执行实测明细'});
    await expect(execution.locator('tbody tr')).toHaveCount(2);
    await expect(execution).toContainText('10.00 帧/s');
    await expect(execution).toContainText('20.00 帧/s');
    const snapshots = page.getByRole('region', {name: '调度时网络观测明细'});
    await expect(snapshots).toContainText('12.50 ms');
    await expect(snapshots).toContainText('20.00 Mbit/s');
    await expect(snapshots).toContainText('0.400 s');
    await expect(snapshots).toContainText('历史片段HTTP观测');
    await page.getByText('统计口径与单位', {exact: true}).click();
    await expect(page.locator('.measurement-method')).toContainText('1 MiB = 1024² B');
    fs.mkdirSync('../docs/screenshots', {recursive: true});
    await page.screenshot({path: '../docs/screenshots/task-metrics-fixture-desktop.png', fullPage: true});
    await page.getByRole('button', {name: '调度决策', exact: true}).click();
    await expect(page.getByRole('heading', {name: '算法输入与选择依据'})).toBeVisible();
    await page.getByRole('button', {name: '传输与吞吐', exact: true}).click();
    await expect(cards).toContainText('8.00 Mbit/s');
    expect(errors).toEqual([]);
});

test('shows missing measures and no allocation without inventing zero rates', async ({page}) => {
    const task = fixture();
    task.status = 'TIMED_OUT';
    task.completed = 0;
    task.attempts = [];
    task.segments.forEach(segment => {segment.status = 'TIMED_OUT'; segment.result = null; segment.active_attempt = null;});
    const errors = await openFixture(page, () => task);
    await expect(page.getByRole('status')).toContainText('暂无有效片段测量');
    await expect(page.locator('.measurement-cards .metric > strong')).toHaveText(['—', '—', '—', '0 / 2']);
    await expect(page.getByText('尚未产生节点分配，暂无调度网络快照。')).toBeVisible();
    await expect(page.locator('.task-metrics')).not.toContainText('NaN');
    await expect(page.locator('.task-metrics')).not.toContainText('Infinity');
    expect(errors).toEqual([]);
});

test('refreshes partial results while keeping metrics scoped to effective attempts', async ({page}) => {
    const task = fixture();
    const finalSegment = structuredClone(task.segments[1]);
    task.status = 'RUNNING';
    task.finished = null;
    task.completed = 1;
    task.segments[1].status = 'RUNNING';
    task.segments[1].result.attempt_id = 'obsolete-attempt';
    const errors = await openFixture(page, () => task);
    await expect(page.locator('.measurement-cards .metric > strong')).toHaveText(['976.56 KiB', '8.00 Mbit/s', '10.00 帧/s', '1 / 2']);
    task.segments[1] = finalSegment;
    task.status = 'SUCCEEDED';
    task.completed = 2;
    task.finished = task.created + 5;
    await expect(page.locator('.measurement-cards .metric > strong')).toHaveText(['3.81 MiB', '8.00 Mbit/s', '16.00 帧/s', '2 / 2'], {timeout: 10000});
    expect(errors).toEqual([]);
});

test('keeps narrow screens usable with scrolling tables and visible units', async ({page}) => {
    const task = fixture();
    await page.setViewportSize({width: 390, height: 844});
    const errors = await openFixture(page, () => task);
    await expect(page.locator('.measurement-cards')).toContainText('8.00 Mbit/s');
    const region = page.getByRole('region', {name: '片段执行实测明细'});
    expect(await region.evaluate(element => element.scrollWidth > element.clientWidth)).toBe(true);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1)).toBe(true);
    fs.mkdirSync('../docs/screenshots', {recursive: true});
    await page.screenshot({path: '../docs/screenshots/task-metrics-fixture-mobile.png', fullPage: true});
    expect(errors).toEqual([]);
});
