import test from 'node:test';
import assert from 'node:assert/strict';
import {taskMeasurements, metricValue, byteValue, durationValue} from '../src/taskMetrics.js';

function segment(id, result = {}, extra = {}) {
    return {id, ordinal: Number(id), status: 'SUCCEEDED', active_attempt: 'try-' + id,
        result: {attempt_id: 'try-' + id, actual_node: 'lab-1', ...result}, ...extra};
}

test('weights rates by elapsed time and counts each valid segment once', () => {
    const metrics = taskMeasurements({total: 2, segments: [
        segment('0', {input_bytes: 1000000, input_transfer_seconds: 1, sampled_frames: 20, elapsed_seconds: 2, measured_fps: 10}),
        segment('1', {input_bytes: 3000000, input_transfer_seconds: 6, sampled_frames: 60, elapsed_seconds: 3, measured_fps: 20}),
    ], attempts: [{id: 'old', segment_id: '0', status: 'FAILED', node_id: 'lab-2'}]});
    assert.equal(metrics.inputBytes, 4000000);
    assert.ok(Math.abs(metrics.receiveMbps - 32 / 7) < 1e-12);
    assert.equal(metrics.rows[0].receiveMbps, 8);
    assert.equal(metrics.rows[1].receiveMbps, 4);
    assert.equal(metrics.processingFps, 16);
    assert.equal(metrics.resultCount, 2);
    assert.equal(metrics.transferCount, 2);
});

test('excludes stale results and unfinished segments even when a result object exists', () => {
    const metrics = taskMeasurements({segments: [
        segment('0', {input_bytes: 9000000, attempt_id: 'old'}),
        segment('1', {input_bytes: 9000000}, {status: 'RUNNING'}),
        segment('2', {input_bytes: 1000}),
    ]});
    assert.equal(metrics.inputBytes, 1000);
    assert.equal(metrics.resultCount, 1);
    assert.equal(metrics.rows[0].nodeId, undefined);
    assert.equal(metrics.receiveMbps, null);
});

test('missing, zero-duration and invalid values never create fake zero rates', () => {
    const metrics = taskMeasurements({total: 5, segments: [
        segment('0', {input_bytes: 100, input_transfer_seconds: 0, elapsed_seconds: 0, sampled_frames: 10}),
        segment('1', {input_bytes: null, input_transfer_seconds: 1}),
        segment('2', {input_bytes: -1, input_transfer_seconds: 1, measured_fps: Infinity}),
        segment('3', {input_bytes: '100', input_transfer_seconds: 1, measured_fps: NaN}),
        segment('4'),
    ]});
    assert.equal(metrics.inputBytes, 100);
    assert.equal(metrics.inputCount, 1);
    assert.equal(metrics.transferCount, 0);
    assert.equal(metrics.receiveMbps, null);
    assert.equal(metrics.processingFps, null);
    assert.equal(metrics.rows[2].processingFps, null);
    assert.equal(taskMeasurements({}).inputBytes, null);
});

test('partial results use only paired measures and respect the worker timing floor', () => {
    const metrics = taskMeasurements({total: 3, segments: [
        segment('0', {input_bytes: 1000, input_transfer_seconds: 0.0001, sampled_frames: 1, elapsed_seconds: 0.0002}),
        segment('1', {input_bytes: 9000}),
        segment('2', {}, {status: 'RUNNING'}),
    ]});
    assert.equal(metrics.inputBytes, 10000);
    assert.equal(metrics.receiveMbps, 8);
    assert.equal(metrics.processingFps, 1000);
    assert.equal(metrics.transferCount, 1);
    assert.equal(metrics.resultCount, 2);
    assert.equal(metrics.total, 3);
});

test('uses the selected node historical snapshot without falling back to another candidate', () => {
    const metrics = taskMeasurements({segments: [segment('0')], attempts: [
        {id: 'first', segment_id: '0', node_id: 'lab-2', decision: {candidates: [
            {node_id: 'lab-1', snapshot: {bandwidth_mbps: 999}},
            {node_id: 'lab-2', snapshot_at: 123, transfer_seconds: 0.4, network_source: 'saved observation',
                snapshot: {bandwidth_mbps: 20, observed_http_rtt_ms: 5}},
        ]}},
        {id: 'second', node_id: 'lab-3'},
    ]});
    assert.equal(metrics.snapshots[0].bandwidthMbps, 20);
    assert.equal(metrics.snapshots[0].rttMs, 5);
    assert.equal(metrics.snapshots[0].observedAt, 123);
    assert.equal(metrics.snapshots[1].bandwidthMbps, null);
    assert.equal(metrics.snapshots[1].transferSeconds, null);
});

test('formats bytes, units and missing data consistently', () => {
    assert.equal(byteValue(0), '0 B');
    assert.equal(byteValue(1024), '1.00 KiB');
    assert.equal(byteValue(1048576), '1.00 MiB');
    assert.equal(byteValue(null), '—');
    assert.equal(metricValue(Infinity, 'Mbit/s'), '—');
    assert.equal(metricValue(0, 'ms'), '0.00 ms');
    assert.equal(durationValue(0), '—');
    assert.equal(durationValue(0.00001), '< 0.001 s');
});
