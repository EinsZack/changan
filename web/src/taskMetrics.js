const nonnegative = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
const positive = value => nonnegative(value) !== null && value > 0 ? value : null;
const sum = (rows, key) => rows.reduce((total, row) => total + row[key], 0);

export function taskMeasurements(task) {
    const segments = task.segments || [];
    const attempts = task.attempts || [];
    const rows = segments.map(segment => {
        const result = segment.status === 'SUCCEEDED' && segment.active_attempt &&
            segment.result?.attempt_id === segment.active_attempt ? segment.result : null;
        const inputBytes = nonnegative(result?.input_bytes);
        const transferSeconds = positive(result?.input_transfer_seconds) ?? positive(result?.download_seconds);
        const processingSeconds = positive(result?.elapsed_seconds);
        const sampledFrames = nonnegative(result?.sampled_frames);
        const receiveMbps = inputBytes !== null && transferSeconds !== null ?
            inputBytes * 8 / Math.max(transferSeconds, 0.001) / 1e6 : null;
        return {
            id: segment.id, ordinal: segment.ordinal, status: segment.status,
            hasResult: Boolean(result), nodeId: result?.actual_node,
            inputBytes, transferSeconds, receiveMbps, processingSeconds, sampledFrames,
            processingFps: nonnegative(result?.measured_fps),
        };
    });
    const inputs = rows.filter(row => row.inputBytes !== null);
    const transfers = rows.filter(row => row.receiveMbps !== null);
    const processing = rows.filter(row => row.sampledFrames !== null && row.processingSeconds !== null);
    const snapshots = attempts.map(attempt => {
        const candidate = attempt.decision?.candidates?.find(item => item.node_id === attempt.node_id);
        const snapshot = candidate?.snapshot || {};
        return {
            id: attempt.id, ordinal: segments.find(segment => segment.id === attempt.segment_id)?.ordinal,
            nodeId: attempt.node_id, status: attempt.status,
            observedAt: positive(candidate?.snapshot_at),
            networkObservedAt: positive(snapshot.network_observed_at),
            networkExpiresAt: positive(snapshot.network_expires_at),
            networkSampleCount: positive(snapshot.network_sample_count),
            rttMs: nonnegative(snapshot.observed_http_rtt_ms),
            bandwidthMbps: positive(snapshot.bandwidth_mbps),
            transferSeconds: nonnegative(candidate?.transfer_seconds),
            source: candidate?.network_source || snapshot.network_source || '未记录来源',
        };
    });
    return {
        rows, snapshots, total: task.total || segments.length,
        resultCount: rows.filter(row => row.hasResult).length,
        inputCount: inputs.length, transferCount: transfers.length, processingCount: processing.length,
        inputBytes: inputs.length ? sum(inputs, 'inputBytes') : null,
        receiveMbps: transfers.length ? sum(transfers, 'inputBytes') * 8 /
            transfers.reduce((total, row) => total + Math.max(row.transferSeconds, 0.001), 0) / 1e6 : null,
        processingFps: processing.length ? sum(processing, 'sampledFrames') /
            processing.reduce((total, row) => total + Math.max(row.processingSeconds, 0.001), 0) : null,
    };
}

export function metricValue(value, unit, digits = 2) {
    return nonnegative(value) === null ? '—' : value.toFixed(digits) + ' ' + unit;
}

export function byteValue(value) {
    if (nonnegative(value) === null) return '—';
    if (value < 1024) return value.toFixed(0) + ' B';
    if (value < 1024 ** 2) return (value / 1024).toFixed(2) + ' KiB';
    return (value / 1024 ** 2).toFixed(2) + ' MiB';
}

export function durationValue(value) {
    if (positive(value) === null) return '—';
    return value < 0.001 ? '< 0.001 s' : value.toFixed(3) + ' s';
}
