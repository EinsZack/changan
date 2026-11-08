"""Recent input-download observations, never nominal link capacity or live probes."""
import math
from . import db

SAMPLE_TTL_SECONDS = 300
MAX_SAMPLES = 8
NETWORK_KEYS = ('bandwidth_mbps', 'network_source', 'network_observed_at',
                'network_expires_at', 'network_sample_count', 'network_sample_attempt_ids',
                'network_sample_time_source', 'network_window_seconds')


def positive(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0 else None


def container_network_snapshot(node, cluster, now):
    snapshot = {'bandwidth_mbps': None,
                'network_source': '暂无近期分片下载实测（尚无样本或样本已过期）',
                'network_observed_at': None, 'network_expires_at': None,
                'network_sample_count': 0, 'network_sample_attempt_ids': [],
                'network_window_seconds': SAMPLE_TTL_SECONDS,
                'network_sample_time_source': 'server_attempt_started_at'}
    rows = db.query("SELECT id,started,finished,result,workload,decision FROM attempts WHERE node_id=? AND backend='k3s' AND status='SUCCEEDED' AND finished>? ORDER BY finished DESC LIMIT 32",
                    (node['id'], now - SAMPLE_TTL_SECONDS))
    samples = []
    for row in rows:
        # Server assignment time is a conservative lower bound for download time.
        # This also works for old images and does not depend on node clock sync.
        at = positive(row['started'])
        if at is None or not now - SAMPLE_TTL_SECONDS < at <= row['finished'] <= now:
            continue
        work = db.loads(row['workload'], {})
        decision = db.loads(row['decision'], {})
        selected = next((c.get('snapshot', {}) for c in decision.get('candidates', []) if c['node_id'] == node['id']), {})
        uid = work.get('node_uid') or selected.get('node_uid')
        if not uid or uid != node['uid'] or work.get('cluster') != cluster:
            continue
        result = db.loads(row['result'], {})
        size = positive(result.get('input_bytes'))
        seconds = positive(result.get('input_transfer_seconds')) or positive(result.get('download_seconds'))
        if size is None or seconds is None:
            continue
        samples.append((row['id'], at, size, max(seconds, .001)))
        if len(samples) == MAX_SAMPLES:
            break
    if samples:
        rate = sum(s[2] for s in samples) * 8 / sum(s[3] for s in samples) / 1e6
        if positive(rate) is None:
            return snapshot
        scope = '；同机 K3s 逻辑节点' if node['kind'] == 'k3s_logical' else ''
        snapshot.update(bandwidth_mbps=rate,
                        network_source='近期成功分片 HTTP 下载实测（含请求与落盘' + scope + '）',
                        network_observed_at=max(s[1] for s in samples),
                        network_expires_at=min(s[1] for s in samples) + SAMPLE_TTL_SECONDS,
                        network_sample_count=len(samples), network_sample_attempt_ids=[s[0] for s in samples])
    return snapshot


def overlay_pool_network(snapshot, authority):
    # A pool-provided value (including explicit unknown) replaces the local estimate.
    # Never attach local sample metadata to a different pool measurement.
    if 'bandwidth_mbps' not in authority:
        return
    for key in NETWORK_KEYS:
        snapshot.pop(key, None)
    snapshot.update({key: authority[key] for key in NETWORK_KEYS if key in authority})
    snapshot['bandwidth_mbps'] = positive(authority['bandwidth_mbps'])
    snapshot['network_source'] = authority.get('network_source') or '外部资源池网络快照'
    snapshot.setdefault('network_observed_at', authority.get('observed_at'))
    snapshot.setdefault('network_expires_at', float(authority['observed_at']) + 8)
