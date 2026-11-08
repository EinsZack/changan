#!/usr/bin/env python
"""Real 24-segment regression on the isolated K3s validation service (8410)."""
import json
import math
import sys
import time
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.config import API_KEY


def main():
    c = httpx.Client(base_url='http://127.0.0.1:8410', headers={'X-API-Key': API_KEY}, timeout=30, trust_env=False)
    terminal = ('SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT')
    production = httpx.get('http://127.0.0.1:8310/api/tasks', headers={'X-API-Key': API_KEY}, trust_env=False).raise_for_status().json()
    assert all(t['status'] in terminal for t in production), 'shared lab cluster has production work'
    assert all(t['status'] in terminal for t in c.get('/api/tasks').raise_for_status().json()), 'validation has active tasks'
    tid = c.post('/api/tasks', json={'name': '网络统计修复验收 · 24分片闭环', 'segment_seconds': 1,
        'sample_fps': 10, 'algorithm': 'resource_aware', 'max_retries': 0,
        'attempt_timeout': 180, 'deadline_seconds': 600}).raise_for_status().json()['id']
    print('validation task', tid, flush=True)
    for _ in range(600):
        t = c.get('/api/tasks/' + tid).raise_for_status().json()
        if t['status'] in terminal and all(s['cleanup_state'] == 'DONE' for s in t['sessions']):
            break
        time.sleep(.5)
    assert t['status'] == 'SUCCEEDED', (t['status'], t.get('error'))
    assert len(t['sessions']) == 4 and all(s['model_load_count'] == 1 for s in t['sessions'])
    assert t['total'] == 24 and len(t['attempts']) == 24
    for seg in t['segments']:
        r = seg['result']
        assert r['input_transfer_seconds'] == r['download_seconds'] > 0
        assert math.isclose(r['observed_transfer_mbps'], r['input_bytes'] * 8 / max(r['input_transfer_seconds'], .001) / 1e6)
    known, unknown = [], []
    for a in t['attempts']:
        selected = next(x for x in a['decision']['candidates'] if x['node_id'] == a['node_id'])
        if selected['transfer_seconds'] is None:
            unknown.append(a['id'])
            continue
        s = selected['snapshot']
        assert s['bandwidth_mbps'] > 0 and 0 < s['network_sample_count'] <= 8
        assert s['network_expires_at'] > a['decision']['at']
        assert a['id'] not in s['network_sample_attempt_ids']
        seg = next(seg for seg in t['segments'] if seg['id'] == a['segment_id'])
        assert selected['transfer_seconds'] == round(seg['result']['input_bytes'] * 8 / s['bandwidth_mbps'] / 1e6, 4)
        known.append(a['id'])
    assert known, 'no later decision used transfer observations'
    for _ in range(20):
        nodes = c.get('/api/nodes').raise_for_status().json()
        if all(n['snapshot'].get('bandwidth_mbps') for n in nodes):
            break
        time.sleep(.5)
    assert len(nodes) == 4 and all(n['snapshot'].get('bandwidth_mbps') for n in nodes)
    report = {'task_id': tid, 'status': t['status'], 'total': t['total'], 'sampled_frames': t['result']['sampled_frames'],
        'sessions': len(t['sessions']), 'model_loads': sum(s['model_load_count'] for s in t['sessions']),
        'measured_dispatches': len(known), 'unknown_dispatches': len(unknown),
        'node_networks': [{k: n['snapshot'].get(k) for k in ['node_id', 'bandwidth_mbps', 'network_sample_count',
            'network_observed_at', 'network_expires_at', 'network_source']} for n in nodes],
        'image': t['metadata']['application']['image'], 'all_cleaned': all(s['cleanup_state'] == 'DONE' for s in t['sessions']),
        'environment': 'same server / four K3s logical nodes / CPU ONNX; no THOR', 'at': time.time()}
    assert report['all_cleaned']
    output = ROOT / 'docs/20261108_network_metrics'
    output.mkdir(exist_ok=True)
    (output / 'integration.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    (output / 'validation-task.json').write_text(c.get('/api/tasks/' + tid + '/export').raise_for_status().text)
    print(json.dumps(report, ensure_ascii=False), flush=True)
    c.close()


if __name__ == '__main__':
    main()
