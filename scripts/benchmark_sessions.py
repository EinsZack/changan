#!/usr/bin/env python
"""Controlled A/B comparison on the isolated instance, with identical image/CPU/input.

A uses the legacy one-attempt entry point retained in the same approved image;
B uses task sessions. Only this validation script may change a task's frozen
execution mode before its first dispatch. No user-facing mode toggle is added.
"""
import hashlib
import json
import os
import statistics
import sys
import time
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app import db
from app.config import API_KEY, DATA

base = os.getenv('CHANGAN_TEST_BASE', 'http://127.0.0.1:8410')
assert DATA.name == 'k3s-validation' and base.endswith(':8410'), 'isolated validation only'
c = httpx.Client(base_url=base, timeout=30, trust_env=False, headers={'X-API-Key': API_KEY})
report = {'at': time.time(), 'environment': 'single server, 4 k3d/K3s agents, CPU ONNX; not a vehicle performance claim',
          'input_sha256': hashlib.sha256((ROOT / 'assets/samples/demo.mp4').read_bytes()).hexdigest(),
          'conditions': 'same image, model, 1-second segments, 15 requested sample fps, 2 CPU limit per container; cached image; sequential tests', 'runs': []}
output = ROOT / 'docs/session-ab-benchmark.json'
terminal = ('SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT')
for mode in ['A', 'B', 'B', 'A', 'A', 'B']:
    live = httpx.get('http://127.0.0.1:8310/api/tasks', trust_env=False, headers={'X-API-Key': API_KEY}).raise_for_status().json()
    assert all(t['status'] in terminal for t in live), 'production work must not overlap benchmark'
    assert not db.query("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')"), 'validation work must be idle'
    params = {'name': f'{mode}正式对照 · {len(report["runs"])+1}', 'algorithm': 'resource_aware',
              'segment_seconds': 1, 'sample_fps': 15, 'attempt_timeout': 300, 'deadline_seconds': 600, 'node_ids': ['benchmark-hold']}
    tid = c.post('/api/tasks', json=params).raise_for_status().json()['id']
    with db.connect() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row = conn.execute('SELECT params,metadata FROM tasks WHERE id=?', (tid,)).fetchone()
        p = db.loads(row['params']); p['node_ids'] = []
        m = db.loads(row['metadata']); m['application']['execution_mode'] = 'attempt_job' if mode == 'A' else 'task_session'
        conn.execute('UPDATE tasks SET params=?,metadata=? WHERE id=?', (db.dumps(p), db.dumps(m), tid))
    print(mode, tid, 'started', flush=True)
    until = time.monotonic() + 650
    while time.monotonic() < until:
        t = c.get('/api/tasks/' + tid).raise_for_status().json()
        if t['status'] in terminal and all(a['cleanup_state'] == 'DONE' for a in t['attempts']) and all(s['cleanup_state'] == 'DONE' for s in t['sessions']):
            break
        time.sleep(.5)
    assert t['status'] == 'SUCCEEDED', t['error']
    assert len(t['attempts']) == 24 and t['result']['sampled_frames'] == 240
    if report['runs']:
        assert t['result']['counts'] == report['runs'][0]['counts']
        assert t['metadata']['application']['image'] == report['runs'][0]['image']
    events = c.get('/api/tasks/' + tid + '/events').raise_for_status().json()
    loads = sum(s['model_load_count'] for s in t['sessions']) if mode == 'B' else sum(e['type'] == 'container_stage' and e['payload'].get('state') == 'MODEL_LOADING' for e in events)
    row = {'mode': mode, 'task_id': tid, 'elapsed_seconds': t['finished'] - t['created'], 'cleanup_observed_seconds': time.time() - t['created'],
           'pods': len({a['execution_id'] for a in t['attempts']}), 'model_loads': loads,
           'sampled_frames': t['result']['sampled_frames'], 'counts': t['result']['counts'], 'image': t['metadata']['application']['image'],
           'inference_seconds_sum': sum(s['result']['elapsed_seconds'] for s in t['segments']),
           'input_download_seconds_sum': sum(s['result']['download_seconds'] for s in t['segments'])}
    assert row['pods'] == (24 if mode == 'A' else 4)
    assert loads == (24 if mode == 'A' else 4)
    report['runs'].append(row)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(row, ensure_ascii=False), flush=True)
for mode in ('A', 'B'):
    report[mode + '_median_seconds'] = statistics.median(r['elapsed_seconds'] for r in report['runs'] if r['mode'] == mode)
report['elapsed_reduction_percent'] = (1 - report['B_median_seconds'] / report['A_median_seconds']) * 100
report['speedup_ratio'] = report['A_median_seconds'] / report['B_median_seconds']
output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({k: v for k, v in report.items() if k.endswith('seconds') or k in ('elapsed_reduction_percent', 'speedup_ratio')}, ensure_ascii=False), flush=True)
