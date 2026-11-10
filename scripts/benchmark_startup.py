#!/usr/bin/env python
"""Submit real multi-node video tasks and summarise per-node startup/participation from persisted records.

Usage: python scripts/benchmark_startup.py --label before --runs 3 --out docs/xxx/before.json
       python scripts/benchmark_startup.py --summarize task_a task_b     (existing tasks only)
Times are seconds relative to the task 'started' timestamp. Reads the formal database read-only.
"""
import argparse, json, sqlite3, statistics, sys, time
from pathlib import Path
import httpx
ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / 'data/control.sqlite'
TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT'}


def rows(c, sql, *args):
    return [dict(r) for r in c.execute(sql, args).fetchall()]


def summarize(task_id):
    c = sqlite3.connect('file:' + str(DB) + '?mode=ro', uri=True); c.row_factory = sqlite3.Row
    t = rows(c, 'SELECT * FROM tasks WHERE id=?', task_id)[0]
    t0 = t['started'] or t['created']
    events = rows(c, 'SELECT type,at,payload FROM events WHERE task_id=? ORDER BY id', task_id)
    nodes = []
    for s in rows(c, 'SELECT * FROM execution_sessions WHERE task_id=? ORDER BY node_id', task_id):
        w = json.loads(s['workload'] or '{}'); rt = w.get('inference_runtime') or {}
        claimed = next((e['at'] for e in events if e['type'] == 'session_claimed' and json.loads(e['payload']).get('session_id') == s['id']), None)
        done = rows(c, "SELECT count(*) n, max(finished) last FROM attempts WHERE session_id=? AND status='SUCCEEDED'", s['id'])[0]
        nodes.append({'node': s['node_id'], 'created': round(s['created'] - t0, 2),
                      'claimed': round(claimed - t0, 2) if claimed else None,
                      'ready': round(s['ready_at'] - t0, 2) if s['ready_at'] else None,
                      'warmup_s': round(rt.get('timings', {}).get('warmup_seconds', 0), 2) if rt else None,
                      'backend': rt.get('actual_backend'), 'segments': done['n'],
                      'last_done': round(done['last'] - t0, 2) if done['last'] else None})
    segs = rows(c, 'SELECT status,retries,result FROM segments WHERE task_id=?', task_id)
    frames = sum(json.loads(s['result'])['sampled_frames'] for s in segs if s['result'])
    return {'task_id': task_id, 'status': t['status'],
            'end_to_end_s': round((t['finished'] or time.time()) - t0, 2) if t['finished'] else None,
            'segments': f"{sum(s['status'] == 'SUCCEEDED' for s in segs)}/{len(segs)}", 'frames': frames,
            'retries': sum(s['retries'] for s in segs),
            'reassigned': sum(e['type'] == 'segment_reassigned' for e in events),
            'faults': sum(e['type'] in ('attempt_failed', 'session_api_unavailable', 'delivery_unknown') for e in events),
            'nodes': nodes}


def run(params, timeout):
    r = httpx.post('http://127.0.0.1:8310/api/tasks', json=params, timeout=10, trust_env=False)
    r.raise_for_status(); tid = r.json()['id']
    end = time.time() + timeout
    while time.time() < end:
        status = httpx.get('http://127.0.0.1:8310/api/tasks/' + tid, timeout=10, trust_env=False).json()['status']
        if status in TERMINAL:
            break
        time.sleep(1)
    time.sleep(12)  # let sessions be cleaned so the next run starts from cold Pods
    return tid


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--label', default='run'); p.add_argument('--runs', type=int, default=1)
    p.add_argument('--segment-seconds', type=float, default=1); p.add_argument('--sample-fps', type=float, default=15)
    p.add_argument('--out'); p.add_argument('--summarize', nargs='*')
    a = p.parse_args()
    if a.summarize:
        out = [summarize(t) for t in a.summarize]
        for r in out: print(json.dumps(r, ensure_ascii=False), flush=True)
    else:
        out = []
        for i in range(a.runs):
            params = {'name': f'启动对照-{a.label}-{i + 1}', 'source': 'demo', 'algorithm': 'resource_aware',
                      'segment_seconds': a.segment_seconds, 'sample_fps': a.sample_fps}
            out.append(summarize(run(params, 600)))
            print(json.dumps(out[-1], ensure_ascii=False), flush=True)
    done = [r['end_to_end_s'] for r in out if r['end_to_end_s']]
    report = {'label': a.label, 'runs': out, 'median_end_to_end_s': statistics.median(done) if done else None}
    if a.out:
        Path(a.out).parent.mkdir(parents=True, exist_ok=True); Path(a.out).write_text(json.dumps(report, ensure_ascii=False, indent=1))
    print(json.dumps({k: v for k, v in report.items() if k != 'runs'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
