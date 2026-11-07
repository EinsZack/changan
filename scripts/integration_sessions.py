#!/usr/bin/env python
"""Real K3s session lifecycle acceptance; faults are confined to the validation instance."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path
import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app import db
from app.config import API_KEY, DATA

BASE = os.getenv('CHANGAN_TEST_BASE', 'http://127.0.0.1:8410')
client = httpx.Client(base_url=BASE, timeout=30, trust_env=False, headers={'X-API-Key': API_KEY})
terminal = ('SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT')
report = {'at': time.time(), 'environment': 'one server / four k3d K3s logical nodes / CPU ONNX; no THOR hardware', 'cases': []}
output = ROOT / 'docs/session-integration-report.json'


def get(tid):
    return client.get('/api/tasks/' + tid).raise_for_status().json()


def wait(tid, condition, timeout=300):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        t = get(tid)
        if condition(t):
            return t
        time.sleep(.5)
    raise AssertionError('Timeout ' + tid + ': ' + json.dumps(get(tid), ensure_ascii=False)[:1200])


def settled(t):
    return t['status'] in terminal and all(s['cleanup_state'] == 'DONE' for s in t.get('sessions', [])) and all(a['cleanup_state'] == 'DONE' for a in t['attempts'])


def create(name, **params):
    return client.post('/api/tasks', json={'name': 'B方案验收 · ' + name, 'segment_seconds': 1,
        'sample_fps': 15, 'algorithm': 'round_robin', 'attempt_timeout': 180, 'deadline_seconds': 600, **params}).raise_for_status().json()['id']


def record(name, t, **extra):
    row = {'name': name, 'task_id': t['id'], 'status': t['status'], 'total': t['total'], 'attempts': len(t['attempts']),
           'sessions': len(t['sessions']), 'nodes': sorted({s['node_id'] for s in t['sessions']}),
           'model_loads': sum(s['model_load_count'] for s in t['sessions']),
           'session_completed': [s['completed'] for s in t['sessions']], 'all_cleaned': settled(t),
           'elapsed_seconds': t['finished'] - t['created'], **extra}
    report['cases'].append(row)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    folder = DATA / 'evidence/sessions'; folder.mkdir(parents=True, exist_ok=True)
    (folder / (name + '.json')).write_text(client.get('/api/tasks/' + t['id'] + '/export').raise_for_status().text)
    print(json.dumps(row, ensure_ascii=False), flush=True)


def check(t, expected_sessions=None):
    assert t['status'] == 'SUCCEEDED', t.get('error')
    assert t['result']['coverage'] == 1
    assert t['result']['sampled_frames'] > 0
    assert all(a['workload'].get('pod_uid') for a in t['attempts'] if a['status'] == 'SUCCEEDED')
    assert sum(s['completed'] for s in t['sessions']) == t['total']
    if expected_sessions is not None:
        assert len(t['sessions']) == expected_sessions
        assert len(t['attempts']) == t['total']
        assert all(s['model_load_count'] == 1 for s in t['sessions'])
    for s in t['sessions']:
        aids = [a for a in t['attempts'] if a['session_id'] == s['id'] and a['execution_id']]
        assert len({a['execution_id'] for a in aids}) <= 1
    for url in t['result']['previews']:
        assert client.get(url).status_code == 200


def kubectl(*args):
    return subprocess.run([sys.executable, 'scripts/cluster.py', 'kubectl', *args], check=True, cwd=ROOT, capture_output=True, text=True)


def restart(*services):
    subprocess.run([sys.executable, 'scripts/manage.py', 'restart', *services], check=True, cwd=ROOT)
    for _ in range(40):
        try:
            if client.get('/health').is_success:
                return
        except httpx.TransportError:
            pass
        time.sleep(.5)
    raise AssertionError('API did not return')


def main():
    assert DATA.name == 'k3s-validation' and BASE.endswith(':8410'), 'use isolated validation instance'
    db.init()
    for _ in range(40):
        try:
            if client.get('/health').is_success:break
        except httpx.TransportError:pass
        time.sleep(.5)
    normal = wait(create('四节点连续分片'), settled)
    check(normal, 4)
    assert all(s['completed'] > 1 for s in normal['sessions'])
    record('four-node-reuse', normal)
    single = wait(create('单节点连续分片', node_ids=['lab-3'], sample_fps=2), settled)
    check(single, 1)
    assert single['sessions'][0]['completed'] == 24
    record('single-node-reuse', single)
    tid = create('取消时回收复用容器', segment_seconds=4)
    wait(tid, lambda t: any(a['status'] == 'RUNNING' for a in t['attempts']))
    client.post('/api/tasks/' + tid + '/cancel').raise_for_status()
    t = wait(tid, settled)
    assert t['status'] == 'CANCELED' and t['result'] is None
    record('cancel', t)
    if '--faults' not in sys.argv:
        return
    # These node faults must never overlap a user's live production workload.
    prod = httpx.get('http://127.0.0.1:8310/api/tasks', trust_env=False, headers={'X-API-Key': API_KEY}).raise_for_status().json()
    assert all(t['status'] in terminal for t in prod), 'production has active work'
    tid = create('控制面进程重启复用', max_retries=0)
    running = wait(tid, lambda t: len(t['sessions']) == 4 and all(s['model_load_count'] == 1 for s in t['sessions']))
    uids = {s['id']: s['workload']['pod_uid'] for s in running['sessions']}
    restart('api', 'container-gateway')
    t = wait(tid, settled); check(t, 4)
    assert {s['id']: s['workload']['pod_uid'] for s in t['sessions']} == uids
    record('api-gateway-restart', t, same_pods=True)
    tid = create('Pod中断后更换会话', segment_seconds=4, node_ids=['lab-1'])
    running = wait(tid, lambda t: any(a['status'] == 'RUNNING' for a in t['attempts']))
    a = next(a for a in running['attempts'] if a['status'] == 'RUNNING')
    kubectl('delete', 'pod', a['workload']['pod_name'], '-n', os.getenv('CHANGAN_NAMESPACE', 'changan'), '--wait=false')
    t = wait(tid, settled); check(t)
    assert any(a['status'] in ('FAILED', 'TIMED_OUT') for a in t['attempts'])
    assert len(t['sessions']) == 2
    assert len({s['workload'].get('pod_uid') for s in t['sessions']}) == 2
    record('pod-interruption', t)
    # Remove/restore the node registration label to exercise automatic membership discovery.
    nodes = client.get('/api/nodes').raise_for_status().json()
    node4 = next(n['snapshot']['kube_node'] for n in nodes if n['id'] == 'lab-4')
    try:
        kubectl('label', 'node', node4, 'changan.io/node-id-')
        time.sleep(3)
        tid = create('任务期间节点加入', node_ids=['lab-1', 'lab-4'], sample_fps=15)
        wait(tid, lambda t: any(a['status'] == 'RUNNING' for a in t['attempts']))
        assert {s['node_id'] for s in get(tid)['sessions']} == {'lab-1'}
        kubectl('label', 'node', node4, 'changan.io/node-id=lab-4', '--overwrite')
        t = wait(tid, settled); check(t, 2)
        assert {s['node_id'] for s in t['sessions']} == {'lab-1', 'lab-4'}
        record('node-join', t)
    finally:
        kubectl('label', 'node', node4, 'changan.io/node-id=lab-4', '--overwrite')
    # Pause exactly one project-owned agent, causing real heartbeat loss, then restore it.
    tid = create('节点临时退出及恢复', node_ids=['lab-1', 'lab-2'])
    running = wait(tid, lambda t: any(a['status'] == 'RUNNING' and a['node_id'] == 'lab-1' for a in t['attempts']))
    lost = next(s for s in running['sessions'] if s['node_id'] == 'lab-1')
    node_name = lost['workload']['kube_node']
    assert node_name.startswith('k3d-changan-dev-agent-')
    env = os.environ.copy(); env['DOCKER_HOST'] = 'unix://' + str(ROOT / 'data/k3s/docker.sock')
    docker = str(ROOT / 'vendor/docker/docker')
    try:
        subprocess.run([docker, 'pause', node_name], check=True, env=env)
        observed = wait(tid, lambda t: any(s['id'] == lost['id'] and s['status'] == 'STOPPING' for s in t['sessions']), timeout=90)
        assert next(s for s in observed['sessions'] if s['id'] == lost['id'])['cleanup_state'] == 'PENDING'
        assert any(a['session_id'] == lost['id'] and a['status'] in ('FAILED', 'TIMED_OUT') for a in observed['attempts'])
    finally:
        subprocess.run([docker, 'unpause', node_name], check=True, env=env)
    t = wait(tid, settled); check(t)
    record('node-loss-recovery', t, lost_session=lost['id'], held_slot_until_cleanup=True)
    tid = create('缺失镜像准备超时', node_ids=['not-yet-eligible'], max_retries=0, attempt_timeout=10)
    with db.connect() as c:
        row = c.execute('SELECT params,metadata FROM tasks WHERE id=?', (tid,)).fetchone()
        p = db.loads(row['params']); p['node_ids'] = ['lab-1']
        m = db.loads(row['metadata']); m['application']['image'] = 'changan-registry:5000/changan/missing@sha256:' + '0' * 64
        c.execute('UPDATE tasks SET params=?,metadata=? WHERE id=?', (db.dumps(p), db.dumps(m), tid))
    t = wait(tid, settled)
    assert t['status'] == 'FAILED' and t['result'] is None
    record('missing-image', t)
    print('Session lifecycle acceptance passed', flush=True)


if __name__ == '__main__':
    main()
