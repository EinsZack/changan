#!/usr/bin/env python
"""Accept real native-Mac/Thor video execution and record per-board evidence."""
import argparse
from datetime import datetime, timezone
import gzip
import json
from pathlib import Path
import time

import httpx

TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT'}


def settings(root):
    result = {}
    for line in (root/'config/platform.env').read_text().splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            key, value = line.split('=', 1)
            result[key] = value.strip().strip('"').strip("'")
    return result


def redact(value):
    if isinstance(value, dict):
        return {key: ('[REDACTED]' if any(word in key.lower() for word in
                    ('token', 'password', 'secret', 'api_key', 'authorization'))
                     else redact(item)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def settled(task):
    return task['status'] in TERMINAL and all(
        item['cleanup_state'] == 'DONE'
        for item in task['sessions'] + task['attempts'])


def check_success(task, nodes, expected_segments, expected_frames):
    """Reject a complete-looking task unless every selected physical node really executed."""
    assert settled(task) and task['status'] == 'SUCCEEDED', task.get('error')
    assert task['completed'] == task['total'] == expected_segments
    assert task['result']['coverage'] == 1
    assert task['result']['sampled_frames'] == expected_frames
    assert {a['actual_node'] for a in task['attempts']} == set(nodes)
    assert len(task['attempts']) == expected_segments, 'Unexpected retries or missing attempts'
    assert all(a['status'] == 'SUCCEEDED' and a['workload'].get('pod_uid')
               for a in task['attempts'])
    sessions = task['sessions']
    assert len(sessions) == len(nodes)
    assert {s['node_id'] for s in sessions} == set(nodes)
    assert len({s['workload']['kube_node'] for s in sessions}) == len(nodes)
    assert len({s['workload']['pod_uid'] for s in sessions}) == len(nodes)
    assert sum(s['completed'] for s in sessions) == expected_segments
    per_node = []
    for session in sessions:
        assert session['status'] == 'CLOSED' and session['model_load_count'] == 1
        assert session['completed'] >= (2 if len(nodes) > 1 else expected_segments)
        runtime = session['workload']['inference_runtime']
        assert runtime['actual_backend'] == 'cuda' and not runtime.get('fallback_reason')
        assert runtime['cuda_compute_events'] > 0
        attempts = [a for a in task['attempts'] if a['session_id'] == session['id']]
        assert len(attempts) == session['completed']
        assert {a['actual_node'] for a in attempts} == {session['node_id']}
        assert {a['workload']['pod_uid'] for a in attempts} == {session['workload']['pod_uid']}
        assert len({a['execution_id'] for a in attempts}) == 1 and attempts[0]['execution_id']
        per_node.append({'node': session['node_id'], 'kube_node': session['workload']['kube_node'],
                         'pod_uid': session['workload']['pod_uid'], 'segments': session['completed'],
                         'model_load_count': 1, 'actual_backend': 'cuda',
                         'cuda_compute_events': runtime['cuda_compute_events'],
                         'gpu_uuid': runtime['gpu']['uuid'], 'cleanup': 'DONE'})
    # This is a control-plane observation of overlapping attempt lifetimes, not kernel profiling.
    overlaps = []
    overlapping_nodes = set()
    for index, left in enumerate(task['attempts']):
        for right in task['attempts'][index+1:]:
            if left['actual_node'] == right['actual_node']:
                continue
            if all(a.get('started') is not None and a.get('finished') is not None for a in (left, right)):
                seconds = min(left['finished'], right['finished']) - max(left['started'], right['started'])
                if seconds > 0:
                    overlaps.append(seconds)
                    overlapping_nodes.update((left['actual_node'], right['actual_node']))
    if len(nodes) > 1:
        assert overlaps, 'No recorded overlap across actual nodes'
        assert overlapping_nodes == set(nodes), 'Some selected nodes never overlapped another node'
    return {'task_id': task['id'], 'status': task['status'], 'segments': expected_segments,
            'sampled_frames': expected_frames, 'algorithm': task['params']['algorithm'],
            'elapsed_seconds': task['finished'] - task['created'],
            'counts': task['result']['counts'], 'per_node': per_node,
            'overlapping_attempt_pairs': len(overlaps),
            'overlapping_nodes': sorted(overlapping_nodes),
            'largest_attempt_overlap_seconds': max(overlaps, default=0), 'cleanup': 'DONE'}


def cluster_snapshot(root):
    from kubernetes import client, config
    cfg = client.Configuration()
    config.load_kube_config(str(root/'config/admin-kubeconfig.yaml'), client_configuration=cfg)
    cfg.retries = 0
    with client.ApiClient(cfg) as api:
        core = client.CoreV1Api(api)
        batch = client.BatchV1Api(api)
        nodes = [{'name': n.metadata.name, 'uid': n.metadata.uid,
                  'physical_host': n.metadata.labels.get('changan.io/physical-host'),
                  'board_serial_sha256': (n.metadata.annotations or {}).get('changan.io/board-serial-sha256'),
                  'ready': any(c.type == 'Ready' and c.status == 'True' for c in n.status.conditions),
                  'gpu_slots': int(n.status.allocatable.get('nvidia.com/gpu', '0'))}
                 for n in core.list_node(_request_timeout=(3, 10)).items]
        return {'nodes': nodes,
                'pods': [p.metadata.name for p in core.list_namespaced_pod(
                    'changan', _request_timeout=(3, 10)).items],
                'jobs': [j.metadata.name for j in batch.list_namespaced_job(
                    'changan', _request_timeout=(3, 10)).items]}


def run(root, nodes, output, cancel=False):
    root = Path(root).resolve()
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {'checked_at': datetime.now(timezone.utc).isoformat(),
              'environment': 'native Mac control plane; real ARM64 Jetson AGX Thor boards',
              'nodes': nodes, 'cases': []}
    credentials = settings(root)
    with httpx.Client(base_url='http://127.0.0.1:8310', trust_env=False, timeout=45,
                      headers={'X-API-Key': credentials.get('CHANGAN_API_KEY', '')}) as client:
        get = lambda tid: client.get('/api/tasks/'+tid).raise_for_status().json()
        current = client.get('/api/tasks').raise_for_status().json()
        assert all(t['status'] in TERMINAL for t in current), 'There is already active work'
        initial = cluster_snapshot(root)
        selected = [n for n in initial['nodes'] if n['name'] in nodes]
        assert len(selected) == len(nodes) and all(n['ready'] and n['gpu_slots'] == 1 for n in selected)
        for key in ('uid', 'physical_host', 'board_serial_sha256'):
            assert all(n[key] for n in selected) and len({n[key] for n in selected}) == len(nodes), key
        assert not initial['pods'] and not initial['jobs'], 'Validation namespace must be idle'
        report['cluster_before'] = initial
        with (root/'current/assets/samples/demo.mp4').open('rb') as video:
            upload = client.post('/api/uploads', files={'file': ('thor-cluster-demo.mp4', video, 'video/mp4')}).raise_for_status().json()
        assert abs(upload['metadata']['duration'] - 24) < .1
        source = upload['source']

        def save():
            (output/'cluster-acceptance.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')

        def record(name, task, summary):
            exported = client.get('/api/tasks/'+task['id']+'/export').raise_for_status().json()
            safe = json.dumps(redact(exported), ensure_ascii=False).encode()
            assert all(not value or value.encode() not in safe for key, value in credentials.items()
                       if any(word in key for word in ('TOKEN', 'API_KEY', 'PASSWORD', 'SECRET')))
            with gzip.open(output/(task['id']+'.json.gz'), 'wb') as stream:
                stream.write(safe)
            summary.update({'case': name, 'previews_readable': 0})
            if task.get('result'):
                for url in task['result'].get('previews', []):
                    image = client.get(url).raise_for_status()
                    assert image.content and image.headers.get('content-type', '').startswith('image/')
                    summary['previews_readable'] += 1
            report['cases'].append(summary)
            save()
            print(json.dumps(summary, ensure_ascii=False), flush=True)

        def create(name, targets, algorithm, seconds):
            payload = {'name': name, 'source': source, 'algorithm': algorithm, 'node_ids': targets,
                       'architecture': 'arm64', 'segment_seconds': seconds, 'sample_fps': 10,
                       'max_retries': 0, 'attempt_timeout': 300, 'deadline_seconds': 600}
            tid = client.post('/api/tasks', json=payload).raise_for_status().json()['id']
            print(json.dumps({'created_task': tid, 'nodes': targets, 'algorithm': algorithm}), flush=True)
            return tid

        def wait(tid, condition, timeout=660):
            until = time.monotonic()+timeout
            last = None
            while time.monotonic() < until:
                task = get(tid)
                marker = (task['status'], task['completed'],
                          tuple((s['node_id'], s['status'], s['cleanup_state']) for s in task['sessions']))
                if marker != last:
                    print(json.dumps({'task': tid, 'status': task['status'], 'completed': task['completed'],
                                      'total': task['total'], 'sessions': marker[2]}, ensure_ascii=False), flush=True)
                    last = marker
                if condition(task):
                    return task
                time.sleep(.2)
            raise RuntimeError('Timed out waiting for '+tid)

        single = wait(create(nodes[-1]+' · 独立CUDA视频全流程', [nodes[-1]], 'round_robin', 4), settled)
        record('single-'+nodes[-1], single, check_success(single, [nodes[-1]], 6, 240))
        if len(nodes) > 1:
            for algorithm in ('round_robin', 'resource_aware'):
                task = wait(create(f'{len(nodes)}节点Thor · '+algorithm+' · 真实视频分片协同', nodes, algorithm, 1), settled)
                record(algorithm, task, check_success(task, nodes, 24, 240))
            if cancel:
                tid = create(f'{len(nodes)}节点Thor · 运行中取消与容器回收', nodes, 'round_robin', 1)
                try:
                    wait(tid, lambda t: {a['actual_node'] for a in t['attempts']
                         if a['status'] == 'RUNNING'} == set(nodes), timeout=120)
                    client.post('/api/tasks/'+tid+'/cancel').raise_for_status()
                except BaseException:
                    client.post('/api/tasks/'+tid+'/cancel')
                    raise
                task = wait(tid, settled)
                assert task['status'] == 'CANCELED' and task['result'] is None
                assert {s['node_id'] for s in task['sessions']} == set(nodes)
                record('cancel-all' if len(nodes) > 2 else 'cancel-both', task, {'task_id': tid, 'status': 'CANCELED', 'cleanup': 'DONE',
                       'per_node': [{'node': s['node_id'], 'model_load_count': s['model_load_count'],
                                    'cleanup': s['cleanup_state']} for s in task['sessions']]})
        until = time.monotonic()+30
        while True:
            final = cluster_snapshot(root)
            if not final['pods'] and not final['jobs']:
                break
            assert time.monotonic() < until, final
            time.sleep(1)
        report['cluster_after'] = final
        report['passed'] = True
        save()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True)
    parser.add_argument('--nodes', nargs='+', default=['thor1', 'thor2'])
    parser.add_argument('--output', required=True)
    parser.add_argument('--cancel', action='store_true')
    args = parser.parse_args()
    assert len(set(args.nodes)) == len(args.nodes)
    run(args.root, args.nodes, args.output, args.cancel)


if __name__ == '__main__':
    main()
