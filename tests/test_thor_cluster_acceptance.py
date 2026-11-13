"""Ensure real-board acceptance cannot pass from labels or a partial/CPU result."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('validate_thor_cluster',
    Path(__file__).resolve().parents[1]/'scripts/validate_thor_cluster.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def complete_task(nodes=None):
    nodes = nodes or ['thor1', 'thor2']
    sessions = []
    attempts = []
    for i, node in enumerate(nodes):
        sid = 'session-'+node
        pod = 'pod-'+node
        sessions.append({'id': sid, 'node_id': node, 'status': 'CLOSED', 'completed': 2,
                         'model_load_count': 1, 'cleanup_state': 'DONE',
                         'workload': {'kube_node': node, 'pod_uid': pod,
                            'inference_runtime': {'actual_backend': 'cuda', 'fallback_reason': None,
                              'cuda_compute_events': 203, 'gpu': {'uuid': 'same-tegra-uuid'}}}})
        for part in range(2):
            attempts.append({'actual_node': node, 'session_id': sid, 'status': 'SUCCEEDED',
                             'execution_id': pod, 'cleanup_state': 'DONE',
                             'started': 10+part*2+i*.2, 'finished': 11+part*2+i*.2,
                             'workload': {'pod_uid': pod}})
    return {'id': 'task-proof', 'status': 'SUCCEEDED', 'created': 1, 'finished': 15,
            'completed': len(nodes)*2, 'total': len(nodes)*2, 'params': {'algorithm': 'round_robin'},
            'result': {'coverage': 1, 'sampled_frames': len(nodes)*20, 'counts': {}},
            'sessions': sessions, 'attempts': attempts}


def test_two_real_nodes_may_report_same_tegra_gpu_uuid():
    summary = module.check_success(complete_task(), ['thor1', 'thor2'], 4, 40)
    assert len(summary['per_node']) == 2 and summary['overlapping_attempt_pairs'] > 0


@pytest.mark.parametrize('fault', ['cpu', 'not-cleaned', 'partial', 'one-physical-node', 'no-overlap'])
def test_incomplete_or_misidentified_execution_cannot_pass(fault):
    task = deepcopy(complete_task())
    if fault == 'cpu':
        task['sessions'][1]['workload']['inference_runtime']['actual_backend'] = 'cpu'
    elif fault == 'not-cleaned':
        task['attempts'][1]['cleanup_state'] = 'PENDING'
    elif fault == 'partial':
        task['result']['sampled_frames'] = 30
    elif fault == 'one-physical-node':
        task['sessions'][1]['workload']['kube_node'] = 'thor1'
    else:
        for attempt in task['attempts']:
            if attempt['actual_node'] == 'thor2':
                attempt['started'] += 20
                attempt['finished'] += 20
    with pytest.raises(AssertionError):
        module.check_success(task, ['thor1', 'thor2'], 4, 40)


def test_evidence_removes_nested_credentials():
    assert module.redact({'event': {'session_token': 'private', 'node': 'thor2'},
                          'items': [{'api_key': 'private'}]}) == {
        'event': {'session_token': '[REDACTED]', 'node': 'thor2'},
        'items': [{'api_key': '[REDACTED]'}]}


def test_three_real_nodes_each_execute_with_distinct_containers():
    nodes = ['thor1', 'thor2', 'thor3']
    summary = module.check_success(complete_task(nodes), nodes, 6, 60)
    assert {row['node'] for row in summary['per_node']} == set(nodes)
    assert summary['overlapping_nodes'] == nodes
    assert len({row['pod_uid'] for row in summary['per_node']}) == 3


def test_third_node_must_really_execute():
    nodes = ['thor1', 'thor2', 'thor3']
    task = complete_task(nodes)
    for attempt in task['attempts']:
        if attempt['actual_node'] == 'thor3':
            attempt['actual_node'] = 'thor2'
    with pytest.raises(AssertionError):
        module.check_success(task, nodes, 6, 60)


def test_third_node_must_overlap_another_real_node():
    nodes = ['thor1', 'thor2', 'thor3']
    task = complete_task(nodes)
    for attempt in task['attempts']:
        if attempt['actual_node'] == 'thor3':
            attempt['started'] += 20
            attempt['finished'] += 20
    with pytest.raises(AssertionError, match='Some selected nodes never overlapped'):
        module.check_success(task, nodes, 6, 60)
