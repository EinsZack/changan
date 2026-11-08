import asyncio
import time
from types import SimpleNamespace as NS
import pytest
from app import db
from app.network_metrics import container_network_snapshot, overlay_pool_network
from app.container_execution import ContainerExecution
from app.scheduler import choose
from test_scheduler import node as scheduler_node
from test_state_machine import seeded

NODE = {'id': 'lab-1', 'uid': 'node-1', 'kind': 'k3s_logical'}
NOW = 1800000000


@pytest.fixture
def samples():
    engine, tid = seeded()
    db.execute("DELETE FROM attempts WHERE id='a1'")
    def add(aid='a1', size=1000000, seconds=1, started=NOW-10, finished=NOW-1,
            status='SUCCEEDED', uid='node-1', cluster='test', legacy=False, **extra):
        result = {'input_bytes': size, 'download_seconds' if legacy else 'input_transfer_seconds': seconds, **extra}
        work = {'node_uid': uid, 'cluster': cluster}
        db.execute('INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,started,finished,result,decision,backend,workload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (aid, tid, 's1', 'lab-1', 'test-token', status, started or NOW, NOW, started, finished,
                    db.dumps(result), '{}', 'k3s', db.dumps(work)))
    yield engine, add
    asyncio.run(engine.close())


def test_recent_bytes_and_times_weighted_across_container_protocols(samples):
    _, add = samples
    add('a1', size=1000000, seconds=1)
    add('a2', size=3000000, seconds=6, started=NOW-20, legacy=True, observed_transfer_mbps=999999)
    s = container_network_snapshot(NODE, 'test', NOW)
    assert s['bandwidth_mbps'] == pytest.approx(32/7)
    assert s['network_sample_count'] == 2
    assert s['network_observed_at'] == NOW-10
    assert s['network_expires_at'] == NOW+280
    assert '同机 K3s' in s['network_source']
    assert s['network_sample_time_source'] == 'server_attempt_started_at'


@pytest.mark.parametrize('changes', [
    {'started': NOW-301}, {'started': None}, {'started': NOW+1}, {'finished': NOW+1},
    {'uid': 'replaced-node'}, {'uid': None}, {'cluster': 'other'},
    {'status': 'FAILED'}, {'status': 'RUNNING'},
    {'size': 0}, {'size': -1}, {'size': True}, {'size': '1000'},
    {'seconds': 0}, {'seconds': float('nan')}, {'seconds': float('inf')},
])
def test_missing_invalid_stale_foreign_and_failed_samples_are_unknown(samples, changes):
    _, add = samples
    add(**changes)
    s = container_network_snapshot(NODE, 'test', NOW)
    assert s['bandwidth_mbps'] is None
    assert s['network_sample_count'] == 0
    assert s['network_observed_at'] is None


def test_only_eight_recent_valid_samples_and_consistent_timing_floor(samples):
    _, add = samples
    for i in range(10):
        add('a'+str(i), size=1000, seconds=.0001, started=NOW-30+i, finished=NOW-20+i)
    s = container_network_snapshot(NODE, 'test', NOW)
    assert s['network_sample_count'] == 8
    assert s['bandwidth_mbps'] == pytest.approx(8)
    assert s['network_sample_attempt_ids'] == ['a'+str(i) for i in range(9, 1, -1)]


def test_legacy_identity_falls_back_to_saved_decision_and_history_is_not_rewritten(samples):
    _, add = samples
    add(legacy=True, uid=None)
    decision = {'candidates': [{'node_id': 'lab-1', 'snapshot': {'node_uid': 'node-1'}}]}
    db.execute("UPDATE attempts SET decision=? WHERE id='a1'", (db.dumps(decision),))
    assert container_network_snapshot(NODE, 'test', NOW)['bandwidth_mbps'] == 8
    assert db.loads(db.query("SELECT decision FROM attempts WHERE id='a1'", one=True)['decision']) == decision


def test_pool_bandwidth_replaces_local_metadata_but_absent_value_keeps_observations(samples):
    _, add = samples
    add()
    s = container_network_snapshot(NODE, 'test', NOW)
    overlay_pool_network(s, {'observed_at': NOW, 'network_source': 'not a bandwidth sample'})
    assert s['bandwidth_mbps'] == 8 and 'HTTP' in s['network_source']
    overlay_pool_network(s, {'bandwidth_mbps': 20, 'observed_at': NOW, 'network_source': 'pool'})
    assert s['bandwidth_mbps'] == 20 and s['network_source'] == 'pool'
    assert s['network_observed_at'] == NOW and s['network_expires_at'] == NOW+8
    assert 'network_sample_attempt_ids' not in s and 'network_sample_count' not in s
    overlay_pool_network(s, {'bandwidth_mbps': None, 'observed_at': NOW})
    assert s['bandwidth_mbps'] is None


@pytest.mark.asyncio
async def test_k3s_refresh_connects_real_result_fields_to_scheduler(samples):
    engine, add = samples
    now = time.time()
    add(started=now-10, finished=now-1, legacy=True)
    c = ContainerExecution(engine)
    class Nodes:
        cluster = 'test'
        async def nodes(self):
            return [NODE | {'name': 'agent-0', 'healthy': True, 'architecture': 'amd64',
                            'memory': 2048, 'active': 0, 'capacity': 1, 'runtime': 'containerd',
                            'kubelet': 'test', 'physical_host': 'one-host'}]
    c.kube = Nodes()
    c.application = lambda: {'architecture': 'amd64'}
    engine.resource_provider = NS(enabled=False)
    await c.refresh()
    n = next(n for n in engine.nodes() if n['id'] == 'lab-1')
    assert n['snapshot']['bandwidth_mbps'] == 8
    assert 'observed_http_rtt_ms' not in n['snapshot']
    decision = choose([n], {}, 4, 1000000, {'method': 'resource_aware'})
    assert decision['candidates'][0]['transfer_seconds'] == 1


def test_network_cost_affects_resource_aware_ranking_without_mutating_input():
    a, b = scheduler_node('a'), scheduler_node('b')
    a['snapshot'].update(bandwidth_mbps=1)
    b['snapshot'].update(bandwidth_mbps=10)
    d = choose([a, b], {}, 4, 1000000, {'method': 'resource_aware'})
    assert d['target_node'] == 'b'
    assert [r['transfer_seconds'] for r in d['candidates']] == [8, .8]
    a['snapshot']['network_expires_at'] = time.time()-1
    d = choose([a], {}, 4, 1000000, {'method': 'resource_aware'})
    assert d['candidates'][0]['transfer_seconds'] is None
    assert d['candidates'][0]['snapshot']['bandwidth_mbps'] is None
    assert a['snapshot']['bandwidth_mbps'] == 1
    assert '过期' in d['candidates'][0]['network_source']


@pytest.mark.parametrize('value', [0, -1, float('nan'), float('inf'), '10', True, None])
def test_invalid_bandwidth_does_not_make_fake_cost_or_break_scheduling(value):
    n = scheduler_node()
    n['snapshot']['bandwidth_mbps'] = value
    d = choose([n], {}, 4, 1000, {'method': 'resource_aware'})
    assert d['target_node'] == n['id']
    assert d['candidates'][0]['transfer_seconds'] is None
