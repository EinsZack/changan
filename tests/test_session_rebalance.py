"""Cold sessions must not hold undelivered segments while ready containers sit idle (2026-11-10)."""
import time
import httpx
import pytest
from app import db
from app.main import app
from app.kube import session_manifest, CUDA_CACHE_MOUNT
from app.session_execution import get_session
from test_session_execution import setup_session, sh


def cold_session(tid, ready=False, delivered=False):
    """session_one (lab-1) is READY and idle; session_two (lab-2) is still loading and holds s2."""
    db.execute("UPDATE execution_sessions SET active_attempt=NULL WHERE id='session_one'")
    db.execute("UPDATE attempts SET status='SUCCEEDED',finished=? WHERE id='a1'", (time.time(),))
    db.execute("UPDATE segments SET status='SUCCEEDED' WHERE id='s1'")
    now = time.time()
    w = {'execution_mode': 'task_session', 'job_name': 'video-session-two', 'job_uid': 'j2', 'pod_uid': 'p2',
         'pod_name': 'pod-b', 'node_name': 'agent-1', 'kube_node': 'agent-1', 'model_sha256': 'modelhash', 'memory_mb': 1024}
    db.execute("INSERT INTO execution_sessions(id,task_id,node_id,token,status,created,updated,heartbeat_at,ready_at,active_attempt,workload) VALUES('session_two',?,'lab-2','token-two-s',?,?,?,?,?,'a2',?)",
               (tid, 'READY' if ready else 'STARTING', now - 30, now, now, now if ready else None, db.dumps(w)))
    db.execute("INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision,backend,session_id,workload,cleanup_state,execution_id) VALUES('a2',?,'s2','lab-2','token-two',?,?,?,'{}','k3s','session_two',?,'PENDING',?)",
               (tid, 'ACCEPTED' if delivered else 'WAITING_SESSION', now - 30, now - 30, db.dumps(w | {'session_id': 'session_two', 'input_sha256': 'x'}), 'p2' if delivered else None))
    db.execute("UPDATE segments SET status=?,active_attempt='a2' WHERE id='s2'", ('ACCEPTED' if delivered else 'WAITING_SESSION',))


@pytest.mark.asyncio
async def test_idle_ready_session_takes_segment_parked_on_cold_session():
    e, sessions, tid = setup_session()
    cold_session(tid)
    assert sessions.rebalance(tid) == 1
    assert db.query("SELECT status,retries FROM segments WHERE id='s2'", one=True) == {'status': 'PENDING', 'retries': 0}
    assert db.query("SELECT status FROM attempts WHERE id='a2'", one=True)['status'] == 'CANCELED'
    cold = get_session('session_two')
    assert cold['status'] == 'STARTING' and cold['active_attempt'] is None  # stays alive for later work
    ev = db.query("SELECT payload FROM events WHERE type='segment_reassigned'", one=True)
    assert db.loads(ev['payload'])['retry_counted'] is False
    # The ready lab-1 session is now the only schedulable node for this task's segment.
    nodes = {n['id']: n for n in e.nodes(tid)}
    assert not nodes['lab-1'].get('session_blocked') and nodes['lab-2'].get('session_blocked')
    # Late readiness of the cold container cannot resurrect the revoked assignment.
    db.execute("UPDATE execution_sessions SET status='READY',ready_at=? WHERE id='session_two'", (time.time(),))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        r = await c.post('/internal/sessions/session_two/next', headers=sh('p2', 'token-two-s'))
        assert r.status_code == 200 and r.json()['action'] == 'wait'
    assert sessions.rebalance(tid) == 0
    await e.close()


@pytest.mark.asyncio
async def test_rebalance_never_moves_delivered_or_ready_work():
    e, sessions, tid = setup_session()
    cold_session(tid, delivered=True)
    assert sessions.rebalance(tid) == 0
    assert db.query("SELECT status FROM attempts WHERE id='a2'", one=True)['status'] == 'ACCEPTED'
    await e.close()


@pytest.mark.asyncio
async def test_rebalance_leaves_ready_session_assignment():
    e, sessions, tid = setup_session()
    cold_session(tid, ready=True)
    assert sessions.rebalance(tid) == 0
    await e.close()


@pytest.mark.asyncio
async def test_rebalance_waits_when_pending_work_already_covers_idle_sessions():
    e, sessions, tid = setup_session()
    cold_session(tid)
    db.execute("INSERT INTO segments(id,task_id,ordinal,start,end,path,status) VALUES('s3',?,2,8,12,'unused','PENDING')", (tid,))
    assert sessions.rebalance(tid) == 0
    db.execute("UPDATE execution_sessions SET active_attempt='a1' WHERE id='session_one'")
    db.execute("UPDATE segments SET status='SUCCEEDED' WHERE id='s3'")
    assert sessions.rebalance(tid) == 0  # no idle ready session
    await e.close()


def test_gpu_session_gets_persistent_writable_cuda_cache():
    s = {'id': 'session_x', 'task_id': 'task_x', 'node_id': 'lab-1'}
    w = {'job_name': 'video-session-x', 'image': 'i@sha256:a', 'architecture': 'amd64', 'kube_node': 'agent-0', 'gpu_access': True,
         'cuda_cache': {'host_path': '/var/lib/rancher/k3s/changan-cache/cuda', 'max_bytes': 1024**3}}
    spec = session_manifest(s, w, {'min_memory_mb': 256, 'attempt_timeout': 5}, 60)['spec']['template']['spec']
    env = {x['name']: x.get('value') for x in spec['containers'][0]['env']}
    assert env['CUDA_CACHE_PATH'] == CUDA_CACHE_MOUNT and env['CUDA_CACHE_MAXSIZE'] == str(1024**3)
    vol = next(v for v in spec['volumes'] if v['name'] == 'cuda-cache')
    assert vol['hostPath'] == {'path': '/var/lib/rancher/k3s/changan-cache/cuda', 'type': 'Directory'}
    assert {'name': 'cuda-cache', 'mountPath': CUDA_CACHE_MOUNT} in spec['containers'][0]['volumeMounts']
    assert spec['containers'][0]['securityContext']['readOnlyRootFilesystem'] is True
    # Without a prepared node directory the cache is still writable for the Pod lifetime.
    w['cuda_cache'] = {'host_path': None}
    spec = session_manifest(s, w, {'min_memory_mb': 256, 'attempt_timeout': 5}, 60)['spec']['template']['spec']
    assert next(v for v in spec['volumes'] if v['name'] == 'cuda-cache')['emptyDir']
    # CPU-only sessions are unchanged.
    w['gpu_access'] = False
    spec = session_manifest(s, w, {'min_memory_mb': 256, 'attempt_timeout': 5}, 60)['spec']['template']['spec']
    assert all(v['name'] != 'cuda-cache' for v in spec['volumes'])


def test_cuda_cache_settings_validation(monkeypatch):
    from app.container_execution import cuda_cache_settings
    monkeypatch.delenv('CHANGAN_CUDA_CACHE_HOSTPATH', raising=False)
    assert cuda_cache_settings() == {'host_path': None, 'max_bytes': 1024**3}
    monkeypatch.setenv('CHANGAN_CUDA_CACHE_HOSTPATH', 'relative/path')
    with pytest.raises(ValueError):
        cuda_cache_settings()
    monkeypatch.setenv('CHANGAN_CUDA_CACHE_HOSTPATH', '/var/lib/rancher/k3s/changan-cache/cuda')
    monkeypatch.setenv('CHANGAN_CUDA_CACHE_MAX_BYTES', str(8 * 1024**3))
    with pytest.raises(ValueError):
        cuda_cache_settings()


@pytest.mark.asyncio
async def test_control_plane_stall_defers_heartbeat_expiry_but_real_loss_still_detected():
    e, sessions, tid = setup_session()
    db.execute("UPDATE execution_sessions SET heartbeat_at=?", (time.time() - 40,))
    sessions.last_step = time.time() - 20  # coordinator could not run for 20 s
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'READY'
    sessions.heartbeat_grace_until = 0; sessions.last_step = time.time()
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'STOPPING'
    await e.close()
