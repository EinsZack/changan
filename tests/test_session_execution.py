import hashlib
import time
from types import SimpleNamespace as NS
import httpx
import pytest
from app import db
from app.main import app
from app.config import MODEL_ID
from app.container_execution import ContainerExecution
from app.session_execution import get_session, patch_workload
from app.kube import session_manifest
from app.scheduler import choose
from test_state_machine import seeded
from test_container_execution import FakeKube, pod


def setup_session():
    e, tid = seeded()
    ce = ContainerExecution(e); e.containers = ce
    w = {'execution_mode': 'task_session', 'job_name': 'video-session-one', 'job_uid': 'j1',
         'pod_uid': 'p1', 'pod_name': 'pod-a1', 'node_name': 'agent-0', 'kube_node': 'agent-0', 'node_uid': 'n1',
         'image': 'image@sha256:abc', 'architecture': 'amd64', 'model_id': MODEL_ID,
         'model_sha256': 'modelhash', 'memory_mb': 1024}
    now = time.time()
    db.execute("INSERT INTO execution_sessions(id,task_id,node_id,token,status,created,updated,heartbeat_at,ready_at,active_attempt,workload,model_load_count) VALUES('session_one',?,'lab-1','session-token','READY',?,?,?,?,'a1',?,1)", (tid, now, now, now, now, db.dumps(w)))
    db.execute("UPDATE attempts SET backend='k3s',session_id='session_one',workload=?,cleanup_state='PENDING',execution_id='p1',actual_node='lab-1',started=? WHERE id='a1'", (db.dumps(w | {'session_id': 'session_one', 'input_sha256': 'inputhash'}), now))
    snapshot = {'node_id': 'lab-1', 'node_uid': 'n1', 'kube_node': 'agent-0', 'healthy': True,
                'models': [MODEL_ID], 'available_memory_mb': 2048, 'memory_budget_mb': 3072,
                'active': 1, 'capacity': 1, 'resource_source': 'kubernetes'}
    db.execute("UPDATE nodes SET observed=?,snapshot=?,error=NULL WHERE id='lab-1'", (now, db.dumps(snapshot)))
    meta = db.loads(db.query('SELECT metadata FROM tasks WHERE id=?', (tid,), one=True)['metadata'])
    meta['application'] = w
    db.execute('UPDATE tasks SET metadata=? WHERE id=?', (db.dumps(meta), tid))
    job = NS(metadata=NS(uid='j1', labels={'changan.io/session': 'session_one'}), status=NS(conditions=[]))
    ce.kube = FakeKube(job, [pod()])
    ce.kube.cluster = 'test'; ce.kube.namespace = 'changan'
    return e, ce.sessions, tid


def sh(uid='p1', token='session-token'):
    return {'x-session-token': token, 'x-pod-uid': uid}


def ah(aid='a1', uid='p1'):
    return {'x-attempt-token': 'test-token' if aid == 'a1' else 'token-two', 'x-pod-uid': uid}


def result(tid, aid='a1', seg='s1'):
    return {'attempt_id': aid, 'task_id': tid, 'segment_id': seg, 'actual_node': 'lab-1', 'session_id': 'session_one',
            'model_id': MODEL_ID, 'model_sha256': 'modelhash', 'input_sha256': 'inputhash', 'pod_uid': 'p1',
            'sampled_frames': 1, 'frames': [{'seconds': 0}], 'counts': {'person': 1},
            'previews': ['preview-0.jpg'], 'artifacts': [{'name': 'preview-0.jpg', 'bytes': 3, 'sha256': hashlib.sha256(b'jpg').hexdigest()}]}


async def finish(client, tid, aid='a1', seg='s1'):
    response = await client.put('/internal/container/' + aid + '/artifacts/preview-0.jpg', content=b'jpg', headers=ah(aid) | {'x-content-sha256': hashlib.sha256(b'jpg').hexdigest()})
    assert response.status_code == 200, response.text
    b = {'seq': 1, 'status': 'FINALIZING', 'result': result(tid, aid, seg)}
    response = await client.post('/internal/container/' + aid + '/events', headers=ah(aid), json=b)
    assert response.status_code == 200, response.text
    return b


@pytest.mark.asyncio
async def test_two_segments_share_live_pod_and_one_model_load():
    e, sessions, tid = setup_session()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        b = await finish(c, tid)
        assert get_session('session_one')['active_attempt'] is None
        assert get_session('session_one')['completed'] == 1
        await sessions.reconcile()
        assert not sessions.owner.kube.deleted
        assert db.task_detail(tid)['segments'][0]['status'] == 'SUCCEEDED'
        assert (await c.post('/internal/sessions/session_one/next', headers=sh())).json()['action'] == 'wait'
        now = time.time()
        db.execute("INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision) VALUES('a2',?,'s2','lab-1','token-two','DISPATCHING',?,?,'{}')", (tid, now, now))
        db.execute("UPDATE segments SET active_attempt='a2',status='DISPATCHING' WHERE id='s2'")
        task = db.query('SELECT * FROM tasks WHERE id=?', (tid,), one=True)
        await sessions.submit(task, {}, 'a2', {'input_sha256': 'inputhash'})
        assigned = (await c.post('/internal/sessions/session_one/next', headers=sh())).json()
        assert assigned['payload']['attempt_id'] == 'a2'
        assert (await c.post('/internal/container/a1/events', headers=ah(), json=b)).json()['duplicate']
        await finish(c, tid, 'a2', 's2')
        session = get_session('session_one')
        assert session['completed'] == 2 and session['model_load_count'] == 1
        assert len(db.task_detail(tid)['sessions']) == 1
        assert {a['execution_id'] for a in db.task_detail(tid)['attempts']} == {'p1'}
        assert not sessions.owner.kube.deleted
    await e.close()


@pytest.mark.asyncio
async def test_failed_segment_retries_without_destroying_healthy_session():
    e, sessions, tid = setup_session()
    b = {'seq': 1, 'status': 'FAILED', 'error': 'invalid video'}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        assert (await c.post('/internal/container/a1/events', headers=ah(), json=b)).status_code == 200
        assert (await c.post('/internal/container/a1/events', headers=ah(), json=b)).json()['duplicate']
    assert get_session('session_one')['status'] == 'READY'
    assert get_session('session_one')['active_attempt'] is None
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'", one=True) == {'status': 'PENDING', 'retries': 1}
    await sessions.reconcile()
    assert not sessions.owner.kube.deleted
    await e.close()


@pytest.mark.asyncio
async def test_task_cancel_fences_session_and_holds_physical_slot_until_cleanup():
    e, sessions, tid = setup_session()
    e.fail_task(tid, 'cancel', 'CANCELED')
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'STOPPING'
    assert next(n for n in e.nodes() if n['id'] == 'lab-1')['reserved'] == 1
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        assert (await c.post('/internal/sessions/session_one/next', headers=sh())).status_code == 409
        assert (await c.post('/internal/container/a1/events', headers=ah(), json={'seq': 1, 'status': 'RUNNING'})).status_code == 409
    sessions.owner.kube.job_value = None; sessions.owner.kube.pod_values = []
    await sessions.reconcile()
    assert get_session('session_one')['cleanup_state'] == 'DONE'
    assert db.query("SELECT cleanup_state FROM attempts WHERE id='a1'", one=True)['cleanup_state'] == 'DONE'
    assert next(n for n in e.nodes() if n['id'] == 'lab-1')['reserved'] == 0
    await e.close()


@pytest.mark.asyncio
async def test_lost_session_requeues_work_but_does_not_release_unreachable_node():
    e, sessions, tid = setup_session()
    db.execute("UPDATE execution_sessions SET heartbeat_at=?", (time.time() - 60,))
    db.execute("UPDATE nodes SET observed=?,error='offline' WHERE id='lab-1'", (time.time() - 60,))
    await sessions.reconcile()
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'", one=True) == {'status': 'PENDING', 'retries': 1}
    sessions.owner.kube.job_value = None; sessions.owner.kube.pod_values = []
    await sessions.reconcile()
    assert get_session('session_one')['cleanup_state'] == 'PENDING'
    assert next(n for n in e.nodes() if n['id'] == 'lab-1')['reserved'] == 1
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        assert (await c.post('/internal/sessions/session_one/heartbeat', headers=sh())).status_code == 409
        assert (await c.post('/internal/container/a1/events', headers=ah(), json={'seq': 1, 'status': 'FINALIZING', 'result': result(tid)})).status_code == 409
    await e.close()


@pytest.mark.asyncio
async def test_result_auth_and_incomplete_result_do_not_free_assignment():
    e, sessions, tid = setup_session()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        assert (await c.post('/internal/sessions/session_one/next', headers=sh(token='other'))).status_code == 401
        assert (await c.post('/internal/sessions/session_one/next', headers=sh(uid='other'))).status_code == 409
        b = {'seq': 1, 'status': 'FINALIZING', 'result': result(tid)}
        assert (await c.post('/internal/container/a1/events', headers=ah(), json=b)).status_code == 422
        assert get_session('session_one')['active_attempt'] == 'a1'
        assert db.query("SELECT status FROM attempts WHERE id='a1'", one=True)['status'] == 'RUNNING'
    await e.close()


@pytest.mark.asyncio
async def test_idle_session_is_available_only_to_its_own_task():
    e, sessions, tid = setup_session()
    db.execute("UPDATE execution_sessions SET active_attempt=NULL")
    db.execute("UPDATE attempts SET status='SUCCEEDED'")
    params = db.loads(db.query('SELECT params FROM tasks WHERE id=?', (tid,), one=True)['params'])
    own = choose(e.nodes(tid), params, 4, 100, {'method': 'round_robin'})
    other = choose(e.nodes('other-task'), params, 4, 100, {'method': 'round_robin'})
    assert own['target_node'] == 'lab-1'
    assert other['target_node'] is None
    assert next(n for n in e.nodes() if n['id'] == 'lab-1')['reserved'] == 1
    await sessions.reconcile()
    assert not sessions.owner.kube.deleted
    await e.close()


@pytest.mark.asyncio
async def test_busy_session_cannot_be_assigned_twice_even_with_multiple_slots():
    e, sessions, tid = setup_session()
    snap = db.loads(db.query("SELECT snapshot FROM nodes WHERE id='lab-1'", one=True)['snapshot'])
    snap['capacity'] = 4
    db.execute("UPDATE nodes SET snapshot=? WHERE id='lab-1'", (db.dumps(snap),))
    n = next(n for n in e.nodes(tid) if n['id'] == 'lab-1')
    assert n['session_blocked']
    await e.close()


@pytest.mark.asyncio
async def test_model_readiness_ack_is_idempotent_and_secret_not_exported():
    e, sessions, tid = setup_session()
    db.execute("UPDATE execution_sessions SET status='STARTING',ready_at=NULL,model_load_count=0")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        for _ in range(2):
            r = await c.post('/internal/sessions/session_one/ready', headers=sh(), json={'model_sha256': 'modelhash', 'model_load_seconds': .2})
            assert r.status_code == 200, r.text
    assert get_session('session_one')['model_load_count'] == 1
    detail = db.task_detail(tid)
    assert 'token' not in detail['sessions'][0] and 'token' not in detail['attempts'][0]
    await e.close()


@pytest.mark.asyncio
async def test_attempt_timeout_does_not_expire_idle_or_old_but_healthy_session():
    e, sessions, tid = setup_session()
    db.execute('UPDATE execution_sessions SET created=?', (time.time() - 350,))
    db.execute("UPDATE attempts SET created=? WHERE id='a1'", (time.time() - 350,))
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'READY'
    db.execute("UPDATE attempts SET started=? WHERE id='a1'", (time.time() - 350,))
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'STOPPING'
    assert db.query("SELECT status FROM attempts WHERE id='a1'", one=True)['status'] == 'TIMED_OUT'
    await e.close()


@pytest.mark.asyncio
async def test_controller_restart_recovers_existing_session_without_new_job():
    e, sessions, tid = setup_session()
    replacement = ContainerExecution(e)
    replacement.kube = sessions.owner.kube
    await replacement.sessions.reconcile()
    assert not replacement.kube.deleted
    assert len(db.task_detail(tid)['sessions']) == 1
    assert get_session('session_one')['active_attempt'] == 'a1'
    await e.close()


@pytest.mark.asyncio
async def test_session_job_has_task_lifetime_and_node_affinity():
    e, sessions, tid = setup_session()
    s = get_session('session_one'); w = db.loads(s['workload'])
    manifest = session_manifest(s, w, {'min_memory_mb': 256, 'attempt_timeout': 5}, 600)
    assert manifest['spec']['activeDeadlineSeconds'] == 630
    assert manifest['metadata']['labels']['changan.io/session'] == s['id']
    assert 'changan.io/attempt' not in manifest['metadata']['labels']
    assert manifest['spec']['template']['spec']['nodeSelector']['changan.io/node-id'] == 'lab-1'
    await e.close()


@pytest.mark.asyncio
async def test_pod_death_retries_without_touching_completed_segments():
    e, sessions, tid = setup_session()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as c:
        await finish(c, tid)
    sessions.owner.kube.pod_values = [pod('Failed')]
    await sessions.reconcile()
    assert get_session('session_one')['status'] == 'STOPPING'
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'", one=True) == {'status': 'SUCCEEDED', 'retries': 0}
    await e.close()


@pytest.mark.asyncio
async def test_stopping_transaction_recovers_interrupted_failure_requeue():
    e, sessions, tid = setup_session()
    db.execute("UPDATE execution_sessions SET status='STOPPING',error='interrupted stop'")
    patch_workload('session_one', {'stop_status': 'TIMED_OUT'})
    await sessions.reconcile()
    assert get_session('session_one')['active_attempt'] is None
    assert db.query("SELECT status FROM attempts WHERE id='a1'", one=True)['status'] == 'TIMED_OUT'
    assert db.query("SELECT retries FROM segments WHERE id='s1'", one=True)['retries'] == 1
    await sessions.reconcile()
    assert db.query("SELECT retries FROM segments WHERE id='s1'", one=True)['retries'] == 1
    await e.close()


@pytest.mark.asyncio
async def test_external_pool_reservation_spans_whole_session():
    e, sessions, tid = setup_session()
    class Pool:
        enabled = True
        def __init__(self):self.reserved=[];self.released=[]
        async def reserve(self,*args):self.reserved.append(args);return 'lease1'
        async def release(self,rid):self.released.append(rid)
    e.resource_provider=Pool()
    s=get_session('session_one');w=db.loads(s['workload']);w.pop('job_uid')
    db.execute("UPDATE execution_sessions SET workload=?",(db.dumps(w),))
    async def submit(manifest,payload):
        assert payload['session_id']=='session_one' and 'attempt_id' not in payload
        return sessions.owner.kube.job_value
    sessions.owner.kube.submit=submit
    task=db.query('SELECT * FROM tasks WHERE id=?',(tid,),one=True)
    await sessions.ensure_started(get_session('session_one'),task)
    await sessions.ensure_started(get_session('session_one'),task)
    assert len(e.resource_provider.reserved)==1
    assert e.resource_provider.reserved[0][2]=='session_one'
    assert e.resource_provider.reserved[0][-1]>=600
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        await finish(c,tid)
    await sessions.reconcile()
    assert not e.resource_provider.released
    e.fail_task(tid,'cancel','CANCELED');await sessions.reconcile()
    assert not e.resource_provider.released
    sessions.owner.kube.job_value=None;sessions.owner.kube.pod_values=[]
    await sessions.reconcile();await sessions.reconcile()
    assert e.resource_provider.released==['lease1']
    await e.close()


@pytest.mark.asyncio
async def test_cancel_before_session_creation_leaves_no_live_assignment():
    e, sessions, tid = setup_session()
    db.execute("UPDATE attempts SET session_id=NULL,workload=?,status='DISPATCHING' WHERE id='a1'", (db.dumps({'execution_mode':'task_session'}),))
    db.execute('DELETE FROM execution_sessions')
    e.fail_task(tid,'cancel before submit','CANCELED')
    task=db.query('SELECT * FROM tasks WHERE id=?',(tid,),one=True)
    await sessions.submit(task,{},'a1',{'input_sha256':'inputhash'})
    assert not db.query('SELECT id FROM execution_sessions')
    assert db.query("SELECT status,cleanup_state FROM attempts WHERE id='a1'",one=True)=={'status':'CANCELED','cleanup_state':'DONE'}
    await e.close()


@pytest.mark.asyncio
async def test_second_pod_cannot_take_over_claimed_session():
    e, sessions, tid = setup_session()
    sessions.owner.kube.pod_values=[pod(uid='other-pod')]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        r=await c.post('/internal/sessions/session_one/claim',headers=sh(uid='other-pod'))
        assert r.status_code==409
    assert db.loads(get_session('session_one')['workload'])['pod_uid']=='p1'
    await e.close()


@pytest.mark.asyncio
async def test_removed_node_registration_fences_existing_session():
    e, sessions, tid = setup_session()
    db.execute("UPDATE nodes SET error='节点不在 K3s 可执行节点集合',observed=NULL WHERE id='lab-1'")
    await sessions.reconcile()
    assert get_session('session_one')['status']=='STOPPING'
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'",one=True)=={'status':'PENDING','retries':1}
    await e.close()


@pytest.mark.asyncio
async def test_api_outage_keeps_healthy_session_and_does_not_start_duplicate_job():
    e, sessions, tid = setup_session()
    async def outage(name):raise TimeoutError('unreachable')
    sessions.owner.kube.job=outage
    await sessions.reconcile()
    assert get_session('session_one')['status']=='READY'
    assert get_session('session_one')['cleanup_state']=='PENDING'
    assert db.query("SELECT retries FROM segments WHERE id='s1'",one=True)['retries']==0
    assert len(db.task_detail(tid)['sessions'])==1
    await e.close()
