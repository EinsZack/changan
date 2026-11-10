"""Task-scoped, reusable node containers. Assignment and container lifetimes are separate.

The gateway commits validated results and releases only the assignment. The
coordinator owns session creation/cleanup and physical resource reservations.
An unhealthy session is fenced before retrying its work on another node.
"""
import hashlib
import json
import os
import secrets
import time
from fastapi import APIRouter, HTTPException, Request
from . import db
from .config import DATA, TERMINAL
from .incidents import reconcile_observation
from .kube import session_manifest
from .execution_timing import validate_runtime, validate_startup, validate_segment, pod_lifecycle

router = APIRouter()
MODE = 'task_session'
HEARTBEAT_TIMEOUT = 30
STARTUP_TIMEOUT = 180


def get_session(sid, conn=None):
    if conn is not None:
        row = conn.execute('SELECT * FROM execution_sessions WHERE id=?', (sid,)).fetchone()
        return dict(row) if row else None
    return db.query('SELECT * FROM execution_sessions WHERE id=?', (sid,), one=True)


def patch_workload(sid, changes):
    # Never overwrite claim/readiness information written by the other process.
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        s = get_session(sid, c)
        w = db.loads(s['workload'])
        if all(w.get(key) == value for key, value in changes.items()):
            return
        for key, value in changes.items():
            if key in ('job_uid', 'pod_uid', 'pod_name', 'node_name') and w.get(key) and w[key] != value:
                raise ValueError('Session identity changed: ' + key)
            w[key] = value
        c.execute('UPDATE execution_sessions SET workload=?,updated=? WHERE id=?', (db.dumps(w), time.time(), sid))


def session_valid(s, task, uid=None):
    if not s or s['cleanup_state'] == 'DONE' or s['status'] in ('STOPPING', 'CLOSED') or task['status'] in TERMINAL:
        raise HTTPException(409, '执行会话已停止')
    w = db.loads(s['workload'])
    if uid is not None and (not w.get('pod_uid') or w['pod_uid'] != uid):
        raise HTTPException(409, 'Pod 未持有会话')
    return w


def authenticate_session(sid, request, require_claim=True):
    s = get_session(sid)
    if not s or not secrets.compare_digest(request.headers.get('x-session-token', ''), s['token']):
        raise HTTPException(401, '会话认证失败')
    task = db.query('SELECT * FROM tasks WHERE id=?', (s['task_id'],), one=True)
    w = session_valid(s, task, request.headers.get('x-pod-uid') if require_claim else None)
    return s, w, task


def authenticate_attempt(a, request, allow_completed=False):
    if not secrets.compare_digest(request.headers.get('x-attempt-token', ''), a['token']):
        raise HTTPException(401, '尝试认证失败')
    w = db.loads(a['workload'], {})
    if not w.get('pod_uid') or w['pod_uid'] != request.headers.get('x-pod-uid'):
        raise HTTPException(409, 'Pod 未持有分片')
    # A lost final response may be retried after task completion/cleanup. Read-only ack.
    if allow_completed and a['status'] in ('SUCCEEDED', 'FAILED') and a['finished']:
        return a, w
    s = get_session(a['session_id'])
    task = db.query('SELECT * FROM tasks WHERE id=?', (a['task_id'],), one=True)
    session_valid(s, task, w['pod_uid'])
    segment = db.query('SELECT active_attempt FROM segments WHERE id=?', (a['segment_id'],), one=True)
    if a['status'] in TERMINAL or s['active_attempt'] != a['id'] or segment['active_attempt'] != a['id']:
        raise HTTPException(409, '分片执行权已失效')
    return a, w


class SessionExecution:
    def __init__(self, owner):
        self.owner = owner
        self.engine = owner.engine

    def scheduling_nodes(self, nodes, task_id):
        for n in nodes:
            s = db.query("SELECT * FROM execution_sessions WHERE task_id=? AND node_id=? AND cleanup_state!='DONE'", (task_id, n['id']), one=True)
            if not s:
                continue
            snap = dict(n['snapshot'])
            w = db.loads(s['workload'])
            if s['status'] != 'READY' or s['active_attempt']:
                n['session_blocked'] = '本任务会话正在准备、处理分片或等待清理'
                continue
            # This task already owns this slot/memory. Other tasks still see it reserved.
            n['reserved'] = max(0, n['reserved'] - 1)
            snap['active'] = max(0, snap.get('active', 0) - 1)
            snap['available_memory_mb'] = min(snap.get('memory_budget_mb', 0), snap.get('available_memory_mb', 0) + w['memory_mb'])
            snap['reusable_session_id'] = s['id']
            snap['session_model_ready'] = True
            n['snapshot'] = snap
        return nodes

    async def submit(self, task, segment, aid, payload):
        app = db.loads(task['metadata'])['application']
        params = db.loads(task['params'])
        a = db.query('SELECT * FROM attempts WHERE id=?', (aid,), one=True)
        node = db.query('SELECT * FROM nodes WHERE id=?', (a['node_id'],), one=True)
        snapshot = db.loads(node['snapshot'])
        now = time.time()
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            current = c.execute('SELECT * FROM attempts WHERE id=?', (aid,)).fetchone()
            parent = c.execute('SELECT status FROM tasks WHERE id=?', (task['id'],)).fetchone()
            if parent['status'] in TERMINAL or current['status'] in TERMINAL:
                if parent['status'] in TERMINAL and current['status'] not in TERMINAL:
                    c.execute("UPDATE attempts SET status='CANCELED',finished=?,updated=?,error='分配前父任务已终止' WHERE id=?", (now, now, aid))
                c.execute("UPDATE attempts SET cleanup_state='DONE' WHERE id=? AND session_id IS NULL", (aid,))
                return
            s = c.execute("SELECT * FROM execution_sessions WHERE task_id=? AND node_id=? AND cleanup_state!='DONE'", (task['id'], a['node_id'])).fetchone()
            if s:
                s = dict(s)
                if s['status'] in ('STOPPING', 'CLOSED') or s['active_attempt'] not in (None, aid):
                    raise ValueError('会话不可接收新分片')
                w = db.loads(s['workload'])
            else:
                sid = db.uid('session')
                w = app | {'job_name': 'video-' + sid.replace('_', '-'), 'cluster': self.owner.client().cluster,
                           'namespace': self.owner.client().namespace, 'target_node': a['node_id'],
                           'kube_node': snapshot['kube_node'], 'node_uid': snapshot.get('node_uid'),
                           'memory_mb': max(1024, app.get('container_memory_floor_mb',0), params['min_memory_mb']), 'created_at': now,
                           'gpu_access': bool(app.get('timing_version') and snapshot.get('gpu_available'))}
                s = {'id': sid, 'task_id': task['id'], 'node_id': a['node_id'], 'token': secrets.token_urlsafe(32)}
                c.execute("INSERT INTO execution_sessions(id,task_id,node_id,token,status,created,updated,workload) VALUES(?,?,?,?,'STARTING',?,?,?)",
                          (sid, task['id'], a['node_id'], s['token'], now, now, db.dumps(w)))
                db.event(task['id'], 'session_created', {'stage': 'execute', 'state': 'STARTING', 'session_id': sid, 'node_id': a['node_id'], 'job_name': w['job_name'], 'execution_mode': MODE}, conn=c)
            aw = w | {'session_id': s['id'], 'input_sha256': payload['input_sha256']}
            # Preserve an assignment already delivered by the gateway during recovery.
            if not current['session_id']:
                c.execute("UPDATE attempts SET backend='k3s',session_id=?,workload=?,status='WAITING_SESSION',cleanup_state='PENDING' WHERE id=?", (s['id'], db.dumps(aw), aid))
                c.execute("UPDATE segments SET status='WAITING_SESSION' WHERE active_attempt=?", (aid,))
                c.execute('UPDATE execution_sessions SET active_attempt=?,updated=? WHERE id=?', (aid, now, s['id']))
        await self.ensure_started(get_session(s['id']), task)

    async def ensure_started(self, s, task):
        w = db.loads(s['workload'])
        if w.get('job_uid') or s['status'] in ('STOPPING', 'CLOSED'):
            return
        params = db.loads(task['params'])
        remaining = max(1, task['created'] + params['deadline_seconds'] - time.time())
        if self.engine.resource_provider.enabled and not s['reservation_id']:
            node = db.query('SELECT snapshot FROM nodes WHERE id=?', (s['node_id'],), one=True)
            snap = db.loads(node['snapshot'])
            # Use one idempotency key/lease for the whole task-node session.
            rid = await self.engine.resource_provider.reserve(s['node_id'], s['task_id'], s['id'],
                {'memory_mb': w['memory_mb'], 'slots': 1, 'scope': 'task_session'}, snap.get('snapshot_version'), int(remaining) + 300)
            db.execute('UPDATE execution_sessions SET reservation_id=? WHERE id=?', (rid, s['id']))
        base = os.getenv('CHANGAN_CONTAINER_BASE', self.engine.base)
        payload = {'execution_mode': MODE, 'session_id': s['id'], 'session_token': s['token'],
                   'task_id': s['task_id'], 'target_node': s['node_id'], 'base_url': base,
                   'model_sha256': w['model_sha256'], 'heartbeat_seconds': 2, 'lease_seconds': 20,
                   'timing_version': w.get('timing_version'), 'model_policy': w.get('model_policy','cpu'),
                   'model_device_id': w.get('model_device_id',0)}
        if not w.get('job_submit_started_at'):
            patch_workload(s['id'], {'job_submit_started_at': time.time()})
        job = await self.owner.client().submit(session_manifest(s, w, params, remaining), payload)
        patch_workload(s['id'], {'job_uid': job.metadata.uid, 'job_submit_finished_at': time.time()})

    def stop(self, sid, reason, failure_status='FAILED'):
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            s = get_session(sid, c)
            if s['status'] == 'CLOSED':
                return
            already_stopping = s['status'] == 'STOPPING'
            w = db.loads(s['workload'])
            if already_stopping:
                failure_status = w.get('stop_status', failure_status)
            else:
                w['stop_status'] = failure_status
            # Revoke before allowing any business retry, even during a network partition.
            c.execute("UPDATE execution_sessions SET status='STOPPING',error=?,updated=?,workload=? WHERE id=?", (reason, time.time(), db.dumps(w), sid))
            if not already_stopping:db.event(s['task_id'], 'session_stopping', {'stage': 'execute', 'state': 'CLEANING', 'session_id': sid, 'node_id': s['node_id'], 'message': reason}, conn=c)
        if s['active_attempt']:
            self.engine.attempt_failure(s['active_attempt'], reason, failure_status)
            db.execute('UPDATE execution_sessions SET active_attempt=NULL WHERE id=? AND active_attempt=?', (sid, s['active_attempt']))

    async def reconcile(self):
        # Recover a crash between persisting a dispatch decision and attaching its session.
        for a in db.query("SELECT * FROM attempts WHERE backend='k3s' AND session_id IS NULL AND cleanup_state!='DONE' AND json_extract(workload,'$.execution_mode')=?", (MODE,)):
            task = db.query('SELECT * FROM tasks WHERE id=?', (a['task_id'],), one=True)
            if task['status'] in TERMINAL:
                self.engine.attempt_failure(a['id'], '父任务已终止', 'CANCELED')
                db.execute("UPDATE attempts SET cleanup_state='DONE' WHERE id=?", (a['id'],))
                continue
            seg = db.query('SELECT * FROM segments WHERE id=?', (a['segment_id'],), one=True)
            from pathlib import Path
            try:
                await self.submit(task, seg, a['id'], {'input_sha256': hashlib.sha256(Path(seg['path']).read_bytes()).hexdigest()})
            except Exception:
                # Keep the durable decision pending while an external API is uncertain.
                pass
        for s in db.query("SELECT * FROM execution_sessions WHERE cleanup_state!='DONE' ORDER BY created"):
            try:
                await self.reconcile_one(s)
            except Exception as e:
                w = db.loads(get_session(s['id'])['workload'])
                if time.time() - w.get('last_api_error', 0) > 15:
                    patch_workload(s['id'], {'last_api_error': time.time()})
                    db.event(s['task_id'], 'session_api_unavailable', {'stage': 'execute', 'state': 'UNKNOWN', 'session_id': s['id'], 'message': '集群状态待核对，保留会话资源', 'error_type': type(e).__name__})
        if self.engine.resource_provider.enabled:
            for s in db.query("SELECT * FROM execution_sessions WHERE cleanup_state='DONE' AND reservation_id IS NOT NULL AND reservation_released=0"):
                try:
                    await self.engine.resource_provider.release(s['reservation_id'])
                    db.execute('UPDATE execution_sessions SET reservation_released=1 WHERE id=?', (s['id'],))
                except Exception:
                    pass

    async def reconcile_one(self, s):
        sid = s['id']
        task = db.query('SELECT * FROM tasks WHERE id=?', (s['task_id'],), one=True)
        params = db.loads(task['params'])
        now = time.time()
        node = db.query('SELECT * FROM nodes WHERE id=?', (s['node_id'],), one=True)
        snap = db.loads(node['snapshot'], {}) if node else {}
        w = db.loads(s['workload'])
        if s['status'] == 'STOPPING':
            self.stop(sid, s['error'] or '恢复中断的会话回收', w.get('stop_status', 'FAILED'))
        elif task['status'] in TERMINAL:
            self.stop(sid, '任务结束，回收执行会话', 'CANCELED')
        elif not s['ready_at'] and now - s['created'] > min(STARTUP_TIMEOUT, params['attempt_timeout']):
            self.stop(sid, '容器或模型准备超时', 'TIMED_OUT')
        elif s['heartbeat_at'] and now - s['heartbeat_at'] > HEARTBEAT_TIMEOUT:
            self.stop(sid, '会话心跳超时，隔离旧执行并重新分配分片', 'TIMED_OUT')
        elif node and node['error'] == '节点不在 K3s 可执行节点集合':
            self.stop(sid, '节点已退出可执行节点集合，重新分配未完成分片')
        elif node and not node['error'] and now - (node['observed'] or 0) < 8 and (
                not snap.get('healthy') or (w.get('node_uid') and snap.get('node_uid') != w['node_uid'])):
            self.stop(sid, '节点退出或身份改变，隔离旧执行并重新分配分片')
        elif s['active_attempt']:
            a = db.query('SELECT * FROM attempts WHERE id=?', (s['active_attempt'],), one=True)
            if a and a['started'] and a['status'] not in TERMINAL and now - a['started'] > params['attempt_timeout']:
                self.stop(sid, '分片执行超时，回收异常会话', 'TIMED_OUT')
        s = get_session(sid)
        k = self.owner.client()
        job = await k.job(w['job_name'])
        pods = await k.pods(w['job_name'])
        if job:
            if job.metadata.labels.get('changan.io/session') != sid or (w.get('job_uid') and job.metadata.uid != w['job_uid']):
                raise ValueError('Session Job identity mismatch; refusing deletion')
            patch_workload(sid, {'job_uid': job.metadata.uid})
        details = []
        for p in pods:
            cs = p.status.container_statuses or []
            reasons = [state.reason for container in cs for state in (container.state.waiting, container.state.terminated) if state]
            details.append({'name': p.metadata.name, 'uid': p.metadata.uid, 'node_name': p.spec.node_name,
                            'phase': p.status.phase, 'reasons': reasons, 'image_id': cs[0].image_id if cs else None})
        changes = {'pods': details}
        # The coordinator alone owns lifecycle; readiness writes a different top-level key.
        lifecycle = dict(w.get('lifecycle', {}))
        for p in pods:
            previous = lifecycle.get(p.metadata.uid, {})
            if previous.get('startup_complete'):
                lifecycle[p.metadata.uid] = previous | pod_lifecycle(p)
            else:
                events = []
                if hasattr(k, 'pod_events'):
                    try:
                        events = await k.pod_events(p.metadata.uid)
                    except Exception:
                        # A missing event is unknown evidence, not zero image-pull time.
                        pass
                evidence = pod_lifecycle(p, events)
                lifecycle[p.metadata.uid] = previous | evidence
                if s['ready_at'] and evidence.get('container_started_at') and evidence.get('image_ready_at'):
                    lifecycle[p.metadata.uid]['startup_complete'] = True
        if lifecycle:
            changes['lifecycle'] = lifecycle
        if any(p.metadata.uid == w.get('pod_uid') and p.status.phase in ('Succeeded', 'Failed') for p in pods):
            changes['termination_observed'] = True
        patch_workload(sid, changes)
        if s['status'] != 'STOPPING':
            selected = next((p for p in pods if p.metadata.uid == w.get('pod_uid')), None)
            failed = (selected and (selected.metadata.deletion_timestamp or selected.status.phase in ('Succeeded', 'Failed')))
            if job:
                failed = failed or any(c.type == 'Failed' and c.status == 'True' for c in (job.status.conditions or []))
            if failed or (w.get('job_uid') and not job):
                logs = {}
                for p in pods:
                    try:
                        logs[p.metadata.name] = (await k.logs(p.metadata.name)).replace(s['token'], '[REDACTED]')
                    except Exception:
                        pass
                if logs:
                    patch_workload(sid, {'failure_logs': logs})
                self.stop(sid, '执行容器退出或被删除，重新分配未完成分片')
            elif not job and not pods:
                await self.ensure_started(s, task)
        s = get_session(sid)
        if s['status'] != 'STOPPING':
            return
        if job:
            await k.delete(w['job_name'])
        if job or pods:
            return
        w = db.loads(s['workload'])
        # A vanished API object is not proof that a disconnected machine stopped.
        healthy = node and not node['error'] and now - (node['observed'] or 0) < 8 and snap.get('healthy') and (not w.get('node_uid') or snap.get('node_uid') == w['node_uid'])
        if w.get('pod_uid') and not healthy and not w.get('termination_observed'):
            return
        await k.delete_secret(w['job_name'])
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            c.execute("UPDATE execution_sessions SET status='CLOSED',cleanup_state='DONE',finished=?,updated=? WHERE id=?", (time.time(), time.time(), sid))
            c.execute("UPDATE attempts SET cleanup_state='DONE' WHERE session_id=?", (sid,))
            db.event(s['task_id'], 'session_cleaned', {'stage': 'execute', 'state': 'CLEANED', 'session_id': sid, 'job_name': w['job_name']}, conn=c)


@router.post('/internal/sessions/{sid}/claim')
async def claim_session(sid: str, request: Request):
    from .container_execution import controller
    s, w, task = authenticate_session(sid, request, False)
    job = await controller.client().job(w['job_name'])
    if not job or job.metadata.labels.get('changan.io/session') != sid or (w.get('job_uid') and w['job_uid'] != job.metadata.uid):
        raise HTTPException(409, '会话 Job 尚未匹配')
    uid = request.headers.get('x-pod-uid')
    pods = await controller.client().pods(w['job_name'])
    pod = next((p for p in pods if p.metadata.uid == uid), None)
    if not pod or pod.metadata.deletion_timestamp or pod.spec.node_name != w['kube_node'] or not any(o.uid == job.metadata.uid for o in (pod.metadata.owner_references or [])):
        raise HTTPException(409, 'Pod 来源或目标节点不匹配')
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        s = get_session(sid, c)
        task = c.execute('SELECT * FROM tasks WHERE id=?', (s['task_id'],)).fetchone()
        w = session_valid(s, task)
        if w.get('pod_uid') and w['pod_uid'] != uid:
            raise HTTPException(409, '另一个 Pod 已持有会话')
        if w.get('job_uid') and w['job_uid'] != job.metadata.uid:
            raise HTTPException(409, 'Job 身份已改变')
        first = not w.get('pod_uid')
        w.update({'job_uid': job.metadata.uid, 'pod_uid': uid, 'pod_name': pod.metadata.name, 'node_name': pod.spec.node_name})
        c.execute('UPDATE execution_sessions SET workload=?,heartbeat_at=?,updated=? WHERE id=?', (db.dumps(w), time.time(), time.time(), sid))
        if first:
            w['claim_received_at'] = time.time()
            c.execute('UPDATE execution_sessions SET workload=? WHERE id=?', (db.dumps(w),sid))
            db.event(s['task_id'], 'session_claimed', {'stage': 'execute', 'state': 'MODEL_LOADING', 'session_id': sid, 'pod_uid': uid, 'node_id': s['node_id']}, conn=c)
    return {'accepted': True}


@router.post('/internal/sessions/{sid}/heartbeat')
async def heartbeat(sid: str, request: Request):
    s, w, task = authenticate_session(sid, request)
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        s = get_session(sid, c)
        task = c.execute('SELECT * FROM tasks WHERE id=?', (s['task_id'],)).fetchone()
        session_valid(s, task, request.headers.get('x-pod-uid'))
        c.execute('UPDATE execution_sessions SET heartbeat_at=? WHERE id=?', (time.time(), sid))
    return {'action': 'continue'}


@router.post('/internal/sessions/{sid}/ready')
async def ready(sid: str, request: Request):
    s, w, task = authenticate_session(sid, request)
    body = await request.json()
    seconds = body.get('model_load_seconds')
    import math
    if body.get('model_sha256') != w['model_sha256'] or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds < 0:
        raise HTTPException(422, '模型就绪证据无效')
    if w.get('timing_version') == 1:
        validate_runtime(body.get('inference_runtime'), w)
        validate_startup(body.get('startup_timing'), seconds)
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        s = get_session(sid, c)
        task = c.execute('SELECT * FROM tasks WHERE id=?', (s['task_id'],)).fetchone()
        w = session_valid(s, task, request.headers.get('x-pod-uid'))
        if s['ready_at'] and w.get('timing_version') == 1:
            if w.get('inference_runtime') != body.get('inference_runtime') or w.get('startup_timing') != body.get('startup_timing'):
                raise HTTPException(409, '会话就绪证据已冻结')
        if not s['ready_at']:
            if w.get('timing_version') == 1:
                w.update(inference_runtime=body['inference_runtime'], startup_timing=body['startup_timing'])
                c.execute('UPDATE execution_sessions SET workload=? WHERE id=?', (db.dumps(w),sid))
            now = time.time()
            c.execute("UPDATE execution_sessions SET status='READY',ready_at=?,model_load_count=1,model_load_seconds=?,heartbeat_at=?,updated=? WHERE id=?", (now, seconds, now, now, sid))
            db.event(s['task_id'], 'session_ready', {'stage': 'execute', 'state': 'READY', 'session_id': sid, 'pod_uid': w['pod_uid'], 'model_load_seconds': seconds, 'model_load_count': 1}, conn=c)
    return {'accepted': True}


@router.post('/internal/sessions/{sid}/next')
async def next_assignment(sid: str, request: Request):
    s, w, task = authenticate_session(sid, request)
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        s = get_session(sid, c)
        task = c.execute('SELECT * FROM tasks WHERE id=?', (s['task_id'],)).fetchone()
        w = session_valid(s, task, request.headers.get('x-pod-uid'))
        if not s['ready_at'] or not s['active_attempt']:
            return {'action': 'wait', 'retry_after': .25}
        a = c.execute('SELECT * FROM attempts WHERE id=?', (s['active_attempt'],)).fetchone()
        seg = c.execute('SELECT * FROM segments WHERE id=?', (a['segment_id'],)).fetchone()
        if a['status'] in TERMINAL or seg['active_attempt'] != a['id']:
            raise HTTPException(409, '分片已失效')
        aw = db.loads(a['workload']) | {key: w[key] for key in ('pod_uid', 'pod_name', 'node_name', 'job_uid')}
        if w.get('timing_version') == 1:
            aw['inference_runtime'] = w['inference_runtime']
        if not a['execution_id']:
            now = time.time()
            c.execute("UPDATE attempts SET execution_id=?,actual_node=?,status='ACCEPTED',workload=?,started=?,updated=? WHERE id=?", (w['pod_uid'], s['node_id'], db.dumps(aw), now, now, a['id']))
            c.execute("UPDATE segments SET status='ACCEPTED' WHERE id=?", (seg['id'],))
            db.event(s['task_id'], 'dispatch_ack', {'stage': 'execute', 'state': 'ACCEPTED', 'identity_matched': True, 'task_id': s['task_id'], 'segment_id': a['segment_id'], 'attempt_id': a['id'], 'actual_node': s['node_id'], 'execution_id': w['pod_uid'], 'session_id': sid, 'job_name': w['job_name'], 'pod_name': w['pod_name'], 'kube_node': w['node_name']}, a['id'], c)
        params = db.loads(task['params'])
        base = str(request.base_url).rstrip('/') + '/internal/container/' + a['id']
        payload = {'attempt_id': a['id'], 'task_id': s['task_id'], 'segment_id': a['segment_id'], 'token': a['token'], 'session_id': sid,
                   'target_node': s['node_id'], 'base_url': base, 'input_url': base + '/input', 'callback_url': base + '/events',
                   'input_sha256': aw['input_sha256'], 'model_sha256': w['model_sha256'],
                   'params': {'sample_fps': params['sample_fps'], 'confidence': params['confidence']}}
    reconcile_observation(a['id'])
    return {'action': 'execute', 'payload': payload}


def validate_result(a, w, result):
    expected = {'attempt_id': a['id'], 'task_id': a['task_id'], 'segment_id': a['segment_id'], 'actual_node': a['node_id'],
                'model_id': w['model_id'], 'model_sha256': w['model_sha256'], 'pod_uid': w['pod_uid'], 'input_sha256': w['input_sha256'], 'session_id': a['session_id']}
    if not isinstance(result, dict) or any(result.get(k) != v for k, v in expected.items()):
        raise HTTPException(422, '结果来源不一致')
    if result.get('sampled_frames', 0) <= 0 or len(result.get('frames', [])) != result['sampled_frames'] or not isinstance(result.get('counts'), dict):
        raise HTTPException(422, '采样结果不完整')
    if w.get('timing_version') == 1:
        validate_segment(result, w.get('inference_runtime'))
    names = result.get('previews', [])
    manifest = result.get('artifacts', [])
    if not names or len(set(names)) != len(names) or len(manifest) != len(names) or set(names) != {m.get('name') for m in manifest}:
        raise HTTPException(422, '缺少产物清单')
    urls = []
    for m in manifest:
        if m.get('name') not in ('preview-0.jpg', 'preview-1.jpg', 'preview-2.jpg'):
            raise HTTPException(422, '产物名无效')
        path = DATA / 'tasks' / a['task_id'] / a['id'] / m['name']
        if not path.is_file() or path.stat().st_size != m.get('bytes') or hashlib.sha256(path.read_bytes()).hexdigest() != m.get('sha256'):
            raise HTTPException(422, '产物未持久化或摘要不符')
        urls.append('/api/tasks/' + a['task_id'] + '/artifacts/' + a['id'] + '/' + m['name'])
    return result | {'preview_urls': urls, 'image': w['image'], 'job_uid': w['job_uid'], 'job_name': w['job_name'], 'kube_node': w['node_name'], 'execution_backend': 'k3s', 'execution_mode': MODE}


async def attempt_event(aid, request):
    a = db.query('SELECT * FROM attempts WHERE id=?', (aid,), one=True)
    a, w = authenticate_attempt(a, request, allow_completed=True)
    raw = await request.body()
    if len(raw) > 16 * 1024**2:
        raise HTTPException(413, '结果超过16MiB')
    try:
        b = json.loads(raw)
    except ValueError:
        raise HTTPException(422, '事件JSON无效')
    seq, state = b.get('seq'), b.get('status')
    if not isinstance(seq, int) or seq < 1 or state not in ('RUNNING', 'UPLOADING', 'FINALIZING', 'FAILED'):
        raise HTTPException(422, '事件状态或序号无效')
    if a['status'] in TERMINAL:
        if seq == a['last_seq'] and ((state == 'FINALIZING' and a['status'] == 'SUCCEEDED') or (state == 'FAILED' and a['status'] == 'FAILED')):
            return {'accepted': True, 'duplicate': True}
        raise HTTPException(409, '分片已结束')
    if seq <= a['last_seq']:
        return {'accepted': True, 'duplicate': True}
    result = validate_result(a, w, b.get('result')) if state == 'FINALIZING' else None
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        a = c.execute('SELECT * FROM attempts WHERE id=?', (aid,)).fetchone()
        s = get_session(a['session_id'], c)
        task = c.execute('SELECT * FROM tasks WHERE id=?', (a['task_id'],)).fetchone()
        seg = c.execute('SELECT * FROM segments WHERE id=?', (a['segment_id'],)).fetchone()
        if seq <= a['last_seq']:
            return {'accepted': True, 'duplicate': True}
        session_valid(s, task, request.headers.get('x-pod-uid'))
        if a['status'] in TERMINAL or s['active_attempt'] != aid or seg['active_attempt'] != aid:
            raise HTTPException(409, '分片执行权已失效')
        order = {'ACCEPTED': 0, 'RUNNING': 1, 'UPLOADING': 2, 'FINALIZING': 3}
        if state != 'FAILED' and order.get(state, 0) < order.get(a['status'], 0):
            raise HTTPException(409, '执行阶段不可回退')
        now = time.time()
        final = state in ('FINALIZING', 'FAILED')
        status = 'SUCCEEDED' if state == 'FINALIZING' else state
        progress = 1 if result else max(0, min(.99, float(b.get('progress', 0))))
        error = str(b.get('error', '分片执行失败'))[:1000] if state == 'FAILED' else None
        c.execute('UPDATE attempts SET status=?,last_seq=?,progress=?,result=?,error=?,finished=?,updated=? WHERE id=?',
                  (status, seq, progress, db.dumps(result) if result else None, error, now if final else None, now, aid))
        if state == 'FAILED':
            retries = seg['retries'] + 1
            next_status = 'PENDING' if retries <= db.loads(task['params'])['max_retries'] else 'FAILED'
            c.execute('UPDATE segments SET status=?,retries=? WHERE id=?', (next_status, retries, seg['id']))
        else:
            c.execute('UPDATE segments SET status=?,result=? WHERE id=?', (status, db.dumps(result) if result else None, seg['id']))
        if final:
            c.execute('UPDATE execution_sessions SET active_attempt=NULL,completed=completed+?,heartbeat_at=?,updated=? WHERE id=?', (int(bool(result)), now, now, s['id']))
        else:
            c.execute('UPDATE execution_sessions SET heartbeat_at=? WHERE id=?', (now, s['id']))
        db.event(a['task_id'], 'attempt_failed' if state == 'FAILED' else 'execution', {'stage': 'execute', 'state': status, 'node_id': a['node_id'], 'segment_id': seg['id'], 'session_id': s['id'], 'pod_uid': w['pod_uid'], 'progress': progress, 'seq': seq, 'message': error}, aid, c)
    reconcile_observation(aid)
    return {'accepted': True, 'container_reused': True}
