import asyncio, time, json, secrets, hashlib, logging, os
from pathlib import Path
import httpx
from . import db, video
from .config import DATA, ROOT, TOKEN, MODEL_ID, TERMINAL
from .scheduler import choose
from .alerts import record_terminal
from .resource_provider import ResourceProvider
from .incidents import observe_node, reconcile_observation
log=logging.getLogger('changan.engine')
class Engine:
    def __init__(self):
        self.client=httpx.AsyncClient(timeout=4,trust_env=False,headers={'X-Internal-Token':TOKEN})
        self.resource_provider=ResourceProvider(self.client)
        self.preparing={}; self.lock=asyncio.Lock(); self.cursor=0; self.last_refresh=0; self.base='http://127.0.0.1:8310'
        self.running=True
        self.backend=os.getenv('CHANGAN_BACKEND','k3s')
        if self.backend not in ('k3s','process'):raise ValueError('CHANGAN_BACKEND 无效')
        self.containers=None
        if self.backend=='k3s':
            from .container_execution import ContainerExecution
            self.containers=ContainerExecution(self)
    async def close(self):
        self.running=False
        for task in self.preparing.values(): task.cancel()
        await self.client.aclose()
    def source_path(self,source):
        if source=='demo': return ROOT/'assets/samples/demo.mp4'
        if not source.startswith('upload_') or not source.replace('_','').isalnum(): raise ValueError('无效视频标识')
        file=DATA/'uploads'/f'{source}.mp4'
        if not file.exists(): raise ValueError('上传视频不存在')
        return file
    def create(self,body,trigger='service',strategy=None,key=None):
        params=body.model_dump()
        self.source_path(params['source'])
        algorithm=db.query("SELECT body FROM configs WHERE category='algorithm' AND id=?",(params['algorithm'],),one=True)
        if not algorithm: raise ValueError('调度算法不存在')
        workflow=db.query("SELECT body FROM configs WHERE category='workflow' AND id=?",(params['workflow_id'],),one=True)
        if not workflow or not db.loads(workflow['body']).get('enabled',True): raise ValueError('工作流不存在或已禁用')
        if params['workflow_id']!='video-analysis': raise ValueError('此任务入口仅支持视频分析模板，能力验证请使用对应入口')
        metadata={'algorithm_snapshot':db.loads(algorithm['body']),'workflow_snapshot':db.loads(workflow['body']),'execution_backend':self.backend}
        if self.containers:metadata['application']=self.containers.application()
        id=db.uid('task'); now=time.time()
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            if key:
                old=c.execute('SELECT id,params FROM tasks WHERE idempotency_key=?',(key,)).fetchone()
                if old:
                    if db.loads(old['params'])!=params: raise ValueError('幂等键对应参数不一致')
                    return old['id']
            c.execute('INSERT INTO tasks(id,name,source,status,created,updated,params,metadata,trigger_type,strategy_id,idempotency_key) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(id,body.name,body.source,'QUEUED',now,now,db.dumps(params),db.dumps(metadata),trigger,strategy,key))
            db.event(id,'created',{'state':'SUCCEEDED','stage':'submit','params':params,'trigger_type':trigger,'strategy_id':strategy},conn=c)
        return id
    async def prepare(self,id):
        try:
            task=db.query('SELECT * FROM tasks WHERE id=?',(id,),one=True); params=db.loads(task['params'])
            db.execute("UPDATE tasks SET status='PREPARING',started=COALESCE(started,?),updated=? WHERE id=? AND status IN ('QUEUED','PREPARING')",(time.time(),time.time(),id))
            db.event(id,'stage',{'stage':'split','state':'RUNNING','message':'校验输入并真实切分视频'})
            folder=DATA/'tasks'/id; folder.mkdir(parents=True,exist_ok=True)
            info,segments=await asyncio.to_thread(video.split,self.source_path(task['source']),folder,params['segment_seconds'])
            info.update(db.loads(task['metadata'],{}))
            with db.connect() as c:
                if c.execute('SELECT status FROM tasks WHERE id=?',(id,)).fetchone()[0] in TERMINAL: return
                for seg in segments:
                    c.execute('INSERT OR IGNORE INTO segments(id,task_id,ordinal,start,end,path) VALUES(?,?,?,?,?,?)',(db.uid('seg'),id,seg['ordinal'],seg['start'],seg['end'],seg['path']))
                c.execute("UPDATE tasks SET status='RUNNING',metadata=?,total=?,updated=? WHERE id=?",(db.dumps(info),len(segments),time.time(),id))
                db.event(id,'stage',{'stage':'split','state':'SUCCEEDED','input':info,'segments':len(segments)},conn=c)
        except asyncio.CancelledError: raise
        except Exception as e: self.fail_task(id,str(e))
    def fail_task(self,id,error,state='FAILED'):
        now=time.time()
        with db.connect() as c:
            changed=c.execute("UPDATE tasks SET status=?,error=?,finished=?,updated=? WHERE id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')",(state,error,now,now,id)).rowcount
            if changed:
                c.execute("UPDATE segments SET status=? WHERE task_id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')",(state,id))
                db.event(id,'task_terminal',{'stage':'complete','state':state,'message':error},conn=c)
                record_terminal(c,id,state,error,now)
    async def refresh(self):
        if self.containers:
            await self.containers.refresh(); return
        nodes=db.query('SELECT * FROM nodes')
        pool_snapshots=None
        if self.resource_provider.enabled:
            try: pool_snapshots=await self.resource_provider.snapshots()
            except Exception:
                # Never silently substitute worker readings when the authoritative pool is unavailable.
                for n in nodes:
                    db.execute('UPDATE nodes SET error=? WHERE id=?',('资源池快照不可用',n['id']))
                    observe_node(n,False,'资源池快照不可用')
                return
        async def fetch(n):
            start=time.monotonic()
            try:
                r=await self.client.get(n['endpoint']+'/health'); r.raise_for_status(); snapshot=r.json()
                snapshot['observed_http_rtt_ms']=round((time.monotonic()-start)*1000,3)
                observed=time.time()
                if pool_snapshots is not None:
                    authority=pool_snapshots.get(n['id'])
                    if not authority:raise ValueError('节点不在资源池快照中')
                    if authority['node_id']!=snapshot['node_id']:raise ValueError('资源池与执行器标识不一致')
                    for key in ['available_memory_mb','bandwidth_mbps','network_source','tags','snapshot_version','architecture','location','capacity','active','input_reachable']:
                        if key in authority:snapshot[key]=authority[key]
                    snapshot['healthy']=snapshot['healthy'] and authority.get('healthy',False)
                    snapshot['resource_source']='external_pool'
                    observed=min(observed,float(authority['observed_at']))
                db.execute('UPDATE nodes SET snapshot=?,observed=?,error=NULL WHERE id=?',(db.dumps(snapshot),observed,n['id']))
                observe_node(n,time.time()-observed<8,'资源池快照过期')
            except Exception as e:
                db.execute('UPDATE nodes SET error=? WHERE id=?',(type(e).__name__,n['id']))
                observe_node(n,False,type(e).__name__)
        await asyncio.gather(*(fetch(n) for n in nodes))
    def nodes(self,task_id=None):
        nodes=db.query('SELECT * FROM nodes')
        for n in nodes:
            n['snapshot']=db.loads(n['snapshot'],{})
            n['reserved']=db.query("SELECT count(*) AS n FROM attempts WHERE node_id=? AND session_id IS NULL AND (status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT') OR (backend='k3s' AND cleanup_state!='DONE'))",(n['id'],),one=True)['n']
            n['reserved']+=db.query("SELECT count(*) n FROM execution_sessions WHERE node_id=? AND cleanup_state!='DONE'",(n['id'],),one=True)['n']
            n['online']=bool(not n.get('error') and n['observed'] and time.time()-n['observed']<8)
        if task_id and self.containers:return self.containers.sessions.scheduling_nodes(nodes,task_id)
        return nodes
    async def dispatch(self,task,segment):
        params=db.loads(task['params']); frozen=db.loads(task['metadata'],{}).get('algorithm_snapshot')
        algorithm={'body':db.dumps(frozen)} if frozen else db.query("SELECT body FROM configs WHERE id=? AND category='algorithm'",(params['algorithm'],),one=True)
        scheduling_params=dict(params)
        if self.containers:
            floor=db.loads(task['metadata'],{}).get('application',{}).get('container_memory_floor_mb',0)
            scheduling_params['min_memory_mb']=max(params['min_memory_mb'],floor)
        decision=choose(self.nodes(task['id']),scheduling_params,segment['end']-segment['start'],Path(segment['path']).stat().st_size,db.loads(algorithm['body']),self.cursor)
        if not decision['target_node']:
            last=db.query("SELECT at FROM events WHERE task_id=? AND type='no_candidate' ORDER BY id DESC LIMIT 1",(task['id'],),one=True)
            if not last or time.time()-last['at']>5:
                db.event(task['id'],'no_candidate',{'stage':'schedule','state':'PENDING','message':'暂无满足约束的执行节点，截止时限内有界等待','decision':decision})
            return False
        self.cursor+=1; now=time.time(); aid=db.uid('try'); token=secrets.token_urlsafe(24)
        if self.resource_provider.enabled and (not self.containers or db.loads(task['metadata'],{}).get('application',{}).get('execution_mode')!='task_session'):
            try:
                snap=next(c['snapshot'] for c in decision['candidates'] if c['node_id']==decision['target_node'])
                reservation=await self.resource_provider.reserve(decision['target_node'],task['id'],aid,{'memory_mb':params['min_memory_mb'],'slots':1},snap.get('snapshot_version'),params['attempt_timeout']+30)
                decision['reservation_id']=reservation
            except Exception as e:
                db.event(task['id'],'reservation_rejected',{'stage':'schedule','state':'PENDING','node_id':decision['target_node'],'error_type':type(e).__name__})
                return
        decision['input']={'task_id':task['id'],'segment_id':segment['id'],'params':params,'duration_seconds':segment['end']-segment['start']}
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            s=c.execute('SELECT status FROM segments WHERE id=?',(segment['id'],)).fetchone()
            parent=c.execute('SELECT status FROM tasks WHERE id=?',(task['id'],)).fetchone()
            if s[0]!='PENDING' or parent[0]!='RUNNING':
                # Lease expiry provides cleanup if cancellation races reservation; reconciliation handles persisted attempts.
                return
            c.execute('INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision) VALUES(?,?,?,?,?,?,?,?,?)',(aid,task['id'],segment['id'],decision['target_node'],token,'DISPATCHING',now,now,db.dumps(decision)))
            if self.containers:
                c.execute("UPDATE attempts SET backend='k3s',workload=?,cleanup_state='PENDING' WHERE id=?",(db.dumps({'execution_mode':db.loads(task['metadata'])['application'].get('execution_mode','attempt_job')}),aid))
            c.execute("UPDATE segments SET active_attempt=?,status='DISPATCHING' WHERE id=?",(aid,segment['id']))
            db.event(task['id'],'decision',{'stage':'schedule','state':'SUCCEEDED','decision':decision},aid,c)
        node=db.query('SELECT * FROM nodes WHERE id=?',(decision['target_node'],),one=True)
        payload={'task_id':task['id'],'segment_id':segment['id'],'attempt_id':aid,'token':token,'target_node':node['id'],'input_url':self.base+f"/internal/segments/{segment['id']}",'input_sha256':hashlib.sha256(Path(segment['path']).read_bytes()).hexdigest(),'callback_url':self.base+'/internal/events','params':{'sample_fps':params['sample_fps'],'confidence':params['confidence']}}
        db.event(task['id'],'dispatch_request',{'stage':'execute','state':'DISPATCHING','task_id':task['id'],'segment_id':segment['id'],'attempt_id':aid,'target_node':node['id'],'input_sha256':payload['input_sha256'],'input_bytes':Path(segment['path']).stat().st_size,'params':payload['params']},aid)
        try:
            if self.containers:
                await self.containers.submit(task,segment,aid,payload); return
            r=await self.client.post(node['endpoint']+'/executions',json=payload)
            if r.status_code==409:
                self.attempt_failure(aid,'执行节点资源竞争或拒绝提交'); return
            r.raise_for_status()
            receipt=r.json()
            valid=all(receipt.get(k)==v for k,v in {'task_id':task['id'],'segment_id':segment['id'],'attempt_id':aid,'node_id':node['id']}.items())
            db.event(task['id'],'dispatch_ack',{'stage':'execute','state':receipt.get('status'),'identity_matched':valid,'task_id':receipt.get('task_id'),'segment_id':receipt.get('segment_id'),'attempt_id':aid,'actual_node':receipt.get('node_id'),'execution_id':receipt.get('id'),'seq':receipt.get('seq')},aid)
            await self.ingest(receipt)
        except Exception as e:
            # Unknown delivery is reconciled by attempt id before retry, not resubmitted blindly.
            db.execute("UPDATE attempts SET status='UNKNOWN' WHERE id=? AND status='DISPATCHING'",(aid,))
            db.event(task['id'],'delivery_unknown',{'stage':'execute','state':'UNKNOWN','message':'下发响应未确认，将按尝试ID核对','error_type':type(e).__name__},aid)
    def attempt_failure(self,id,error,status='FAILED'):
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE'); a=c.execute('SELECT * FROM attempts WHERE id=?',(id,)).fetchone()
            if not a or a['status'] in TERMINAL: return
            now=time.time()
            c.execute('UPDATE attempts SET status=?,error=?,finished=?,updated=? WHERE id=?',(status,error,now,now,id))
            task=c.execute('SELECT * FROM tasks WHERE id=?',(a['task_id'],)).fetchone(); s=c.execute('SELECT * FROM segments WHERE id=?',(a['segment_id'],)).fetchone()
            if s['active_attempt']==id and task['status'] not in TERMINAL:
                retries=s['retries']+1; max_retries=db.loads(task['params'])['max_retries']
                c.execute('UPDATE segments SET status=?,retries=? WHERE id=?',('PENDING' if retries<=max_retries else 'FAILED',retries,s['id']))
            db.event(a['task_id'],'attempt_failed',{'stage':'execute','state':status,'message':error},id,c)
    async def ingest(self,body):
        id=body.get('attempt_id'); a=db.query('SELECT * FROM attempts WHERE id=?',(id,),one=True)
        if not a or not secrets.compare_digest(body.get('token',''),a['token']): return {'accepted':False,'reason':'unknown_attempt'}
        if body.get('node_id')!=a['node_id'] or body.get('task_id')!=a['task_id'] or body.get('segment_id')!=a['segment_id']:
            self.attempt_failure(id,'执行回执标识与目标不一致'); return {'accepted':False,'reason':'identity_mismatch'}
        state=body.get('status'); seq=body.get('seq',0)
        if state not in ['ACCEPTED','RUNNING','SUCCEEDED','FAILED','CANCELED']: return {'accepted':False,'reason':'invalid_state'}
        if a['status']=='UNKNOWN' and seq==a['last_seq'] and state in ['ACCEPTED','RUNNING']:
            db.execute('UPDATE attempts SET status=? WHERE id=?',(state,id))
            db.execute('UPDATE segments SET status=? WHERE active_attempt=?',(state,id))
            reconcile_observation(id)
            return {'accepted':False,'reason':'reconciled_without_new_event'}
        if seq<=a['last_seq'] or a['status'] in TERMINAL:
            if seq==a['last_seq'] and state==a['status']:
                reconcile_observation(id)
            return {'accepted':False,'reason':'duplicate_or_terminal'}
        if state in ['FAILED','CANCELED']:
            db.execute('UPDATE attempts SET execution_id=?,actual_node=?,last_seq=? WHERE id=?',(body['id'],body['node_id'],seq,id))
            self.attempt_failure(id,body.get('error') or state,state); reconcile_observation(id); return {'accepted':True}
        result=body.get('result')
        if state=='SUCCEEDED':
            if not result or result.get('attempt_id')!=id or result.get('task_id')!=a['task_id'] or result.get('segment_id')!=a['segment_id'] or result.get('actual_node')!=a['node_id'] or result.get('model_id')!=MODEL_ID or result.get('sampled_frames',0)<=0 or len(result.get('frames',[]))!=result.get('sampled_frames'):
                self.attempt_failure(id,'结果完整性或来源校验失败'); return {'accepted':False,'reason':'invalid_result'}
            folder=DATA/'tasks'/a['task_id']/id; folder.mkdir(parents=True,exist_ok=True)
            node=db.query('SELECT endpoint FROM nodes WHERE id=?',(a['node_id'],),one=True)
            stored=result.get('preview_urls',[]) if a.get('backend')=='k3s' else []
            for name in ([] if a.get('backend')=='k3s' else result.get('previews',[])):
                if name not in ['preview-0.jpg','preview-1.jpg','preview-2.jpg']: continue
                try:
                    r=await self.client.get(node['endpoint']+f"/executions/{body['id']}/artifacts/{name}"); r.raise_for_status()
                    (folder/name).write_bytes(r.content); stored.append(f"/api/tasks/{a['task_id']}/artifacts/{id}/{name}")
                except Exception: pass
            result['preview_urls']=stored
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            a=c.execute('SELECT * FROM attempts WHERE id=?',(id,)).fetchone(); s=c.execute('SELECT * FROM segments WHERE id=?',(a['segment_id'],)).fetchone(); task=c.execute('SELECT * FROM tasks WHERE id=?',(a['task_id'],)).fetchone()
            if a['status'] in TERMINAL or seq<=a['last_seq'] or s['active_attempt']!=id or task['status'] in TERMINAL: return {'accepted':False,'reason':'stale_attempt'}
            now=time.time()
            c.execute('UPDATE attempts SET status=?,last_seq=?,progress=?,updated=?,started=CASE WHEN ?=\'RUNNING\' THEN COALESCE(started,?) ELSE started END,finished=?,execution_id=?,actual_node=?,result=? WHERE id=?',(state,seq,min(1,max(0,body.get('progress',0))),now,state,now,now if state=='SUCCEEDED' else None,body['id'],body['node_id'],db.dumps(result) if result else None,id))
            c.execute('UPDATE segments SET status=?,result=? WHERE id=?',(state,db.dumps(result) if result else None,s['id']))
            db.event(a['task_id'],'execution',{'stage':'execute','state':state,'node_id':body['node_id'],'segment_id':s['id'],'progress':body.get('progress',0),'seq':seq},id,c)
        reconcile_observation(id)
        return {'accepted':True}
    async def reconcile(self):
        if self.containers:await self.containers.reconcile()
        attempts=db.query("SELECT * FROM attempts WHERE backend='process' AND status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')")
        for a in attempts:
            task=db.query('SELECT status,params FROM tasks WHERE id=?',(a['task_id'],),one=True)
            node=db.query('SELECT endpoint FROM nodes WHERE id=?',(a['node_id'],),one=True)
            try:
                r=await self.client.get(node['endpoint']+'/executions/by-attempt/'+a['id'])
                if r.status_code==404:
                    self.attempt_failure(a['id'],'按幂等键核对：执行端未接收'); continue
                r.raise_for_status(); state=r.json()
                if task['status'] in TERMINAL or time.time()-a['created']>db.loads(task['params'])['attempt_timeout']:
                    await self.client.post(node['endpoint']+f"/executions/{state['id']}/cancel")
                    self.attempt_failure(a['id'],'父任务终止' if task['status'] in TERMINAL else '执行超时已请求取消','CANCELED' if task['status'] in TERMINAL else 'TIMED_OUT')
                else: await self.ingest(state)
            except Exception:
                if task['status'] in TERMINAL or time.time()-a['updated']>db.loads(task['params'])['attempt_timeout']:
                    self.attempt_failure(a['id'],'失联超时，旧执行可能仍在运行，后续结果按尝试令牌隔离','TIMED_OUT')
                elif a['status']!='UNKNOWN':
                    db.execute("UPDATE attempts SET status='UNKNOWN' WHERE id=?",(a['id'],))
                    db.event(a['task_id'],'worker_unknown',{'stage':'execute','state':'UNKNOWN','message':'执行端失联，等待核对'},a['id'])
    def aggregate(self,task):
        segments=db.query('SELECT * FROM segments WHERE task_id=? ORDER BY ordinal',(task['id'],))
        if not segments or len(segments)!=task['total'] or any(s['status']!='SUCCEEDED' or not s['result'] for s in segments): return
        counts={}; frames=[]; previews=[]; provenance=[]
        for s in segments:
            r=db.loads(s['result'])
            if r['attempt_id']!=s['active_attempt']: self.fail_task(task['id'],'聚合时有效尝试不一致'); return
            for label,n in r['counts'].items(): counts[label]=counts.get(label,0)+n
            for f in r['frames']: frames.append(f|{'video_seconds':round(s['start']+f['seconds'],4),'segment_id':s['id'],'node_id':r['actual_node']})
            previews+=r.get('preview_urls',[])
            provenance.append({'segment_id':s['id'],'attempt_id':r['attempt_id'],'node_id':r['actual_node'],'model_id':r['model_id'],'model_sha256':r['model_sha256'],'start':s['start'],'end':s['end'],**{k:r.get(k) for k in ['execution_backend','execution_mode','session_id','image','job_name','job_uid','pod_uid','kube_node']}})
        result={'counts':counts,'frames':frames,'previews':previews,'provenance':provenance,'coverage':1,'sampled_frames':len(frames),'counting_rule':'采样帧目标出现次数，非唯一对象数量','elapsed_seconds':time.time()-task['created'],'execution_mode':'真实推理；节点类型见provenance及资源页'}
        folder=DATA/'tasks'/task['id']; (folder/'result.json').write_text(db.dumps(result))
        with db.connect() as c:
            changed=c.execute("UPDATE tasks SET status='SUCCEEDED',result=?,completed=total,finished=?,updated=? WHERE id=? AND status='RUNNING'",(db.dumps(result),time.time(),time.time(),task['id'])).rowcount
            if changed: db.event(task['id'],'stage',{'stage':'aggregate','state':'SUCCEEDED','sampled_frames':len(frames),'coverage':1},conn=c)
    async def schedules(self):
        from .schemas import TaskInput
        now=time.time()
        for s in db.query("SELECT * FROM strategies WHERE enabled=1 AND kind='periodic' AND next_run<=?",(now,)):
            key=f"schedule:{s['id']}:{s['next_run']}"
            try: self.create(TaskInput(**db.loads(s['params'])),'periodic',s['id'],key)
            except Exception as e: log.exception('Schedule failed: %s',e)
            db.execute('UPDATE strategies SET last_run=?,next_run=? WHERE id=?',(now,now+s['interval_seconds'],s['id']))
    async def maintain(self,force=False):
        """Slow control work: reservation release, K3s/pool refresh, reconciliation, periodic triggers."""
        began=time.monotonic(); phases={}
        if self.resource_provider.enabled:
            for a in db.query("SELECT id,decision FROM attempts WHERE status IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT') AND (backend='process' OR cleanup_state='DONE') AND json_extract(decision,'$.reservation_id') IS NOT NULL AND json_extract(decision,'$.reservation_released') IS NULL"):
                decision=db.loads(a['decision'])
                try:
                    await self.resource_provider.release(decision['reservation_id'])
                    decision['reservation_released']=True
                    db.execute('UPDATE attempts SET decision=? WHERE id=?',(db.dumps(decision),a['id']))
                except Exception: pass
        if force or time.monotonic()-self.last_refresh>=2:
            t=time.monotonic()
            try:await self.refresh()
            except Exception:log.exception('Resource refresh unavailable')
            phases['refresh']=round((time.monotonic()-t)*1000,1); t=time.monotonic()
            await self.reconcile(); phases['reconcile']=round((time.monotonic()-t)*1000,1)
            await self.schedules(); self.last_refresh=time.monotonic()
        total=(time.monotonic()-began)*1000
        if total>1500:log.warning('Slow coordinator maintenance %.0f ms: %s',total,phases)
    async def dispatch_pass(self):
        """Fast path: progress task state and assign PENDING segments to eligible nodes."""
        began=time.monotonic(); phases={}
        for id,t in list(self.preparing.items()):
            if t.done(): self.preparing.pop(id,None)
        tasks=db.query("SELECT * FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT') ORDER BY created")
        tasks.sort(key=lambda t:(-db.loads(t['params'])['priority'],t['created']))
        for task in tasks:
            params=db.loads(task['params'])
            if time.time()-task['created']>params['deadline_seconds']:
                self.fail_task(task['id'],'任务截止时限已到','TIMED_OUT'); continue
            if task['status'] in ['QUEUED','PREPARING']:
                if task['id'] not in self.preparing and len(self.preparing)<2: self.preparing[task['id']]=asyncio.create_task(self.prepare(task['id']))
                continue
            if db.query("SELECT id FROM segments WHERE task_id=? AND status='FAILED'",(task['id'],),one=True):
                self.fail_task(task['id'],'必要片段达到重试上限，聚合失败'); continue
            count=db.query("SELECT count(*) n FROM segments WHERE task_id=? AND status='SUCCEEDED'",(task['id'],),one=True)['n']
            if count!=task['completed']:db.execute('UPDATE tasks SET completed=? WHERE id=?',(count,task['id']))
            if count==task['total']: self.aggregate(task); continue
            batch_started=time.monotonic()
            if self.containers and db.loads(task['metadata'],{}).get('application',{}).get('execution_mode')=='task_session':
                # Ready-but-idle containers take segments still parked on cold sessions (no retry counted).
                self.containers.sessions.rebalance(task['id'])
            pending=db.query("SELECT * FROM segments WHERE task_id=? AND status='PENDING' ORDER BY ordinal",(task['id'],))
            if not pending:continue
            before=db.query('SELECT count(*) n FROM attempts WHERE task_id=?',(task['id'],),one=True)['n']
            for s in pending:
                if await self.dispatch(task,s) is False:break
            after=db.query('SELECT count(*) n FROM attempts WHERE task_id=?',(task['id'],),one=True)['n']
            phases['task:'+task['id'][-6:]]=round((time.monotonic()-batch_started)*1000,1)
            if after>before:
                db.event(task['id'],'dispatch_batch',{'stage':'schedule','state':'SUCCEEDED','assigned':after-before,'pending_at_start':len(pending),'duration_ms':(time.monotonic()-batch_started)*1000,'boundary':'本轮待分片读取、候选查询和执行后端提交确认；不含周期资源刷新与核对（独立循环）','complete_global_assignment':after-before==len(pending)})
        total=(time.monotonic()-began)*1000
        if total>1000:log.warning('Slow dispatch pass %.0f ms: %s',total,phases)
    async def tick(self):
        # Single-step form kept for tests/tools; the service runs both halves in separate loops.
        await self.maintain(); await self.dispatch_pass()
    async def _repeat(self,step,lock,interval,name):
        while self.running:
            try:
                async with lock: await step()
            except asyncio.CancelledError: raise
            except Exception: log.exception('Coordinator %s failed',name)
            await asyncio.sleep(interval)
    async def loop(self):
        # Before 2026-11-10 one 0.4 s loop ran refresh/reconcile (0.6-4 s on the shared disk and
        # ~15 sequential K3s calls) ahead of dispatch, so an idle container waited 1-3 s for its next
        # segment. Dispatch now has its own short loop; every state change is still guarded by
        # BEGIN IMMEDIATE re-checks, as already required for the separate gateway process.
        self.maintenance_lock=asyncio.Lock()
        await asyncio.gather(self._repeat(self.maintain,self.maintenance_lock,.5,'maintenance'),
                             self._repeat(self.dispatch_pass,self.lock,.15,'dispatch'))
