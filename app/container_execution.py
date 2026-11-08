"""Durable business attempts mapped to K3s Jobs, with fenced Pod ownership."""
import asyncio, hashlib, os, time, secrets
from pathlib import Path
from fastapi import APIRouter, Request, HTTPException
from . import db
from .config import DATA, MODEL, MODEL_ID, TERMINAL
from .kube import Kube, job_manifest
from .incidents import observe_node, reconcile_observation
from .network_metrics import container_network_snapshot, overlay_pool_network
router=APIRouter()
controller=None

def workload(a):return db.loads(a.get('workload'),{})
def save(a,w):
    # Gateway and coordinator are separate processes: merge observations under one lock.
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        row=c.execute('SELECT workload FROM attempts WHERE id=?',(a['id'],)).fetchone()
        current=db.loads(row['workload'],{})
        merged=current|w
        for key in ('pod_uid','pod_name','node_name','job_uid','application_phase','failure'):
            if key in current:merged[key]=current[key]
        c.execute('UPDATE attempts SET workload=? WHERE id=?',(db.dumps(merged),a['id']))
def stage(a,state,message=None):
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        old=c.execute('SELECT status FROM attempts WHERE id=?',(a['id'],)).fetchone()
        if not old or old['status'] in TERMINAL or old['status']==state:return
        if old['status']=='CLEANING' and state!='CLEANING':return
        order={'MODEL_LOADING':1,'RUNNING':2,'UPLOADING':3,'FINALIZING':4}
        if state in ('SUBMITTED','PENDING','PULLING','CONTAINER_STARTING') and old['status'] in order:return
        if state in order and old['status'] in order and order[state]<order[old['status']]:return
        c.execute('UPDATE attempts SET status=?,updated=? WHERE id=?',(state,time.time(),a['id']))
        c.execute('UPDATE segments SET status=? WHERE active_attempt=?',(state,a['id']))
        db.event(a['task_id'],'container_stage',{'stage':'execute','state':state,'message':message or state},a['id'],c)

class ContainerExecution:
    def __init__(self,engine):
        global controller
        self.engine=engine;self.kube=None;controller=self
        from .session_execution import SessionExecution
        self.sessions=SessionExecution(self)
    def client(self):
        if self.kube is None:self.kube=Kube()
        return self.kube
    def application(self):
        image=os.getenv('CHANGAN_VIDEO_IMAGE','')
        if '@sha256:' not in image:raise ValueError('CHANGAN_VIDEO_IMAGE 必须固定为镜像摘要')
        return {'id':'video-analysis','version':'2','execution_mode':'task_session','image':image,'architecture':os.getenv('CHANGAN_IMAGE_ARCH','amd64'),
                'model_id':MODEL_ID,'model_sha256':hashlib.sha256(MODEL.read_bytes()).hexdigest(),
                'runtime':'ONNX Runtime CPU','image_pull_secret':os.getenv('CHANGAN_IMAGE_PULL_SECRET')}
    async def refresh(self):
        old={n['id']:n for n in db.query('SELECT * FROM nodes')}
        try:
            nodes=await self.client().nodes()
            app=self.application()
            pool=await self.engine.resource_provider.snapshots() if self.engine.resource_provider.enabled else None
            if len({n['id'] for n in nodes})!=len(nodes):raise ValueError('多个 Kubernetes 节点重复使用同一业务节点标签')
            seen=set()
            for n in nodes:
                id=n['id'];seen.add(id);now=time.time()
                reserved_row=db.query("SELECT count(*) n,COALESCE(sum(max(1024,json_extract(t.params,'$.min_memory_mb'))),0) memory FROM attempts a JOIN tasks t ON t.id=a.task_id WHERE a.node_id=? AND a.backend='k3s' AND a.session_id IS NULL AND a.cleanup_state!='DONE'",(id,),one=True)
                sessions=db.query("SELECT count(*) n,COALESCE(sum(json_extract(workload,'$.memory_mb')),0) memory FROM execution_sessions WHERE node_id=? AND cleanup_state!='DONE'",(id,),one=True)
                reserved_row['n']+=sessions['n'];reserved_row['memory']+=sessions['memory']
                reserved=reserved_row['n']
                s={'node_id':id,'kube_node':n['name'],'node_uid':n['uid'],'healthy':n['healthy'],
                   'models':[MODEL_ID] if n['architecture']==app['architecture'] else [],
                   'architecture':n['architecture'],'tags':['cpu','onnx','container','k3s'],
                   'available_memory_mb':max(0,n['memory']-reserved_row['memory']-max(0,n['active']-reserved)*1024),
                   'memory_budget_mb':n['memory'],'active':n['active'],'capacity':n['capacity'],
                   'runtime':n['runtime'],'kubelet':n['kubelet'],'physical_host':n['physical_host'],
                   'resource_source':'kubernetes','location':self.client().cluster}
                s.update(container_network_snapshot(n,self.client().cluster,now))
                last=db.query("SELECT result FROM attempts WHERE node_id=? AND backend='k3s' AND status='SUCCEEDED' ORDER BY finished DESC LIMIT 1",(id,),one=True)
                if last:s['measured_fps']=db.loads(last['result'],{}).get('measured_fps')
                observed=now
                if pool is not None:
                    authority=pool.get(id)
                    if not authority or authority['node_id']!=id:raise ValueError('资源池缺少节点映射')
                    overlay_pool_network(s,authority)
                    for key in ['tags','snapshot_version','location','input_reachable']:
                        if key in authority:s[key]=authority[key]
                    s['available_memory_mb']=min(s['available_memory_mb'],authority.get('available_memory_mb',0))
                    s['capacity']=min(s['capacity'],authority.get('capacity',1))
                    s['active']=max(s['active'],authority.get('active',0))
                    s['healthy']=s['healthy'] and authority.get('healthy',False)
                    s['resource_source']='external_pool+kubernetes'
                    observed=min(now,float(authority['observed_at']))
                previous=old.get(id)
                db.execute('INSERT INTO nodes(id,endpoint,cluster_id,kind,observed,snapshot) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET endpoint=excluded.endpoint,cluster_id=excluded.cluster_id,kind=excluded.kind,observed=excluded.observed,snapshot=excluded.snapshot,error=NULL',
                    (id,'k8s://'+n['name'],self.client().cluster,n['kind'],observed,db.dumps(s)))
                if previous:observe_node(previous,s['healthy'] and now-observed<8,'Kubernetes 节点未就绪或资源池快照过期')
            for id,n in old.items():
                if id not in seen:
                    db.execute('UPDATE nodes SET observed=NULL,error=? WHERE id=?',('节点不在 K3s 可执行节点集合',id))
                    observe_node(n,False,'节点不在 K3s 可执行节点集合')
        except Exception as e:
            for n in old.values():
                db.execute('UPDATE nodes SET error=? WHERE id=?',('K3s/资源池快照不可用: '+type(e).__name__,n['id']))
                observe_node(n,False,'K3s/资源池快照不可用')
            raise
    async def submit(self,task,segment,aid,payload):
        if db.loads(task['metadata'])['application'].get('execution_mode')=='task_session':
            return await self.sessions.submit(task,segment,aid,payload)
        a=db.query('SELECT * FROM attempts WHERE id=?',(aid,),one=True)
        app=db.loads(task['metadata'])['application']
        node=db.query('SELECT snapshot FROM nodes WHERE id=?',(a['node_id'],),one=True)
        snapshot=db.loads(node['snapshot'])
        base=os.getenv('CHANGAN_CONTAINER_BASE',self.engine.base)
        w={**app,'job_name':'video-'+aid.replace('_','-'),'cluster':self.client().cluster,
           'namespace':self.client().namespace,'target_node':a['node_id'],'kube_node':snapshot['kube_node'],'input_sha256':payload['input_sha256'],'created_at':time.time()}
        db.execute("UPDATE attempts SET backend='k3s',workload=?,cleanup_state='PENDING' WHERE id=?",(db.dumps(w),aid))
        payload.update({'input_url':base+'/internal/container/'+aid+'/input',
            'callback_url':base+'/internal/container/'+aid+'/events',
            'base_url':base+'/internal/container/'+aid,'model_sha256':app['model_sha256'],
            'image':app['image']})
        job=await self.client().submit(job_manifest(a,w,db.loads(task['params'])),payload)
        w=workload(db.query('SELECT * FROM attempts WHERE id=?',(aid,),one=True))
        w['job_uid']=job.metadata.uid;save(a,w)
        if not w.get('pod_uid'):stage(a,'SUBMITTED','K3s 已接收 Job，等待 Pod 分配及镜像准备')
        db.event(a['task_id'],'job_submitted',{'stage':'execute','state':'SUBMITTED','job_name':w['job_name'],'job_uid':w['job_uid'],'image':w['image']},aid)
    async def reconcile(self):
        await self.sessions.reconcile()
        # Drain old per-attempt Jobs during upgrades, without reinterpreting history.
        for a in db.query("SELECT * FROM attempts WHERE backend='k3s' AND session_id IS NULL AND cleanup_state!='DONE' AND COALESCE(json_extract(workload,'$.execution_mode'),'attempt_job')!='task_session'"):
            try:await self.reconcile_one(a)
            except Exception as e:
                if time.time()-workload(a).get('last_api_error',0)>15:
                    w=workload(db.query('SELECT * FROM attempts WHERE id=?',(a['id'],),one=True))
                    w['last_api_error']=time.time();save(a,w)
                    db.event(a['task_id'],'container_api_unavailable',{'stage':'execute','state':'UNKNOWN','message':'K3s 状态待核对，保留资源占用','error_type':type(e).__name__},a['id'])
    async def reconcile_one(self,a):
        w=workload(a);k=self.client();name=w['job_name']
        job=await k.job(name);pods=await k.pods(name)
        a=db.query('SELECT * FROM attempts WHERE id=?',(a['id'],),one=True);w=workload(a)
        t=db.query('SELECT * FROM tasks WHERE id=?',(a['task_id'],),one=True)
        if job and not w.get('job_uid'):
            if job.metadata.labels.get('changan.io/attempt')!=a['id']:raise ValueError('Job身份冲突')
            w['job_uid']=job.metadata.uid;save(a,w)
        if job and job.metadata.uid!=w.get('job_uid'):raise ValueError('Job UID changed; refusing cleanup')
        live=[p for p in pods if p.status.phase not in ('Succeeded','Failed')]
        timedout=time.time()-a['created']>db.loads(t['params'])['attempt_timeout']
        if t['status'] in TERMINAL or a['status'] in TERMINAL or timedout or w.get('cleanup_requested'):
            if a['status'] not in TERMINAL:
                stage(a,'CLEANING','等待旧容器终止和 Job 清理后释放资源')
            w['cleanup_requested']=True
            w.setdefault('terminal_after_cleanup','CANCELED' if t['status'] in TERMINAL else 'TIMED_OUT')
            save(a,w)
            if job:await k.delete(name)
            if job or pods:return
            if w.get('pod_uid'):
                node=db.query('SELECT observed,snapshot,error FROM nodes WHERE id=?',(a['node_id'],),one=True)
                if not node or node['error'] or time.time()-(node['observed'] or 0)>8 or not db.loads(node['snapshot'],{}).get('healthy'):
                    # A vanished API object alone does not prove a disconnected machine stopped.
                    return
            await k.delete_secret(name)
            if a['status'] not in TERMINAL:self.engine.attempt_failure(a['id'],w.get('failure') or '旧容器已清理',w['terminal_after_cleanup'])
            db.execute("UPDATE attempts SET cleanup_state='DONE' WHERE id=?",(a['id'],))
            db.event(a['task_id'],'container_cleaned',{'stage':'execute','state':'CLEANED','job_name':name},a['id'])
            return
        if not job:
            # submit response can be lost; absence is definitive only after querying job and pods.
            if not pods:
                if not w.get('job_uid') and time.time()-a['created']<30:return
                await k.delete_secret(name)
                self.engine.attempt_failure(a['id'],'K3s Job 不存在，已核实无旧 Pod')
                db.execute("UPDATE attempts SET cleanup_state='DONE' WHERE id=?",(a['id'],))
            return
        details=[]
        for p in pods:
            status=p.status;cs=(status.container_statuses or [])
            reasons=[]
            for c in cs:
                if c.state.waiting:reasons.append(c.state.waiting.reason)
                if c.state.terminated:reasons.append(c.state.terminated.reason)
            details.append({'name':p.metadata.name,'uid':p.metadata.uid,'node_name':p.spec.node_name,
                'phase':status.phase,'reasons':reasons,'image_id':cs[0].image_id if cs else None,
                'started_at':status.start_time.timestamp() if status.start_time else None})
        latest=db.query('SELECT * FROM attempts WHERE id=?',(a['id'],),one=True);w=workload(latest)
        if w.get('pods')!=details:
            w['pods']=details;save(a,w)
            db.event(a['task_id'],'pod_observed',{'stage':'execute','state':latest['status'],'pods':details},a['id'])
        # An exited process is necessary, but a validated durable result is also required.
        if not live and (pods or any(c.type=='Failed' and c.status=='True' for c in job.status.conditions or [])):
            selected=next((p for p in pods if p.metadata.uid==w.get('pod_uid')),None)
            if selected and selected.status.phase=='Succeeded' and latest.get('result'):
                body={'attempt_id':a['id'],'task_id':a['task_id'],'segment_id':a['segment_id'],'node_id':a['node_id'],
                      'token':a['token'],'id':w['pod_uid'],'seq':latest['last_seq']+1,'status':'SUCCEEDED','progress':1,
                      'result':db.loads(latest['result'])}
                await self.engine.ingest(body)
            else:
                logs={}
                for p in pods:
                    try:logs[p.metadata.name]=(await k.logs(p.metadata.name)).replace(a['token'],'[REDACTED]')
                    except Exception:pass
                if logs:w['failure_logs']=logs
                w['failure']=w.get('failure') or '容器终止且缺少有效结果: '+','.join(r for p in details for r in p['reasons'])
                w['terminal_after_cleanup']='FAILED'
            # Cleanup first, then permit business retry / resource release.
            w['cleanup_requested']=True;save(a,w)
            await k.delete(name)
            return
        if latest['status'] in ('DISPATCHING','UNKNOWN','SUBMITTED','PENDING','PULLING','CONTAINER_STARTING'):
            waiting=any(p['reasons'] for p in details)
            state='PULLING' if waiting else 'PENDING' if details else 'SUBMITTED'
            if any(p['phase']=='Running' for p in details):state=w.get('application_phase','CONTAINER_STARTING')
            stage(a,state,','.join(r for p in details for r in p['reasons']) or '等待 Pod 运行')

def authenticate(aid,request,require_claim=True):
    a=db.query('SELECT * FROM attempts WHERE id=?',(aid,),one=True)
    if not a or a['backend']!='k3s' or not secrets.compare_digest(request.headers.get('x-attempt-token',''),a['token']):raise HTTPException(401,'尝试认证失败')
    if a.get('session_id'):
        from .session_execution import authenticate_attempt
        return authenticate_attempt(a,request)
    w=workload(a);s=db.query('SELECT active_attempt FROM segments WHERE id=?',(a['segment_id'],),one=True)
    t=db.query('SELECT status FROM tasks WHERE id=?',(a['task_id'],),one=True)
    if a['status'] in TERMINAL or t['status'] in TERMINAL or s['active_attempt']!=aid or w.get('cleanup_requested') or a['status']=='CLEANING':raise HTTPException(409,'执行尝试已终止')
    if require_claim and (not w.get('pod_uid') or request.headers.get('x-pod-uid')!=w['pod_uid']):raise HTTPException(409,'Pod 未获得本次执行权')
    return a,w

@router.post('/internal/container/{aid}/claim')
async def claim(aid:str,request:Request):
    current=db.query('SELECT session_id FROM attempts WHERE id=?',(aid,),one=True)
    if current and current.get('session_id'):raise HTTPException(409,'任务会话通过领取接口确认分片')
    a,w=authenticate(aid,request,False)
    if not w.get('job_uid'):
        job=await controller.client().job(w['job_name'])
        if not job or job.metadata.labels.get('changan.io/attempt')!=aid:raise HTTPException(409,'Job 尚未确认')
        w['job_uid']=job.metadata.uid
    expected_job_uid=w['job_uid']
    pods=await controller.client().pods(w['job_name'])
    uid=request.headers.get('x-pod-uid')
    p=next((p for p in pods if p.metadata.uid==uid),None)
    n=db.query('SELECT snapshot FROM nodes WHERE id=?',(a['node_id'],),one=True)
    if not p or p.metadata.deletion_timestamp or p.spec.node_name!=db.loads(n['snapshot']).get('kube_node') or not any(o.uid==w.get('job_uid') for o in p.metadata.owner_references or []):raise HTTPException(409,'Pod、Job 或节点身份未匹配')
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        current=c.execute('SELECT * FROM attempts WHERE id=?',(aid,)).fetchone();w=db.loads(current['workload'],{})
        parent=c.execute('SELECT status FROM tasks WHERE id=?',(a['task_id'],)).fetchone()[0]
        if current['status'] in TERMINAL or parent in TERMINAL or w.get('cleanup_requested'):raise HTTPException(409,'执行尝试已终止')
        if w.get('pod_uid') and w['pod_uid']!=uid:raise HTTPException(409,'其他 Pod 已持有执行权')
        if w.get('job_uid') and w['job_uid']!=expected_job_uid:raise HTTPException(409,'Job 身份发生改变')
        w['job_uid']=expected_job_uid
        first=not w.get('pod_uid')
        w.update({'pod_uid':uid,'pod_name':p.metadata.name,'node_name':p.spec.node_name})
        c.execute('UPDATE attempts SET workload=?,execution_id=?,actual_node=? WHERE id=?',(db.dumps(w),uid,a['node_id'],aid))
        if first:db.event(a['task_id'],'dispatch_ack',{'stage':'execute','state':'ACCEPTED','identity_matched':True,'task_id':a['task_id'],'segment_id':a['segment_id'],'attempt_id':aid,'actual_node':a['node_id'],'execution_id':uid,'job_name':w['job_name'],'pod_name':p.metadata.name,'kube_node':p.spec.node_name},aid,c)
    reconcile_observation(aid)
    return {'accepted':True}

@router.get('/internal/container/{aid}/input')
async def container_input(aid:str,request:Request):
    from fastapi.responses import FileResponse
    a,_=authenticate(aid,request)
    s=db.query('SELECT path FROM segments WHERE id=?',(a['segment_id'],),one=True)
    return FileResponse(s['path'])

@router.put('/internal/container/{aid}/artifacts/{name}')
async def upload_artifact(aid:str,name:str,request:Request):
    a,w=authenticate(aid,request)
    if a['status']=='FINALIZING':raise HTTPException(409,'结果已封存')
    if name not in ('preview-0.jpg','preview-1.jpg','preview-2.jpg'):raise HTTPException(422,'产物名称不允许')
    folder=DATA/'tasks'/a['task_id']/aid;folder.mkdir(parents=True,exist_ok=True)
    tmp=folder/(name+'.'+secrets.token_hex(6)+'.tmp');size=0;digest=hashlib.sha256()
    try:
        with tmp.open('wb') as f:
            async for chunk in request.stream():
                size+=len(chunk)
                if size>8*1024**2:raise HTTPException(413,'产物超过8MiB')
                digest.update(chunk);f.write(chunk)
            f.flush();os.fsync(f.fileno())
        if not size or digest.hexdigest()!=request.headers.get('x-content-sha256'):raise HTTPException(422,'产物摘要不一致')
        # Recheck ownership after streaming, before publishing any artifact.
        authenticate(aid,request)
        # Immutable artifacts make a validated final result stable even if an upload
        # races the result transaction. Repeated identical uploads are idempotent.
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            current=c.execute('SELECT status FROM attempts WHERE id=?',(aid,)).fetchone()
            if current['status'] in TERMINAL or current['status']=='FINALIZING':raise HTTPException(409,'结果已封存')
            destination=folder/name
            if destination.exists():
                if hashlib.sha256(destination.read_bytes()).hexdigest()!=digest.hexdigest():raise HTTPException(409,'已上传产物不可更改')
            else:tmp.replace(destination)
        fd=os.open(folder,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
        return {'name':name,'sha256':digest.hexdigest(),'bytes':size}
    finally:tmp.unlink(missing_ok=True)

@router.post('/internal/container/{aid}/events')
async def container_event(aid:str,request:Request):
    current=db.query('SELECT session_id FROM attempts WHERE id=?',(aid,),one=True)
    if current and current.get('session_id'):
        from .session_execution import attempt_event
        return await attempt_event(aid,request)
    a,w=authenticate(aid,request)
    raw=await request.body()
    if len(raw)>16*1024**2:raise HTTPException(413,'结果超过16MiB')
    import json
    b=json.loads(raw);seq=b.get('seq');state=b.get('status')
    if not isinstance(seq,int) or seq<1:raise HTTPException(422,'事件序号无效')
    if seq<=a['last_seq']:return {'accepted':True,'duplicate':True}
    if state not in ('MODEL_LOADING','RUNNING','UPLOADING','FINALIZING','FAILED'):raise HTTPException(422,'状态无效')
    order={'MODEL_LOADING':1,'RUNNING':2,'UPLOADING':3,'FINALIZING':4}
    if state!='FAILED' and order.get(state,0)<order.get(a['status'],0):raise HTTPException(409,'执行阶段不可回退')
    result=b.get('result')
    if state=='FINALIZING':
        if not result or any(result.get(k)!=v for k,v in {'attempt_id':aid,'task_id':a['task_id'],'segment_id':a['segment_id'],'actual_node':a['node_id'],'model_id':w['model_id'],'model_sha256':w['model_sha256'],'pod_uid':w['pod_uid'],'input_sha256':w['input_sha256']}.items()):raise HTTPException(422,'结果来源不一致')
        if result.get('sampled_frames',0)<=0 or len(result.get('frames',[]))!=result['sampled_frames']:raise HTTPException(422,'采样结果不完整')
        names=result.get('previews',[])
        manifest=result.get('artifacts',[])
        if not names or len(set(names))!=len(names) or set(names)!={m.get('name') for m in manifest}:raise HTTPException(422,'缺少产物清单')
        urls=[]
        for m in manifest:
            if m.get('name') not in ('preview-0.jpg','preview-1.jpg','preview-2.jpg'):raise HTTPException(422,'产物名无效')
            path=DATA/'tasks'/a['task_id']/aid/m['name']
            if not path.is_file() or path.stat().st_size!=m.get('bytes') or hashlib.sha256(path.read_bytes()).hexdigest()!=m.get('sha256'):raise HTTPException(422,'产物未持久化或摘要不符')
            urls.append('/api/tasks/'+a['task_id']+'/artifacts/'+aid+'/'+m['name'])
        result.update({'preview_urls':urls,'image':w['image'],'job_uid':w['job_uid'],'job_name':w['job_name'],'kube_node':w['node_name'],'execution_backend':'k3s'})
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        current=c.execute('SELECT * FROM attempts WHERE id=?',(aid,)).fetchone()
        parent=c.execute('SELECT status FROM tasks WHERE id=?',(a['task_id'],)).fetchone()[0]
        currentw=db.loads(current['workload'],{})
        if current['status'] in TERMINAL or parent in TERMINAL or currentw.get('cleanup_requested'):raise HTTPException(409,'执行尝试已终止')
        if seq<=current['last_seq']:return {'accepted':True,'duplicate':True}
        currentw['application_phase']='FINALIZING' if state=='FAILED' else state
        if state=='FAILED':
            currentw['failure']=str(b.get('error','容器执行失败'))[:1000]
        c.execute('UPDATE attempts SET last_seq=?,progress=?,updated=?,started=COALESCE(started,?),result=COALESCE(?,result),workload=? WHERE id=?',
          (seq,max(0,min(.99,float(b.get('progress',0)))),time.time(),time.time(),db.dumps(result) if result else None,db.dumps(currentw),aid))
    stage(a,'FINALIZING' if state=='FAILED' else state,b.get('error'))
    reconcile_observation(aid)
    return {'accepted':True}
