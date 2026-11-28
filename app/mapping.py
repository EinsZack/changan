"""Second application template. Shares task/attempt ownership and K3s lifecycle with video.
The legacy segments table is now typed work-unit storage; mapping has no time range.
No user-selected images, arbitrary commands or generic workloads are exposed in v1.
"""
import asyncio, hashlib, json, math, os, re, secrets, time, zipfile
from pathlib import Path
from fastapi import APIRouter, File, UploadFile, Request, HTTPException
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from . import db
from .config import DATA, TERMINAL
from workers.photo_bundle import photos, MAX_ZIP

ID='aerial-mapping-odx'
ARTIFACTS={'orthophoto.tif','preview.jpg','report.pdf','engine.json','shots.geojson','dsm.tif','cloud.laz'}
MIME={'tif':'image/tiff','jpg':'image/jpeg','pdf':'application/pdf','json':'application/json','geojson':'application/geo+json','laz':'application/octet-stream'}
router=APIRouter()
class MappingInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    workflow_id: Literal['aerial-mapping-odx']
    name: str=Field(default='航拍照片建图',min_length=1,max_length=120)
    source: str=Field(pattern=r'^dataset_[a-f0-9]{16}$')
    algorithm: Literal['resource_aware','round_robin']='resource_aware'
    preset: Literal['standard','preview']='standard'
    execution_mode: Literal['single','collaborative']='single'
    max_nodes: int=Field(default=2,ge=2,le=3)
    submodel_size: int=Field(default=40,ge=4,le=250)
    split_overlap_m: float=Field(default=35,ge=1,le=500,allow_inf_nan=False)
    min_registered_ratio: float=Field(default=0.90,ge=0.5,le=1,allow_inf_nan=False)
    concurrency: int=Field(default=4,ge=1,le=8)
    resolution_cm: float=Field(default=10,ge=2,le=100,allow_inf_nan=False)
    min_memory_mb: int=Field(default=16384,ge=8192,le=49152)
    workspace_gb: int=Field(default=32,ge=16,le=64)
    deadline_seconds: int=Field(default=14400,ge=300,le=86400)
    attempt_timeout: int=Field(default=10800,ge=300,le=86400)
    max_retries: int=Field(default=0,ge=0,le=2)
    pipeline_version: int|None=Field(default=None,ge=1,le=2,description='协同DAG版本；缺省取CHANGAN_MAPPING_PIPELINE_VERSION')
    priority: int=Field(default=0,ge=0,le=10)
    node_ids: list[str]=Field(default_factory=list,max_length=64)
    architecture: Literal['arm64']='arm64'
    required_tags: list[str]=Field(default_factory=list,max_length=20)
    pool_cluster_id: str|None=Field(default=None,max_length=100)
    pool_labels: list[str]=Field(default_factory=list,max_length=20)


def sha(file):
    with Path(file).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def application():
    image=os.getenv('CHANGAN_MAPPING_IMAGE','')
    if not re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}',image):raise ValueError('建图镜像尚未配置为固定摘要')
    return {'id':ID,'version':'1','protocol_version':1,'execution_mode':'batch_job','image':image,'architecture':'arm64','engine':'ODX','engine_version':'3.8.6','gpu_access':False,'container_memory_floor_mb':8192,'image_pull_secret':os.getenv('CHANGAN_IMAGE_PULL_SECRET')}

def dataset(source):
    row=db.query('SELECT * FROM datasets WHERE id=?',(source,),one=True)
    if not row:raise ValueError('照片数据集不存在')
    row['manifest']=db.loads(row['manifest']);return row

@router.get('/api/applications')
def applications():
    try: enabled=bool(application()) and os.getenv('CHANGAN_BACKEND','k3s')=='k3s'
    except ValueError: enabled=False
    return [{'id':'video-analysis','name':'视频分析','enabled':True,'input_type':'video','execution_mode':'task_session'},
            {'id':ID,'name':'航拍建图','enabled':enabled,'input_type':'photo_bundle','execution_mode':'batch_job'}]

@router.get('/api/datasets')
def datasets():
    return [{k:v for k,v in r.items() if k!='path'} for r in db.query('SELECT id,name,bytes,sha256,image_count,created FROM datasets ORDER BY created DESC LIMIT 100')]

def inspect_bundle(path):
    manifest=[]
    with zipfile.ZipFile(path) as z:
        for i in photos(z):
            with z.open(i) as f:h=hashlib.file_digest(f,'sha256').hexdigest()
            manifest.append({'path':i.filename,'bytes':i.file_size,'sha256':h})
    return manifest

@router.post('/api/datasets')
async def upload_dataset(file:UploadFile=File(...)):
    did=db.uid('dataset');folder=DATA/'datasets';folder.mkdir(exist_ok=True);tmp=folder/(did+'.partial');path=folder/(did+'.zip');size=0;h=hashlib.sha256()
    try:
        with tmp.open('xb') as f:
            while chunk:=await file.read(1048576):
                size+=len(chunk)
                if size>MAX_ZIP:raise HTTPException(413,'照片ZIP上限2GiB')
                h.update(chunk);f.write(chunk)
        manifest=await asyncio.to_thread(inspect_bundle,tmp);tmp.replace(path)
        db.execute('INSERT INTO datasets(id,name,path,bytes,sha256,image_count,manifest,created) VALUES(?,?,?,?,?,?,?,?)',(did,Path(file.filename or 'photos.zip').name[:120],str(path),size,h.hexdigest(),len(manifest),db.dumps(manifest),time.time()))
        return {'id':did,'name':file.filename,'bytes':size,'sha256':h.hexdigest(),'image_count':len(manifest),'files':manifest,'note':'只纳入images/或park22/中的原照片；根目录示例成果不进入计算'}
    except HTTPException:raise
    except (ValueError,zipfile.BadZipFile,RuntimeError,EOFError) as e:raise HTTPException(422,str(e)) from e
    finally:
        tmp.unlink(missing_ok=True)
        if not db.query('SELECT id FROM datasets WHERE id=?',(did,),one=True):path.unlink(missing_ok=True)


def create(engine,body,key=None,trigger='service',strategy=None):
    if engine.backend!='k3s':raise ValueError('建图模板仅支持K3s容器执行')
    d=dataset(body.source);params=body.model_dump();app=application();now=time.time();tid=db.uid('task')
    if body.execution_mode=='collaborative':
        version=body.pipeline_version or pipeline_version()
        # v1 and v2 stay separately pinned so both generations can be compared in one deployment.
        image=os.getenv('CHANGAN_MAPPING_COLLAB_V2_IMAGE' if version>=2 else 'CHANGAN_MAPPING_COLLAB_IMAGE','')
        if not re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}',image):raise ValueError('协同建图镜像尚未配置为固定摘要'+('（v2需CHANGAN_MAPPING_COLLAB_V2_IMAGE）' if version>=2 else ''))
        app=dict(app,image=image,version='2',pipeline_version=version,artifact_cache=artifact_cache() if version>=2 else None)
        if math.ceil(d['image_count']/body.submodel_size)>8:raise ValueError('本版最多8个子模型，请增加每组目标照片数')
        if body.submodel_size>=d['image_count']:raise ValueError('每个子模型的目标照片数必须小于输入照片数')
        if body.node_ids and len(set(body.node_ids))<2:raise ValueError('协同模式至少需要两个候选节点')
    if body.attempt_timeout>body.deadline_seconds:raise ValueError('单次超时不能超过任务截止时限')
    alg=db.query("SELECT body FROM configs WHERE category='algorithm' AND id=?",(body.algorithm,),one=True)
    if not alg:raise ValueError('调度算法不存在')
    workflow=db.query("SELECT body FROM configs WHERE category='workflow' AND id=?",(ID,),one=True)
    if not workflow or not db.loads(workflow['body']).get('enabled',True):raise ValueError('建图工作流不存在或已禁用')
    meta={'application':app,'input':{k:d[k] for k in ['id','name','bytes','sha256','image_count']},'algorithm_snapshot':db.loads(alg['body']),'workflow_snapshot':db.loads(workflow['body']),'execution_backend':'k3s'}
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE')
        if key:
            old=c.execute('SELECT id,params FROM tasks WHERE idempotency_key=?',(key,)).fetchone()
            if old:
                if MappingInput(**db.loads(old['params'])).model_dump()!=params:raise ValueError('幂等键对应参数不一致')
                return old['id']
        c.execute('INSERT INTO tasks(id,name,source,status,created,updated,params,metadata,total,idempotency_key,started,trigger_type,strategy_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(tid,body.name,body.source,'RUNNING',now,now,db.dumps(params),db.dumps(meta),1,key,now,trigger,strategy))
        if body.execution_mode=='collaborative':
            from .mapping_pipeline import add
            add(c,tid,0,d['path'],'prepare')
        else:c.execute('INSERT INTO segments(id,task_id,ordinal,start,end,path,kind) VALUES(?,?,0,0,0,?,?)',(db.uid('unit'),tid,d['path'],'photo_reconstruction'))
        db.event(tid,'created',{'stage':'validate','state':'SUCCEEDED','application_id':ID,'image_count':d['image_count'],'trigger_type':trigger,'strategy_id':strategy},conn=c)
    return tid


def pipeline_version():
    """Collaborative DAG generation. v2 needs a collaborative image whose worker fuses
    alignment into toolchain units; enable only after publishing that image digest."""
    raw=os.getenv('CHANGAN_MAPPING_PIPELINE_VERSION','1').strip()
    if raw not in ('1','2'):raise ValueError('CHANGAN_MAPPING_PIPELINE_VERSION 仅支持1或2')
    return int(raw)

CACHE_MOUNT='/cache/mapping'

def artifact_cache():
    """Optional node-local stage-archive cache, frozen into the task snapshot.
    The directory must already exist on every Thor (UID/GID 10001, 0700), like the CUDA cache."""
    path=os.getenv('CHANGAN_MAPPING_CACHE_HOSTPATH','').strip()
    if not path:return None
    if not path.startswith('/') or '..' in Path(path).parts:raise ValueError('CHANGAN_MAPPING_CACHE_HOSTPATH 必须为节点上的绝对路径')
    size=int(os.getenv('CHANGAN_MAPPING_CACHE_MAX_GB','20'))
    if not 1<=size<=200:raise ValueError('CHANGAN_MAPPING_CACHE_MAX_GB 须在1～200之间')
    return {'host_path':path,'max_bytes':size*1024**3}

def candidate_params(params):
    return dict(params,application_id=ID,min_cpu_cores=params['concurrency'],min_gpu_count=0)

def manifest(a,w,params):
    from .kube import job_manifest
    m=job_manifest(a,w,params);spec=m['spec']['template']['spec'];container=spec['containers'][0];container['name']='mapping'
    for meta in (m['metadata'],m['spec']['template']['metadata']):
        meta['labels'].update({'app':'changan-mapping','changan.io/application':ID})
    spec['terminationGracePeriodSeconds']=20
    container['resources']={'requests':{'cpu':str(params['concurrency']),'memory':str(params['min_memory_mb'])+'Mi','ephemeral-storage':str(params['workspace_gb'])+'Gi'},'limits':{'cpu':str(params['concurrency']),'memory':str(params['min_memory_mb'])+'Mi','ephemeral-storage':str(params['workspace_gb'])+'Gi'}}
    spec['volumes'][1]['emptyDir']['sizeLimit']=str(params['workspace_gb']-2)+'Gi';spec['volumes'][2]['emptyDir']['sizeLimit']='2Gi'
    container['env'] += [{'name':'HOME','value':'/work'},{'name':'TMPDIR','value':'/tmp'},{'name':'PYTHONDONTWRITEBYTECODE','value':'1'}]
    cache=w.get('artifact_cache')
    if cache and w.get('phase'):
        spec['volumes'].append({'name':'mapping-cache','hostPath':{'path':cache['host_path'],'type':'Directory'}})
        container['volumeMounts'].append({'name':'mapping-cache','mountPath':CACHE_MOUNT})
        container['env']+=[{'name':'CHANGAN_MAPPING_CACHE','value':CACHE_MOUNT},{'name':'CHANGAN_MAPPING_CACHE_MAX_BYTES','value':str(int(cache['max_bytes']))}]
    return m

async def submit(controller,task,unit,aid,payload):
    from .container_execution import save,stage
    a=db.query('SELECT * FROM attempts WHERE id=?',(aid,),one=True);app=db.loads(task['metadata'])['application'];snapshot=db.loads(db.query('SELECT snapshot FROM nodes WHERE id=?',(a['node_id'],),one=True)['snapshot']);params=db.loads(task['params']);base=os.getenv('CHANGAN_CONTAINER_BASE',controller.engine.base)+'/internal/container/'+aid
    w={**app,'job_name':'mapping-'+aid.replace('_','-'),'cluster':controller.client().cluster,'namespace':controller.client().namespace,'kube_node':snapshot['kube_node'],'target_node':a['node_id'],'input_sha256':payload['input_sha256'],'memory_mb':params['min_memory_mb'],'cpu_cores':params['concurrency'],'created_at':time.time()}
    if params.get('execution_mode')=='collaborative':
        from .mapping_pipeline import get,inputs,input_hash
        spec=get(unit['id']);entries=inputs(unit)
        if entries:payload['input_sha256']=input_hash(entries)
        w.update(phase=spec['phase'],submodel=spec['submodel'],inputs=entries,input_sha256=payload['input_sha256'])
        payload.update(phase=spec['phase'],submodel=spec['submodel'],inputs=entries,pipeline_version=app.get('pipeline_version',1))
    db.execute("UPDATE attempts SET workload=?,cleanup_state='PENDING',backend='k3s' WHERE id=?",(db.dumps(w),aid));payload.update(base_url=base,input_url=base+'/input',callback_url=base+'/events',params=params)
    job=await controller.client().submit(manifest(a,w,params),payload);save(a,{'job_uid':job.metadata.uid});stage(a,'SUBMITTED','建图Job已提交，等待目标Thor执行')
    db.event(a['task_id'],'job_submitted',{'stage':'execute','state':'SUBMITTED','job_name':w['job_name'],'job_uid':job.metadata.uid,'image':w['image'],'application_id':ID},aid)


def required(preset):
    return {'orthophoto.tif','preview.jpg','report.pdf','engine.json','shots.geojson'} | ({'dsm.tif','cloud.laz'} if preset=='standard' else set())

def validate_result(a,w,r,*,verify_content=False):
    if w.get('phase'):
        from .mapping_pipeline import validate_result as validate_stage
        return validate_stage(a,w,r,verify_content)
    if not isinstance(r,dict):raise ValueError('缺少结果manifest')
    expected={'application_id':ID,'engine_version':'3.8.6','attempt_id':a['id'],'task_id':a['task_id'],'work_unit_id':a['segment_id'],'actual_node':a['node_id'],'pod_uid':w['pod_uid'],'input_sha256':w['input_sha256'],'device':'cpu'}
    if any(r.get(k)!=v for k,v in expected.items()):raise ValueError('建图结果来源不一致')
    task=db.query('SELECT params,metadata FROM tasks WHERE id=?',(a['task_id'],),one=True);params=db.loads(task['params']);meta=db.loads(task['metadata']);q=r.get('quality',{})
    if not isinstance(q,dict):raise ValueError('无效质量摘要')
    if q.get('images')!=meta['input']['image_count'] or not isinstance(q.get('registered'),int) or not 0<q['registered']<=q['images'] or not q.get('crs'):raise ValueError('重建覆盖或坐标信息无效')
    if not all(type(q.get(k)) is int and q[k]>0 for k in ('width','height')):raise ValueError('无效栅格尺寸')
    def vector(v,n):return isinstance(v,list) and len(v)==n and all(type(i) in (int,float) and math.isfinite(i) for i in v)
    if not vector(q.get('pixel_size'),2) or min(q['pixel_size'])<=0 or not isinstance(q.get('corners'),list) or len(q['corners'])!=4 or not all(vector(v,2) for v in q['corners']):raise ValueError('无效空间范围')
    if params['preset']=='standard' and (type(q.get('points')) is not int or q['points']<=0):raise ValueError('缺少点云计数')
    items=r.get('artifacts')
    if not isinstance(items,list) or not all(isinstance(m,dict) and isinstance(m.get('name'),str) and type(m.get('bytes')) is int and m['bytes']>0 and re.fullmatch('[0-9a-f]{64}',str(m.get('sha256',''))) for m in items):raise ValueError('产物清单格式无效')
    names=[m['name'] for m in items]
    if len(set(names))!=len(names) or set(names)!=required(params['preset']):raise ValueError('必需建图产物不完整')
    for m in items:
        file=DATA/'tasks'/a['task_id']/a['id']/m['name'];stored=db.query('SELECT bytes,sha256 FROM artifacts WHERE attempt_id=? AND name=?',(a['id'],m['name']),one=True)
        if not stored or stored['bytes']!=m.get('bytes') or stored['sha256']!=m.get('sha256') or not file.is_file() or file.stat().st_size!=stored['bytes']:raise ValueError('产物未持久化或摘要不一致')
        if verify_content and sha(file)!=stored['sha256']:raise ValueError('落盘产物内容摘要不符')
    for m in items:m['url']='/api/tasks/'+a['task_id']+'/artifacts/'+a['id']+'/'+m['name']
    return r|{'image':w['image'],'job_uid':w['job_uid'],'pod_uid':w['pod_uid'],'kube_node':w['node_name'],'execution_backend':'k3s','execution_mode':'batch_job'}

async def event(a,w,request):
    from .container_execution import authenticate,stage
    b=await request.json();seq=b.get('seq');state=b.get('status')
    if not isinstance(seq,int) or seq<1 or state not in ('RUNNING','UPLOADING','FINALIZING','FAILED'):raise HTTPException(422,'事件格式无效')
    if seq<=a['last_seq']:return {'accepted':True,'duplicate':True}
    if {'RUNNING':1,'UPLOADING':2,'FINALIZING':3}.get(state,4)<{'RUNNING':1,'UPLOADING':2,'FINALIZING':3}.get(a['status'],0):raise HTTPException(409,'阶段不可回退')
    result=None
    if state=='FINALIZING':
        try:result=await asyncio.to_thread(validate_result,a,w,b.get('result'))
        except ValueError as e:raise HTTPException(422,str(e))
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE');authenticate(a['id'],request);cur=c.execute('SELECT workload,last_seq FROM attempts WHERE id=?',(a['id'],)).fetchone()
        if seq<=cur['last_seq']:return {'accepted':True,'duplicate':True}
        currentw=db.loads(cur['workload']);currentw['application_phase']='FINALIZING' if state=='FAILED' else state
        if state=='FAILED':currentw['failure']=str(b.get('error','建图失败'))[:1000]
        c.execute('UPDATE attempts SET workload=?,last_seq=?,updated=?,started=COALESCE(started,?),result=COALESCE(?,result) WHERE id=?',(db.dumps(currentw),seq,time.time(),time.time(),db.dumps(result) if result else None,a['id']))
        db.event(a['task_id'],'mapping_stage',{'stage':str(b.get('phase','complete'))[:60],'state':'SUCCEEDED' if b.get('transition')=='Finished' else state,'message':b.get('error'),'seq':seq},a['id'],c)
    stage(a,'FINALIZING' if state=='FAILED' else state);return {'accepted':True}

def complete(a,body):
    """Only the coordinator calls this after checking Job/Pod success."""
    w=db.loads(a['workload']);r=validate_result(a,w,body.get('result'),verify_content=True)
    with db.connect() as c:
        c.execute('BEGIN IMMEDIATE');s=c.execute('SELECT active_attempt FROM segments WHERE id=?',(a['segment_id'],)).fetchone();t=c.execute('SELECT status FROM tasks WHERE id=?',(a['task_id'],)).fetchone();cur=c.execute('SELECT status FROM attempts WHERE id=?',(a['id'],)).fetchone()
        if cur[0] in TERMINAL or t[0] in TERMINAL or s[0]!=a['id']:return {'accepted':False}
        now=time.time();c.execute("UPDATE attempts SET status='SUCCEEDED',finished=?,updated=?,result=?,progress=1 WHERE id=?",(now,now,db.dumps(r),a['id']));c.execute("UPDATE segments SET status='SUCCEEDED',result=? WHERE id=?",(db.dumps(r),a['segment_id']))
        if w.get('phase'):
            from .mapping_pipeline import complete as complete_stage
            complete_stage(a,r,c)
            return {'accepted':True}
        c.execute("UPDATE tasks SET status='SUCCEEDED',completed=1,finished=?,updated=?,result=? WHERE id=?",(now,now,db.dumps(r),a['task_id']));db.event(a['task_id'],'stage',{'stage':'complete','state':'SUCCEEDED','message':'K3s进程成功且必需建图成果已持久化'},a['id'],c)
    return {'accepted':True}
