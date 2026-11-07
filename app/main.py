import asyncio, time, json, os, secrets, csv, io, hashlib, math, hmac
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Header, UploadFile, File, Depends
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from .config import DATA, ROOT, TOKEN, API_KEY, TERMINAL
from . import db
from .engine import Engine
from .alerts import router as alerts_router
from .schemas import TaskInput,NodeInput,StrategyInput,CandidateInput
from .capabilities import run_capabilities, connector_call, redact
engine=Engine()
@asynccontextmanager
async def lifespan(app):
    db.init();engine.base=os.getenv('CHANGAN_PUBLIC_BASE','http://127.0.0.1:8310')
    loop=asyncio.create_task(engine.loop())
    yield
    loop.cancel()
    try: await loop
    except asyncio.CancelledError: pass
    await engine.close()
app=FastAPI(title='车群协同任务编排 API',version='0.5.0',lifespan=lifespan)
app.include_router(alerts_router)
from .container_execution import router as container_router
app.include_router(container_router)
from .session_execution import router as session_router
app.include_router(session_router)
def browser_session():
    return hmac.new(API_KEY.encode(),b'changan-browser-session-v1',hashlib.sha256).hexdigest()
@app.post('/api/session')
def open_session(body:dict):
    if API_KEY and not secrets.compare_digest(body.get('api_key',''),API_KEY):raise HTTPException(401,'API密钥错误')
    response=JSONResponse({'authenticated':True})
    response.set_cookie('changan_session',browser_session(),httponly=True,samesite='strict',max_age=28800)
    return response
@app.middleware('http')
async def access(request:Request,call_next):
    if API_KEY and request.url.path.startswith('/api/') and request.url.path!='/api/session':
        supplied=request.headers.get('X-API-Key','')
        if not secrets.compare_digest(supplied,API_KEY) and not secrets.compare_digest(request.cookies.get('changan_session',''),browser_session()): return JSONResponse({'detail':'需要API密钥'},401)
    if request.method not in ['GET','HEAD','OPTIONS']:
        origin=request.headers.get('origin')
        if origin and origin.split('://')[-1]!=request.headers.get('host'): return JSONResponse({'detail':'拒绝跨站修改请求'},403)
    return await call_next(request)
def internal(x_internal_token:str=Header(default='')):
    if not secrets.compare_digest(x_internal_token,TOKEN): raise HTTPException(401,'内部认证失败')
@app.get('/health')
def health(): return {'status':'ok','service':'changan-orchestrator','version':'0.5.0','time':time.time()}
@app.get('/api/overview')
def overview():
    totals=db.query('SELECT status,count(*) n FROM tasks GROUP BY status')
    daily=db.query("SELECT date(created,'unixepoch','+8 hours') day,count(*) total,sum(status='SUCCEEDED') succeeded,sum(status='FAILED') failed,sum(status='CANCELED') canceled,sum(status='TIMED_OUT') timed_out FROM tasks GROUP BY day ORDER BY day DESC LIMIT 30")
    by_workflow=db.query("SELECT json_extract(params,'$.workflow_id') workflow_id,count(*) total,sum(status='SUCCEEDED') succeeded FROM tasks GROUP BY workflow_id")
    attempts=db.query('SELECT count(*) n,AVG(finished-created) average_seconds FROM attempts WHERE finished IS NOT NULL',one=True)
    return {'totals':{r['status']:r['n'] for r in totals},'daily':daily,'by_workflow':by_workflow,'nodes':engine.nodes(),'attempts':attempts,'mode':engine.backend,'cluster_id':os.getenv('CHANGAN_CLUSTER_ID','changan-dev'),'environment_note':os.getenv('CHANGAN_ENVIRONMENT_NOTE','开发服务器逻辑节点；未完成 THOR 实机验证'),'metric_note':'业务完成率按父任务完整聚合计算；不是甲方尚待确认口径的99%调度成功率'}
@app.get('/api/nodes')
def nodes(): return engine.nodes()
@app.post('/api/scheduling/candidates')
def candidates(body:CandidateInput):
    from .scheduler import choose
    row=db.query("SELECT body FROM configs WHERE category='algorithm' AND id=?",(body.params.algorithm,),one=True)
    if not row: raise HTTPException(422,'调度算法不存在')
    result=choose(engine.nodes(),body.params.model_dump(),body.duration_seconds,body.input_bytes,db.loads(row['body']))
    result['requirements']=body.params.model_dump()
    result['resource_source']='external_pool' if engine.resource_provider.enabled else ('kubernetes' if engine.containers else 'worker_adapter')
    result['snapshot_policy_seconds']=8
    return result
@app.get('/api/node-incidents')
def node_incidents(task_id:str|None=None):
    from .incidents import list_incidents
    return list_incidents(task_id)

@app.post('/api/nodes')
def add_node(body:NodeInput):
    if engine.containers:raise HTTPException(409,'K3s 节点由集群标签注册；请使用节点接入脚本')
    from urllib.parse import urlparse
    parsed=urlparse(body.endpoint)
    if parsed.scheme not in ['http','https'] or not parsed.hostname or parsed.username: raise HTTPException(422,'执行器地址无效')
    db.execute('INSERT INTO nodes(id,endpoint,cluster_id,kind) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET endpoint=excluded.endpoint,cluster_id=excluded.cluster_id,kind=excluded.kind',(body.id,body.endpoint.rstrip('/'),body.cluster_id,body.kind))
    return {'id':body.id}
@app.post('/api/nodes/{id}/control/{action}')
async def node_control(id:str,action:str):
    n=db.query('SELECT * FROM nodes WHERE id=?',(id,),one=True)
    if not n: raise HTTPException(404)
    if action in ['enable','disable']:
        db.execute('UPDATE nodes SET enabled=? WHERE id=?',(action=='enable',id));return {'id':id,'enabled':action=='enable'}
    if n['kind']!='server_process': raise HTTPException(403,'此操作只用于服务器测试执行器')
    r=await engine.client.post(n['endpoint']+'/controls/'+action)
    if not r.is_success: raise HTTPException(r.status_code,r.text)
    return r.json()
@app.post('/api/uploads')
async def upload(file:UploadFile=File(...)):
    id=db.uid('upload');folder=DATA/'uploads';folder.mkdir(exist_ok=True);path=folder/f'{id}.mp4';size=0
    try:
        with path.open('wb') as out:
            while chunk:=await file.read(1024*1024):
                size+=len(chunk)
                if size>512*1024*1024: raise HTTPException(413,'视频大小上限512MB')
                out.write(chunk)
        from .video import metadata
        info=await asyncio.to_thread(metadata,path)
        if info['duration']>600: raise HTTPException(422,'视频时长上限10分钟')
        return {'source':id,'filename':file.filename,'metadata':info}
    except Exception:
        path.unlink(missing_ok=True);raise
@app.get('/api/sources/{source}/video')
def source_video(source:str):
    try: path=engine.source_path(source)
    except ValueError as e: raise HTTPException(404,str(e))
    return FileResponse(path,media_type='video/mp4')
@app.get('/api/tasks/{task_id}/segments/{segment_id}/video')
def segment_video(task_id:str,segment_id:str):
    row=db.query("SELECT path FROM segments WHERE id=? AND task_id=?",(segment_id,task_id),one=True)
    if not row: raise HTTPException(404,'片段不存在')
    return FileResponse(row['path'],media_type='video/mp4')

@app.post('/api/tasks')
def create_task(body:TaskInput,idempotency_key:str|None=Header(default=None)):
    try: id=engine.create(body,key=idempotency_key)
    except ValueError as e: raise HTTPException(422,str(e))
    return {'id':id,'task':db.task_detail(id)}
@app.get('/api/tasks')
def list_tasks(status:str|None=None,q:str='',workflow_id:str|None=None,node_id:str|None=None,after:float|None=None,before:float|None=None,limit:int=100):
    sql='SELECT * FROM tasks WHERE 1=1';args=[]
    if status: sql+=' AND status=?';args.append(status)
    if q: sql+=' AND (name LIKE ? OR id LIKE ?)';args.extend(['%'+q+'%']*2)
    if workflow_id:sql+=" AND json_extract(params,'$.workflow_id')=?";args.append(workflow_id)
    if node_id:sql+=' AND id IN (SELECT task_id FROM attempts WHERE node_id=?)';args.append(node_id)
    if after is not None:sql+=' AND created>=?';args.append(after)
    if before is not None:sql+=' AND created<=?';args.append(before)
    rows=db.query(sql+' ORDER BY created DESC LIMIT ?',args+[min(max(limit,1),500)])
    for t in rows:
        for k in ['params','metadata','result']: t[k]=db.loads(t[k])
        if t['result']:t['result']={k:v for k,v in t['result'].items() if k not in ['frames','previews']}
    return rows
@app.get('/api/tasks/{id}')
def task(id:str):
    result=db.task_detail(id)
    if not result: raise HTTPException(404,'任务不存在')
    return result
@app.get('/api/tasks/{id}/events')
def events(id:str,after:int=0):
    rows=db.query('SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id LIMIT 1000',(id,after))
    for r in rows:r['payload']=db.loads(r['payload'])
    return rows
@app.post('/api/tasks/{id}/cancel')
async def cancel_task(id:str):
    t=db.task_detail(id)
    if not t: raise HTTPException(404)
    if t['status'] not in TERMINAL:engine.fail_task(id,'用户取消','CANCELED')
    await engine.reconcile()
    return db.task_detail(id)
@app.post('/api/tasks/{id}/rerun')
def rerun(id:str):
    t=db.task_detail(id)
    if not t:raise HTTPException(404)
    p=t['params'];p['name']=t['name']+' · 重跑'
    return {'id':engine.create(TaskInput(**p))}
@app.get('/api/tasks/{id}/export')
def export(id:str,format:str='json'):
    t=task(id)
    if format=='csv':
        s=io.StringIO();w=csv.writer(s);w.writerow(['task_id','segment_id','attempt_id','target_node','actual_node','status','created','started','finished'])
        for a in t['attempts']:w.writerow([id,a['segment_id'],a['id'],a['node_id'],a['actual_node'],a['status'],a['created'],a['started'],a['finished']])
        return Response('\ufeff'+s.getvalue(),media_type='text/csv',headers={'Content-Disposition':f'attachment; filename="{id}.csv"'})
    t['events']=events(id);return Response(db.dumps(t),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="{id}.json"'})
@app.get('/api/tasks/{id}/video')
def task_video(id:str):
    t=task(id);return FileResponse(engine.source_path(t['source']))
@app.get('/api/tasks/{id}/artifacts/{attempt}/{name}')
def artifact(id:str,attempt:str,name:str):
    if name not in ['preview-0.jpg','preview-1.jpg','preview-2.jpg']:raise HTTPException(404)
    a=db.query('SELECT id FROM attempts WHERE id=? AND task_id=?',(attempt,id),one=True)
    if not a:raise HTTPException(404)
    path=DATA/'tasks'/id/attempt/name
    if not path.exists():raise HTTPException(404)
    return FileResponse(path)
@app.get('/internal/segments/{id}',dependencies=[Depends(internal)])
def segment(id:str):
    s=db.query('SELECT path FROM segments WHERE id=?',(id,),one=True)
    if not s:raise HTTPException(404)
    return FileResponse(s['path'])
@app.post('/internal/events',dependencies=[Depends(internal)])
async def internal_event(body:dict):return await engine.ingest(body)
@app.get('/api/strategies')
def strategies():
    rows=db.query('SELECT * FROM strategies ORDER BY created DESC')
    for r in rows:r['params']=db.loads(r['params'])
    return rows
@app.post('/api/strategies')
def add_strategy(body:StrategyInput):
    id=db.uid('strategy');now=time.time()
    db.execute('INSERT INTO strategies VALUES(?,?,?,?,?,?,?,?,?)',(id,body.name,body.kind,body.interval_seconds,body.enabled,body.params.model_dump_json(),now+body.interval_seconds if body.kind=='periodic' else None,None,now))
    return {'id':id}
@app.put('/api/strategies/{id}')
def update_strategy(id:str,body:StrategyInput):
    count=db.execute('UPDATE strategies SET name=?,kind=?,interval_seconds=?,enabled=?,params=?,next_run=? WHERE id=?',(body.name,body.kind,body.interval_seconds,body.enabled,body.params.model_dump_json(),time.time()+body.interval_seconds if body.kind=='periodic' else None,id))
    if not count:raise HTTPException(404)
    return {'id':id}
@app.delete('/api/strategies/{id}')
def delete_strategy(id:str):db.execute('DELETE FROM strategies WHERE id=?',(id,));return {'deleted':id}
@app.post('/api/strategies/{id}/trigger')
def trigger(id:str):
    s=db.query('SELECT * FROM strategies WHERE id=?',(id,),one=True)
    if not s:raise HTTPException(404)
    if not s['enabled']:raise HTTPException(409,'策略已停用')
    return {'id':engine.create(TaskInput(**db.loads(s['params'])),'service',id)}
@app.get('/api/configs/{category}')
def configs(category:str):
    rows=db.query('SELECT * FROM configs WHERE category=? ORDER BY created',(category,))
    for r in rows:r['body']=redact(db.loads(r['body']))
    return rows
@app.post('/api/configs/{category}')
def save_config(category:str,body:dict):
    if category not in ['algorithm','workflow','connector','auth','script']:raise HTTPException(422,'类别无效')
    data=body.get('body',{});id=body.get('id') or db.uid(category);name=body.get('name',id)
    if category=='algorithm':
        if data.get('method') not in ['round_robin','resource_aware']:raise HTTPException(422,'中期仅支持两个已实现的算法内核，上传JSON参数配置')
        if any(not isinstance(v,(int,float)) or not math.isfinite(v) or v<0 or v>100 for v in data.get('weights',{}).values()):raise HTTPException(422,'权重必须为0到100')
    if category=='auth' and data.get('type') not in ['none','env_bearer']:raise HTTPException(422,'中期支持无鉴权和环境令牌插件')
    if category=='connector' and data.get('method','GET') not in ['GET','POST']:raise HTTPException(422,'中期支持GET/POST')
    if category=='workflow' and id=='video-analysis' and data.get('nodes')!=['validate','split','snapshot','schedule','execute','aggregate']:raise HTTPException(422,'中期固定视频模板的节点顺序不可修改')
    if category=='script':raise HTTPException(422,'中期执行经审核的内置Python/Shell脚本，任意脚本上传后续实现')
    existing=db.query('SELECT category FROM configs WHERE id=?',(id,),one=True)
    if existing and existing['category']!=category:raise HTTPException(409,'配置ID已被其他类别使用')
    now=time.time();db.execute('INSERT INTO configs VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,body=excluded.body,updated=excluded.updated',(id,category,name,db.dumps(data),now,now))
    return {'id':id}
@app.get('/api/configs/{category}/{id}/export')
def config_export(category:str,id:str):
    row=db.query('SELECT * FROM configs WHERE id=? AND category=?',(id,category),one=True)
    if not row:raise HTTPException(404)
    row['body']=redact(db.loads(row['body']))
    return Response(db.dumps(row),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="config.json"'})
@app.delete('/api/configs/{category}/{id}')
def delete_config(category:str,id:str):
    if id in ['round_robin','resource_aware','video-analysis','capability-demo','none']:raise HTTPException(409,'内置配置不可删除')
    db.execute('DELETE FROM configs WHERE category=? AND id=?',(category,id));return {'deleted':id}
@app.post('/api/connectors/{id}/call')
async def call_connector(id:str,body:dict={}):
    c=db.query("SELECT * FROM configs WHERE id=? AND category='connector'",(id,),one=True)
    if not c:raise HTTPException(404)
    try:return await connector_call(c,body or None)
    except Exception as e:raise HTTPException(422,str(e))
@app.post('/api/capabilities/run')
async def capabilities():return await run_capabilities(engine)
@app.get('/api/capabilities')
def capability_history():
    folder=DATA/'capabilities'
    return [json.loads(p.read_text()) for p in sorted(folder.glob('*.json'),key=lambda p:p.stat().st_mtime,reverse=True)[:30]] if folder.exists() else []
@app.get('/api/models')
def models():
    from .config import MODEL,MODEL_ID
    return [{'id':MODEL_ID,'name':'YOLOX Tiny · COCO','runtime':'ONNX Runtime CPU','input_size':416,'sha256':hashlib.sha256(MODEL.read_bytes()).hexdigest() if MODEL.exists() else None,'source':'https://github.com/Megvii-BaseDetection/YOLOX','license':'Apache-2.0','labels':['person','bicycle','car','motorcycle','bus','truck']}]
@app.get('/api/delivery/{name}')
def delivery(name:str):
    allowed={'changan-orchestrator-midterm-0.1.0.tar.gz','SHA256SUMS','交付物说明.md','使用说明.md'}
    if name not in allowed:raise HTTPException(404)
    path=ROOT/'releases'/name
    if not path.is_file():raise HTTPException(404)
    return FileResponse(path,filename=name)
web_directory=Path(os.getenv('CHANGAN_WEB_DIR',str(ROOT/'web/dist')))
if web_directory.exists():app.mount('/',StaticFiles(directory=web_directory,html=True),name='web')
