import os, time, json, sqlite3, threading, platform, uuid, hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import httpx, psutil
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import FileResponse
from pydantic import BaseModel
from app.config import DATA, TOKEN, MODEL, MODEL_ID
from workers.inference import Detector, analyze
NODE=os.getenv('NODE_ID','lab-1')
KIND=os.getenv('NODE_KIND','server_process')
ROOT=Path(os.getenv('WORKER_DATA',str(DATA/'workers'/NODE))).resolve(); ROOT.mkdir(parents=True,exist_ok=True)
DB=ROOT/'worker.sqlite'; lock=threading.RLock(); pool=ThreadPoolExecutor(max_workers=1)
client=httpx.Client(timeout=10,trust_env=False)
detector=None; drained=False; fail_once=False; measured_fps=None; measured_bandwidth=None

def connect():
    c=sqlite3.connect(DB,timeout=30); c.row_factory=sqlite3.Row; return c

def get(id):
    with connect() as c:
        row=c.execute('SELECT * FROM executions WHERE id=?',(id,)).fetchone()
        return dict(row) if row else None

def update(id, **fields):
    fields['updated']=time.time()
    with lock,connect() as c:
        c.execute('UPDATE executions SET '+','.join(k+'=?' for k in fields)+' WHERE id=?',(*fields.values(),id))

def state(row):
    request=json.loads(row['request'])
    return {k:row[k] for k in ['id','status','progress','seq','created','updated','error']}|{'node_id':NODE,'attempt_id':request['attempt_id'],'task_id':request['task_id'],'segment_id':request['segment_id'],'token':request['token'],'result':json.loads(row['result']) if row['result'] else None}

def notify(id):
    row=get(id); request=json.loads(row['request'])
    try:
        r=client.post(request['callback_url'],json=state(row),headers={'X-Internal-Token':TOKEN}); r.raise_for_status()
    except Exception: pass  # durable state is available through GET for reconciliation

def transition(id,status,**fields):
    with lock:
        row=get(id)
        if row['status'] in ['SUCCEEDED','FAILED','CANCELED'] and status != row['status']: return
        update(id,status=status,seq=row['seq']+1,**fields)
    notify(id)

def run(id):
    global measured_fps, measured_bandwidth, fail_once
    row=get(id); req=json.loads(row['request']); out=ROOT/id; out.mkdir(exist_ok=True)
    if row['status']=='CANCELED': return
    try:
        if fail_once:
            fail_once=False; raise RuntimeError('受控故障注入：执行失败一次')
        transition(id,'RUNNING')
        video=out/'input.mp4'
        download_started=time.monotonic()
        if not video.exists():
            with client.stream('GET',req['input_url'],headers={'X-Internal-Token':TOKEN},timeout=60) as r:
                r.raise_for_status()
                with video.open('wb') as f:
                    for chunk in r.iter_bytes(1024*1024): f.write(chunk)
        download_seconds=time.monotonic()-download_started
        measured_bandwidth=video.stat().st_size*8/max(download_seconds,.001)/1e6
        if hashlib.sha256(video.read_bytes()).hexdigest()!=req['input_sha256']: raise ValueError('输入片段校验失败')
        last=[0.]
        def report(p,n):
            if time.monotonic()-last[0]>=.5:
                last[0]=time.monotonic(); transition(id,'RUNNING',progress=p)
        def canceled(): return get(id)['status']=='CANCELED'
        result=analyze(detector,video,out,req['params'],report,canceled)
        if canceled(): return
        measured_fps=result['measured_fps']
        result.update({'input_transfer_seconds':download_seconds,'input_bytes':video.stat().st_size,'execution_id':id,'actual_node':NODE,'kind':KIND,'model_id':MODEL_ID,'task_id':req['task_id'],'segment_id':req['segment_id'],'attempt_id':req['attempt_id']})
        (out/'result.json').write_text(json.dumps(result,ensure_ascii=False))
        transition(id,'SUCCEEDED',progress=1,result=json.dumps(result,ensure_ascii=False))
    except InterruptedError:
        if get(id)['status']!='CANCELED': transition(id,'CANCELED')
    except Exception as e:
        if get(id)['status']!='CANCELED': transition(id,'FAILED',error=str(e)[:500])

@asynccontextmanager
async def lifespan(app):
    global detector
    with connect() as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('CREATE TABLE IF NOT EXISTS executions(id TEXT PRIMARY KEY, attempt_id TEXT UNIQUE, request TEXT NOT NULL,status TEXT NOT NULL,progress REAL DEFAULT 0,seq INTEGER DEFAULT 0,created REAL,updated REAL,error TEXT,result TEXT)')
        c.execute("UPDATE executions SET status='FAILED',error='执行器重启，旧执行已中断',seq=seq+1,updated=? WHERE status IN ('ACCEPTED','RUNNING')",(time.time(),))
    detector=Detector(MODEL)
    yield
    client.close(); pool.shutdown(wait=False,cancel_futures=True)
app=FastAPI(title='Changan execution worker',lifespan=lifespan)
def auth(x_internal_token: str=Header(default='')):
    import secrets
    if not secrets.compare_digest(x_internal_token,TOKEN): raise HTTPException(401,'执行器认证失败')

@app.get('/health',dependencies=[Depends(auth)])
def health():
    with connect() as c: active=c.execute("SELECT count(*) FROM executions WHERE status IN ('ACCEPTED','RUNNING')").fetchone()[0]
    return {'node_id':NODE,'kind':KIND,'healthy':not drained,'architecture':platform.machine(),'location':os.getenv('NODE_LOCATION','server-lab'),'models':[MODEL_ID] if detector else [],'runtime':'onnxruntime-cpu','capacity':1,'active':active,'available_memory_mb':psutil.virtual_memory().available//1048576,'cpu_percent':psutil.cpu_percent(),'measured_fps':measured_fps,'observed_at':time.time(),'tags':['cpu','onnx'],'network_source':'最近片段HTTP传输实测（服务器回环，非车端网络）' if measured_bandwidth else '带宽尚未实测','bandwidth_mbps':measured_bandwidth,'input_reachable':True,'resource_scope':'共享服务器实测，非独立车端资源'}

class Submission(BaseModel):
    task_id:str; segment_id:str; attempt_id:str; token:str; target_node:str
    input_url:str; input_sha256:str; callback_url:str; params:dict

@app.post('/executions',dependencies=[Depends(auth)])
def submit(body:Submission):
    if body.target_node!=NODE: raise HTTPException(409,'指定节点与实际节点不一致')
    with lock,connect() as c:
        old=c.execute('SELECT * FROM executions WHERE attempt_id=?',(body.attempt_id,)).fetchone()
        if old:
            req=json.loads(old['request'])
            if req!=body.model_dump(): raise HTTPException(409,'幂等键已绑定其他参数')
            return state(dict(old))
        if drained: raise HTTPException(409,'节点已停用')
        if c.execute("SELECT count(*) FROM executions WHERE status IN ('ACCEPTED','RUNNING')").fetchone()[0]>=1: raise HTTPException(409,'执行槽忙')
        id='exec_'+uuid.uuid4().hex[:16]; now=time.time()
        c.execute('INSERT INTO executions(id,attempt_id,request,status,seq,created,updated) VALUES(?,?,?,?,1,?,?)',(id,body.attempt_id,body.model_dump_json(),'ACCEPTED',now,now))
    pool.submit(run,id)
    return state(get(id))
@app.get('/executions/by-attempt/{attempt_id}',dependencies=[Depends(auth)])
def by_attempt(attempt_id:str):
    with connect() as c: row=c.execute('SELECT * FROM executions WHERE attempt_id=?',(attempt_id,)).fetchone()
    if not row: raise HTTPException(404,'未接收此尝试')
    return state(dict(row))
@app.get('/executions/{id}',dependencies=[Depends(auth)])
def detail(id:str):
    row=get(id)
    if not row: raise HTTPException(404,'执行不存在')
    return state(row)
@app.post('/executions/{id}/cancel',dependencies=[Depends(auth)])
def cancel(id:str):
    row=get(id)
    if not row: raise HTTPException(404,'执行不存在')
    if row['status'] not in ['SUCCEEDED','FAILED','CANCELED']: transition(id,'CANCELED')
    return state(get(id))
@app.get('/executions/{id}/artifacts/{name}',dependencies=[Depends(auth)])
def artifact(id:str,name:str):
    if name not in ['result.json','preview-0.jpg','preview-1.jpg','preview-2.jpg']: raise HTTPException(404)
    file=ROOT/id/name
    if not file.is_file() or file.parent.parent!=ROOT: raise HTTPException(404)
    return FileResponse(file)
@app.post('/controls/{action}',dependencies=[Depends(auth)])
def control(action:str):
    global drained,fail_once
    if KIND!='server_process': raise HTTPException(403,'仅测试执行器可注入故障')
    if action=='drain': drained=True
    elif action=='resume': drained=False
    elif action=='fail-once': fail_once=True
    else: raise HTTPException(400,'未知操作')
    return {'node_id':NODE,'action':action,'injected':True}
