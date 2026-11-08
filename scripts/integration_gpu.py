#!/usr/bin/env python
"""Real GPU task-session acceptance, confined to the validation API and database."""
import json,os,subprocess,sys,time
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from app import db
from app.config import DATA,API_KEY
BASE='http://127.0.0.1:8410'
c=httpx.Client(base_url=BASE,timeout=30,trust_env=False,headers={'X-API-Key':API_KEY})
terminal={'SUCCEEDED','FAILED','CANCELED','TIMED_OUT'}
output=ROOT/'docs/gpu-20261108';output.mkdir(exist_ok=True)
report={'environment':'one shared development server; four CPU logical nodes and one selected RTX5090 GPU logical node; no THOR hardware','cases':[]}

def get(tid):return c.get('/api/tasks/'+tid).raise_for_status().json()
def wait(tid,predicate,timeout=240):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:t=get(tid)
        except httpx.TransportError:
            report['poll_transport_errors']=report.get('poll_transport_errors',0)+1
            time.sleep(.5);continue
        if predicate(t):return t
        time.sleep(.25)
    raise AssertionError('Timed out: '+tid+' '+str(get(tid).get('error')))
def settled(t):return t['status'] in terminal and all(s['cleanup_state']=='DONE' for s in t['sessions']) and all(a['cleanup_state']=='DONE' for a in t['attempts'])
def create(name,**params):
    p=dict(name='GPU验收 · '+name,inference_backend='cuda',node_ids=['gpu-1'],segment_seconds=1,sample_fps=15,attempt_timeout=180,deadline_seconds=300,max_retries=2)
    p.update(params);return c.post('/api/tasks',json=p).raise_for_status().json()['id']
def record(name,t,**extra):
    r={'name':name,'task_id':t['id'],'status':t['status'],'segments':t['total'],'completed':t['completed'],'sessions':len(t['sessions']),'model_loads':sum(s['model_load_count'] for s in t['sessions']),'session_completed':[s['completed'] for s in t['sessions']],'all_cleaned':settled(t),'seconds':t['finished']-t['created'],'images':sorted({s['workload']['image'] for s in t['sessions']}),**extra}
    report['cases'].append(r);(output/'integration-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');(DATA/'evidence/gpu').mkdir(parents=True,exist_ok=True);(DATA/'evidence/gpu'/(name+'.json')).write_text(json.dumps(t,ensure_ascii=False));print(json.dumps(r,ensure_ascii=False),flush=True)
def check(t):
    assert t['status']=='SUCCEEDED',t.get('error')
    assert t['result']['coverage']==1 and t['result']['sampled_frames']==240
    for a in t['attempts']:
        if a['status']=='SUCCEEDED':
            v=next(seg['result'] for seg in t['segments'] if seg['result'] and seg['result']['attempt_id']==a['id'])['inference'];assert v['backend']=='cuda' and v['provider']=='CUDAExecutionProvider' and v['gpu_compute_events']>0
            assert v['gpu']['uuid']==a['workload']['expected_gpu_uuid']
    assert all(s['model_load_count']==1 for s in t['sessions'] if s['completed'])
def kube(*args):return subprocess.run([sys.executable,'scripts/cluster.py','kubectl',*args],cwd=ROOT,check=True,capture_output=True,text=True)

def main():
    assert DATA.name=='k3s-validation','Use isolated validation DATA'
    assert not [t for t in c.get('/api/tasks?limit=500').json() if t['status'] not in terminal]
    t=wait(create('单GPU连续24片'),settled);check(t)
    assert len(t['sessions'])==1 and t['sessions'][0]['completed']==24 and len({a['execution_id'] for a in t['attempts']})==1
    record('gpu-b-reuse',t,gpu=t['sessions'][0]['workload']['inference'],inference_seconds=sum(seg['result']['inference_seconds'] for seg in t['segments']))
    t=wait(create('四CPU节点回归',inference_backend='cpu',node_ids=['lab-1','lab-2','lab-3','lab-4']),settled)
    assert t['status']=='SUCCEEDED' and t['result']['sampled_frames']==240 and len(t['sessions'])==4
    assert all(seg['result']['inference']['backend']=='cpu' for seg in t['segments']);record('cpu-four-nodes',t)
    t=wait(create('CPU节点不得承接CUDA',node_ids=['lab-1'],deadline_seconds=10),settled)
    assert t['status']=='TIMED_OUT' and not t['attempts'];record('no-silent-cpu-fallback',t)
    # One GPU belongs to the entire first task until cleanup. The second task waits.
    a=create('GPU占用与取消',segment_seconds=1)
    held=wait(a,lambda t:bool(t['sessions']) and t['sessions'][0]['status']=='READY')
    b=create('GPU等待及释放后接续',sample_fps=2)
    time.sleep(.5);second=get(b)
    assert not second['sessions'] and not second['attempts']
    c.post('/api/tasks/'+a+'/cancel').raise_for_status();first=wait(a,settled)
    assert first['status']=='CANCELED';record('cancel-and-gpu-release',first,waiting_task=b)
    t=wait(b,settled);assert t['status']=='SUCCEEDED' and t['result']['sampled_frames']==48;record('gpu-wait-then-run',t)
    # Coordinator/gateway restart must preserve the same live Pod and loaded model.
    tid=create('控制面重启复用')
    before=wait(tid,lambda t:len(t['sessions'])==1 and t['sessions'][0]['completed']>=2)
    uid=before['sessions'][0]['workload']['pod_uid']
    subprocess.run([sys.executable,'scripts/manage.py','restart','api','container-gateway'],cwd=ROOT,check=True,capture_output=True)
    for _ in range(40):
        try:
            if c.get('/health').is_success:break
        except httpx.TransportError:pass
        time.sleep(.25)
    t=wait(tid,settled);check(t)
    assert len(t['sessions'])==1 and t['sessions'][0]['workload']['pod_uid']==uid;record('restart-reuses-gpu-session',t)
    # Delete only this validation task Pod; incomplete work is retried in a fresh session.
    tid=create('GPU容器中断恢复')
    before=wait(tid,lambda t:len(t['sessions'])==1 and t['sessions'][0]['completed']>=2)
    kube('delete','pod',before['sessions'][0]['workload']['pod_name'],'-n','changan','--wait=false')
    t=wait(tid,settled);check(t);assert len(t['sessions'])>=2;record('gpu-pod-recovery',t)
    print('GPU integration complete',flush=True)
if __name__=='__main__':main()
