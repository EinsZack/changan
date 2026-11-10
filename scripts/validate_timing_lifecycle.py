#!/usr/bin/env python
"""Real GPU-session fallback, cancellation and restart checks in isolated data."""
import json
import gzip
import os
import subprocess
import sys
import time
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from app.config import DATA,API_KEY
from app import db
TERMINAL={'SUCCEEDED','FAILED','CANCELED','TIMED_OUT'}

def main():
    assert DATA.name=='k3s-timing-validation'
    out=ROOT/'docs/gpu-timing-20261110'
    c=httpx.Client(base_url='http://127.0.0.1:8410',timeout=30,trust_env=False,headers={'X-API-Key':API_KEY})
    cases=[]
    def get(tid):return c.get('/api/tasks/'+tid).raise_for_status().json()
    def wait(tid,condition,timeout=500):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            try:
                t=get(tid)
                if condition(t):return t
            except httpx.TransportError:
                pass
            time.sleep(.3)
        raise AssertionError('timeout '+tid)
    def settled(t):
        return t['status'] in TERMINAL and all(s['cleanup_state']=='DONE' for s in t['sessions']) and all(a['cleanup_state']=='DONE' for a in t['attempts'])
    def create(name,device=0):
        assert not db.query("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')")
        tid=c.post('/api/tasks',json={'name':'GPU计时可靠性 · '+name,'segment_seconds':1,'sample_fps':15,
            'attempt_timeout':300,'deadline_seconds':900,'node_ids':['validation-hold']}).raise_for_status().json()['id']
        prepared=wait(tid,lambda t:t['status']=='RUNNING',90)
        assert not prepared['attempts']
        with db.connect() as conn:
            conn.execute('BEGIN IMMEDIATE')
            t=conn.execute('SELECT params,metadata FROM tasks WHERE id=?',(tid,)).fetchone()
            p=db.loads(t['params']);p['node_ids']=['lab-1']
            m=db.loads(t['metadata']);m['application']['model_device_id']=device
            conn.execute('UPDATE tasks SET params=?,metadata=? WHERE id=?',(db.dumps(p),db.dumps(m),tid))
        print(name,tid,'started',flush=True);return tid
    def save(name,t,**extra):
        cases.append({'case':name,'task_id':t['id'],'status':t['status'],'sessions':len(t['sessions']),
            'attempts':len(t['attempts']),'completed':t['completed'],'all_cleaned':settled(t),**extra})
        (out/'lifecycle.json').write_text(json.dumps(cases,ensure_ascii=False,indent=2)+'\n')
        (out/'exports').mkdir(exist_ok=True)
        (out/'exports'/(t['id']+'.json.gz')).write_bytes(gzip.compress(c.get('/api/tasks/'+t['id']+'/export').raise_for_status().content,mtime=0))
        print(json.dumps(cases[-1],ensure_ascii=False),flush=True)
    tid=create('CUDA不可用时明确回退CPU',device=99)
    t=wait(tid,settled)
    assert t['status']=='SUCCEEDED' and len(t['sessions'])==1 and len(t['attempts'])==24
    runtime=t['sessions'][0]['workload']['inference_runtime']
    assert runtime['actual_backend']=='cpu' and 'GPU index 99 unavailable' in runtime['fallback_reason']
    assert all(s['result']['inference_runtime']==runtime for s in t['segments'])
    save('fallback',t,actual_backend='cpu',fallback_reason=runtime['fallback_reason'])
    tid=create('模型初始化期间取消')
    wait(tid,lambda t:any(s['workload'].get('pod_uid') and not s['ready_at'] for s in t['sessions']),120)
    c.post('/api/tasks/'+tid+'/cancel').raise_for_status()
    t=wait(tid,settled)
    assert t['status']=='CANCELED' and t['completed']==0
    save('cancel_during_startup',t)
    tid=create('推理期间服务重启')
    before=wait(tid,lambda t:any(s['completed']>=1 for s in t['sessions']))
    session=before['sessions'][0]
    env=json.loads((DATA/'environment.json').read_text())
    subprocess.run([sys.executable,'scripts/manage.py','restart','api','container-gateway'],env=os.environ|env,check=True)
    t=wait(tid,settled)
    assert t['status']=='SUCCEEDED' and len(t['attempts'])==24 and len(t['sessions'])==1
    after=t['sessions'][0]
    assert after['workload']['pod_uid']==session['workload']['pod_uid']
    assert after['workload']['inference_runtime']==session['workload']['inference_runtime']
    assert after['model_load_count']==1
    save('coordinator_gateway_restart',t,same_pod=True,same_runtime=True,model_loads=1)
if __name__=='__main__':main()
