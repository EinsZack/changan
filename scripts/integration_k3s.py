#!/usr/bin/env python
"""Real K3s acceptance. Fault cases only run against the isolated validation database."""
import os,sys,time,json,subprocess
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
import httpx
from app import db
from app.config import DATA,API_KEY
base=os.getenv('CHANGAN_TEST_BASE','http://127.0.0.1:8410')
c=httpx.Client(base_url=base,trust_env=False,timeout=30,headers={'X-API-Key':API_KEY})
report={'environment':'single-server k3d/K3s logical nodes; CPU ONNX; not THOR','at':time.time(),'cases':[]}
out=Path('docs/k3s-integration-report.json')
def get(tid):return c.get('/api/tasks/'+tid).raise_for_status().json()
def wait(tid,predicate,timeout=360):
    until=time.monotonic()+timeout
    while time.monotonic()<until:
        t=get(tid)
        if predicate(t):return t
        time.sleep(1)
    raise AssertionError('wait timeout: '+json.dumps({'id':tid,'task':get(tid)},ensure_ascii=False)[:2000])
def settled(t):return t['status'] in ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT') and all(a['cleanup_state']=='DONE' for a in t['attempts'])
def create(name,**p):
    r=c.post('/api/tasks',json={'name':'K3s验收 · '+name,'algorithm':'round_robin','attempt_timeout':180,'deadline_seconds':600,**p});r.raise_for_status();return r.json()['id']
def record(name,t,**extra):
    result={'name':name,'task_id':t['id'],'status':t['status'],'segments':t['total'],'attempts':len(t['attempts']),
      'nodes':sorted(set(a['actual_node'] for a in t['attempts'] if a['actual_node'])),'all_cleaned':all(a['cleanup_state']=='DONE' for a in t['attempts']),
      'sampled_frames':(t.get('result') or {}).get('sampled_frames'),'elapsed_seconds':(t['finished'] or time.time())-t['created'],**extra}
    report['cases'].append(result);out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    evidence=DATA/'evidence';evidence.mkdir(exist_ok=True)
    (evidence/(name+'.json')).write_text(c.get('/api/tasks/'+t['id']+'/export').text)
    print(json.dumps(result,ensure_ascii=False),flush=True)
def success(t):
    assert t['status']=='SUCCEEDED',t.get('error')
    assert len(t['attempts'])==t['total']
    assert t['result']['coverage']==1
    for a in t['attempts']:
        assert a['backend']=='k3s' and a['actual_node']==a['node_id']
        assert all(a['workload'].get(k) for k in ['job_uid','pod_uid','image','node_name'])
    for url in t['result']['previews']:assert c.get(url).status_code==200
assert c.get('/api/overview').json()['mode']=='k3s'
cold=wait(create('四节点视频闭环'),settled)
success(cold);assert len({a['node_id'] for a in cold['attempts']})==4
record('four-node',cold)
warm=wait(create('缓存镜像视频闭环'),settled)
success(warm);assert warm['result']['counts']==cold['result']['counts']
record('warm',warm,counts_equal=True)
single=wait(create('指定单节点',node_ids=['lab-3'],segment_seconds=24),settled)
success(single);assert {a['actual_node'] for a in single['attempts']}=={'lab-3'}
record('single-node',single)
cancel_id=create('取消及清理',sample_fps=15,segment_seconds=24)
wait(cancel_id,lambda t:any(a['status']=='RUNNING' for a in t['attempts']))
c.post('/api/tasks/'+cancel_id+'/cancel').raise_for_status()
canceled=wait(cancel_id,settled)
assert canceled['status']=='CANCELED' and canceled['result'] is None
record('cancel',canceled)
if '--faults' in sys.argv:
    assert DATA.name=='k3s-validation' and base.endswith(':8410'),'fault tests require isolated validation instance'
    restart_id=create('管理服务重启恢复',sample_fps=15,segment_seconds=4,max_retries=0)
    wait(restart_id,lambda t:any(a['status']=='RUNNING' for a in t['attempts']))
    subprocess.run([sys.executable,'scripts/manage.py','restart','api'],check=True)
    for _ in range(30):
        try:
            if c.get('/health').is_success:break
        except httpx.TransportError:pass
        time.sleep(1)
    recovered=wait(restart_id,settled)
    success(recovered);record('coordinator-restart',recovered)
    failed_id=create('Pod中断后的业务重试',sample_fps=15,segment_seconds=24,max_retries=1)
    running=wait(failed_id,lambda t:any(a['status']=='RUNNING' for a in t['attempts']))
    a=next(a for a in running['attempts'] if a['status']=='RUNNING')
    subprocess.run([sys.executable,'scripts/cluster.py','kubectl','delete','pod',a['workload']['pod_name'],'-n','changan','--wait=false'],check=True)
    recovered=wait(failed_id,settled)
    assert recovered['status']=='SUCCEEDED' and len(recovered['attempts'])==2
    assert recovered['attempts'][0]['status'] in ('FAILED','TIMED_OUT')
    record('pod-interruption-retry',recovered)
    bad_id=create('缺失镜像超时清理',node_ids=['test-no-node'],segment_seconds=24,max_retries=0,attempt_timeout=20)
    with db.connect() as conn:
        row=conn.execute('SELECT params,metadata FROM tasks WHERE id=?',(bad_id,)).fetchone()
        params=db.loads(row['params']);params['node_ids']=['lab-1']
        meta=db.loads(row['metadata']);meta['application']['image']='changan-registry:5000/changan/missing@sha256:'+'0'*64
        conn.execute('UPDATE tasks SET params=?,metadata=? WHERE id=?',(db.dumps(params),db.dumps(meta),bad_id))
    failed=wait(bad_id,settled)
    assert failed['status']=='FAILED' and failed['result'] is None
    evidence=c.get('/api/tasks/'+bad_id+'/events').json()
    assert any('ImagePull' in json.dumps(e) or 'ErrImage' in json.dumps(e) for e in evidence)
    record('missing-image',failed)
print('K3s acceptance passed',flush=True)
