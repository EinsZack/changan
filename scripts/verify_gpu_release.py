#!/usr/bin/env python
"""Post-deployment GPU and CPU smoke tasks plus read-only history comparison."""
import json,time,sqlite3,hashlib,sys
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from app.config import API_KEY
c=httpx.Client(base_url='http://127.0.0.1:8310',timeout=30,trust_env=False,headers={'X-API-Key':API_KEY})
for _ in range(60):
    try:
        health=c.get('/health').raise_for_status().json()
        if health['version']=='0.6.0':break
    except httpx.TransportError:pass
    time.sleep(.5)
else:raise AssertionError('new API not ready')
report={'health':health,'profiles':c.get('/api/execution-profiles').raise_for_status().json(),'tasks':[]}
for backend in ['cuda','cpu']:
    params={'name':'0.6.0正式验证 · '+backend.upper(),'segment_seconds':4,'sample_fps':2,'deadline_seconds':300}
    # CUDA uses the configured default; CPU is explicit.
    if backend=='cpu':params.update(inference_backend='cpu',node_ids=['lab-1'])
    tid=c.post('/api/tasks',json=params).raise_for_status().json()['id']
    for _ in range(500):
        t=c.get('/api/tasks/'+tid).raise_for_status().json()
        if t['status'] in ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED') and all(s['cleanup_state']=='DONE' for s in t['sessions']):break
        time.sleep(.5)
    else:raise AssertionError('task timeout '+tid)
    assert t['status']=='SUCCEEDED',t.get('error')
    assert t['result']['sampled_frames']==48 and t['completed']==6
    assert all(s['model_load_count']==1 for s in t['sessions'])
    assert all(s['result']['inference']['backend']==backend for s in t['segments'])
    (ROOT/'docs/gpu-20261108'/('production-'+backend+'.json')).write_text(json.dumps(t,ensure_ascii=False,indent=2)+'\n')
    row={'backend':backend,'task_id':tid,'status':t['status'],'segments':t['total'],'frames':t['result']['sampled_frames'],'sessions':len(t['sessions']),'model_loads':sum(s['model_load_count'] for s in t['sessions']),'seconds':t['finished']-t['created'],'all_cleaned':True}
    report['tasks'].append(row);print(json.dumps(row),flush=True)
expected=json.loads((ROOT/'data/backups/20261108-gpu/history-fingerprints.json').read_text())
db=sqlite3.connect(ROOT/'data/control.sqlite')
for table,rows in expected.items():
    actual={str(r[0]):hashlib.sha256(json.dumps(r,ensure_ascii=False,separators=(',',':')).encode()).hexdigest() for r in db.execute('SELECT * FROM '+table+' ORDER BY id')}
    assert all(actual.get(id)==value for id,value in rows.items()),'History changed: '+table
report['preserved_history_tasks']=len(expected['tasks']);report['history_unchanged']=True
report['nodes']=c.get('/api/nodes').raise_for_status().json()
assert all(n['online'] for n in report['nodes'])
gpu=next(n['snapshot'] for n in report['nodes'] if n['id']=='gpu-1');assert gpu['gpu_available']==1 and gpu['gpu_telemetry_fresh']
(ROOT/'docs/gpu-20261108/production-verification.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
print('History preserved:',report['preserved_history_tasks'],flush=True)
