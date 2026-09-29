"""Exercise real HTTP services and ONNX execution. Leaves auditable task records."""
import httpx,time,json,subprocess,sys,os
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
c=httpx.Client(base_url='http://127.0.0.1:8310',timeout=30,trust_env=False)
report={'started':time.time(),'environment':'four processes on one server, real CPU inference, not vehicle acceptance','checks':[],'runs':[]}
def record(name,details):report['checks'].append({'name':name,'passed':True,'details':details});print(name,details,flush=True)
def post(path,p=None):
    r=c.post(path,json=p if p is not None else {});r.raise_for_status();return r.json()
def run(name,**params):
    r=post('/api/tasks',{'name':name,**params});return r['id']
def wait(id,timeout=150,until=None):
    started=time.monotonic()
    while time.monotonic()-started<timeout:
        try:t=c.get('/api/tasks/'+id).json()
        except httpx.HTTPError:time.sleep(.4);continue
        if until and until(t):return t
        if t['status'] in ['SUCCEEDED','FAILED','CANCELED','TIMED_OUT']:
            if until:raise AssertionError('Task terminated before condition: '+str(t['status']))
            report['runs'].append({k:t[k] for k in ['id','name','status','created','finished','total','completed','error']})
            return t
        time.sleep(.25)
    raise TimeoutError(id)
def assert_success(t):
    assert t['status']=='SUCCEEDED',(t['id'],t['error'])
    assert t['completed']==t['total']==6
    assert len(t['result']['provenance'])==6
    assert all(a['actual_node']==a['node_id'] for a in t['attempts'] if a['status']=='SUCCEEDED')
def restart(*names):
    subprocess.run([sys.executable,str(ROOT/'scripts/manage.py'),'restart',*names],check=True,cwd=ROOT)
    for _ in range(30):
        try:
            if c.get('/health').is_success:return
        except httpx.HTTPError:pass
        time.sleep(.25)
for _ in range(40):
    try:
        nodes=c.get('/api/nodes').json()
        if len(nodes)==4 and all(n['online'] for n in nodes):break
    except Exception:pass
    time.sleep(.5)
record('four_real_worker_processes',len(nodes))
for n in nodes:post(f"/api/nodes/{n['id']}/control/enable");post(f"/api/nodes/{n['id']}/control/resume")
single=wait(run('验证 · 单执行器基线',node_ids=['lab-1'],algorithm='round_robin'));assert_success(single)
rr=wait(run('验证 · 四执行器轮询',algorithm='round_robin'));assert_success(rr)
aware=wait(run('验证 · 四执行器资源感知'));assert_success(aware)
record('three_baselines',{'single':single['result']['elapsed_seconds'],'round_robin':rr['result']['elapsed_seconds'],'resource_aware':aware['result']['elapsed_seconds'],'counts_match':single['result']['counts']==rr['result']['counts']==aware['result']['counts']})
assert single['result']['counts']==rr['result']['counts']==aware['result']['counts']
for n in nodes:post(f"/api/nodes/{n['id']}/control/fail-once")
retry=wait(run('验证 · 受控失败后重试'));assert_success(retry)
assert any(a['status']=='FAILED' for a in retry['attempts'])
record('bounded_retry_with_new_attempts',{'attempts':len(retry['attempts']),'failed':sum(a['status']=='FAILED' for a in retry['attempts'])})
# Consume any fail-once controls not used by the retry task before later cases.
restart('worker-1','worker-2','worker-3','worker-4')
id=run('验证 · 管理服务重启恢复',sample_fps=10)
wait(id,until=lambda t:len(t['attempts'])>=2 and any(a['status']=='RUNNING' for a in t['attempts']))
restart('api');recovered=wait(id);assert_success(recovered)
record('coordinator_restart_reconciliation',id)
id=run('验证 · 执行器进程重启',node_ids=['lab-1'],sample_fps=10)
wait(id,until=lambda t:any(a['status']=='RUNNING' for a in t['attempts']))
restart('worker-1');recovered=wait(id);assert_success(recovered)
assert any(a['status']=='FAILED' for a in recovered['attempts'])
record('worker_restart_retry',id)
id=run('验证 · 运行中取消',sample_fps=10)
wait(id,until=lambda t:any(a['status']=='RUNNING' for a in t['attempts']))
post('/api/tasks/'+id+'/cancel');canceled=wait(id)
assert canceled['status']=='CANCELED' and canceled['result'] is None
record('cancel_no_false_success',id)
id=run('验证 · 无候选节点超时',node_ids=['not-a-node'],deadline_seconds=10)
t=wait(id);assert t['status']=='TIMED_OUT' and not t['attempts'] and not t['result']
record('no_candidate_deadline',id)
strategy=post('/api/strategies',{'name':'验证 · 周期触发','kind':'periodic','interval_seconds':15,'params':{'name':'验证 · 周期自动运行'}})['id']
started=time.monotonic();periodic=None
while time.monotonic()-started<35:
    matches=[t for t in c.get('/api/tasks').json() if t['strategy_id']==strategy]
    if matches:periodic=matches[0]['id'];break
    time.sleep(.4)
assert periodic
c.put('/api/strategies/'+strategy,json={'name':'验证 · 周期触发（测试后停用）','kind':'periodic','interval_seconds':15,'enabled':False,'params':{'name':'验证 · 周期自动运行'}}).raise_for_status()
t=wait(periodic);assert_success(t);assert t['trigger_type']=='periodic'
record('periodic_trigger_persistence',periodic)
strategy=post('/api/strategies',{'name':'演示 · 手动服务触发','kind':'service','params':{'name':'服务触发视频分析'}})['id']
t=wait(post('/api/strategies/'+strategy+'/trigger')['id']);assert_success(t);assert t['strategy_id']==strategy
record('service_trigger_linkage',t['id'])
cap=post('/api/capabilities/run');assert cap['status']=='SUCCEEDED',cap
record('five_actual_capabilities',{n['kind']:n['status'] for n in cap['nodes']})
connector=post('/api/configs/connector',{'name':'管理服务健康检查','body':{'url':'http://127.0.0.1:8310/health','method':'GET','auth_id':'none'}})['id']
assert post('/api/connectors/'+connector+'/call')['output']['status']=='ok'
record('registered_api_connector',connector)
export=c.get('/api/tasks/'+aware['id']+'/export').json();assert len(export['events'])>5 and len(export['attempts'])>=6
record('audit_export',{'events':len(export['events']),'attempts':len(export['attempts'])})
report['finished']=time.time();report['passed']=True
(ROOT/'docs/test-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print('ALL INTEGRATION CHECKS PASSED',flush=True)
