#!/usr/bin/env python
"""Remove only the idle lab-4 video image, then verify an actual registry pull and inference."""
import os,sys,json,time,subprocess,asyncio
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from app.config import DATA
from app.kube import Kube
import httpx
assert DATA.name=='k3s-validation'
k=Kube()
pods=asyncio.run(k.call(k.core.list_namespaced_pod,k.namespace)).items
assert not pods,'Cold check requires an idle namespace'
env=os.environ.copy();env['PATH']=str(Path.cwd()/'vendor/docker')+':'+env['PATH'];env['DOCKER_HOST']='unix://'+str(Path.cwd()/'data/k3s/docker.sock')
def images():
    return json.loads(subprocess.check_output(['docker','exec','k3d-changan-dev-agent-3','crictl','--timeout=60s','images','-o','json'],env=env))['images']
ref=os.environ['CHANGAN_VIDEO_IMAGE']
if any(ref in i.get('repoDigests',[]) for i in images()):
    subprocess.run(['docker','exec','k3d-changan-dev-agent-3','crictl','--timeout=60s','rmi',ref],env=env,check=True)
assert not any(ref in i.get('repoDigests',[]) for i in images()),'image still cached'

c=httpx.Client(base_url='http://127.0.0.1:8410',trust_env=False,timeout=30)
r=c.post('/api/tasks',json={'name':'K3s验收 · 清缓存后真实拉取镜像','node_ids':['lab-4'],'segment_seconds':24,'max_retries':0,'attempt_timeout':180});r.raise_for_status();tid=r.json()['id']
for _ in range(240):
    t=c.get('/api/tasks/'+tid).raise_for_status().json()
    if t['status'] in ('SUCCEEDED','FAILED','TIMED_OUT') and all(a['cleanup_state']=='DONE' for a in t['attempts']):break
    time.sleep(1)
assert t['status']=='SUCCEEDED',t['error']
uid=t['attempts'][0]['workload']['pod_uid']
events=asyncio.run(k.call(k.core.list_namespaced_event,k.namespace,field_selector='involvedObject.uid='+uid)).items
pulls=[e.message for e in events if e.reason in ('Pulling','Pulled')]
assert any(e.reason=='Pulling' for e in events) and any(e.reason=='Pulled' for e in events)
case={'name':'cold-image-pull','task_id':tid,'status':t['status'],'segments':t['total'],'attempts':len(t['attempts']),'nodes':['lab-4'],'sampled_frames':t['result']['sampled_frames'],'all_cleaned':True,'elapsed_seconds':t['finished']-t['created'],'kubernetes_pull_events':pulls}
report=Path('docs/k3s-integration-report.json');data=json.loads(report.read_text());data['cases'].append(case);report.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
(DATA/'evidence/cold-image-pull.json').write_text(c.get('/api/tasks/'+tid+'/export').text)
print(json.dumps(case,ensure_ascii=False))
