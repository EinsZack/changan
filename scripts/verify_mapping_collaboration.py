"""Read-only acceptance against a real finished run. No mocking, no job submission.
Reads only management API responses and downloaded output bytes. No credentials
or raw node snapshots in the portable report. Does not establish GCP accuracy.
"""
import argparse,hashlib,json,os,sys
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import Request,urlopen

def verify(base,tid):
    def request(path):
        if not path.startswith('/api/') or '://' in path or '..' in path:raise ValueError('unexpected artifact or API path')
        return urlopen(Request(base.rstrip('/')+path,headers={'X-API-Key':os.getenv('CHANGAN_API_KEY','')}),timeout=60)
    def load(path):
        with request(path) as f:return json.load(f)
    task=load('/api/tasks/'+tid);events=[]
    while True:
        page=load('/api/tasks/%s/events?after=%d'%(tid,events[-1]['id'] if events else 0));events.extend(page)
        if len(page)<1000:break
    assert task['id']==tid and task['status']=='SUCCEEDED' and task['params']['execution_mode']=='collaborative','需要已完成的真实协同任务'
    units={u['id']:u for u in task['work_units']};attempts={a['id']:a for a in task['attempts']};producer={uid:attempts[u['active_attempt']] for uid,u in units.items()}
    plan=task['metadata']['mapping_plan'];groups={g['id'] for g in plan['submodels']};version=int(task['metadata']['application'].get('pipeline_version',1));assert task['completed']==task['total']==2*len(groups)+(2 if version>=2 else 3)==len(units)
    rows=[];windows=[]
    for u in task['work_units']:
        a=producer[u['id']];w=a['workload'];phase=u['phase']
        assert a['actual_node']==a['node_id'] and a['status']==u['status']=='SUCCEEDED' and w['job_uid'] and w['pod_uid']
        for parent in u['dependencies']:assert producer[parent]['finished']<=a['created'],'stage barrier was bypassed'
        assert {(x['unit_id'],x['name']) for x in w['inputs']}=={(x['unit_id'],x['name']) for x in u['sources']}
        for entry in w['inputs']:assert producer[entry['unit_id']]['id']==entry['attempt_id'],'stale stage input'
        marks=[e for e in events if e['type']=='mapping_stage' and e['attempt_id']==a['id'] and e['payload']['stage']==phase]
        starts=[e['at'] for e in marks if e['payload']['state']=='RUNNING'];ends=[e['at'] for e in marks if e['payload']['state']=='SUCCEEDED'];assert starts and ends
        start,end=min(starts),max(ends)
        if phase in ('reconstruct','toolchain'):windows.append((start,end,a['actual_node'],phase))
        result=next(s['result'] for s in task['segments'] if s['id']==u['id'])
        rows.append({'phase':phase,'submodel':u['submodel'],'unit':u['id'],'attempt':a['id'],'actual_node':a['actual_node'],'pod_uid':w['pod_uid'],'job_uid':w['job_uid'],'compute_start':start,'compute_end':end,'metrics':result['metrics'],'input_artifacts':len(w['inputs']),'cleanup_state':a['cleanup_state']})
    overlap={}
    for phase in ('reconstruct','toolchain'):
        ranges=[w for w in windows if w[3]==phase];points=sorted(set(x for w in ranges for x in w[:2]));duration=0;peak=0
        for start,end in zip(points,points[1:]):
            n=len({w[2] for w in ranges if w[0]<=start and w[1]>=end});peak=max(peak,n)
            if n>=2:duration+=end-start
        overlap[phase]={'seconds':round(duration,3),'peak_actual_compute_nodes':peak}
    # Fail the demonstration check if a distributed task ran sequentially.
    assert all(o['seconds']>0 for o in overlap.values()),'两波计算未都观测到跨节点时间重叠'
    nodes=sorted({r['actual_node'] for r in rows if r['phase'] in ('reconstruct','toolchain')});assert len(nodes)>=2
    quality=task['result']['quality'];assert set(quality['submodels'])==groups
    published=[]
    for x in task['result']['artifacts']:
        h=hashlib.sha256();size=0
        with request(x['url']) as f:
            while b:=f.read(1048576):h.update(b);size+=len(b)
        assert h.hexdigest()==x['sha256'] and size==x['bytes'],'下载摘要不符'
        published.append({'name':x['name'],'sha256':h.hexdigest(),'bytes':size})
    return {'task_id':tid,'task_name':task['name'],'image':task['metadata']['application']['image'],'input_sha256':task['metadata']['input']['sha256'],'preset':task['params']['preset'],'wall_seconds':round(task['finished']-task['started'],3),'actual_nodes':nodes,'source_images':len(plan['images']),'groups':{g['id']:len(g['images']) for g in plan['submodels']},'overlap_edges':[{k:v for k,v in e.items() if k!='images'}|{'common_images':len(e['images'])} for e in plan['overlap_edges']],'verified_units':rows,'cross_node_compute_overlap':overlap,'download_bytes':sum(r['metrics']['download_bytes'] for r in rows),'upload_bytes':sum(r['metrics']['upload_bytes'] for r in rows),'quality':{k:v for k,v in quality.items() if k!='corners'},'verified_published_artifacts':published}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('task');parser.add_argument('--api',default='http://127.0.0.1:8310');parser.add_argument('--output',type=Path);a=parser.parse_args();report=json.dumps(verify(a.api,a.task),ensure_ascii=False,indent=2)+'\n'
    if a.output:a.output.write_text(report)
    else:print(report)
