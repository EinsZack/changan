#!/usr/bin/env python
"""Run a real video upload/task acceptance against the deployed native Mac platform."""
import argparse, gzip, json, time
from pathlib import Path
import httpx

def run(root,node,expected='cuda'):
    root=Path(root).resolve()
    settings={}
    for line in (root/'config/platform.env').read_text().splitlines():
        if '=' in line:
            k,v=line.split('=',1);settings[k]=v.strip().strip('"').strip("'")
    out=root/'data/evidence/thor-native-20261111';out.mkdir(parents=True,exist_ok=True)
    c=httpx.Client(base_url='http://127.0.0.1:8310',headers={'X-API-Key':settings.get('CHANGAN_API_KEY','')},trust_env=False,timeout=60)
    with (root/'current/assets/samples/demo.mp4').open('rb') as video:
        uploaded=c.post('/api/uploads',files={'file':('thor-demo.mp4',video,'video/mp4')}).raise_for_status().json()
    p={'name':'Thor1真实'+expected.upper()+' · 上传视频单节点全流程','source':uploaded['source'],'algorithm':'round_robin',
       'segment_seconds':4,'sample_fps':10,'attempt_timeout':300,'deadline_seconds':600,
       'max_retries':0,'node_ids':[node],'architecture':'arm64'}
    task=c.post('/api/tasks',json=p).raise_for_status().json()['id']
    print(json.dumps({'created_task':task,'upload':uploaded['source']}),flush=True)
    until=time.monotonic()+660;last=None
    while time.monotonic()<until:
        t=c.get('/api/tasks/'+task).raise_for_status().json()
        marker=(t['status'],t['completed'],tuple(s['status'] for s in t['sessions']))
        if marker!=last:
            print(json.dumps({'task':task,'state':t['status'],'completed':t['completed'],'total':t['total'],'sessions':[{'state':s['status'],'completed':s['completed'],'cleanup':s['cleanup_state']} for s in t['sessions']]}),flush=True);last=marker
        if t['status'] in ['SUCCEEDED','FAILED','CANCELED','TIMED_OUT'] and all(s['cleanup_state']=='DONE' for s in t['sessions']):break
        time.sleep(1)
    else:raise RuntimeError('Real task acceptance timed out')
    export=c.get('/api/tasks/'+task+'/export').raise_for_status().content
    with gzip.open(out/(task+'.json.gz'),'wb') as f:f.write(export)
    (out/'last-task.txt').write_text(task)
    assert t['status']=='SUCCEEDED',t.get('error')
    assert t['total']==6 and t['completed']==6
    assert t['result']['coverage']==1 and t['result']['sampled_frames']==240
    assert len(t['attempts'])==6 and {a['actual_node'] for a in t['attempts']}=={node}
    assert len(t['sessions'])==1 and t['sessions'][0]['model_load_count']==1 and t['sessions'][0]['completed']==6
    assert all(a['cleanup_state']=='DONE' for a in t['attempts'])
    runtime=t['sessions'][0]['workload']['inference_runtime']
    assert runtime['actual_backend']==expected,runtime
    if expected=='cuda':assert runtime['cuda_compute_events']>0,runtime
    for url in t['result']['previews']:c.get(url).raise_for_status()
    summary={'task_id':task,'status':t['status'],'elapsed_seconds':t['finished']-t['created'],
             'segments':t['total'],'sampled_frames':t['result']['sampled_frames'],'counts':t['result']['counts'],
             'actual_nodes':sorted({a['actual_node'] for a in t['attempts']}),'model_load_count':t['sessions'][0]['model_load_count'],
             'sessions':len(t['sessions']),'previews_readable':len(t['result']['previews']),'cleanup':'DONE',
             'image':t['metadata']['application']['image'],'inference_runtime':runtime}
    (out/'video-summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(summary,ensure_ascii=False),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--node',default='thor1')
    p.add_argument('--expect-backend',choices=['cpu','cuda'],default='cuda')
    a=p.parse_args();run(a.root,a.node,a.expect_backend)
