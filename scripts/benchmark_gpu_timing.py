#!/usr/bin/env python
"""Controlled video CPU/CUDA comparison on an isolated API; no task-level UI switch."""
import argparse
import json
import gzip
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
import httpx
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from app import db
from app.config import DATA, API_KEY

TERMINAL={'SUCCEEDED','FAILED','CANCELED','TIMED_OUT'}
OUT=ROOT/'docs/gpu-timing-20261110'

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--pairs',type=int,default=3);parser.add_argument('--resume',action='store_true');args=parser.parse_args()
    assert DATA.name=='k3s-timing-validation', 'isolated data only'
    OUT.mkdir(exist_ok=True)
    client=httpx.Client(base_url='http://127.0.0.1:8410',timeout=30,trust_env=False,headers={'X-API-Key':API_KEY})
    report={'environment':'one server, four logical K3s agents; shared RTX 5090; not THOR',
            'conditions':'same image/model/input, 24 x 1s, 15 requested fps (240 actual frames), CPU request1/limit2, GPU visible for both policies; sequential; per-task model load and warmup',
            'runs':[],'output_consistency':[]}
    if args.resume:
        report=json.loads((OUT/'benchmark.json').read_text())
        report.setdefault('output_consistency',[])
    def record():
        (OUT/'benchmark.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    def run(policy,nodes,name):
        previous=next((r for r in report['runs'] if r['name']==name),None)
        if previous:
            return client.get('/api/tasks/'+previous['task_id']).raise_for_status().json()
        assert not db.query("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')")
        production=client.get('http://127.0.0.1:8310/api/tasks').raise_for_status().json()
        assert all(t['status'] in TERMINAL for t in production), 'production must be idle'
        tid=client.post('/api/tasks',json={'name':name,'algorithm':'round_robin','segment_seconds':1,
            'sample_fps':15,'attempt_timeout':300,'deadline_seconds':900,'node_ids':['benchmark-hold']}).raise_for_status().json()['id']
        # Wait for split's metadata write; the hold node prevents first dispatch.
        prepare_until=time.monotonic()+90
        while time.monotonic()<prepare_until:
            pending=client.get('/api/tasks/'+tid).raise_for_status().json()
            if pending['status']=='RUNNING':
                break
            assert pending['status'] not in TERMINAL
            time.sleep(.1)
        assert pending['status']=='RUNNING' and not pending['attempts']
        with db.connect() as c:
            c.execute('BEGIN IMMEDIATE')
            task=c.execute('SELECT params,metadata FROM tasks WHERE id=?',(tid,)).fetchone()
            p=db.loads(task['params']);p['node_ids']=nodes
            m=db.loads(task['metadata']);m['application']['model_policy']=policy
            c.execute('UPDATE tasks SET params=?,metadata=? WHERE id=?',(db.dumps(p),db.dumps(m),tid))
        print(name,tid,'started',flush=True)
        until=time.monotonic()+950
        while time.monotonic()<until:
            t=client.get('/api/tasks/'+tid).raise_for_status().json()
            if t['status'] in TERMINAL and all(s['cleanup_state']=='DONE' for s in t.get('sessions',[])) and all(a['cleanup_state']=='DONE' for a in t['attempts']):
                break
            time.sleep(.5)
        (OUT/'exports').mkdir(exist_ok=True)
        (OUT/'exports'/(tid+'.json.gz')).write_bytes(gzip.compress(client.get('/api/tasks/'+tid+'/export').raise_for_status().content,mtime=0))
        assert t['status']=='SUCCEEDED', (t.get('error'),[(s.get('error'),s.get('workload',{}).get('failure_logs')) for s in t['sessions']])
        assert len(t['attempts'])==24 and t['result']['sampled_frames']==240
        assert len(t['sessions'])==len(nodes)
        assert all(s['model_load_count']==1 for s in t['sessions'])
        assert sum(s['completed'] for s in t['sessions'])==24
        for s in t['sessions']:
            assert len({a['execution_id'] for a in t['attempts'] if a['session_id']==s['id']})==1
            assert s['workload']['gpu_access'], 'CPU/GPU policies must use equally GPU-capable containers'
            assert s['workload']['inference_runtime']['actual_backend']==('cpu' if policy=='cpu' else 'cuda')
        rs=[s['result'] for s in t['segments']]
        row={'task_id':tid,'name':name,'policy':policy,'nodes':nodes,'image':t['metadata']['application']['image'],
             'input_sha256':t['metadata']['sha256'],'params':{k:t['params'][k] for k in ('sample_fps','confidence','segment_seconds')},
             'elapsed_seconds':t['finished']-t['created'],'frames':t['result']['sampled_frames'],
             'forward_seconds_sum':sum(r['analysis_timing']['inference_seconds'] for r in rs),
             'analysis_seconds_sum':sum(r['analysis_timing']['analysis_seconds'] for r in rs),
             'download_seconds_sum':sum(r['download_seconds'] for r in rs),
             'counts':t['result']['counts'],'sessions':[{'node':s['node_id'],'model_load_count':s['model_load_count'],
                'ready_seconds':s['ready_at']-s['created'],'model_seconds':s['model_load_seconds'],
                'runtime':s['workload']['inference_runtime'],'lifecycle':s['workload'].get('lifecycle')} for s in t['sessions']]}
        row['forward_ms_per_frame']=row['forward_seconds_sum']/row['frames']*1000
        report['runs'].append(row);record()
        print(json.dumps({k:v for k,v in row.items() if k not in ('sessions','counts','params')},ensure_ascii=False),flush=True)
        return t
    # First wave records new-image/startup cost and verifies all original lab nodes using real CUDA.
    run('auto',['lab-1','lab-2','lab-3','lab-4'],'GPU启动与四lab复用验证')
    reference=None
    for i in range(args.pairs):
        for policy in (['cpu','auto'] if i%2==0 else ['auto','cpu']):
            task=run(policy,['lab-1'],f'CPU/GPU同条件对照 {i+1} · {policy}')
            if reference is None:
                reference=task
            else:
                confidence_delta=box_delta=0.
                for sa,sb in zip(reference['segments'],task['segments']):
                    assert sa['ordinal']==sb['ordinal']
                    for fa,fb in zip(sa['result']['frames'],sb['result']['frames']):
                        assert fa['frame']==fb['frame'] and len(fa['detections'])==len(fb['detections'])
                        for da,dbb in zip(fa['detections'],fb['detections']):
                            assert da['label']==dbb['label']
                            confidence_delta=max(confidence_delta,abs(da['confidence']-dbb['confidence']))
                            box_delta=max(box_delta,max(abs(a-b) for a,b in zip(da['box'],dbb['box'])))
                assert confidence_delta<=.001 and box_delta<=.2
                report['output_consistency'].append({'reference':reference['id'],'task_id':task['id'],
                    'same_labels_and_counts':True,'max_confidence_difference':confidence_delta,
                    'max_box_coordinate_difference_pixels':box_delta,'confidence_tolerance':.001,'box_tolerance_pixels':.2})
                record()
    comparisons=report['runs'][1:]
    if comparisons:
        first=comparisons[0]
        assert all(r['image']==first['image'] and r['input_sha256']==first['input_sha256'] and r['params']==first['params'] for r in comparisons)
        assert all(r['counts']==first['counts'] for r in comparisons), 'detection count changed'
        for policy in ('cpu','auto'):
            values=[r for r in comparisons if r['policy']==policy]
            report[policy+'_median']={key:statistics.median(r[key] for r in values) for key in ('elapsed_seconds','forward_ms_per_frame','analysis_seconds_sum')}
        report['steady_forward_ratio']=report['cpu_median']['forward_ms_per_frame']/report['auto_median']['forward_ms_per_frame']
        report['whole_task_ratio']=report['cpu_median']['elapsed_seconds']/report['auto_median']['elapsed_seconds']
    record()
    print('COMPLETED',json.dumps({k:v for k,v in report.items() if k not in ('runs','conditions')},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
