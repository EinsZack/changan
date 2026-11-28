"""Real-Thor A/B benchmark for aerial mapping: single board vs multi-board collaborative v1/v2.
Runs on the Mac next to the management API. Creates ordinary tasks through POST /api/tasks,
one at a time (no two tasks share boards), interleaves configurations across repetitions so
board/thermal drift does not favour one arm, and records only task ids and API-reported times.
Stage breakdown afterwards: scripts/mapping_stage_breakdown.py on a read-only DB copy.

  python scripts/bench_mapping_thor.py --dataset dataset_f4653a052c984606 --preset standard \
      --submodel-size 6 --repeat 3 --output data/evidence/mapping-parallel-20261128/brighton.json
"""
import argparse,json,os,time
from pathlib import Path
import httpx

TERMINAL={'SUCCEEDED','FAILED','CANCELED','TIMED_OUT'}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--api',default='http://127.0.0.1:8310');ap.add_argument('--dataset',required=True)
    ap.add_argument('--preset',choices=['standard','preview'],required=True);ap.add_argument('--submodel-size',type=int,required=True)
    ap.add_argument('--overlap',type=float,default=25);ap.add_argument('--nodes',default='thor1,thor2,thor3')
    ap.add_argument('--arms',default='single,v1,v2');ap.add_argument('--repeat',type=int,default=3)
    ap.add_argument('--concurrency',type=int,default=4);ap.add_argument('--output',type=Path,required=True)
    a=ap.parse_args();nodes=a.nodes.split(',');arms=a.arms.split(',')
    http=httpx.Client(base_url=a.api,headers={'X-API-Key':os.getenv('CHANGAN_API_KEY','')},timeout=30,trust_env=False)
    def idle():
        while any(t['status'] not in TERMINAL for t in http.get('/api/tasks',params={'limit':50}).raise_for_status().json()):time.sleep(5)
    runs=json.loads(a.output.read_text()) if a.output.exists() else []
    a.output.parent.mkdir(parents=True,exist_ok=True)
    for r in range(a.repeat):
        for arm in arms[r%len(arms):]+arms[:r%len(arms)]:
            idle();time.sleep(10)   # let previous Pods finish cleanup and node snapshots settle
            body={'workflow_id':'aerial-mapping-odx','name':'多板建图实测 · %s · 第%d轮'%(arm,r+1),'source':a.dataset,'preset':a.preset,
                  'algorithm':'round_robin','concurrency':a.concurrency,'resolution_cm':10,'min_registered_ratio':0.9,'max_retries':0,
                  'deadline_seconds':3600,'attempt_timeout':2400}
            if arm=='single':body.update(execution_mode='single',node_ids=[nodes[r%len(nodes)]])
            else:body.update(execution_mode='collaborative',node_ids=nodes,max_nodes=len(nodes),submodel_size=a.submodel_size,split_overlap_m=a.overlap,pipeline_version=int(arm[1]))
            res=http.post('/api/tasks',json=body);res.raise_for_status();tid=res.json()['id'];print(arm,r+1,tid,flush=True)
            while (t:=http.get('/api/tasks/'+tid).raise_for_status().json())['status'] not in TERMINAL:time.sleep(3)
            row={'arm':arm,'round':r+1,'task_id':tid,'status':t['status'],'wall_seconds':round((t['finished'] or time.time())-t['created'],3),
                 'actual_nodes':sorted({x['actual_node'] for x in t['attempts'] if x.get('actual_node')}),'quality':{k:v for k,v in ((t.get('result') or {}).get('quality') or {}).items() if k!='corners'},'error':t.get('error')}
            runs.append(row);a.output.write_text(json.dumps(runs,ensure_ascii=False,indent=1));print(json.dumps(row,ensure_ascii=False),flush=True)

if __name__=='__main__':main()
