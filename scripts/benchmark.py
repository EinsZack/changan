import httpx,time,json,statistics
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
c=httpx.Client(base_url='http://127.0.0.1:8310',trust_env=False,timeout=15)
assert not any(t['status'] not in ['SUCCEEDED','FAILED','CANCELED','TIMED_OUT'] for t in c.get('/api/tasks').json()), '基线测试前必须等待其他任务结束'
results=[]
for round in range(3):
    for name,algorithm,nodes in [('单执行器','round_robin',['lab-1']),('四执行器轮询','round_robin',[]),('四执行器资源感知','resource_aware',[])]:
        response=c.post('/api/tasks',json={'name':f'基线对比 {round+1} · {name}','algorithm':algorithm,'node_ids':nodes});response.raise_for_status()
        task=c.get('/api/tasks/'+response.json()['id']).json()
        started=time.monotonic()
        while not task['finished'] and time.monotonic()-started<120:
            time.sleep(.25);task=c.get('/api/tasks/'+task['id']).json()
        assert task['status']=='SUCCEEDED',task
        results.append({'round':round+1,'name':name,'id':task['id'],'elapsed_seconds':task['finished']-task['created'],'counts':task['result']['counts'],'sha256':task['metadata']['sha256'],'sampled_frames':task['result']['sampled_frames']})
        print(results[-1],flush=True)
summary={name:{'n':3,'mean_seconds':statistics.mean(r['elapsed_seconds'] for r in results if r['name']==name),'min_seconds':min(r['elapsed_seconds'] for r in results if r['name']==name),'max_seconds':max(r['elapsed_seconds'] for r in results if r['name']==name)} for name in ['单执行器','四执行器轮询','四执行器资源感知']}
report={'at':time.time(),'scope':'同一服务器，CPU ONNX，固定24秒视频，2fps，4秒片段，阈值0.35，各三次顺序执行；不含车端网络且不代表验收性能','runs':results,'summary':summary,'identical_counts':all(r['counts']==results[0]['counts'] for r in results),'identical_input':len(set(r['sha256'] for r in results))==1}
(ROOT/'docs/benchmark-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps(summary,ensure_ascii=False),flush=True)
