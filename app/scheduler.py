import time, math
from .config import MODEL_ID

def choose(nodes, params, duration, input_bytes, algorithm, cursor=0):
    began=time.monotonic(); now=time.time(); rows=[]
    weights=algorithm.get('weights',{})
    for n in nodes:
        s=n.get('snapshot') or {}; why=[]
        if n.get('error')=='资源池快照不可用': why.append('资源池快照不可用')
        if not n.get('enabled'): why.append('节点已停用')
        if now-(n.get('observed') or 0)>8: why.append('资源快照过期或节点失联')
        if not s.get('healthy',False): why.append('执行器不健康')
        if s.get('node_id')!=n['id']: why.append('实际节点标识不匹配')
        if params.get('node_ids') and n['id'] not in params['node_ids']: why.append('不在指定节点范围')
        if MODEL_ID not in s.get('models',[]): why.append('模型不兼容')
        if params.get('location') and params['location']!=s.get('location'): why.append('节点位置不匹配')
        if params.get('architecture') and params['architecture']!=s.get('architecture'): why.append('处理器架构不匹配')
        if not set(params.get('required_tags',[])).issubset(s.get('tags',[])): why.append('缺少必要标签')
        if s.get('available_memory_mb',0)<params.get('min_memory_mb',256): why.append('可用内存不足')
        if max(s.get('active',0),n.get('reserved',0))>=s.get('capacity',1): why.append('执行槽已占用或预留')
        if not s.get('input_reachable',True): why.append('输入通道不可达')
        fps=s.get('measured_fps') or 8.0
        compute=duration*params.get('sample_fps',2)/max(fps,0.1)
        bandwidth=s.get('bandwidth_mbps')
        transfer=input_bytes*8/(bandwidth*1e6) if bandwidth else 0
        queue=s.get('active',0)*compute
        score=compute*weights.get('compute',1)+transfer*weights.get('transfer',1)+queue*weights.get('queue',1)
        rows.append({'node_id':n['id'],'eligible':not why,'reasons':why,'score':round(score,4),'compute_seconds':round(compute,4),'transfer_seconds':round(transfer,4) if bandwidth else None,'estimate_source':'实测推理吞吐' if s.get('measured_fps') else '启动估计，尚未校准','network_source':s.get('network_source','未提供，传输成本暂不参与'),'snapshot':s,'snapshot_at':n.get('observed')})
    eligible=[r for r in rows if r['eligible']]
    chosen=None
    if eligible:
        if algorithm.get('method')=='round_robin': chosen=sorted(eligible,key=lambda r:r['node_id'])[cursor%len(eligible)]
        else: chosen=min(eligible,key=lambda r:(r['score'],r['node_id']))
    return {'algorithm':algorithm,'candidates':rows,'target_node':chosen['node_id'] if chosen else None,'duration_ms':(time.monotonic()-began)*1000,'at':now,'estimate_only':True}
