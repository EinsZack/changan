import time, math
from .config import MODEL_ID
from .network_metrics import positive
from .pool_bridge import requirement_reasons

def choose(nodes, params, duration, input_bytes, algorithm, cursor=0):
    began=time.monotonic(); now=time.time(); rows=[]
    weights=algorithm.get('weights',{})
    for n in nodes:
        s=dict(n.get('snapshot') or {}); why=[]
        if n.get('error'): why.append(n['error'])
        if not n.get('enabled'): why.append('节点已停用')
        if n.get('session_blocked'):why.append(n['session_blocked'])
        if now-(n.get('observed') or 0)>8: why.append('资源快照过期或节点失联')
        if not s.get('healthy',False): why.append('执行器不健康')
        if s.get('node_id')!=n['id']: why.append('实际节点标识不匹配')
        if params.get('node_ids') and n['id'] not in params['node_ids']: why.append('不在指定节点范围')
        mapping=params.get('application_id')=='aerial-mapping-odx'
        if mapping:
            if s.get('architecture')!='arm64':why.append('建图镜像要求arm64')
            if (s.get('cpu_cores') or 0)<params.get('concurrency',4):why.append('建图CPU资源不足')
            if (s.get('ephemeral_storage_gb') or 0)<params.get('workspace_gb',32):why.append('建图临时存储预算不足或未知')
        if not mapping and MODEL_ID not in s.get('models',[]): why.append('模型不兼容')
        if params.get('location') and params['location']!=s.get('location'): why.append('节点位置不匹配')
        if params.get('architecture') and params['architecture']!=s.get('architecture'): why.append('处理器架构不匹配')
        if not set(params.get('required_tags',[])).issubset(s.get('tags',[])): why.append('缺少必要标签')
        if s.get('available_memory_mb',0)<max(512 if 'kubernetes' in s.get('resource_source','') else 0,params.get('min_memory_mb',256)): why.append('可用内存不足')
        if max(s.get('active',0),n.get('reserved',0))>=s.get('capacity',1): why.append('执行槽已占用或预留')
        if not s.get('input_reachable',True): why.append('输入通道不可达')
        pool=n.get('pool')
        if pool:
            # Stage 1 (resource pool): management eligibility + task requirements; enforced only in enforce mode.
            pool=dict(pool,reasons=list(pool.get('reasons',[]))+requirement_reasons(pool,params),enforced=pool.get('mode')=='enforce')
            if pool['enforced']:why.extend(pool['reasons'])
        fps=s.get('measured_fps') or 8.0
        compute=duration*params.get('sample_fps',2)/max(fps,0.1)
        bandwidth=positive(s.get('bandwidth_mbps'))
        expires=s.get('network_expires_at')
        if expires is not None and (positive(expires) is None or expires<=now):
            bandwidth=None
            s['network_source']='网络观测已过期，等待新的下载实测或资源池快照'
        s['bandwidth_mbps']=bandwidth
        transfer=input_bytes*8/(bandwidth*1e6) if bandwidth else 0
        queue=s.get('active',0)*compute
        score=compute*weights.get('compute',1)+transfer*weights.get('transfer',1)+queue*weights.get('queue',1)
        if mapping:
            compute=None;score=(1-s.get('available_memory_mb',0)/max(1,s.get('memory_budget_mb',1)))+float(s.get('active',0))+(transfer/60 if bandwidth else 0)
        rows.append({'node_id':n['id'],'cached_input_bytes':n.get('cached_input_bytes',0),'eligible':not why,'reasons':why,'score':round(score,4),'compute_seconds':round(compute,4) if compute is not None else None,'transfer_seconds':round(transfer,4) if bandwidth else None,'estimate_source':'建图：内存/占用及传输评分；计算耗时尚未校准' if mapping else '实测推理吞吐' if s.get('measured_fps') else '启动估计，尚未校准','network_source':s.get('network_source','未提供，传输成本暂不参与'),'snapshot':s,'snapshot_at':n.get('observed'),'pool':pool})
    eligible=[r for r in rows if r['eligible']]
    locality=None
    if eligible and params.get('application_id')=='aerial-mapping-odx':
        # Data locality for the mapping DAG: an eligible node already holding this unit's
        # stage archives skips a multi-hundred-MB transfer through the management plane.
        best=max(r['cached_input_bytes'] for r in eligible)
        if best>=32*1024**2:
            eligible=[r for r in eligible if r['cached_input_bytes']==best]
            locality={'cached_input_bytes':best,'nodes':[r['node_id'] for r in eligible],'rule':'优先已缓存本单元输入归档的合格节点；缓存仅为提示，执行端仍校验SHA256'}
    chosen=None
    if eligible:
        if algorithm.get('method')=='round_robin': chosen=sorted(eligible,key=lambda r:r['node_id'])[cursor%len(eligible)]
        else: chosen=min(eligible,key=lambda r:(r['score'],r['node_id']))
    return {'algorithm':algorithm,'candidates':rows,'locality':locality,'target_node':chosen['node_id'] if chosen else None,'duration_ms':(time.monotonic()-began)*1000,'at':now,'estimate_only':True}
