"""Validation and preservation of measured startup/segment evidence (v1)."""
import math
from fastapi import HTTPException

def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0

def require(condition, message):
    if not condition:
        raise HTTPException(422, message)

def validate_runtime(runtime, workload):
    require(isinstance(runtime, dict), '缺少实际推理后端')
    require(runtime.get('policy') == workload.get('model_policy', 'cpu'), '模型策略不一致')
    backend = runtime.get('actual_backend')
    require(backend in ('cpu', 'cuda'), '实际推理后端无效')
    require(isinstance(runtime.get('providers'), list) and bool(runtime['providers']), '缺少执行提供程序')
    require(runtime['providers'][0] == ('CUDAExecutionProvider' if backend == 'cuda' else 'CPUExecutionProvider'), '执行提供程序不一致')
    timings = runtime.get('timings')
    require(isinstance(timings, dict) and all(number(v) for v in timings.values()), '模型计时无效')
    require(all(k in timings for k in ('initialization_seconds','session_seconds','warmup_seconds','verification_seconds')), '模型分段计时不完整')
    require(sum(timings[k] for k in timings if k != 'initialization_seconds') <= timings['initialization_seconds'] + .05, '模型计时超出初始化时长')
    stages = runtime.get('initialization_stages')
    require(isinstance(stages, list) and len(stages) <= 12, '模型阶段证据无效')
    require(all(isinstance(p, dict) and p.get('name') in ('device','model_load','warmup')
                and number(p.get('started_at')) and number(p.get('seconds')) for p in stages), '模型阶段计时无效')
    if backend == 'cuda':
        require(bool(workload.get('gpu_access')), '容器未声明GPU访问')
        require(number(runtime.get('cuda_compute_events')) and runtime['cuda_compute_events'] > 0, '缺少实际CUDA计算证据')
        gpu = runtime.get('gpu')
        require(isinstance(gpu, dict) and isinstance(gpu.get('uuid'), str) and gpu['uuid'].startswith('GPU-'), '缺少GPU身份')
        require(gpu.get('device_id') == workload.get('model_device_id', 0), 'GPU索引不一致')
        require(not runtime.get('fallback_reason'), 'CUDA结果不能同时声明回退')
    elif runtime['policy'] == 'auto':
        require(isinstance(runtime.get('fallback_reason'), str) and bool(runtime['fallback_reason']), '自动回退缺少原因')
    require(runtime['policy'] != 'cuda' or backend == 'cuda', '强制CUDA不允许静默回退')

def validate_startup(startup, seconds):
    require(isinstance(startup, dict), '缺少程序启动计时')
    keys = ('entry_at','imports_seconds','claim_started_at','claim_seconds','model_started_at','model_ready_at','model_load_seconds')
    require(all(number(startup.get(k)) for k in keys), '程序启动计时无效')
    require(startup['entry_at'] <= startup['claim_started_at'] <= startup['model_started_at'] <= startup['model_ready_at'], '程序启动阶段顺序无效')
    require(abs(startup['model_load_seconds'] - seconds) < .001, '模型计时不一致')

def validate_segment(result, runtime):
    require(result.get('inference_runtime') == runtime, '分片与会话实际后端不一致')
    t = result.get('segment_timing')
    require(isinstance(t, dict) and t.get('version') == 1, '缺少分片分段计时')
    require(number(t.get('started_at')) and number(t.get('total_seconds')), '分片计时无效')
    phases = t.get('phases')
    require(isinstance(phases, list) and all(isinstance(p,dict) for p in phases) and [p.get('name') for p in phases] == ['download','analyze','upload'], '分片阶段不完整')
    end = 0.
    for p in phases:
        require(number(p.get('offset_seconds')) and number(p.get('seconds')), '分片阶段计时无效')
        require(p['offset_seconds'] + .001 >= end, '分片阶段计时重叠')
        end = p['offset_seconds'] + p['seconds']
    require(end <= t['total_seconds'] + .001, '分片阶段超出总时长')
    a = result.get('analysis_timing')
    keys = ('decode_seconds','preprocess_seconds','inference_seconds','postprocess_seconds','preview_seconds','other_seconds','analysis_seconds','analysis_started_at')
    require(isinstance(a, dict) and all(number(a.get(k)) for k in keys), '分析计时无效')
    require(abs(a['analysis_seconds'] - phases[1]['seconds']) < .1, '分析阶段时长不一致')
    require(abs(a['analysis_seconds'] - sum(a[k] for k in keys[:6])) < .01, '分析阶段累积时长不一致')
    forward = 0.; previous = 0.
    for frame in result.get('frames', []):
        require(isinstance(frame,dict), '逐帧结果无效')
        ft = frame.get('timing', {})
        require(isinstance(ft,dict), '逐帧计时无效')
        require(all(number(ft.get(k)) for k in ('inference_offset_seconds','inference_seconds','preprocess_seconds','postprocess_seconds')), '逐帧计时缺失或无效')
        start = ft['inference_offset_seconds']; stop = start + ft['inference_seconds']
        require(start + .001 >= previous and stop <= a['analysis_seconds'] + .001, '逐帧前向区间无效')
        forward += ft['inference_seconds']; previous = stop
    require(abs(forward - a['inference_seconds']) < .001, '逐帧前向与累计计时不一致')

def stamp(value):
    return value.timestamp() if value is not None else None

def pod_lifecycle(pod, events=()):
    """Unknown intervals stay absent. A cache hit is never called an image download."""
    m = pod.metadata; status = pod.status
    result = {'pod_uid': m.uid, 'pod_name': m.name, 'node_name': pod.spec.node_name}
    if getattr(m, 'creation_timestamp', None):
        result['pod_created_at'] = stamp(m.creation_timestamp)
    for condition in getattr(status, 'conditions', None) or []:
        if condition.type == 'PodScheduled' and condition.status == 'True':
            result['scheduled_at'] = stamp(condition.last_transition_time)
    for container in status.container_statuses or []:
        state = container.state
        running = getattr(state, 'running', None)
        terminated = getattr(state, 'terminated', None)
        started = getattr(running or terminated, 'started_at', None)
        if started:
            result['container_started_at'] = stamp(started)
        if terminated:
            result['termination'] = {'reason':getattr(terminated,'reason',None),
                                     'exit_code':getattr(terminated,'exit_code',None),
                                     'finished_at':stamp(getattr(terminated,'finished_at',None))}
    observed = []
    for event in events:
        if event.reason not in ('Pulling','Pulled','Created','Started','Failed','FailedScheduling','BackOff'):
            continue
        at = (getattr(event, 'first_timestamp', None) or getattr(event, 'event_time', None)
              or getattr(event.metadata, 'creation_timestamp', None))
        if at is None:
            continue
        row = {'reason': event.reason, 'at': stamp(at), 'message': (event.message or '')[:400]}
        observed.append(row)
    if observed:
        result['events'] = sorted(observed, key=lambda e: e['at'])
        pulling = [e['at'] for e in observed if e['reason'] == 'Pulling']
        pulled = [e for e in observed if e['reason'] == 'Pulled']
        if pulling:
            result['image_started_at'] = min(pulling)
        if pulled:
            result['image_ready_at'] = max(e['at'] for e in pulled)
            result['image_cached'] = not pulling and any('already present' in e['message'].lower() for e in pulled)
    return result
