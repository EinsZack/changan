export const finite = n => typeof n === 'number' && Number.isFinite(n) && n >= 0;
export const seconds = n => finite(n) ? n.toFixed(3) + ' s' : '未采集';
export const backendName = r => r?.actual_backend === 'cuda' ? 'GPU / CUDA' : r?.actual_backend === 'cpu' ? 'CPU' : '未采集';
export function sessionPhases(s) {
  const w = s.workload || {}, t = w.startup_timing || {};
  const life = Object.values(w.lifecycle || {}).find(p => p.pod_uid === w.pod_uid) || {};
  const phases = [];
  const add = (name, start, end, kind) => {
    if (finite(start) && finite(end) && end >= start) phases.push({name,start,seconds:end-start,kind});
  };
  add('提交容器', w.job_submit_started_at, w.job_submit_finished_at, 'control');
  add('等待调度', life.pod_created_at, life.scheduled_at, 'control');
  if (!life.image_cached) add('镜像拉取', life.image_started_at, life.image_ready_at, 'image');
  add('容器创建与启动', life.image_ready_at, life.container_started_at, 'container');
  add('进程启动', life.container_started_at, t.entry_at, 'container');
  add('程序初始化', t.entry_at, finite(t.entry_at)&&finite(t.imports_seconds)?t.entry_at+t.imports_seconds:null, 'imports');
  add('领取会话', t.claim_started_at, finite(t.claim_started_at)&&finite(t.claim_seconds)?t.claim_started_at+t.claim_seconds:null, 'control');
  for (const p of w.inference_runtime?.initialization_stages || []) {
    const name = {device:'GPU初始化',model_load:'模型加载',warmup:'模型预热'}[p.name];
    if (name) add(name,p.started_at,finite(p.started_at)&&finite(p.seconds)?p.started_at+p.seconds:null,p.name);
  }
  return {phases, life};
}
export function segmentPhases(a) {
  const t = a.result?.segment_timing;
  if (t?.version !== 1 || !finite(t.started_at)) return [];
  return (t.phases || []).filter(p=>finite(p.offset_seconds)&&finite(p.seconds)).map(p=>({
    name:{download:'下载分片',analyze:'视频分析',upload:'上传产物'}[p.name]||p.name,
    kind:p.name,start:t.started_at+p.offset_seconds,seconds:p.seconds
  }));
}
export function taskTiming(task) {
  const results = (task.segments || []).filter(s=>s.status==='SUCCEEDED'&&s.result?.segment_timing?.version===1&&
    s.result?.attempt_id===s.active_attempt).map(s=>s.result);
  const values = key => results.map(r=>r.analysis_timing?.[key]);
  const sum = key => results.length && values(key).every(finite) ? values(key).reduce((a,b)=>a+b,0) : null;
  const frames = results.reduce((n,r)=>n+(r.sampled_frames||0),0), forward=sum('inference_seconds');
  const runtimes = results.map(r=>r.inference_runtime).filter(Boolean);
  return {measured:results.length, frames, forward,
    perFrameMs:frames&&finite(forward)?forward/frames*1000:null,
    analysis:sum('analysis_seconds'),decode:sum('decode_seconds'),
    preprocess:sum('preprocess_seconds'),postprocess:sum('postprocess_seconds'),
    backends:[...new Set(runtimes.map(backendName))].join(' / ')||'未采集'};
}
