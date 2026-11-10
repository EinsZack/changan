import React from 'react';
import {backendName,finite,seconds,sessionPhases,segmentPhases,taskTiming} from './executionTiming';
import './executionTiming.css';

function Track({phases,origin,end}) {
  const span=Math.max(.001,end-origin);
  return <div className="timing-track">{phases.map((p,i)=><span key={i} className={'timing-bar timing-'+p.kind}
    style={{left:Math.max(0,(p.start-origin)/span*100)+'%',width:Math.min(100,Math.max(.15,p.seconds/span*100))+'%'}}
    title={p.name+' · '+seconds(p.seconds)+' · 轴起点后 '+seconds(p.start-origin)} aria-label={p.name+' '+seconds(p.seconds)}/>)}</div>;
}
export default function ExecutionTiming({task}) {
  const sessions=task.sessions||[], measured=sessions.filter(s=>s.workload?.timing_version===1);
  const summary=taskTiming(task);
  const rows=measured.map(s=>({s,...sessionPhases(s)}));
  const all=rows.flatMap(r=>r.phases);
  const end=Math.max(task.finished||task.updated||task.created,...all.map(p=>p.start+p.seconds));
  const origin=task.created;
  const attempts=(task.attempts||[]).map(a=>({...a,result:(task.segments||[]).find(s=>s.status==='SUCCEEDED'&&s.active_attempt===a.id)?.result})).filter(a=>a.result?.segment_timing?.version===1);
  return <section className="panel execution-timing" aria-label="启动与推理分段计时">
    <h2>启动与推理分段计时</h2>
    <p className="note">每个任务在参与节点创建一次容器，模型加载、预热后连续处理分片。启动不计入稳定前向耗时；多节点累计时长不能相加当作任务总耗时。</p>
    <div className="timing-summary"><div><small>任务端到端</small><strong>{task.finished?seconds(task.finished-task.created):'执行中'}</strong></div>
      <div><small>实际推理后端</small><strong>{summary.backends}</strong></div>
      <div><small>模型前向累计</small><strong>{seconds(summary.forward)}</strong></div>
      <div><small>模型前向 / 采样帧</small><strong>{finite(summary.perFrameMs)?summary.perFrameMs.toFixed(3)+' ms':'未采集'}</strong></div></div>
    <h3>节点容器启动时间轴</h3>
    {!measured.length?<p role="status">此任务未采集启动分段计时，历史总耗时仍保留。</p>:<>
      <div className="timing-legend">{[['control','提交与调度'],['image','镜像拉取'],['container','容器启动'],['imports','程序初始化'],['device','GPU初始化'],['model_load','模型加载'],['warmup','预热']].map(([k,v])=><span key={k}><i className={'timing-'+k}/>{v}</span>)}</div>
      <div className="table-scroll"><div className="timing-startup">{rows.map(({s,phases,life})=><div className="timing-startup-row" key={s.id}>
        <div><strong>{s.node_id}</strong><small>{backendName(s.workload?.inference_runtime)} · {s.ready_at?seconds(s.ready_at-s.created)+' 就绪':'等待就绪'}</small></div>
        <div><Track phases={phases} origin={origin} end={end}/><small>{life.image_cached?'镜像已缓存（未重新拉取）':finite(life.image_started_at)&&finite(life.image_ready_at)?'镜像拉取 '+seconds(life.image_ready_at-life.image_started_at):'镜像拉取计时未采集'} · 模型初始化总计 {seconds(s.model_load_seconds)}</small></div>
      </div>)}<div className="timing-axis"><span>提交 +0 s</span><span>+{seconds(end-origin)}</span></div></div></div>
      <details><summary>启动阶段明细与设备</summary>{rows.map(({s,phases})=><div key={s.id} className="timing-evidence"><strong>{s.node_id} · {backendName(s.workload?.inference_runtime)}</strong>
        <p>{s.workload?.inference_runtime?.gpu?.name} {s.workload?.inference_runtime?.gpu?.uuid}</p>
        {s.workload?.inference_runtime?.fallback_reason&&<p className="note caution">CUDA 未启用，回退 CPU：{s.workload.inference_runtime.fallback_reason}</p>}
        <p>CUDA 预热计算事件：{s.workload?.inference_runtime?.cuda_compute_events??'未采集'} · 模型加载次数：{s.model_load_count}</p>
        <div className="timing-stage-list">{phases.map((p,i)=><span key={i}>{p.name} <b>{seconds(p.seconds)}</b></span>)}</div>
      </div>)}</details>
    </>}
    <h3>分片处理时间轴</h3>
    {!attempts.length?<p>尚无已完成的分片分段计时。缺失值显示“未采集”。</p>:<div className="timing-segments">{attempts.map(a=>{
      const r=a.result,t=r.segment_timing,phases=segmentPhases(a),at=r.analysis_timing||{};
      const analysis=phases.find(p=>p.kind==='analyze');
      const forwards=(r.frames||[]).filter(f=>finite(f.timing?.inference_offset_seconds)&&finite(f.timing?.inference_seconds)).map(f=>({
        name:'第 '+f.frame+' 帧模型前向',kind:'forward',start:(analysis?.start||0)+f.timing.inference_offset_seconds,seconds:f.timing.inference_seconds
      }));
      return <details key={a.id}><summary>片段 {(task.segments.find(s=>s.id===a.segment_id)?.ordinal??0)+1} · {a.node_id} · {backendName(r.inference_runtime)} · 分片 {seconds(t.total_seconds)} · 前向 {seconds(at.inference_seconds)}</summary>
        <Track phases={phases} origin={t.started_at} end={t.started_at+t.total_seconds}/>
        <div className="timing-stage-list">{phases.map((p,i)=><span key={i}>{p.name} <b>{seconds(p.seconds)}</b></span>)}</div>
        <p className="note">视频分析内逐帧交替执行解码、前后处理和模型前向。下面仅绘制每帧前向的实际区间；空白包含其他操作。</p>
        {analysis&&<Track phases={forwards} origin={analysis.start} end={analysis.start+at.analysis_seconds}/>}
        <div className="timing-stage-list">{[['decode_seconds','解码'],['preprocess_seconds','预处理'],['inference_seconds','模型前向'],['postprocess_seconds','后处理'],['preview_seconds','预览生成'],['other_seconds','回报与其他']].map(([k,v])=><span key={k}>{v}累计 <b>{seconds(at[k])}</b></span>)}</div>
      </details>;
    })}</div>}
    <p className="note timing-method">计时口径：阶段时长使用单调时钟；时间轴用节点时间定位，跨物理机需同步时钟。CUDA 前向为 ONNX Runtime run 返回 CPU 数组的耗时，包含数据传入、GPU 计算和输出取回。分片总时长截止于结果提交前，确认回执及最终聚合另计。空白表示等待或未采集，未用估计值补齐。</p>
  </section>;
}
