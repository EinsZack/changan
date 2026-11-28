import React,{useEffect,useState} from 'react';
import './mappingProgress.css';
import {computeWindows} from './mappingIntervals';
const labels={prepare:'空间分组',reconstruct:'稀疏重建',align:'统一对齐',toolchain:'地理成果',toolchain_v2:'对齐与地理成果',merge:'融合发布',photo_reconstruction:'整体重建',download:'下载与校验',upload:'上传成果',dataset:'照片准备',opensfm:'相机配准',openmvs:'稠密重建',odm_filterpoints:'点云滤波',odm_meshing:'网格生成',mvs_texturing:'纹理处理',odm_georeferencing:'地理参考',odm_dem:'高程处理',odm_orthophoto:'正射影像',odm_report:'质量报告',odm_postprocess:'成果整理',complete:'结果核验'};
const statuses={PENDING:'等待调度',BLOCKED:'等待前置成果',DISPATCHING:'正在调度',SUBMITTED:'已提交',POD_PENDING:'容器准备',IMAGE_PULLING:'拉取镜像',STARTING:'正在启动',RUNNING:'运行中',UPLOADING:'上传成果',FINALIZING:'核验结果',PULLING:'拉取镜像',CONTAINER_STARTING:'容器启动',SUCCEEDED:'成功',FAILED:'失败',CANCELED:'已取消',TIMED_OUT:'超时',UNKNOWN:'等待核对',CLEANING:'清理中'};
const terminal=s=>['SUCCEEDED','FAILED','CANCELED','TIMED_OUT'].includes(s);
const secs=n=>Number.isFinite(n)?`${n.toFixed(1)}s`:'—';
const clock=n=>new Date(n*1000).toLocaleTimeString();
const short=s=>s?.replace('submodel_','子模型 ')||'';
export default function MappingProgress({task:t,events,onOpenAttempt}){
 const [now,setNow]=useState(Date.now()/1000),[selected,setSelected]=useState(null);
 useEffect(()=>{if(terminal(t.status))return;const id=setInterval(()=>setNow(Date.now()/1000),1000);return()=>clearInterval(id)},[t.status]);
 const coop=t.params.execution_mode==='collaborative',end=t.finished||now,start=t.started||t.created;
 const attempts=t.attempts||[],units=(t.work_units||[]).map(u=>{
  const a=attempts.find(a=>a.id===u.active_attempt),last=events.filter(e=>e.attempt_id===a?.id&&e.type==='mapping_stage').at(-1);
  const missing=(u.dependencies||[]).filter(id=>!t.work_units.some(v=>v.id===id&&v.status==='SUCCEEDED'));
  const state=u.status==='PENDING'&&missing.length?'BLOCKED':u.status;
  if(u.status==='SUCCEEDED'&&last?.payload.state==='FINALIZING')return {...u,a,state,last:null,missing};
  return {...u,a,state,last,missing};
 });
 const successful=units.filter(u=>u.status==='SUCCEEDED').length,running=units.filter(u=>u.a&&!terminal(u.a.status)).length;
 const actualNodes=[...new Set(attempts.filter(a=>a.actual_node).map(a=>a.actual_node))];
 const plan=t.metadata?.mapping_plan,groups=plan?.submodels||[];
 // Pipeline v2 runs the deterministic global alignment inside every toolchain unit (no align Job).
 const fused=(t.metadata?.application?.pipeline_version||1)>=2;
 const phases=coop?(fused?['prepare','reconstruct','toolchain','merge']:['prepare','reconstruct','align','toolchain','merge']):['photo_reconstruction'];
 const phaseLabel=p=>fused&&p==='toolchain'?labels.toolchain_v2:labels[p];
 const chosen=units.find(u=>u.id===selected);
 const nodeNames=[...new Set(attempts.map(a=>a.actual_node||a.node_id).filter(Boolean))];
 const duration=Math.max(1,end-start);
 const compute=computeWindows(units,attempts,events,end);
 const intervals=attempts.filter(a=>a.started).flatMap(a=>[{at:a.started,d:1},{at:a.finished||end,d:-1}]).sort((a,b)=>a.at-b.at||a.d-b.d);
 let active=0,peak=0;for(const x of intervals){active+=x.d;peak=Math.max(peak,active)}
 return <section className="panel mapping-progress" aria-label="建图执行进程">
  <div className="mp-heading"><div><span className="eyebrow">{coop?'COLLABORATIVE MAPPING':'MAPPING EXECUTION'}</span><h2>{coop?'同图多节点协同进程':'建图执行进程'}</h2></div><span className={`mp-status ${t.status}`}>{statuses[t.status]||t.status}</span></div>
  <div className="mp-stats"><div><strong>{successful}<small> / {t.total}</small></strong><span>已核验工作单元</span></div><div><strong>{actualNodes.length}<small>{coop?` / 最多${t.params.max_nodes}`:''}</small></strong><span>实际参与节点</span></div><div><strong>{running}</strong><span>活动执行尝试</span></div><div><strong>{secs(end-start)}</strong><span>执行观测窗口</span></div></div>
  <div className="mp-completion" role="progressbar" aria-label="已核验工作单元完成比例" aria-valuenow={successful} aria-valuemin={0} aria-valuemax={t.total}><span style={{width:`${100*successful/Math.max(1,t.total)}%`}}/></div>
  <p className="note">完成比例按工作单元计数，不代表耗时百分比。{coop&&!plan?'分组完成后生成实际依赖图；不会预填子模型或节点。':'各阶段状态来自实际执行事件与成果校验。'}</p>
  <div className="mp-dag" aria-label="建图阶段依赖图">
   {phases.map((phase,i)=>{const children=units.filter(u=>(u.phase||u.kind)===phase);return <React.Fragment key={phase}>
    {i>0&&<div className="mp-arrow" aria-label="等待前置完成">→</div>}
    <div className="mp-column"><h3><b>{i+1}</b>{phaseLabel(phase)}</h3>{children.length?children.map(u=><button key={u.id} className={`mp-unit ${u.state} ${selected===u.id?'chosen':''}`} onClick={()=>setSelected(selected===u.id?null:u.id)} aria-pressed={selected===u.id}>
     <strong>{short(u.submodel)||labels[phase]}</strong><span className="mp-unit-state">{statuses[u.state]||u.state}</span><small>{u.a?.actual_node?`● ${u.a.actual_node}`:u.a?.node_id?`目标 ${u.a.node_id}`:'尚未分配节点'}</small>
     {u.last&&<small>{labels[u.last.payload.stage]||u.last.payload.stage} · {statuses[u.last.payload.state]||u.last.payload.state}</small>}
    </button>):<div className="mp-placeholder">等待生成工作单元</div>}</div>
   </React.Fragment>})}
  </div>
  {chosen&&<div className="mp-inspector"><div className="mp-heading"><strong>{short(chosen.submodel)} · {labels[chosen.phase||chosen.kind]} · {statuses[chosen.state]||chosen.state}</strong><button onClick={()=>setSelected(null)}>关闭详情</button></div><p>工作单元：{chosen.id}　{chosen.missing.length?`尚有 ${chosen.missing.length} 个前置单元未完成`:'前置依赖已满足'}</p><p>实际节点：{chosen.a?.actual_node||'尚未确认'}　Pod：{chosen.a?.workload?.pod_name||'尚未创建'}</p>{chosen.a?.error&&<p className="error">{chosen.a.error}</p>}{chosen.a&&onOpenAttempt&&<div className="button-row md-actions">
    <button onClick={()=>onOpenAttempt('dispatch',chosen.a.id)}>查看该单元回执</button>
    <button onClick={()=>onOpenAttempt('containers',chosen.a.id)}>查看该单元容器</button>
    {chosen.a.decision&&<button onClick={()=>onOpenAttempt('decision',chosen.a.id)}>查看该单元调度决策</button>}
   </div>}</div>}
  {!terminal(t.status)&&events.filter(e=>e.type==='no_candidate').at(-1)&&units.some(u=>u.state==='PENDING')&&<details className="mp-inspector"><summary>等待可行节点：查看最近一次候选筛选</summary>{events.filter(e=>e.type==='no_candidate').at(-1)?.payload?.decision?.candidates?.map(n=><p key={n.node_id}>{n.node_id}：{n.reasons.join('；')||'可选'}</p>)}</details>}
  {groups.length>0&&<div className="mp-groups"><h3>照片分组与重叠约束</h3><div className="mp-group-grid">{groups.map(g=><div key={g.id}><strong>{short(g.id)}</strong><b>{g.images.length} 张</b><small>包含与相邻子模型共享的照片</small></div>)}</div><p className="note">原始照片 {t.metadata.input.image_count} 张 · 重叠半径 {plan.overlap_m} m · {plan.overlap_edges.map(e=>`${short(e.source)} ↔ ${short(e.target)}：${e.images.length} 张共同照片`).join('；')}</p></div>}
  <div className="mp-heading"><h3>真实节点执行时间轴</h3><span className="note">峰值活动尝试：{peak}（含传输与核验；落点未确认时按目标节点展示）</span></div>
  {attempts.length?<div className="mp-timeline" role="group" aria-label="按已确认或目标节点展示执行尝试，横向为观测时间">
   <div className="mp-ticks"><span>{clock(start)}</span><span>+{secs(duration/2)}</span><span>+{secs(duration)}</span></div>
   {nodeNames.map(node=><div className="mp-node-row" key={node}><strong>{node}</strong><div className="mp-node-track">{attempts.filter(a=>(a.actual_node||a.node_id)===node).map(a=>{const unit=units.find(u=>u.id===a.segment_id),left=100*Math.max(0,a.created-start)/duration,width=100*Math.max(.2,(a.finished||end)-a.created)/duration;return <button key={a.id} onClick={()=>setSelected(a.segment_id)} className={`mp-bar ${a.status} phase-${unit?.phase||'single'}`} style={{left:`${left}%`,width:`${Math.max(.5,Math.min(100-left,width))}%`}} title={`${node} · ${labels[unit?.phase||unit?.kind]} ${short(unit?.submodel)} · ${statuses[a.status]||a.status}\n${clock(a.created)} — ${a.finished?clock(a.finished):'进行中'}\n实际节点${a.actual_node?'已确认':'未确认'}`}><span>{labels[unit?.phase||unit?.kind]} {short(unit?.submodel)}</span></button>})}</div></div>)}
  </div>:<p className="note">等待调度后显示真实节点时间轴。</p>}
  {coop&&<p className="mp-concurrency"><strong>跨节点同时计算：{secs(compute.overlap)}</strong> · 峰值 {compute.peak} 个实际节点。按稀疏重建和地理成果计算阶段的起止事件求交，不把镜像拉取或文件传输计为计算并行。</p>}
 </section>
}
