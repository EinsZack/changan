import React,{useEffect,useState} from 'react';
import {taskTiming,seconds,finite} from './executionTiming';
export default function TimingComparison({first,second,request}) {
  const [data,setData]=useState(null),[error,setError]=useState('');
  useEffect(()=>{
    let active=true;setData(null);setError('');
    Promise.all([request('/api/tasks/'+first),request('/api/tasks/'+second)]).then(v=>{if(active)setData(v)}).catch(e=>{if(active)setError(e.message)});
    return ()=>{active=false};
  },[first,second,request]);
  if(error)return <p className="note">分段对比暂不可用：{error}</p>;
  if(!data)return <p className="note">正在读取分段计时…</p>;
  const [a,b]=data.map(taskTiming);
  const startup=t=>t.sessions?.length&&t.sessions.every(s=>finite(s.model_load_seconds))?Math.max(...t.sessions.map(s=>s.model_load_seconds)):null;
  const rows=[['实际后端',a.backends,b.backends],
    ['最长节点模型初始化（含预热）',seconds(startup(data[0])),seconds(startup(data[1]))],
    ['视频分析累计',seconds(a.analysis),seconds(b.analysis)],
    ['模型前向累计',seconds(a.forward),seconds(b.forward)],
    ['模型前向 / 采样帧',finite(a.perFrameMs)?a.perFrameMs.toFixed(3)+' ms':'未采集',finite(b.perFrameMs)?b.perFrameMs.toFixed(3)+' ms':'未采集']];
  return <><h3>启动与推理对比</h3><div className="table-scroll"><table aria-label="启动与推理对比"><thead><tr><th>分段实测项</th><th>基线运行</th><th>对比运行</th></tr></thead><tbody>{rows.map(r=><tr key={r[0]}>{r.map((v,i)=><td key={i}>{v}</td>)}</tr>)}</tbody></table></div><p className="note">累计耗时可跨并发节点相加，仅用于成本与帧均值分析；任务端到端耗时以提交到聚合完成计。旧记录缺少分段证据时显示“未采集”。</p></>;
}
