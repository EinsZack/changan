import React,{useState,useEffect,useRef} from 'react';
export function InferenceControls({request,value,onChange}) {
 const userSelected=useRef(false);
 const [profiles,setProfiles]=useState(null),[error,setError]=useState('');
 useEffect(()=>{let live=true;request('/api/execution-profiles').then(r=>{if(live){setProfiles(r.profiles);if(!value.inference_backend&&!userSelected.current)onChange('inference_backend',r.default)}}).catch(e=>{if(live)setError(e.message)});return()=>{live=false}},[]);
 const backend=value.inference_backend||'cpu';
 return <><label>推理后端<select aria-label="推理后端" value={backend} onChange={e=>{userSelected.current=true;onChange('inference_backend',e.target.value)}}><option value="cpu">CPU · ONNX Runtime</option><option value="cuda" disabled={profiles?.find(p=>p.id==='cuda')?.available===false}>NVIDIA GPU · CUDA</option></select></label>{backend==='cuda'&&<label>最低空闲显存 / ORT 内存池（MiB）<input aria-label="最低空闲显存" type="number" min="512" max="131072" step="1" required value={value.min_gpu_memory_mb??2048} onChange={e=>onChange('min_gpu_memory_mb',Number(e.target.value))}/></label>}{error&&<p className="error">执行配置读取失败：{error}</p>}</>
}
export function GpuResources({snapshot:s={}}) {
 if(!s.gpu_allocatable&&!s.gpu_uuid)return null;
 return <div className="gpu-resources"><strong>{s.gpu_name||'NVIDIA GPU'}</strong><dl><div><dt>CUDA 运行时</dt><dd>{s.gpu_runtime_ready?'已通过接入验证':'未就绪'}</dd></div><div><dt>可分配 GPU</dt><dd>{s.gpu_available??0} / {s.gpu_allocatable??0}</dd></div><div><dt>设备空闲显存</dt><dd>{s.gpu_telemetry_fresh?`${s.gpu_memory_free_mb} / ${s.gpu_memory_total_mb} MiB`:'观测缺失或过期'}</dd></div><div><dt>设备 GPU 利用率</dt><dd>{s.gpu_telemetry_fresh?`${s.gpu_utilization_percent}%`:'—'}</dd></div></dl><small>{s.gpu_uuid}</small>{s.gpu_registration_error&&<p className="error">{s.gpu_registration_error}</p>}<p className="note">{s.gpu_scope==='server-shared'?'共享服务器显卡：设备观测包含其他进程。':'设备级观测；由节点采集端提供。'}</p></div>
}
export function InferenceEvidence({value:v}) {
 if(!v)return <small>此记录没有后端运行证据</small>;
 return <div className="inference-evidence"><strong>{v.backend==='cuda'?'NVIDIA GPU / CUDA':'CPU / ONNX'}</strong><small>{v.provider} · ORT {v.ort_version}</small>{v.gpu&&<><small>{v.gpu.name}</small><small>{v.gpu.uuid}</small><small>实际 CUDA 计算事件：{v.gpu_compute_events} · 预热 {v.warmup_seconds?.toFixed(3)} 秒</small></>}<details><summary>推理运行证据</summary><pre>{JSON.stringify(v,null,2)}</pre></details></div>
}
