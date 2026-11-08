"""YOLOX ONNX inference, following Apache-2.0 YOLOX pre/postprocessing."""
import cv2, numpy as np, onnxruntime as ort, time, hashlib, json, os, tempfile
from collections import Counter
from pathlib import Path
LABELS={0:'person',1:'bicycle',2:'car',3:'motorcycle',5:'bus',7:'truck'}
class Detector:
    def __init__(self, path, backend='cpu', gpu_memory_mb=2048, expected_gpu_uuid=None):
        if backend not in ('cpu','cuda'):raise ValueError('不支持的推理后端')
        self.backend=backend;self.inference_seconds=0.0;self.inference_calls=0
        provider='CUDAExecutionProvider' if backend=='cuda' else 'CPUExecutionProvider'
        if backend=='cuda':
            if hasattr(ort,'preload_dlls'):ort.preload_dlls()
            if provider not in ort.get_available_providers():raise RuntimeError('当前镜像不支持 CUDAExecutionProvider；禁止静默回退 CPU')
            from .gpu import device_identity
            self.gpu=device_identity()
            if expected_gpu_uuid and self.gpu['uuid']!=expected_gpu_uuid:raise RuntimeError('实际 GPU UUID 与分配节点不一致')
        else:self.gpu=None
        options=ort.SessionOptions();options.intra_op_num_threads=2;options.inter_op_num_threads=1
        options.enable_profiling=True
        options.profile_file_prefix=os.path.join(tempfile.gettempdir(),'changan-ort-'+str(os.getpid()))
        providers=[(provider,{'device_id':0,'gpu_mem_limit':int(gpu_memory_mb)*1048576,'arena_extend_strategy':'kSameAsRequested','cudnn_conv_algo_search':'HEURISTIC','use_tf32':0})] if backend=='cuda' else [provider]
        self.session=ort.InferenceSession(str(path),sess_options=options,providers=providers)
        self.session.disable_fallback()
        if self.session.get_providers()[0]!=provider:raise RuntimeError('推理后端加载失败；禁止静默回退 CPU')
        self.input=self.session.get_inputs()[0];self.size=int(self.input.shape[-1])
        self.sha256=hashlib.sha256(Path(path).read_bytes()).hexdigest()
        # Warmup proves actual graph placement, beyond simply listing an available EP.
        began=time.perf_counter()
        self.session.run(None,{self.input.name:np.zeros((1,3,self.size,self.size),dtype=np.float32)})
        self.warmup_seconds=time.perf_counter()-began
        profile=Path(self.session.end_profiling())
        try:events=json.loads(profile.read_text())
        finally:profile.unlink(missing_ok=True)
        counts=Counter(e.get('args',{}).get('provider') for e in events if e.get('cat')=='Node' and e.get('args',{}).get('provider'))
        gpu_compute=sum(1 for e in events if e.get('args',{}).get('provider')=='CUDAExecutionProvider' and not e.get('args',{}).get('op_name','').startswith('Memcpy'))
        if backend=='cuda' and not gpu_compute:raise RuntimeError('未观察到实际 CUDA 图计算，拒绝把 CPU 执行记作 GPU')
        self.execution={'backend':backend,'provider':provider,'providers':self.session.get_providers(),
                        'ort_version':ort.__version__,'gpu':self.gpu,'warmup_seconds':self.warmup_seconds,
                        'warmup_provider_events':dict(counts),'gpu_compute_events':gpu_compute,
                        'gpu_arena_limit_mb':gpu_memory_mb if backend=='cuda' else None,
                        'cpu_fallback_policy':'CPU shape/helper ops allowed; whole-model fallback forbidden' if backend=='cuda' else 'explicit CPU',
                        'model_sha256':self.sha256}
        self.grid=np.concatenate([np.stack(np.meshgrid(np.arange(self.size//s),np.arange(self.size//s)),axis=-1).reshape(-1,2) for s in [8,16,32]],axis=0)
        self.strides=np.concatenate([np.full(((self.size//s)**2,1),s) for s in [8,16,32]])
    def detect(self, frame, threshold):
        h,w=frame.shape[:2]; ratio=min(self.size/h,self.size/w)
        resized=cv2.resize(frame,(int(w*ratio),int(h*ratio)))
        canvas=np.full((self.size,self.size,3),114,dtype=np.uint8)
        canvas[:resized.shape[0],:resized.shape[1]]=resized
        tensor=np.ascontiguousarray(canvas.transpose(2,0,1),dtype=np.float32)[None]
        began=time.perf_counter()
        pred=self.session.run(None,{self.input.name:tensor})[0][0].copy()
        self.inference_seconds+=time.perf_counter()-began;self.inference_calls+=1
        pred[:,:2]=(pred[:,:2]+self.grid)*self.strides
        pred[:,2:4]=np.exp(np.clip(pred[:,2:4],-10,10))*self.strides
        xy1=(pred[:,:2]-pred[:,2:4]/2)/ratio; xy2=(pred[:,:2]+pred[:,2:4]/2)/ratio
        scores=pred[:,4,None]*pred[:,5:]; results=[]
        for cls,label in LABELS.items():
            mask=scores[:,cls]>=threshold
            boxes=np.concatenate([xy1[mask],xy2[mask]-xy1[mask]],axis=1)
            conf=scores[mask,cls]
            if len(boxes)==0: continue
            indices=cv2.dnn.NMSBoxes(boxes.tolist(),conf.tolist(),threshold,0.45)
            for i in np.asarray(indices).reshape(-1):
                x,y,bw,bh=boxes[i]
                results.append({'label':label,'confidence':round(float(conf[i]),4),'box':[round(float(max(0,x)),1),round(float(max(0,y)),1),round(float(min(w,x+bw)),1),round(float(min(h,y+bh)),1)]})
        return results

def analyze(detector, video, outdir, params, progress, canceled):
    cap=cv2.VideoCapture(str(video))
    if not cap.isOpened(): raise ValueError('视频无法解码')
    fps=cap.get(cv2.CAP_PROP_FPS); total=int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    step=max(1,round(fps/params['sample_fps'])); records=[]; counts={v:0 for v in LABELS.values()}
    started=time.monotonic(); frame_index=0; previews=[]; frame=None
    inference_started=detector.inference_seconds
    try:
        while True:
            if canceled(): raise InterruptedError('任务已取消')
            ok,frame=cap.read()
            if not ok: break
            if frame_index%step==0:
                detections=detector.detect(frame,params['confidence'])
                for d in detections: counts[d['label']]+=1
                records.append({'frame':frame_index,'seconds':round(frame_index/fps,4),'detections':detections})
                if len(previews)<3 and (detections or len(records)==1):
                    annotated=frame.copy()
                    for d in detections:
                        x1,y1,x2,y2=map(int,d['box']); cv2.rectangle(annotated,(x1,y1),(x2,y2),(50,210,120),2)
                        cv2.putText(annotated,f"{d['label']} {d['confidence']:.2f}",(x1,max(15,y1-5)),cv2.FONT_HERSHEY_SIMPLEX,.45,(50,210,120),1)
                    file=outdir/f'preview-{len(previews)}.jpg'; cv2.imwrite(str(file),annotated); previews.append(file.name)
                progress(min((frame_index+1)/max(total,1),.99),len(records))
            frame_index+=1
    finally: cap.release()
    if not records: raise ValueError('未解码到有效采样帧')
    elapsed=time.monotonic()-started
    inference_seconds=detector.inference_seconds-inference_started
    return {'inference':detector.execution,'inference_seconds':inference_seconds,'model_inference_fps':len(records)/max(inference_seconds,.000001),'frames':records,'counts':counts,'sampled_frames':len(records),'decoded_frames':frame_index,'video_fps':fps,'sample_step_frames':step,'elapsed_seconds':elapsed,'measured_fps':len(records)/max(elapsed,.001),'previews':previews,'model_sha256':detector.sha256,'counting_rule':'采样帧目标出现次数，同一对象跨帧可能重复，不是唯一人数/车辆数'}
