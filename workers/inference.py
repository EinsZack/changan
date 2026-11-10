"""YOLOX ONNX inference with explicit provider evidence and monotonic timings."""
import ctypes
import hashlib
import json
import tempfile
import time
import uuid
from collections import Counter
from pathlib import Path
import cv2
import numpy as np
import onnxruntime as ort

LABELS = {0:'person', 1:'bicycle', 2:'car', 3:'motorcycle', 5:'bus', 7:'truck'}


def gpu_identity(device_id):
    lib = ctypes.CDLL('libcuda.so.1')
    def call(name, *args):
        status = getattr(lib, name)(*args)
        if status:
            raise RuntimeError(f'{name}: CUDA error {status}')
    call('cuInit', 0)
    count = ctypes.c_int()
    call('cuDeviceGetCount', ctypes.byref(count))
    if not 0 <= device_id < count.value:
        raise RuntimeError(f'GPU index {device_id} unavailable; visible count {count.value}')
    device = ctypes.c_int()
    call('cuDeviceGet', ctypes.byref(device), device_id)
    raw = (ctypes.c_ubyte * 16)()
    call('cuDeviceGetUuid', ctypes.byref(raw), device)
    name = ctypes.create_string_buffer(256)
    call('cuDeviceGetName', name, 256, device)
    return {'device_id': device_id, 'uuid': 'GPU-' + str(uuid.UUID(bytes=bytes(raw))),
            'name': name.value.decode(), 'visible_devices': count.value}


class Detector:
    def __init__(self, path, policy='cpu', device_id=0, instrument=False):
        if policy not in ('auto', 'cpu', 'cuda'):
            raise ValueError('Unknown model device policy')
        began = time.perf_counter()
        self.sha256 = hashlib.sha256(Path(path).read_bytes()).hexdigest()
        self.runtime = {'policy': policy, 'actual_backend': 'cpu', 'ort_version': ort.__version__,
                        'gpu': None, 'fallback_reason': None, 'cuda_compute_events': 0,
                        'provider_events': {}, 'timings': {}, 'initialization_stages': []}
        candidate = 'cuda' if policy != 'cpu' else 'cpu'
        if candidate == 'cuda':
            gpu_at = time.time(); gpu_began = time.perf_counter()
            try:
                if 'CUDAExecutionProvider' not in ort.get_available_providers():
                    raise RuntimeError('ONNX Runtime 未提供 CUDAExecutionProvider')
                self.runtime['gpu'] = gpu_identity(device_id)
                ort.preload_dlls()
                self.runtime['initialization_stages'].append({'name':'device','started_at':gpu_at,'seconds':time.perf_counter()-gpu_began})
                self._load(path, 'cuda', device_id, True)
            except Exception as error:
                # Auto fallback is explicit evidence, never labelled as CUDA success.
                if policy == 'cuda':
                    raise RuntimeError('CUDA initialization failed: ' + str(error)) from error
                self.runtime['fallback_reason'] = type(error).__name__ + ': ' + str(error)[:500]
                self.runtime['gpu'] = None
                self.runtime['timings']['cuda_attempt_seconds'] = time.perf_counter() - gpu_began
                self._load(path, 'cpu', device_id, instrument)
        else:
            self._load(path, 'cpu', device_id, instrument)
        self.input = self.session.get_inputs()[0]
        self.size = int(self.input.shape[-1])
        self.grid = np.concatenate([np.stack(np.meshgrid(np.arange(self.size//s), np.arange(self.size//s)), axis=-1).reshape(-1,2) for s in [8,16,32]], axis=0)
        self.strides = np.concatenate([np.full(((self.size//s)**2,1),s) for s in [8,16,32]])
        self.runtime['timings']['initialization_seconds'] = time.perf_counter() - began

    def _load(self, path, backend, device_id, instrument):
        options = ort.SessionOptions()
        options.intra_op_num_threads = 2
        options.inter_op_num_threads = 1
        # Profile only one warmup, not every production frame.
        profile_dir = tempfile.TemporaryDirectory(prefix='changan-ort-') if backend == 'cuda' else None
        if profile_dir:
            options.enable_profiling = True
            options.profile_file_prefix = str(Path(profile_dir.name) / 'warmup')
        provider = 'CUDAExecutionProvider' if backend == 'cuda' else 'CPUExecutionProvider'
        providers = [(provider, {'device_id': device_id, 'gpu_mem_limit': 1024**3,
                      'arena_extend_strategy': 'kSameAsRequested', 'cudnn_conv_algo_search': 'HEURISTIC',
                      'use_tf32': 0})] if backend == 'cuda' else [provider]
        session_at = time.time(); began = time.perf_counter()
        session = None
        ended_profile = False
        try:
            session = ort.InferenceSession(str(path), sess_options=options, providers=providers)
            session.disable_fallback()
            if session.get_providers()[0] != provider:
                raise RuntimeError('Requested provider was not activated')
            loaded = time.perf_counter()
            self.runtime['initialization_stages'].append({'name':'model_load','started_at':session_at,'seconds':loaded-began,'backend':backend})
            warmup = 0.
            if instrument:
                model_input = session.get_inputs()[0]
                tensor = np.zeros(model_input.shape, dtype=np.float32)
                warmup_at = time.time(); start = time.perf_counter()
                session.run(None, {model_input.name: tensor})
                warmup = time.perf_counter() - start
                self.runtime['initialization_stages'].append({'name':'warmup','started_at':warmup_at,'seconds':warmup,'backend':backend})
            verification_start = time.perf_counter()
            if profile_dir:
                profile = Path(session.end_profiling())
                ended_profile = True
                events = json.loads(profile.read_text())
                counts = Counter(e.get('args', {}).get('provider') for e in events
                                 if e.get('cat') == 'Node' and e.get('args', {}).get('provider'))
                compute = sum(e.get('args', {}).get('provider') == provider and
                              not e.get('args', {}).get('op_name', '').startswith('Memcpy')
                              for e in events if e.get('cat') == 'Node')
                if compute <= 0:
                    raise RuntimeError('No CUDA compute events in model warmup')
                self.runtime.update(cuda_compute_events=compute, provider_events=dict(counts))
            self.session = session
            self.runtime.update(actual_backend=backend, providers=session.get_providers())
            self.runtime['timings'].update(session_seconds=loaded-began, warmup_seconds=warmup,
                                           verification_seconds=time.perf_counter()-verification_start)
        finally:
            if profile_dir:
                if session is not None and not ended_profile:
                    try:
                        session.end_profiling()
                    except Exception:
                        pass
                profile_dir.cleanup()

    def detect(self, frame, threshold, clock_origin=None):
        began = time.perf_counter()
        h,w = frame.shape[:2]; ratio = min(self.size/h,self.size/w)
        resized = cv2.resize(frame,(int(w*ratio),int(h*ratio)))
        canvas = np.full((self.size,self.size,3),114,dtype=np.uint8)
        canvas[:resized.shape[0],:resized.shape[1]] = resized
        tensor = np.ascontiguousarray(canvas.transpose(2,0,1),dtype=np.float32)[None]
        forward = time.perf_counter()
        # run returns CPU numpy outputs; CUDA time includes H2D, compute and D2H.
        pred = self.session.run(None,{self.input.name:tensor})[0][0].copy()
        post = time.perf_counter()
        pred[:,:2] = (pred[:,:2]+self.grid)*self.strides
        pred[:,2:4] = np.exp(np.clip(pred[:,2:4],-10,10))*self.strides
        xy1 = (pred[:,:2]-pred[:,2:4]/2)/ratio; xy2 = (pred[:,:2]+pred[:,2:4]/2)/ratio
        scores = pred[:,4,None]*pred[:,5:]; results = []
        for cls,label in LABELS.items():
            mask = scores[:,cls]>=threshold
            boxes = np.concatenate([xy1[mask],xy2[mask]-xy1[mask]],axis=1)
            conf = scores[mask,cls]
            if len(boxes)==0: continue
            indices = cv2.dnn.NMSBoxes(boxes.tolist(),conf.tolist(),threshold,0.45)
            for i in np.asarray(indices).reshape(-1):
                x,y,bw,bh = boxes[i]
                results.append({'label':label,'confidence':round(float(conf[i]),4),'box':[round(float(max(0,x)),1),round(float(max(0,y)),1),round(float(min(w,x+bw)),1),round(float(min(h,y+bh)),1)]})
        self.last_timing = {'preprocess_seconds':forward-began, 'inference_seconds':post-forward,
                            'postprocess_seconds':time.perf_counter()-post}
        if clock_origin is not None:
            self.last_timing['inference_offset_seconds'] = forward-clock_origin
        return results


def analyze(detector, video, outdir, params, progress, canceled):
    wall = time.time(); started = time.perf_counter()
    timing = {k:0. for k in ('decode_seconds','preprocess_seconds','inference_seconds','postprocess_seconds','preview_seconds')}
    part = time.perf_counter()
    cap = cv2.VideoCapture(str(video))
    if not cap.isOpened(): raise ValueError('视频无法解码')
    fps = cap.get(cv2.CAP_PROP_FPS); total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    step = max(1,round(fps/params['sample_fps'])); records = []; counts = {v:0 for v in LABELS.values()}
    frame_index = 0; previews = []
    timing['decode_seconds'] += time.perf_counter()-part
    try:
        while True:
            if canceled(): raise InterruptedError('任务已取消')
            part = time.perf_counter(); ok,frame = cap.read()
            timing['decode_seconds'] += time.perf_counter()-part
            if not ok: break
            if frame_index%step==0:
                detections = detector.detect(frame,params['confidence'],started)
                ft = dict(detector.last_timing)
                for key in ('preprocess_seconds','inference_seconds','postprocess_seconds'):
                    timing[key] += ft[key]
                for d in detections: counts[d['label']] += 1
                records.append({'frame':frame_index,'seconds':round(frame_index/fps,4),'detections':detections,'timing':ft})
                if len(previews)<3 and (detections or len(records)==1):
                    part = time.perf_counter(); annotated = frame.copy()
                    for d in detections:
                        x1,y1,x2,y2 = map(int,d['box']); cv2.rectangle(annotated,(x1,y1),(x2,y2),(50,210,120),2)
                        cv2.putText(annotated,f"{d['label']} {d['confidence']:.2f}",(x1,max(15,y1-5)),cv2.FONT_HERSHEY_SIMPLEX,.45,(50,210,120),1)
                    file = outdir/f'preview-{len(previews)}.jpg'
                    if not cv2.imwrite(str(file),annotated): raise ValueError('预览图写入失败')
                    previews.append(file.name); timing['preview_seconds'] += time.perf_counter()-part
                progress(min((frame_index+1)/max(total,1),.99),len(records))
            frame_index += 1
    finally:
        part = time.perf_counter(); cap.release(); timing['decode_seconds'] += time.perf_counter()-part
    if not records: raise ValueError('未解码到有效采样帧')
    elapsed = time.perf_counter()-started
    timing.update(analysis_started_at=wall, analysis_seconds=elapsed,
                  other_seconds=max(0.,elapsed-sum(timing.values())))
    return {'frames':records,'counts':counts,'sampled_frames':len(records),'decoded_frames':frame_index,
            'video_fps':fps,'sample_step_frames':step,'elapsed_seconds':elapsed,'measured_fps':len(records)/max(elapsed,.001),
            'previews':previews,'model_sha256':detector.sha256,'inference_runtime':detector.runtime,
            'analysis_timing':timing,'counting_rule':'采样帧目标出现次数，同一对象跨帧可能重复，不是唯一人数/车辆数'}
