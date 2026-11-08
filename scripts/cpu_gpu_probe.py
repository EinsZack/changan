#!/usr/bin/env python
"""Prove CPU and CUDA model execution in one ordinary container, without fallback."""
import argparse, ctypes, hashlib, json, os, tempfile, time, uuid
from collections import Counter
from pathlib import Path
import numpy as np
import onnxruntime as ort

def gpu_identity(device_id=0):
    lib=ctypes.CDLL('libcuda.so.1')
    def call(name,*args):
        status=getattr(lib,name)(*args)
        if status:raise RuntimeError(f'{name}: CUDA error {status}')
    call('cuInit',0);count=ctypes.c_int();call('cuDeviceGetCount',ctypes.byref(count))
    if not 0 <= device_id < count.value:raise RuntimeError(f'Invalid device index {device_id}; visible GPUs: {count.value}')
    devices=[]
    for index in range(count.value):
        device=ctypes.c_int();call('cuDeviceGet',ctypes.byref(device),index)
        raw=(ctypes.c_ubyte*16)();call('cuDeviceGetUuid',ctypes.byref(raw),device)
        name=ctypes.create_string_buffer(256);call('cuDeviceGetName',name,256,device)
        devices.append({'device_id':index,'uuid':'GPU-'+str(uuid.UUID(bytes=bytes(raw))),'name':name.value.decode()})
    return devices[device_id] | {'visible_devices':count.value,'devices':devices}

def create_session(model,provider,device_id=0):
    opts=ort.SessionOptions();opts.intra_op_num_threads=1;opts.inter_op_num_threads=1
    opts.enable_profiling=True;opts.profile_file_prefix=str(Path(tempfile.gettempdir())/('changan-'+provider))
    providers=[provider] if provider=='CPUExecutionProvider' else [(provider,{'device_id':device_id,'gpu_mem_limit':1024*1024*1024,'arena_extend_strategy':'kSameAsRequested','cudnn_conv_algo_search':'HEURISTIC','use_tf32':0})]
    session=ort.InferenceSession(str(model),sess_options=opts,providers=providers);session.disable_fallback()
    if session.get_providers()[0]!=provider:raise RuntimeError('Requested provider not active')
    return session

def main():
    p=argparse.ArgumentParser();p.add_argument('--cpu-only',action='store_true');args=p.parse_args()
    model=Path('/opt/model/yolox_tiny.onnx');x=np.random.default_rng(20261108).uniform(0,255,(1,3,416,416)).astype(np.float32)
    report={'pod':os.getenv('POD_NAME'),'node':os.getenv('NODE_NAME'),'pid':os.getpid(),'uid':os.getuid(),'ort_version':ort.__version__,'model_sha256':hashlib.sha256(model.read_bytes()).hexdigest(),'mode':'cpu' if args.cpu_only else 'cpu-gpu-cpu','stages':[]}
    cpu=create_session(model,'CPUExecutionProvider');began=time.perf_counter();a=cpu.run(None,{cpu.get_inputs()[0].name:x})[0]
    report['stages'].append({'backend':'cpu','pid':os.getpid(),'seconds':time.perf_counter()-began,'shape':list(a.shape)})
    if not args.cpu_only:
        device_id=int(os.getenv('GPU_DEVICE_ID','0'));report['gpu']=gpu_identity(device_id)
        expected=set(filter(None,os.getenv('EXPECTED_GPU_UUIDS','').split(',')))
        if expected and expected!={d['uuid'] for d in report['gpu']['devices']}:raise RuntimeError('Visible GPUs differ from configured selection')
        ort.preload_dlls();gpu=create_session(model,'CUDAExecutionProvider',device_id);began=time.perf_counter();b=gpu.run(None,{gpu.get_inputs()[0].name:x})[0]
        profile=Path(gpu.end_profiling());events=json.loads(profile.read_text());profile.unlink()
        counts=Counter(e.get('args',{}).get('provider') for e in events if e.get('cat')=='Node' and e.get('args',{}).get('provider'))
        compute=sum(e.get('args',{}).get('provider')=='CUDAExecutionProvider' and not e.get('args',{}).get('op_name','').startswith('Memcpy') for e in events)
        if not compute:raise RuntimeError('No real CUDA compute events')
        report['stages'].append({'backend':'cuda','pid':os.getpid(),'seconds':time.perf_counter()-began,'shape':list(b.shape),'cuda_compute_events':compute,'provider_events':dict(counts)})
        report['cpu_gpu_comparison']={'max_abs_difference':float(np.max(np.abs(a-b))),'rtol':0.002,'atol':0.0002,'allclose':bool(np.allclose(a,b,rtol=.002,atol=.0002))}
        if not report['cpu_gpu_comparison']['allclose']:raise RuntimeError('CPU/CUDA model outputs differ beyond tolerance')
        c=cpu.run(None,{cpu.get_inputs()[0].name:x})[0];assert np.array_equal(a,c)
        report['stages'].append({'backend':'cpu','pid':os.getpid(),'same_cached_session':True,'output_unchanged':True})
    cpu_profile=Path(cpu.end_profiling());cpu_profile.unlink()
    print(json.dumps(report),flush=True)
if __name__=='__main__':main()
