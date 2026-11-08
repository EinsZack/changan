#!/usr/bin/env python
"""Read-only board inventory. Run on the actual board; never installs a driver or labels it ready."""
import ctypes
import json
import platform
import shutil
import subprocess
import time
import uuid
from pathlib import Path


def command(args):
    if not shutil.which(args[0]):return {'available':False}
    try:
        p=subprocess.run(args,capture_output=True,text=True,timeout=15)
        return {'available':True,'returncode':p.returncode,'stdout':p.stdout[-10000:],'stderr':p.stderr[-2000:]}
    except subprocess.TimeoutExpired:return {'available':True,'error':'timeout'}


def main():
    report={'observed_at':time.time(),'machine':platform.machine(),'kernel':platform.release(),
            'os_release':Path('/etc/os-release').read_text() if Path('/etc/os-release').exists() else None,
            'tegra_release':Path('/etc/nv_tegra_release').read_text() if Path('/etc/nv_tegra_release').exists() else None,
            'memory':{line.split(':')[0]:line.split(':')[1].strip() for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith(('MemTotal:','MemAvailable:'))},
            'commands':{name:command(args) for name,args in {
                'k3s':['k3s','--version'], 'nvidia_runtime':['nvidia-container-runtime','--version'],
                'nvidia_toolkit':['nvidia-ctk','--version'], 'nvidia_smi':['nvidia-smi','--query-gpu=name,uuid,driver_version,memory.total,memory.free','--format=csv'],
                'nvidia_packages':['dpkg-query','-W','nvidia-l4t-core','nvidia-jetpack']}.items()},
            'status':'inventory_only; application GPU inference and image compatibility require validation'}
    try:
        cuda=ctypes.CDLL('libcuda.so.1');v=ctypes.c_int();code=cuda.cuInit(0)
        if code:raise RuntimeError('cuInit status '+str(code))
        cuda.cuDriverGetVersion(ctypes.byref(v));report['cuda_driver_version']=v.value
        cuda.cuDeviceGetCount(ctypes.byref(v));report['cuda_visible_devices']=v.value
        report['cuda_devices']=[]
        for index in range(v.value):
            device=ctypes.c_int();cuda.cuDeviceGet(ctypes.byref(device),index)
            raw=(ctypes.c_ubyte*16)();name=ctypes.create_string_buffer(256)
            status=cuda.cuDeviceGetUuid(ctypes.byref(raw),device)
            cuda.cuDeviceGetName(name,256,device)
            report['cuda_devices'].append({'name':name.value.decode(),'uuid':'GPU-'+str(uuid.UUID(bytes=bytes(raw))) if status==0 else None,'uuid_status':status})
    except (OSError,RuntimeError) as e:report['cuda_error']=str(e)
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
