#!/usr/bin/env python
"""Build a CUDA image from an explicit context and print its immutable registry ref."""
import json,os,shutil,subprocess,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
context=ROOT/'tmp/gpu-build/video-context';context.mkdir(parents=True,exist_ok=True)
for name in ['workers','assets/models']:(context/name).mkdir(parents=True,exist_ok=True)
for name in ['__init__.py','inference.py','gpu.py','container_entry.py','attempt_entry.py']:shutil.copy2(ROOT/'workers'/name,context/'workers'/name)
shutil.copy2(ROOT/'assets/models/yolox_tiny.onnx',context/'assets/models/yolox_tiny.onnx')
shutil.copy2(ROOT/'deploy/Dockerfile.video-cuda',context/'Dockerfile')
shutil.copy2(ROOT/'deploy/requirements-video-cuda.txt',context/'requirements-video-cuda.txt')
(context/'wheels').mkdir(exist_ok=True)
allowed={r['file'] for r in json.loads((ROOT/'deploy/video-cuda-wheels.lock.json').read_text())}
for stale in (context/'wheels').glob('*.whl'):
    if stale.name not in allowed:stale.unlink()
for row in json.loads((ROOT/'deploy/video-cuda-wheels.lock.json').read_text()):
    source=ROOT/'tmp/gpu-build/wheels'/row['file'];target=context/'wheels'/row['file']
    if hashlib.sha256(source.read_bytes()).hexdigest()!=row['sha256']:raise ValueError('Wheel checksum: '+row['file'])
    if not target.exists() or target.stat().st_size!=source.stat().st_size or target.stat().st_mtime!=source.stat().st_mtime:shutil.copy2(source,target)
env=os.environ.copy();env['DOCKER_HOST']='unix://'+str(ROOT/'data/k3s/docker.sock')
docker=str(ROOT/'vendor/docker/docker');tag='127.0.0.1:5500/changan/video-cuda:0.6.0'
subprocess.run([docker,'build','--network','host','-t',tag,str(context)],env=env,check=True)
subprocess.run([docker,'push',tag],env=env,check=True)
info=json.loads(subprocess.check_output([docker,'inspect',tag],env=env,text=True))[0]
ref='changan-registry:5000/changan/video-cuda@'+info['RepoDigests'][0].split('@')[1]
(ROOT/'tmp/gpu-build/cuda-image.txt').write_text(ref+'\n');print(ref)
