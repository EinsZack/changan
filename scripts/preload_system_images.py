#!/usr/bin/env python
"""Preload infrastructure images through the project Docker daemon on restricted networks."""
import os,json,subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
ROOT=Path(__file__).resolve().parent.parent
env=os.environ.copy();env['PATH']=str(ROOT/'vendor/docker')+':'+env['PATH']
env['DOCKER_HOST']='unix://'+str(ROOT/'data/k3s/docker.sock')
def run(*args,**kw):return subprocess.run(args,cwd=ROOT,env=env,check=True,**kw)
pods=json.loads(run('docker','exec','k3d-changan-dev-server-0','kubectl','get','pods','-n','kube-system','-o','json',capture_output=True,text=True).stdout)
images={'rancher/mirrored-pause:3.6'}
images.update(c['image'] for p in pods['items'] for c in p['spec']['containers'])
for image in sorted(images):run('docker','pull',image)
tar=ROOT/'data/k3s/system-images.tar'
run('docker','save','-o',str(tar),*sorted(images))
def preload(name):
    with tar.open('rb') as f:run('docker','exec','-i',name,'ctr','-n','k8s.io','images','import','-',stdin=f,stdout=subprocess.DEVNULL)
    print('Infrastructure images loaded:',name,flush=True)
names=['k3d-changan-dev-server-0']+['k3d-changan-dev-agent-'+str(i) for i in range(4)]
with ThreadPoolExecutor(max_workers=3) as pool:list(pool.map(preload,names))
