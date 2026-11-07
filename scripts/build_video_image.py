#!/usr/bin/env python
"""Build the approved video image, push it to the private dev registry and freeze its digest."""
import os,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
env=os.environ.copy();env['PATH']=str(ROOT/'vendor/docker')+':'+env['PATH']
env['DOCKER_HOST']='unix://'+str(ROOT/'data/k3s/docker.sock')
env['DOCKER_BUILDKIT']='0'
tag='127.0.0.1:5500/changan/video:0.4.0'
def run(*args,**kw):return subprocess.run(args,cwd=ROOT,env=env,check=True,**kw)
run('docker','build','--network','host','-f','deploy/Dockerfile.video','-t',tag,'.')
run('docker','push',tag)
image=json.loads(run('docker','image','inspect',tag,capture_output=True,text=True).stdout)[0]
digest=image['RepoDigests'][0].split('@')[1]
ref='changan-registry:5000/changan/video@'+digest
path=ROOT/'.env';lines=path.read_text().splitlines() if path.exists() else []
lines=[line for line in lines if not line.startswith('CHANGAN_VIDEO_IMAGE=')]
lines.append('CHANGAN_VIDEO_IMAGE='+ref)
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:f.write('\n'.join(lines)+'\n')
print('Approved video image:',ref)
