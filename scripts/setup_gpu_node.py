#!/usr/bin/env python
"""Add a development GPU agent without changing the existing CPU nodes or host driver.

The nested node intentionally uses host-matching driver libraries. Real ARM64
boards use their official driver stack instead of this server-only image.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app import db


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--gpu-uuid',required=True)
    args=parser.parse_args()
    assert not db.query("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')"), 'wait for current tasks'
    inventory=subprocess.check_output(['nvidia-smi','--query-gpu=uuid','--format=csv,noheader'],text=True).splitlines()
    assert args.gpu_uuid in inventory,'unknown GPU UUID'
    env=os.environ.copy();env['DOCKER_HOST']='unix://'+str(ROOT/'data/k3s/docker.sock')
    docker=str(ROOT/'vendor/docker/docker')
    def run(*cmd,**kw):return subprocess.run([docker,*cmd],env=env,cwd=ROOT,check=True,**kw)
    node='k3d-changan-dev-gpu-0'
    existing=subprocess.run([docker,'inspect',node],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    if existing.returncode==0:
        print('GPU node already exists; no replacement performed');return
    config=json.loads(run('inspect','k3d-changan-dev-agent-0',capture_output=True,text=True).stdout)[0]
    values=dict(line.split('=',1) for line in config['Config']['Env'])
    selected={k:v for k,v in values.items() if k.startswith('K3S_') and k not in ('K3S_NODE_NAME',)}
    selected['K3S_NODE_NAME']=node
    selected['NVIDIA_VISIBLE_DEVICES']=args.gpu_uuid
    folder=ROOT/'data/k3s/gpu';folder.mkdir(parents=True,exist_ok=True)
    envfile=folder/'node.env';envfile.write_text('\n'.join(k+'='+v for k,v in selected.items())+'\n');envfile.chmod(0o600)
    registry=folder/'registries.yaml'
    run('cp','k3d-changan-dev-agent-0:/etc/rancher/k3s/registries.yaml',str(registry))
    cmd=['run','-d','--name',node,'--hostname',node,'--network','k3d-changan-dev','--privileged',
         '--restart','unless-stopped','--cpus','3','--memory','8g','--env-file',str(envfile),
         '-v',str(registry)+':/etc/rancher/k3s/registries.yaml:ro',
         '-v','changan-gpu-0-node-identity:/etc/rancher/node']
    for directory in ['kubelet','rancher/k3s','cni','log']:
        target='/var/'+('log' if directory=='log' else 'lib/'+directory)
        cmd+=['-v','changan-gpu-0-'+directory.replace('/','-')+':'+target]
    cmd+=['changan/k3s-gpu:1.35.5-driver590','agent','--node-label','changan.io/gpu-enabled=true',
          '--node-label','changan.io/gpu-uuid='+args.gpu_uuid]
    try:run(*cmd)
    finally:envfile.unlink(missing_ok=True)
    print('Added development GPU agent:',node,'GPU:',args.gpu_uuid)


if __name__=='__main__':main()
