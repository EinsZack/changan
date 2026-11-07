#!/usr/bin/env python
"""Idempotent development cluster registration; preserves application history and secrets."""
import json,os,subprocess,sys
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parent.parent
env=os.environ.copy();env['PATH']=str(ROOT/'vendor/docker')+':'+str(ROOT/'vendor/bin')+':'+env['PATH']
env['DOCKER_HOST']='unix://'+str(ROOT/'data/k3s/docker.sock')
def run(*args,**kw):return subprocess.run(args,cwd=ROOT,env=env,check=True,**kw)
def kube(*args,**kw):return run('docker','exec','-i','k3d-changan-dev-server-0','kubectl',*args,**kw)
kube('wait','--for=condition=Ready','nodes','--all','--timeout=180s')
kube('apply','-f','-',input=(ROOT/'deploy/rbac.yaml').read_bytes())
for i in range(4):
    name='k3d-changan-dev-agent-'+str(i)
    kube('label','node',name,'changan.io/node-id=lab-'+str(i+1),'changan.io/kind=k3s_logical',
         'changan.io/physical-host=lab-31','changan.io/slots=1','changan.io/memory-mib=3072','--overwrite')
    run('docker','update','--cpus','3',name,stdout=subprocess.DEVNULL)
run('docker','update','--cpus','2','k3d-changan-dev-server-0',stdout=subprocess.DEVNULL)
network=json.loads(run('docker','network','inspect','k3d-changan-dev',capture_output=True,text=True).stdout)[0]
gateway=network['IPAM']['Config'][0]['Gateway']
policy={'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':'video-egress','namespace':'changan'},
 'spec':{'podSelector':{'matchLabels':{'app':'changan-video'}},'policyTypes':['Ingress','Egress'],'ingress':[],
 'egress':[{'to':[{'ipBlock':{'cidr':gateway+'/32'}}],'ports':[{'protocol':'TCP','port':8311},{'protocol':'TCP','port':8411}]}]}}
kube('apply','-f','-',input=yaml.safe_dump(policy).encode())
admin=ROOT/'data/k3s/admin.yaml'
fd=os.open(admin,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:run('k3d','kubeconfig','get','changan-dev',stdout=f)
run(sys.executable,'scripts/kubeconfig.py',str(admin),str(ROOT/'data/k3s/orchestrator.yaml'))
updates={'CHANGAN_BACKEND':'k3s','CHANGAN_KUBECONFIG':str(ROOT/'data/k3s/orchestrator.yaml'),
 'CHANGAN_NAMESPACE':'changan','CHANGAN_CLUSTER_ID':'changan-dev','CHANGAN_IMAGE_ARCH':'amd64',
 'CHANGAN_CONTAINER_HOST':gateway,'CHANGAN_CONTAINER_BASE':'http://'+gateway+':8311',
 'CHANGAN_ENVIRONMENT_NOTE':'同一开发服务器的4个K3s逻辑节点；未完成THOR实机验证'}
path=ROOT/'.env';lines=path.read_text().splitlines() if path.exists() else []
lines=[line for line in lines if line.partition('=')[0].strip() not in updates]
lines += [k+'='+v for k,v in updates.items()]
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:f.write('\n'.join(lines)+'\n')
print('Development cluster configured. Gateway:',gateway,'Image digest must be configured after push.')
