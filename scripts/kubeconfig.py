#!/usr/bin/env python
"""Write a restricted kubeconfig from an admin kubeconfig; never print credentials."""
import argparse,base64,os,time
from pathlib import Path
import yaml
from kubernetes import client,config
parser=argparse.ArgumentParser()
parser.add_argument('admin');parser.add_argument('output')
args=parser.parse_args()
config.load_kube_config(args.admin)
core=client.CoreV1Api()
for _ in range(30):
    secret=core.read_namespaced_secret('orchestrator-access','changan')
    if secret.data and secret.data.get('token'):break
    time.sleep(1)
else:raise SystemExit('ServiceAccount token 尚未生成')
admin=yaml.safe_load(Path(args.admin).read_text())
cluster=admin['clusters'][0]['cluster']
out={'apiVersion':'v1','kind':'Config','clusters':[{'name':'changan','cluster':cluster}],
 'users':[{'name':'orchestrator','user':{'token':base64.b64decode(secret.data['token']).decode()}}],
 'contexts':[{'name':'changan','context':{'cluster':'changan','user':'orchestrator','namespace':'changan'}}],
 'current-context':'changan'}
path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:yaml.safe_dump(out,f)
os.chmod(path,0o600)
print('Restricted kubeconfig written:',path)
