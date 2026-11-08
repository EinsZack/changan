#!/usr/bin/env python
"""Actual model warmup in an isolated K3s GPU Pod; no application task or fake telemetry."""
import argparse,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
def main():
    p=argparse.ArgumentParser();p.add_argument('--image',required=True);p.add_argument('--node',required=True);p.add_argument('--uuid',required=True);p.add_argument('--name',default='changan-gpu-smoke');args=p.parse_args()
    env=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
    def kube(*cmd,**kw):return subprocess.run([str(ROOT/'vendor/docker/docker'),'exec','-i','k3d-changan-dev-server-0','kubectl',*cmd],env=env,cwd=ROOT,text=True,capture_output=True,check=True,**kw)
    code="import json; from workers.inference import Detector; d=Detector('/opt/model/yolox_tiny.onnx','cuda',2048,"+repr(args.uuid)+"); print(json.dumps(d.execution))"
    spec={'apiVersion':'v1','kind':'Pod','metadata':{'name':args.name,'namespace':'changan'},'spec':{'runtimeClassName':'nvidia','restartPolicy':'Never','automountServiceAccountToken':False,'nodeSelector':{'kubernetes.io/hostname':args.node},'securityContext':{'runAsUser':10001,'runAsGroup':10001,'runAsNonRoot':True,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'check','image':args.image,'imagePullPolicy':'IfNotPresent','command':['python','-c',code],'resources':{'requests':{'cpu':'1','memory':'2048Mi','nvidia.com/gpu':'1'},'limits':{'cpu':'2','memory':'2048Mi','nvidia.com/gpu':'1'}},'securityContext':{'readOnlyRootFilesystem':True,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'tmp','mountPath':'/tmp'}],'env':[{'name':'NVIDIA_DRIVER_CAPABILITIES','value':'compute,utility'}]}],'volumes':[{'name':'tmp','emptyDir':{}}]}}
    kube('create','-f','-',input=json.dumps(spec))
    output=ROOT/'tmp/gpu-evidence';output.mkdir(exist_ok=True)
    for _ in range(240):
        pod=json.loads(kube('get','pod',args.name,'-n','changan','-o','json').stdout)
        if pod['status']['phase'] in ('Succeeded','Failed'):
            logs=kube('logs',args.name,'-n','changan').stdout
            (output/'smoke-pod.json').write_text(json.dumps(pod,indent=2))
            (output/'smoke.log').write_text(logs)
            print(logs,flush=True)
            assert pod['status']['phase']=='Succeeded',pod['status']
            proof=json.loads(logs.splitlines()[-1]);assert proof['gpu_compute_events']>0 and proof['gpu']['uuid']==args.uuid
            (output/'smoke.json').write_text(json.dumps(proof,indent=2))
            kube('delete','pod',args.name,'-n','changan');return
        time.sleep(1)
    raise TimeoutError('Inspect smoke Pod events; no successful inference was observed')
if __name__=='__main__':main()
