#!/usr/bin/env python
"""Create a bounded GPU Pod for a same-image CPU/CUDA comparison; use an idle lab node."""
import base64,hashlib,json,os,subprocess,time
from pathlib import Path
import cv2
ROOT=Path(__file__).resolve().parent.parent
os.chdir(ROOT)
source=ROOT/'assets/samples/demo.mp4'
cap=cv2.VideoCapture(str(source));frames=[];indices=list(range(0,240,20))
for index in indices:
    cap.set(cv2.CAP_PROP_POS_FRAMES,index);ok,frame=cap.read();assert ok
    frame=cv2.resize(frame,(512,384))
    ok,buf=cv2.imencode('.jpg',frame,[cv2.IMWRITE_JPEG_QUALITY,85]);assert ok
    frames.append(base64.b64encode(buf).decode())
cap.release()
image=(ROOT/'tmp/gpu-build/cuda-image.txt').read_text().strip()
payload={'frames':frames,'indices':indices,'input_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'gpu_uuid':'GPU-1ba5825e-38ee-5dc3-2514-eb000355f95b'}
env=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
def kube(*cmd,**kw):
    r=subprocess.run([str(ROOT/'vendor/docker/docker'),'exec','-i','k3d-changan-dev-server-0','kubectl',*cmd],text=True,capture_output=True,env=env,**kw)
    if r.returncode:raise RuntimeError(r.stderr)
    return r
config={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'changan-gpu-benchmark','namespace':'changan'},'data':{'inputs.json':json.dumps(payload),'run.py':(ROOT/'scripts/gpu_compare_worker.py').read_text()}}
kube('create','-f','-',input=json.dumps(config))
pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':'changan-gpu-benchmark','namespace':'changan'},'spec':{'runtimeClassName':'nvidia','restartPolicy':'Never','automountServiceAccountToken':False,'nodeSelector':{'kubernetes.io/hostname':'k3d-changan-dev-gpu-0'},'securityContext':{'runAsUser':10001,'runAsNonRoot':True},'containers':[{'name':'bench','image':image,'command':['python','/bench/run.py'],'env':[{'name':'PYTHONPATH','value':'/opt/app'},{'name':'NVIDIA_DRIVER_CAPABILITIES','value':'compute,utility'}],'resources':{'requests':{'cpu':'1','memory':'2048Mi','nvidia.com/gpu':'1'},'limits':{'cpu':'2','memory':'2048Mi','nvidia.com/gpu':'1'}},'securityContext':{'readOnlyRootFilesystem':True,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'input','mountPath':'/bench','readOnly':True},{'name':'tmp','mountPath':'/tmp'}]}],'volumes':[{'name':'input','configMap':{'name':'changan-gpu-benchmark'}},{'name':'tmp','emptyDir':{}}]}}
kube('create','-f','-',input=json.dumps(pod))
for _ in range(480):
    actual=json.loads(kube('get','pod','changan-gpu-benchmark','-n','changan','-o','json').stdout)
    if actual['status']['phase'] in ('Succeeded','Failed'):
        logs=kube('logs','changan-gpu-benchmark','-n','changan').stdout
        (ROOT/'docs/gpu-20261108/benchmark.log').write_text(logs)
        assert actual['status']['phase']=='Succeeded',logs
        report=json.loads(logs.splitlines()[-1]);report['image']=image;report['at']=time.time();report['environment']='same shared RTX5090 node, same CUDA image, CPU limit 2; fixed 12 JPEG frames resized to 512x384 from demo, excludes video decode/network/scheduling'
        (ROOT/'docs/gpu-20261108/benchmark.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
        kube('delete','pod,configmap','changan-gpu-benchmark','-n','changan')
        assert report['numerical_comparison']['passed'],report['numerical_comparison']
        print(json.dumps(report,ensure_ascii=False),flush=True);break
    time.sleep(1)
else:raise TimeoutError('Benchmark Pod timeout')
