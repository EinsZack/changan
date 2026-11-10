#!/usr/bin/env python
"""Prepare and optionally pre-warm the node-local CUDA JIT cache on the four lab nodes.

Why: onnxruntime-gpu CUDA 12 wheels ship no native sm_120 (RTX 5090) code for some kernels, so the
driver JIT-compiles PTX on first use. Business Pods run as UID 10001 with HOME=/ read-only, so the
default ~/.nv/ComputeCache is never written and every task-scoped Pod repeated 55-78 s of JIT.

  prepare  create <path> inside each lab node container, owned by 10001, mode 0700 (idempotent)
  warm     run one short Job per node with the business image; it fills the cache and reports
           first-forward seconds (repeat to see the cached value)
  status   show cache file count/bytes per node
  seed     copy a complete cache from --from <lab> to the other nodes (identical GPU model and driver)

Measured 2026-11-10 (RTX 5090, driver 590.48.01): the first CUDA forward of a new process JIT-compiles
either a small (~15 MB, ~11 s) or a large (~75 MB more, ~50 s, inside Add_59) set of kernels and
which one happens is nondeterministic. One warm run may therefore cache only the small set. Once a
node holds both (~91 MB) the slow path no longer appeared. Use `seed` to copy such a cache instead
of waiting for every node to hit both paths. CUDA_MODULE_LOADING=EAGER compiles everything but ran
for over 10 minutes and grew past 200 MB, so it is not used.

The cache is GPU/driver specific; the driver invalidates entries itself after a driver change.
This only touches infrastructure; scheduling and the B-mode session lifecycle are unchanged.
"""
import argparse, json, os, sqlite3, subprocess, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
DOCKER=str(ROOT/'vendor/docker/docker') if (ROOT/'vendor/docker/docker').exists() else 'docker'
ENV=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
NODES={f'lab-{i+1}':f'k3d-changan-dev-agent-{i}' for i in range(4)}
SERVER='k3d-changan-dev-server-0'
DEFAULT_PATH='/var/lib/rancher/k3s/changan-cache/cuda'  # Docker volume on k3d nodes: survives node recreation
UID=10001
WARM=r'''
import json,os,time
from pathlib import Path
from workers.inference import Detector
cache=Path(os.environ['CUDA_CACHE_PATH'])
def size():
    files=[p for p in cache.rglob('*') if p.is_file()]
    return {'files':len(files),'bytes':sum(p.stat().st_size for p in files)}
before=size(); began=time.perf_counter()
d=Detector('/opt/model/yolox_tiny.onnx',policy='cuda',instrument=True)
print(json.dumps({'node':os.environ.get('KUBE_NODE_NAME'),'backend':d.runtime['actual_backend'],
  'gpu':(d.runtime.get('gpu') or {}).get('name'),'warmup_seconds':d.runtime['timings'].get('warmup_seconds'),
  'initialization_seconds':time.perf_counter()-began,'cache_before':before,'cache_after':size()}))
'''


def docker(*args):
    return subprocess.run([DOCKER,*args],env=ENV,check=True,text=True,capture_output=True).stdout.strip()


def kubectl(*args, body=None):
    p=subprocess.run([DOCKER,'exec','-i',SERVER,'kubectl',*args],env=ENV,text=True,capture_output=True,
                     input=json.dumps(body) if body is not None else None)
    if p.returncode:raise RuntimeError('kubectl '+' '.join(args[:2])+': '+p.stderr[:500])
    return p.stdout.strip()


def ensure_idle():
    db=ROOT/'data/control.sqlite'
    if not db.exists():return
    with sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True) as c:
        if c.execute("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED')").fetchone():
            raise RuntimeError('有任务在运行；预热Job会占用节点GPU设备槽，请在空闲时执行')


def validate_path(path):
    if not path.startswith('/') or '..' in path.split('/') or path.rstrip('/') in ('','/var','/var/lib','/tmp'):
        raise ValueError('缓存目录必须是专用绝对路径')
    return path.rstrip('/')


def prepare(path, labs):
    for lab in labs:
        docker('exec',NODES[lab],'sh','-c',f'mkdir -p "{path}" && chown {UID}:{UID} "{path}" && chmod 0700 "{path}"')
        print(json.dumps({'lab':lab,'path':path,'stat':docker('exec',NODES[lab],'stat','-c','%u:%g %a',path)}),flush=True)


def warm_manifest(lab, kube_node, image, path):
    name='cuda-warm-'+lab+'-'+str(int(time.time()))
    return {'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'labels':{'app':'changan-cuda-warmup'}},
      'spec':{'backoffLimit':0,'activeDeadlineSeconds':600,'ttlSecondsAfterFinished':600,
       'template':{'metadata':{'labels':{'app':'changan-cuda-warmup'}},
        'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,'runtimeClassName':'nvidia',
         'nodeSelector':{'kubernetes.io/hostname':kube_node},
         'securityContext':{'runAsUser':UID,'runAsGroup':UID,'runAsNonRoot':True,'seccompProfile':{'type':'RuntimeDefault'}},
         'containers':[{'name':'warm','image':image,'imagePullPolicy':'IfNotPresent','command':['python','-c',WARM],
          'env':[{'name':'CUDA_CACHE_PATH','value':'/cache/cuda'},{'name':'CUDA_CACHE_MAXSIZE','value':str(1024**3)},
                 {'name':'KUBE_NODE_NAME','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}}],
          'resources':{'requests':{'cpu':'1','memory':'2048Mi','nvidia.com/gpu':'1'},'limits':{'cpu':'2','memory':'2048Mi','nvidia.com/gpu':'1'}},
          'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
          'volumeMounts':[{'name':'cuda-cache','mountPath':'/cache/cuda'},{'name':'tmp','mountPath':'/tmp'}]}],
         'volumes':[{'name':'cuda-cache','hostPath':{'path':path,'type':'Directory'}},{'name':'tmp','emptyDir':{'sizeLimit':'256Mi'}}]}}}}


def warm(path, labs, namespace):
    ensure_idle()
    from app import config  # noqa: F401  loads CHANGAN_* from .env
    image=os.environ.get('CHANGAN_VIDEO_IMAGE','')
    if '@sha256:' not in image:raise ValueError('CHANGAN_VIDEO_IMAGE 未固定摘要')
    jobs=[]
    try:
        for lab in labs:  # sequential: each node fills its cache without CPU contention from the others
            m=warm_manifest(lab,NODES[lab],image,path)
            kubectl('apply','-n',namespace,'-f','-',body=m)
            name=m['metadata']['name']; jobs.append(name)
            kubectl('wait','-n',namespace,'--for=condition=complete','job/'+name,'--timeout=600s')
            print(kubectl('logs','-n',namespace,'job/'+name,'--tail=5'),flush=True)
    finally:
        for name in jobs:kubectl('delete','job',name,'-n',namespace,'--ignore-not-found')


def status(path, labs):
    for lab in labs:
        files,size=docker('exec',NODES[lab],'sh','-c',f'find "{path}" -type f | wc -l; du -sb "{path}" | cut -f1').split()
        print(json.dumps({'lab':lab,'path':path,'files':int(files),'bytes':int(size)}),flush=True)


def seed(path, source, labs):
    for lab in labs:
        if lab==source:continue
        src=subprocess.Popen([DOCKER,'exec',NODES[source],'tar','-C',path,'-cf','-','.'],env=ENV,stdout=subprocess.PIPE)
        subprocess.run([DOCKER,'exec','-i',NODES[lab],'sh','-c',f'mkdir -p "{path}" && tar -C "{path}" -xf - && chown -R {UID}:{UID} "{path}" && chmod 0700 "{path}"'],
                       env=ENV,stdin=src.stdout,check=True)
        src.stdout.close()
        if src.wait():raise RuntimeError('读取源节点缓存失败: '+source)
    status(path,labs)


def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('action',choices=['prepare','warm','status','seed'])
    p.add_argument('--from',dest='source',choices=list(NODES),help='seed 的源节点')
    p.add_argument('--path',default=DEFAULT_PATH)
    p.add_argument('--node',choices=list(NODES),action='append',help='默认全部lab节点')
    p.add_argument('--namespace',default='changan')
    a=p.parse_args(); path=validate_path(a.path); labs=a.node or list(NODES)
    if a.action=='seed' and not a.source:p.error('seed 需要 --from')
    {'prepare':lambda:prepare(path,labs),'warm':lambda:warm(path,labs,a.namespace),'status':lambda:status(path,labs),
     'seed':lambda:seed(path,a.source,labs)}[a.action]()
    if a.action=='prepare':print('在 .env 中设置 CHANGAN_CUDA_CACHE_HOSTPATH='+path+' 后重启API；之后提交的任务生效。')


if __name__=='__main__':main()
