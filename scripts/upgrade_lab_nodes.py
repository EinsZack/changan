#!/usr/bin/env python
"""Replace idle lab node containers in place; preserve node identity and volumes; Docker assigns the node IP.

The backup contains credentials and must remain in ignored server-local data/.
This tool upgrades node infrastructure only; it does not change task scheduling.
"""
import argparse, datetime, json, os, sqlite3, subprocess, time
from pathlib import Path
import httpx
import psutil
ROOT=Path(__file__).resolve().parent.parent
DOCKER=str(ROOT/'vendor/docker/docker')
ENV=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
NODES={f'lab-{i+1}':f'k3d-changan-dev-agent-{i}' for i in range(4)}
PLUGIN_IMAGE='changan-registry:5000/changan/nvidia-device-plugin@sha256:7fc948405ac1ec49d802a754a9c510730e7ebcd72f2795d8aaf192cf6dbda122'


def validate_mapping(mapping, inventory):
    if set(mapping)!=set(NODES):raise ValueError('Map exactly lab-1 through lab-4')
    for value in mapping.values():
        selected=inventory if value=='all' else value.split(',') if isinstance(value,str) else []
        if not selected or any(v not in inventory for v in selected) or len(set(selected))!=len(selected):raise ValueError('Unknown or invalid physical GPU selection')
    return mapping  # Repeated GPUs across simulated nodes are explicitly allowed.


def docker(*args):
    return subprocess.check_output([DOCKER,*args],env=ENV,text=True).strip()


def kube(*args,body=None):
    p=subprocess.run([DOCKER,'exec','-i','k3d-changan-dev-server-0','kubectl',*args],env=ENV,input=json.dumps(body) if body is not None else None,text=True,capture_output=True)
    if p.returncode:raise RuntimeError('kubectl '+args[0]+': '+p.stderr[:500])
    return p.stdout.strip()


def api(method,path,body=None):
    with httpx.Client(transport=httpx.HTTPTransport(uds=str(ROOT/'data/k3s/docker.sock')),base_url='http://docker',timeout=90) as c:
        r=c.request(method,'/v1.51'+path,json=body)
        r.raise_for_status()
        return r.json() if r.content else None


def private_json(path,value):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as f:json.dump(value,f,indent=2)


def ensure_idle():
    with sqlite3.connect('file:'+str(ROOT/'data/control.sqlite')+'?mode=ro',uri=True) as c:
        if c.execute("SELECT id FROM tasks WHERE status NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELED')").fetchone():raise RuntimeError('Wait for active tasks before replacing nodes')
    records=json.loads((ROOT/'data/processes.json').read_text())
    if 'api' in records:
        r=records['api']
        try:
            p=psutil.Process(r['pid'])
            if abs(p.create_time()-r['created'])<.01 and p.status()!=psutil.STATUS_ZOMBIE:raise RuntimeError('Stop project API before replacing nodes to prevent new task submissions')
        except psutil.Error:pass


def plugin(lab,gpu):
    name='changan-compute-device-'+lab
    return {'apiVersion':'apps/v1','kind':'DaemonSet','metadata':{'name':name,'namespace':'kube-system'},'spec':{'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':{'app':name}},'spec':{'runtimeClassName':'nvidia','nodeSelector':{'kubernetes.io/hostname':NODES[lab]},'containers':[{'name':'device-plugin','image':PLUGIN_IMAGE,'env':[{'name':'NVIDIA_VISIBLE_DEVICES','value':gpu},{'name':'NVIDIA_DRIVER_CAPABILITIES','value':'compute,utility'},{'name':'FAIL_ON_INIT_ERROR','value':'true'},{'name':'DEVICE_ID_STRATEGY','value':'uuid'},{'name':'DEVICE_LIST_STRATEGY','value':'envvar'}],'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'device-plugin','mountPath':'/var/lib/kubelet/device-plugins'}]}],'volumes':[{'name':'device-plugin','hostPath':{'path':'/var/lib/kubelet/device-plugins'}}]}}}}


def mounts(inspect):
    result=[]
    for m in inspect['Mounts']:
        source=m['Name'] if m['Type']=='volume' else m['Source']
        mode=m.get('Mode') or ('rw' if m['RW'] else 'ro')
        result.append(source+':'+m['Destination']+':'+mode)
    return result


def creation(inspect,image,gpu=None,identity=None,registry=None,entrypoints=()):
    c=inspect['Config'];h=inspect['HostConfig'];net=inspect['NetworkSettings']['Networks']
    if len(net)!=1:raise ValueError('Expected one project Docker network')
    network,endpoint=next(iter(net.items()))
    host_keys=['Privileged','Tmpfs','CgroupnsMode','IpcMode','PidMode','Ulimits','RestartPolicy','NanoCpus','Memory','ExtraHosts','PortBindings','LogConfig','SecurityOpt','CapAdd','CapDrop','Sysctls','ShmSize','Init']
    host={k:h[k] for k in host_keys if h.get(k) is not None};host['Binds']=mounts(inspect);host['NetworkMode']=network
    labels=dict(c.get('Labels') or {})
    if gpu:
        host['Binds'] += [str(identity)+':/etc/rancher/node',str(registry)+':/etc/rancher/k3s/registries.yaml:ro']
        values=dict(v.split('=',1) for v in c['Env'] if not v.startswith(('PATH=','CRI_CONFIG_FILE=','NVIDIA_')))
        values.update(K3S_NODE_NAME=c['Hostname'],NVIDIA_VISIBLE_DEVICES=gpu,NVIDIA_DRIVER_CAPABILITIES='compute,utility')
        labels['changan.runtime']='cpu-gpu'
        host['Binds'] += [str(path)+':/bin/'+path.name+':ro' for path in entrypoints]
        entrypoint=c['Entrypoint']
    else:
        values=dict(v.split('=',1) for v in c['Env']);entrypoint=c['Entrypoint']
    return {'Image':image,'Hostname':c['Hostname'],'Env':[k+'='+v for k,v in values.items()],'Entrypoint':entrypoint,'Cmd':c['Cmd'],'Labels':labels,'HostConfig':host,'NetworkingConfig':{'EndpointsConfig':{network:{'Aliases':endpoint.get('Aliases') or []}}}}


def wait_ready(name,uid,start):
    deadline=time.monotonic()+180
    while time.monotonic()<deadline:
        try:
            n=json.loads(kube('get','node',name,'-o','json'))
            assert n['metadata']['uid']==uid,'Node UID changed'
            ready=next(v for v in n['status']['conditions'] if v['type']=='Ready')
            lease=json.loads(kube('get','lease',name,'-n','kube-node-lease','-o','json'))
            renewed=datetime.datetime.fromisoformat(lease['spec']['renewTime'].replace('Z','+00:00')).timestamp()
            if ready['status']=='True' and renewed>=start:return n
        except (RuntimeError,KeyError,StopIteration):pass
        time.sleep(2)
    raise RuntimeError('Node did not return Ready with its preserved UID')


def restore(lab,backup):
    ensure_idle();name=NODES[lab];record=json.loads((backup/lab/'backup.json').read_text())
    kube('delete','daemonset','changan-compute-device-'+lab,'-n','kube-system','--ignore-not-found')
    try:api('POST','/containers/'+name+'/stop?t=20')
    except httpx.HTTPStatusError as e:
        if e.response.status_code not in (304,404):raise
    try:api('DELETE','/containers/'+name)
    except httpx.HTTPStatusError as e:
        if e.response.status_code!=404:raise
    start=time.time();api('POST','/containers/create?name='+name,creation(record['inspect'],record['snapshot_image']));api('POST','/containers/'+name+'/start')
    wait_ready(name,record['node']['metadata']['uid'],start)
    patch={'metadata':{'labels':{'changan.io/compute-runtime':None},'annotations':{'changan.io/visible-gpus':None,'changan.io/gpu-scope':None}},'spec':{'unschedulable':record['node']['spec'].get('unschedulable',False)}}
    kube('patch','node',name,'--type=merge','-p',json.dumps(patch))
    print('Restored original node:',lab,flush=True)


def upgrade(lab,gpu,image,backup):
    ensure_idle();name=NODES[lab];folder=backup/lab;folder.mkdir(mode=0o700,parents=True,exist_ok=True)
    inspect=api('GET','/containers/'+name+'/json')
    if inspect['Config'].get('Labels',{}).get('changan.runtime')=='cpu-gpu':raise RuntimeError('Node is already upgraded; restore its saved baseline before changing the mapping')
    node=json.loads(kube('get','node',name,'-o','json'))
    if node['spec'].get('unschedulable'):raise RuntimeError('Node was already cordoned; inspect before replacing it')
    pods=json.loads(kube('get','pods','-A','--field-selector','spec.nodeName='+name,'-o','json'))['items']
    unexpected=[p['metadata']['name'] for p in pods if p['metadata']['namespace']!='kube-system' and p['status']['phase'] not in ['Succeeded','Failed']]
    if unexpected:raise RuntimeError('Non-system Pods still use node: '+str(unexpected))
    runtime=ROOT/'data/runtime/unified'/lab;runtime.mkdir(mode=0o700,parents=True,exist_ok=True)
    identity=runtime/'node-identity'
    if identity.exists():
        if (identity/'password').read_text().strip()!=docker('exec',name,'cat','/etc/rancher/node/password'):raise ValueError('Saved node identity differs; inspect before replacing')
    else:docker('cp',name+':/etc/rancher/node',str(identity))
    os.chmod(identity,0o700)
    entrydir=runtime/'entrypoints';entrydir.mkdir(exist_ok=True)
    for entry in docker('exec',name,'sh','-c','ls /bin/k3d-entrypoint*.sh').splitlines():
        docker('cp',name+':'+entry,str(entrydir/Path(entry).name))
    entrypoints=sorted(entrydir.glob('*.sh'))
    if not entrypoints:raise RuntimeError('Missing k3d node bootstrap scripts')
    registry=runtime/'registries.yaml';docker('cp',name+':/etc/rancher/k3s/registries.yaml',str(registry));os.chmod(registry,0o600)
    if not (folder/'backup.json').exists():
        snapshot=api('POST','/commit?container='+name+'&repo=changan/node-backup&tag='+lab+'-20261108&pause=true')['Id']
        record={'inspect':inspect,'node':node,'snapshot_image':snapshot,'gpu_selection':gpu,'new_image':image}
        private_json(folder/'backup.json',record)
    kube('cordon',name)
    try:
        api('POST','/containers/'+name+'/stop?t=25');api('DELETE','/containers/'+name)
        body=creation(inspect,image,gpu,identity,registry,entrypoints)
        start=time.time();api('POST','/containers/create?name='+name,body);api('POST','/containers/'+name+'/start')
        current=wait_ready(name,node['metadata']['uid'],start)
        config=docker('exec',name,'cat','/var/lib/rancher/k3s/agent/etc/containerd/config.toml')
        if 'nvidia' not in config:raise RuntimeError('NVIDIA runtime not detected by K3s')
        kube('label','node',name,'changan.io/compute-runtime=cpu-gpu','--overwrite')
        kube('annotate','node',name,'changan.io/visible-gpus='+gpu,'changan.io/gpu-scope=development-shared','--overwrite')
        kube('uncordon',name)
        kube('apply','-f','-',body=plugin(lab,gpu))
        kube('rollout','status','daemonset/changan-compute-device-'+lab,'-n','kube-system','--timeout=180s')
        inventory=subprocess.check_output(['nvidia-smi','--query-gpu=uuid','--format=csv,noheader'],text=True).splitlines()
        expected_count=len(inventory) if gpu=='all' else len(gpu.split(','))
        for _ in range(30):
            current=json.loads(kube('get','node',name,'-o','json'))
            if int(current['status']['allocatable'].get('nvidia.com/gpu',0))==expected_count:break
            time.sleep(2)
        else:raise RuntimeError('Node GPU registration does not match its configured selection')
        result={'lab':lab,'node':name,'uid':current['metadata']['uid'],'gpu_selection':gpu,'gpu_allocatable':expected_count,'image':image,'identity_preserved':True,'ip':next(a['address'] for a in current['status']['addresses'] if a['type']=='InternalIP')}
        (folder/'result.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
    except Exception:
        restore(lab,backup)
        raise


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['upgrade','restore']);p.add_argument('--mapping',required=True);p.add_argument('--backup',required=True);p.add_argument('--node',choices=list(NODES),required=True);args=p.parse_args()
    mapping=json.loads(Path(args.mapping).read_text());inventory=subprocess.check_output(['nvidia-smi','--query-gpu=uuid','--format=csv,noheader'],text=True).splitlines();validate_mapping(mapping,inventory)
    backup=Path(args.backup).resolve();allowed=(ROOT/'data/backups').resolve()
    if not backup.is_relative_to(allowed):raise ValueError('Keep credential backups under ignored data/backups')
    backup.mkdir(parents=True,exist_ok=True,mode=0o700);os.chmod(backup,0o700)
    if args.action=='restore':restore(args.node,backup)
    else:
        image=json.loads((ROOT/'data/runtime/unified/images.json').read_text())['node']['local_ref']
        upgrade(args.node,mapping[args.node],image,backup)
if __name__=='__main__':main()
