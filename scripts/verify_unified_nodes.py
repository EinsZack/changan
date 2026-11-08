#!/usr/bin/env python
"""Verify CPU-only and CPU/GPU operation on unchanged lab node identities."""
import argparse,json,os,subprocess,time
from pathlib import Path
from upgrade_lab_nodes import NODES,kube,validate_mapping
ROOT=Path(__file__).resolve().parent.parent

def manifest(lab,image,gpu=None):
    name='runtime-'+lab+('-both' if gpu else '-cpu')
    env=[{'name':'POD_NAME','valueFrom':{'fieldRef':{'fieldPath':'metadata.name'}}},{'name':'NODE_NAME','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}}]
    resources={'requests':{'cpu':'1','memory':'2Gi'},'limits':{'cpu':'2','memory':'2Gi'}}
    args=[] if gpu else ['--cpu-only']
    if gpu:
        env.append({'name':'EXPECTED_GPU_UUIDS','value':gpu});count=str(len(gpu.split(',')));resources['requests']['nvidia.com/gpu']=count;resources['limits']['nvidia.com/gpu']=count
    spec={'nodeSelector':{'kubernetes.io/hostname':NODES[lab]},'restartPolicy':'Never','activeDeadlineSeconds':600,'automountServiceAccountToken':False,
          'securityContext':{'runAsUser':10001,'runAsGroup':10001,'runAsNonRoot':True,'fsGroup':10001,'seccompProfile':{'type':'RuntimeDefault'}},
          'containers':[{'name':'compute','image':image,'imagePullPolicy':'IfNotPresent','command':['python','/opt/diagnostics/cpu_gpu_probe.py',*args],'env':env,'resources':resources,
                         'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'volumeMounts':[{'name':'tmp','mountPath':'/tmp'}]}],
          'volumes':[{'name':'tmp','emptyDir':{'sizeLimit':'128Mi'}}]}
    if gpu:spec['runtimeClassName']='nvidia'
    return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':'changan-runtime-validation'},'spec':spec}

def main():
    p=argparse.ArgumentParser();p.add_argument('--concurrent',action='store_true',help='Launch the GPU probes together to verify shared physical GPU access');p.add_argument('--mapping',default='data/runtime/unified/gpu-map.json');p.add_argument('--node',choices=list(NODES));p.add_argument('--report',default='docs/unified-nodes-20261108/runtime-verification.json');args=p.parse_args()
    mapping=json.loads((ROOT/args.mapping).read_text());inventory=subprocess.check_output(['nvidia-smi','--query-gpu=uuid','--format=csv,noheader'],text=True).splitlines();validate_mapping(mapping,inventory)
    image=json.loads((ROOT/'data/runtime/unified/images.json').read_text())['business']['cluster_ref']
    out=ROOT/args.report;out.parent.mkdir(parents=True,exist_ok=True)
    ns='changan-runtime-validation';kube('apply','-f','-',body={'apiVersion':'v1','kind':'Namespace','metadata':{'name':ns}})
    report={'image':image,'cases':[],'environment':'one physical server; four general CPU/GPU k3d nodes; shared server GPUs'}
    cases=[(args.node or 'lab-1',None)]+[(n,','.join(inventory) if mapping[n]=='all' else mapping[n]) for n in ([args.node] if args.node else list(NODES))]
    if args.concurrent:cases=[case for case in cases if case[1]]
    try:
        if args.concurrent:
            for lab,gpu in cases:kube('apply','-f','-',body=manifest(lab,image,gpu))
        for lab,gpu in cases:
            pod=manifest(lab,image,gpu);name=pod['metadata']['name']
            print('START',name,flush=True)
            if not args.concurrent:kube('apply','-f','-',body=pod)
            started=time.monotonic()
            while time.monotonic()-started<620:
                state=json.loads(kube('get','pod',name,'-n',ns,'-o','json'))
                if state['status']['phase'] in ['Succeeded','Failed']:break
                time.sleep(2)
            else:raise RuntimeError('Pod timeout: '+name)
            logs=kube('logs',name,'-n',ns)
            (out.parent/(out.stem+'-'+name+'.txt')).write_text(logs+'\n')
            if state['status']['phase']!='Succeeded':raise RuntimeError('Probe failed; inspect saved log: '+name)
            data=json.loads(logs.splitlines()[-1]);status=state['status']['containerStatuses'][0]
            assert data['node']==NODES[lab] and data['uid']==10001
            assert status['restartCount']==0
            assert all(s['pid']==data['pid'] for s in data['stages'])
            if gpu:
                assert {d['uuid'] for d in data['gpu']['devices']}==set(gpu.split(','))
                assert data['gpu']['visible_devices']==len(gpu.split(','))
                assert [s['backend'] for s in data['stages']]==['cpu','cuda','cpu']
                assert data['cpu_gpu_comparison']['allclose']
            evidence={'lab':lab,'pod_uid':state['metadata']['uid'],'container_id':status['containerID'],'image_id':status['imageID'],'phase':state['status']['phase'],'container_lifetime':status['state']['terminated'],'pod_spec':pod['spec'],'result':data}
            report['cases'].append(evidence);out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({'lab':lab,'mode':data['mode'],'pod_uid':state['metadata']['uid'],'gpu':data.get('gpu'),'backends':[s['backend'] for s in data['stages']]}),flush=True)
            kube('delete','pod',name,'-n',ns,'--wait=true','--timeout=60s')
    finally:
        kube('delete','namespace',ns,'--wait=true','--timeout=60s')
    report['concurrent']=args.concurrent
    if args.concurrent:
        starts=[c['container_lifetime']['startedAt'] for c in report['cases']];ends=[c['container_lifetime']['finishedAt'] for c in report['cases']]
        report['all_containers_overlapped']=max(starts)<min(ends)
        if not report['all_containers_overlapped']:raise RuntimeError('Probes succeeded but their lifetimes did not all overlap; rerun warm')
    report['cleaned']=True;out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n');print('PASSED',len(report['cases']),'cases',flush=True)
if __name__=='__main__':main()
