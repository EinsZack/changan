"""Kubernetes API boundary. Every call is bounded and run off the async event loop."""
import asyncio, os
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from kubernetes.utils.quantity import parse_quantity

LABEL = 'changan.io/node-id'
APP = 'changan-video'
class Kube:
    def __init__(self):
        cfg=client.Configuration()
        config.load_kube_config(os.environ['CHANGAN_KUBECONFIG'],client_configuration=cfg)
        cfg.retries=0
        self.api=client.ApiClient(cfg)
        self.core=client.CoreV1Api(self.api); self.batch=client.BatchV1Api(self.api)
        self.namespace=os.getenv('CHANGAN_NAMESPACE','changan')
        self.cluster=os.getenv('CHANGAN_CLUSTER_ID','changan-dev')
    async def call(self,fn,*args,**kwargs):
        return await asyncio.to_thread(fn,*args,**kwargs,_request_timeout=(3,8))
    async def nodes(self):
        nodes=await self.call(self.core.list_node,label_selector=LABEL)
        pods=await self.call(self.core.list_namespaced_pod,self.namespace,label_selector='app='+APP)
        output=[]
        for n in nodes.items:
            labels=n.metadata.labels; ready=any(c.type=='Ready' and c.status=='True' for c in n.status.conditions or [])
            ready=ready and not n.spec.unschedulable and not any(t.effect in ('NoSchedule','NoExecute') for t in n.spec.taints or [])
            active=sum(p.spec.node_name==n.metadata.name and p.status.phase not in ('Succeeded','Failed') for p in pods.items)
            memory=int(parse_quantity(n.status.allocatable.get('memory','0'))/1024**2)
            budget=min(memory,int(labels.get('changan.io/memory-mib',memory)))
            output.append({'id':labels[LABEL],'name':n.metadata.name,'uid':n.metadata.uid,'healthy':ready,
                'architecture':labels.get('kubernetes.io/arch'),'memory':budget,'active':active,
                'gpu_slots':int(n.status.allocatable.get('nvidia.com/gpu','0')),'capacity':int(labels.get('changan.io/slots','1')),'kind':labels.get('changan.io/kind','k3s_node'),
                'runtime':n.status.node_info.container_runtime_version,'kubelet':n.status.node_info.kubelet_version,
                'physical_host':labels.get('changan.io/physical-host',n.metadata.name)})
        return output
    async def job(self,name):
        try:return await self.call(self.batch.read_namespaced_job,name,self.namespace)
        except ApiException as e:
            if e.status==404:return None
            raise
    async def pods(self,name):
        return (await self.call(self.core.list_namespaced_pod,self.namespace,label_selector='job-name='+name)).items
    async def pod_events(self,uid):
        return (await self.call(self.core.list_namespaced_event,self.namespace,field_selector='involvedObject.uid='+uid)).items
    async def submit(self,manifest,payload):
        name=manifest['metadata']['name']
        secret={'apiVersion':'v1','kind':'Secret','metadata':{'name':name,'labels':{'app':APP}},'stringData':{'payload.json':__import__('json').dumps(payload)}}
        try: await self.call(self.core.create_namespaced_secret,self.namespace,secret)
        except ApiException as e:
            if e.status!=409:raise
        try: return await self.call(self.batch.create_namespaced_job,self.namespace,manifest)
        except ApiException as e:
            if e.status!=409:raise
            job=await self.job(name)
            identity='session' if 'session_id' in payload else 'attempt'
            if job.metadata.labels.get('changan.io/'+identity)!=payload[identity+'_id']:raise ValueError('Job identity collision')
            return job
    async def logs(self,name):
        return await self.call(self.core.read_namespaced_pod_log,name,self.namespace,tail_lines=50,limit_bytes=8000)
    async def delete(self,name):
        try:await self.call(self.batch.delete_namespaced_job,name,self.namespace,propagation_policy='Foreground')
        except ApiException as e:
            if e.status!=404:raise
    async def delete_secret(self,name):
        try:await self.call(self.core.delete_namespaced_secret,name,self.namespace)
        except ApiException as e:
            if e.status!=404:raise

def job_manifest(a, workload, params):
    name=workload['job_name']
    return {'apiVersion':'batch/v1','kind':'Job','metadata':{'name':name,'labels':{'app':APP,'changan.io/attempt':a['id']}},
      'spec':{'backoffLimit':0,'activeDeadlineSeconds':params['attempt_timeout']+30,
       'template':{'metadata':{'labels':{'app':APP,'changan.io/attempt':a['id']}},
        'spec':{'restartPolicy':'Never','automountServiceAccountToken':False,
         'imagePullSecrets':[{'name':workload['image_pull_secret']}] if workload.get('image_pull_secret') else [],
         'nodeSelector':{LABEL:a['node_id'],'kubernetes.io/arch':workload['architecture']},
         'affinity':{'nodeAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':{'nodeSelectorTerms':[{'matchFields':[{'key':'metadata.name','operator':'In','values':[workload['kube_node']]}]}]}}},
         'terminationGracePeriodSeconds':10,
         'securityContext':{'runAsUser':10001,'runAsGroup':10001,'runAsNonRoot':True,'fsGroup':10001,'seccompProfile':{'type':'RuntimeDefault'}},
         'containers':[{'name':'video','image':workload['image'],'imagePullPolicy':'IfNotPresent',
          'env':[{'name':'POD_UID','valueFrom':{'fieldRef':{'fieldPath':'metadata.uid'}}},
                 {'name':'POD_NAME','valueFrom':{'fieldRef':{'fieldPath':'metadata.name'}}},
                 {'name':'KUBE_NODE_NAME','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}}],
          'resources':{'requests':{'cpu':'1','memory':str(max(512,workload.get('container_memory_floor_mb',0),params['min_memory_mb']))+'Mi'},
                       'limits':{'cpu':'2','memory':str(max(1024,workload.get('container_memory_floor_mb',0),params['min_memory_mb']))+'Mi'}},
          'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},
          'volumeMounts':[{'name':'payload','mountPath':'/run/task','readOnly':True},{'name':'work','mountPath':'/work'},{'name':'tmp','mountPath':'/tmp'}]}],
         'volumes':[{'name':'payload','secret':{'secretName':name,'defaultMode':288}},
                    {'name':'work','emptyDir':{'sizeLimit':'2Gi'}},{'name':'tmp','emptyDir':{'sizeLimit':'128Mi'}}]}}}}


def session_manifest(session, workload, params, remaining_seconds):
    """One pinned Job for the whole task/node session; segment retries are business-owned."""
    manifest=job_manifest(session,workload,params)
    labels={'app':APP,'changan.io/session':session['id'],'changan.io/task':session['task_id']}
    manifest['metadata']['labels']=labels
    manifest['spec']['template']['metadata']['labels']=dict(labels)
    manifest['spec']['activeDeadlineSeconds']=max(30,int(remaining_seconds)+30)
    if workload.get('gpu_access'):
        spec=manifest['spec']['template']['spec']
        spec['runtimeClassName']='nvidia'
        resources=spec['containers'][0]['resources']
        resources['requests']['nvidia.com/gpu']='1'
        resources['limits']['nvidia.com/gpu']='1'
    return manifest
