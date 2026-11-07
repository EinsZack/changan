import time,hashlib
from types import SimpleNamespace as NS
import pytest,httpx
from app import db
from app.kube import job_manifest
from app.container_execution import ContainerExecution,workload
from app.config import DATA,MODEL_ID
from test_state_machine import seeded
from app.main import app
import app.container_execution as ce

def manifest_fixture():
    return ({'id':'try_test','node_id':'lab-1'},{'job_name':'video-try-test','image':'registry/video@sha256:'+'a'*64,'architecture':'amd64','kube_node':'agent-0'}, {'attempt_timeout':30,'min_memory_mb':256})
def test_job_preserves_business_scheduler_and_runtime_boundary():
    m=job_manifest(*manifest_fixture());s=m['spec'];pod=s['template']['spec']
    assert s['backoffLimit']==0 and pod['restartPolicy']=='Never'
    assert 'nodeName' not in pod and pod['nodeSelector']['changan.io/node-id']=='lab-1'
    assert pod['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms'][0]['matchFields'][0]['values']==['agent-0']
    assert not pod['automountServiceAccountToken']
    assert pod['containers'][0]['securityContext']['readOnlyRootFilesystem']
    assert 'command' not in pod['containers'][0]
    assert pod['volumes'][0]['secret']['secretName']==m['metadata']['name']

def seed_container():
    e,tid=seeded();c=ContainerExecution(e);e.containers=c
    w={'job_name':'video-a1','job_uid':'j1','pod_uid':'p1','node_name':'agent-0','image':'image@sha256:abc','model_id':MODEL_ID,'model_sha256':'modelhash','input_sha256':'inputhash'}
    db.execute("UPDATE attempts SET backend='k3s',workload=?,cleanup_state='PENDING' WHERE id='a1'",(db.dumps(w),))
    return e,c,tid
def headers(uid='p1',token='test-token'):return {'x-attempt-token':token,'x-pod-uid':uid}
def job():return NS(metadata=NS(uid='j1',labels={'changan.io/attempt':'a1'}),status=NS(conditions=[]))
class FakeKube:
    def __init__(self,job_value=None,pod_values=None):self.job_value=job_value;self.pod_values=pod_values or [];self.deleted=[];self.secret_deleted=False
    async def job(self,name):return self.job_value
    async def pods(self,name):return self.pod_values
    async def delete(self,name):self.deleted.append(name)
    async def delete_secret(self,name):self.secret_deleted=True
def pod(phase='Running',uid='p1'):
    return NS(metadata=NS(name='pod-a1',uid=uid,deletion_timestamp=None,owner_references=[NS(uid='j1')]),
       spec=NS(node_name='agent-0'),status=NS(phase=phase,container_statuses=[],start_time=None))

@pytest.mark.asyncio
async def test_cancel_holds_slot_until_actual_resources_gone():
    e,c,tid=seed_container();c.kube=FakeKube(job(),[pod()])
    e.fail_task(tid,'cancel','CANCELED')
    db.execute("UPDATE nodes SET observed=?,snapshot=?,error=NULL WHERE id='lab-1'",(time.time(),db.dumps({'healthy':True})))
    await c.reconcile()
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    assert a['status']=='CLEANING' and a['cleanup_state']=='PENDING'
    assert next(n for n in e.nodes() if n['id']=='lab-1')['reserved']==1
    c.kube.job_value=None;c.kube.pod_values=[]
    await c.reconcile()
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    assert a['status']=='CANCELED' and a['cleanup_state']=='DONE' and c.kube.secret_deleted
    await e.close()

@pytest.mark.asyncio
async def test_api_outage_does_not_retry_or_release():
    e,c,tid=seed_container()
    class Down(FakeKube):
        async def job(self,name):raise TimeoutError()
    c.kube=Down()
    await c.reconcile()
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    assert a['status']=='RUNNING' and a['cleanup_state']=='PENDING'
    assert db.query("SELECT retries FROM segments WHERE id='s1'",one=True)['retries']==0
    await e.close()

@pytest.mark.asyncio
async def test_completed_pod_without_durable_result_is_not_success():
    e,c,tid=seed_container();c.kube=FakeKube(job(),[pod('Succeeded')])
    await c.reconcile()
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    assert a['status']!='SUCCEEDED' and workload(a)['terminal_after_cleanup']=='FAILED'
    c.kube.job_value=None;c.kube.pod_values=[]
    db.execute("UPDATE nodes SET observed=?,snapshot=?,error=NULL WHERE id='lab-1'",(time.time(),db.dumps({'healthy':True})))
    await c.reconcile()
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='FAILED'
    assert db.query("SELECT retries FROM segments WHERE id='s1'",one=True)['retries']==1
    await e.close()

@pytest.mark.asyncio
async def test_attempt_auth_fencing_and_artifact_digest(tmp_path):
    e,c,tid=seed_container()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        url='/internal/container/a1/artifacts/preview-0.jpg'
        assert (await client.put(url,content=b'jpg',headers=headers(token='bad'))).status_code==401
        assert (await client.put(url,content=b'jpg',headers=headers(uid='other'))).status_code==409
        assert (await client.put(url,content=b'jpg',headers=headers())).status_code==422
        h=headers()|{'x-content-sha256':hashlib.sha256(b'jpg').hexdigest()}
        assert (await client.put(url,content=b'jpg',headers=h)).status_code==200
        e.fail_task(tid,'cancel','CANCELED')
        assert (await client.put(url,content=b'jpg',headers=h)).status_code==409
    await e.close()

@pytest.mark.asyncio
async def test_duplicate_pod_cannot_claim_same_attempt():
    e,c,tid=seed_container();c.kube=FakeKube(job(),[pod(uid='p2')])
    db.execute("UPDATE nodes SET snapshot=? WHERE id='lab-1'",(db.dumps({'kube_node':'agent-0'}),))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        assert (await client.post('/internal/container/a1/claim',headers=headers(uid='p2'))).status_code==409
    assert workload(db.query("SELECT * FROM attempts WHERE id='a1'",one=True))['pod_uid']=='p1'
    await e.close()

@pytest.mark.asyncio
async def test_result_requires_full_artifact_manifest_and_model_identity():
    e,c,tid=seed_container()
    r={'attempt_id':'a1','task_id':tid,'segment_id':'s1','actual_node':'lab-1','model_id':MODEL_ID,
       'model_sha256':'modelhash','input_sha256':'inputhash','pod_uid':'p1','sampled_frames':1,'frames':[{}],
       'previews':['preview-0.jpg'],'artifacts':[]}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        result=await client.post('/internal/container/a1/events',headers=headers(),json={'seq':1,'status':'FINALIZING','result':r})
        assert result.status_code==422
        content=b'jpg';digest=hashlib.sha256(content).hexdigest()
        await client.put('/internal/container/a1/artifacts/preview-0.jpg',headers=headers()|{'x-content-sha256':digest},content=content)
        r['artifacts']=[{'name':'preview-0.jpg','sha256':digest,'bytes':3}]
        result=await client.post('/internal/container/a1/events',headers=headers(),json={'seq':1,'status':'FINALIZING','result':r})
        assert result.status_code==200
        assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='FINALIZING'
        assert (await client.post('/internal/container/a1/events',headers=headers(),json={'seq':1,'status':'FINALIZING','result':r})).json()['duplicate']
    await e.close()


@pytest.mark.asyncio
async def test_running_pod_does_not_mean_model_ready():
    e,c,tid=seed_container();c.kube=FakeKube(job(),[pod()])
    db.execute("UPDATE attempts SET status='SUBMITTED' WHERE id='a1'")
    await c.reconcile()
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='CONTAINER_STARTING'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        r=await client.post('/internal/container/a1/events',headers=headers(),json={'seq':1,'status':'MODEL_LOADING'})
        assert r.status_code==200
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='MODEL_LOADING'
    await e.close()


@pytest.mark.asyncio
async def test_coordinator_observation_preserves_concurrent_claim_and_application_phase():
    from app.container_execution import save,stage
    e,c,tid=seed_container()
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    stale=workload(a);stale.pop('pod_uid')
    newer=workload(a)|{'application_phase':'UPLOADING'}
    db.execute("UPDATE attempts SET workload=?,status='UPLOADING' WHERE id='a1'",(db.dumps(newer),))
    save(a,stale|{'pods':[]})
    stage(a,'SUBMITTED')
    latest=db.query("SELECT * FROM attempts WHERE id='a1'",one=True)
    assert workload(latest)['pod_uid']=='p1'
    assert workload(latest)['application_phase']=='UPLOADING'
    assert latest['status']=='UPLOADING'
    await e.close()


@pytest.mark.asyncio
async def test_node_memory_budget_cannot_grow_from_untracked_pods_or_pool_overlay():
    from app.engine import Engine
    e=Engine();c=ContainerExecution(e)
    class Nodes(FakeKube):
        cluster='test-cluster'
        async def nodes(self):
            return [{'id':'lab-1','name':'agent-0','uid':'n1','healthy':True,'architecture':'amd64','memory':3072,'active':1,'capacity':2,'kind':'k3s_logical','runtime':'containerd','kubelet':'test','physical_host':'one-host'}]
    c.kube=Nodes();c.application=lambda:{'architecture':'amd64'}
    await c.refresh()
    s=db.loads(db.query("SELECT snapshot FROM nodes WHERE id='lab-1'",one=True)['snapshot'])
    assert s['available_memory_mb']==2048
    class Pool:
        enabled=True
        async def snapshots(self):
            return {'lab-1':{'node_id':'lab-1','healthy':True,'available_memory_mb':16384,'active':3,'capacity':8,'observed_at':time.time()}}
    e.resource_provider=Pool()
    await c.refresh()
    s=db.loads(db.query("SELECT snapshot FROM nodes WHERE id='lab-1'",one=True)['snapshot'])
    assert s['available_memory_mb']==2048 and s['capacity']==2 and s['active']==3
    await e.close()
