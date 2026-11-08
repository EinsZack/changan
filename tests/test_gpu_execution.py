import copy
import json
import time
import pytest
from fastapi import HTTPException
from app import db
from app.applications import application, bind_node, scheduling_params
from app.gpu_resources import gpu_snapshot, throughput_profiles
from app.inference_evidence import validate_inference
from app.kube import session_manifest
from app.schemas import TaskInput
from app.scheduler import choose
from test_session_execution import setup_session, get_session, patch_workload

UUID='GPU-1ba5825e-38ee-5dc3-2514-eb000355f95b'
IMAGE='registry/video@sha256:'+'1'*64
ARM='registry/video-arm@sha256:'+'2'*64

def telemetry():
    return dict(uuid=UUID,name='test GPU',memory_total_mb=32000,memory_free_mb=28000,memory_used_mb=4000,utilization_percent=10,observed_at=time.time())

def node(raw=None):
    return dict(gpu_allocatable=1,gpu_active=0,labels={'changan.io/gpu-uuid':UUID,'changan.io/cuda-ready':'true'},annotations={'changan.io/gpu-telemetry':json.dumps(raw if raw is not None else telemetry())})

def candidate(**snap):
    from app.config import MODEL_ID
    s=dict(node_id='gpu-1',architecture='amd64',healthy=True,models=[MODEL_ID],tags=['gpu','cuda','onnx'],available_memory_mb=4096,active=0,capacity=1,resource_source='kubernetes',**gpu_snapshot(node(),0))
    s.update(snap)
    return dict(id='gpu-1',enabled=True,observed=time.time(),reserved=0,snapshot=s)

def decide(n,**p):
    return choose([n],dict(inference_backend='cuda',min_gpu_memory_mb=2048,**p),4,1000,{'method':'resource_aware'})

def test_frozen_images_are_selected_by_architecture(monkeypatch):
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE',IMAGE)
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE_ARM64',ARM)
    a=application({'inference_backend':'cuda'})
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE_ARM64',IMAGE)
    assert bind_node(a,{'architecture':'arm64'})['image']==ARM
    assert a['gpu_count']==1 and a['expected_provider']=='CUDAExecutionProvider'
    assert set(scheduling_params({},a)['application_architectures'])=={'amd64','arm64'}
    with pytest.raises(ValueError):bind_node(a,{'architecture':'riscv64'})

@pytest.mark.parametrize('value',['registry/video:latest','registry/video@sha256:abc',''])
def test_unapproved_gpu_image_rejected(monkeypatch,value):
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE',value)
    monkeypatch.delenv('CHANGAN_VIDEO_CUDA_IMAGE_ARM64',raising=False)
    with pytest.raises(ValueError):application({'inference_backend':'cuda'})

@pytest.mark.parametrize('raw',[[],{}, {'uuid':UUID,'observed_at':time.time()}, telemetry()|{'memory_free_mb':float('nan')},telemetry()|{'memory_free_mb':999999},telemetry()|{'uuid':'wrong'},telemetry()|{'observed_at':time.time()-30},telemetry()|{'observed_at':time.time()+60}])
def test_bad_or_expired_gpu_telemetry_cannot_schedule(raw):
    s=gpu_snapshot(node(raw),0)
    assert not s['gpu_telemetry_fresh']
    assert decide(candidate(**s))['target_node'] is None

@pytest.mark.parametrize('changes,reason',[
    ({'gpu_runtime_ready':False},'CUDA 运行环境未就绪'),
    ({'gpu_available':0},'GPU 已占用、预留或未注册'),
    ({'gpu_memory_free_mb':1024},'可用显存不足或未知'),
    ({'gpu_observed_at':time.time()-30},'GPU 资源观测缺失或过期'),
    ({'architecture':'arm64'},'没有匹配节点架构的冻结镜像'),
])
def test_gpu_resource_filter(changes,reason):
    r=decide(candidate(**changes),application_architectures=['amd64'])
    assert r['target_node'] is None and reason in r['candidates'][0]['reasons']

def test_occupied_and_reserved_gpu_do_not_double_subtract():
    n=node();n['gpu_active']=1
    assert gpu_snapshot(n,1)['gpu_available']==0
    assert gpu_snapshot(node(),1)['gpu_available']==0
    assert gpu_snapshot(node(),0)['gpu_available']==1

def test_gpu_identity_and_single_device_registration_required():
    n=node();n['gpu_allocatable']=8
    assert not gpu_snapshot(n,0)['gpu_runtime_ready']
    n=node();n['labels'].pop('changan.io/gpu-uuid')
    assert not gpu_snapshot(n,0)['gpu_runtime_ready']

def test_gpu_manifest_reserves_one_device_and_cpu_does_not():
    s={'id':'s','task_id':'t','node_id':'gpu-1'}
    w=dict(job_name='j',kube_node='node',image=IMAGE,architecture='amd64',gpu_count=1,host_memory_min_mb=2048)
    params=TaskInput().model_dump()
    pod=session_manifest(s,w,params,400)['spec']['template']['spec']
    assert pod['runtimeClassName']=='nvidia'
    c=pod['containers'][0]
    assert c['resources']['limits']['nvidia.com/gpu']=='1'
    assert c['resources']['requests']['memory']=='2048Mi'
    assert pod['securityContext']['runAsNonRoot']
    cpu=session_manifest(s,w|{'gpu_count':0},params,400)['spec']['template']['spec']
    assert 'runtimeClassName' not in cpu and 'nvidia.com/gpu' not in cpu['containers'][0]['resources']['limits']

def proof():
    return dict(backend='cuda',provider='CUDAExecutionProvider',model_sha256='hash',gpu_compute_events=42,gpu={'uuid':UUID,'visible_device_count':1})

@pytest.mark.parametrize('change',[{'backend':'cpu'},{'provider':'CPUExecutionProvider'},{'model_sha256':'bad'},{'gpu_compute_events':0},{'gpu_compute_events':True},{'gpu':{'uuid':'wrong','visible_device_count':1}},{'gpu':{'uuid':UUID,'visible_device_count':8}}])
def test_cuda_result_rejects_fallback_and_wrong_device(change):
    w=dict(inference_backend='cuda',model_sha256='hash',expected_gpu_uuid=UUID)
    with pytest.raises(HTTPException):validate_inference(w,proof()|change)

def test_runtime_proof_passes_and_legacy_results_remain_valid():
    validate_inference(dict(inference_backend='cuda',model_sha256='hash',expected_gpu_uuid=UUID),proof())
    validate_inference({},None)

def test_cpu_history_never_used_for_cuda_score():
    n=candidate(measured_fps=999,inference_profiles={'cpu':{'fps':999,'image':IMAGE}})
    r=decide(n,application_images={'amd64':IMAGE})['candidates'][0]
    assert r['compute_seconds']==1 and '尚未校准' in r['estimate_source']
    n['snapshot']['inference_profiles']['cuda']={'fps':80,'image':ARM}
    assert decide(n,application_images={'amd64':IMAGE})['candidates'][0]['compute_seconds']==1
    n['snapshot']['inference_profiles']['cuda']['image']=IMAGE
    assert decide(n,application_images={'amd64':IMAGE})['candidates'][0]['compute_seconds']==.1

@pytest.mark.asyncio
async def test_own_gpu_session_reuses_reservation_without_requiring_free_vram_again():
    e,sessions,tid=setup_session()
    patch_workload('session_one',{'gpu_count':1,'inference_backend':'cuda','expected_gpu_uuid':UUID})
    db.execute("UPDATE execution_sessions SET active_attempt=NULL WHERE id='session_one'")
    n=candidate(gpu_available=0,gpu_memory_free_mb=100,active=1,memory_budget_mb=4096)
    n['id']='lab-1';n['snapshot']['node_id']='lab-1';n['reserved']=1
    assert decide(n)['target_node'] is None
    own=sessions.scheduling_nodes([copy.deepcopy(n)],tid)
    assert decide(own[0])['target_node']=='lab-1'
    other=sessions.scheduling_nodes([copy.deepcopy(n)],'other-task')
    assert decide(other[0])['target_node'] is None
    await e.close()

@pytest.mark.asyncio
async def test_gpu_identity_change_fences_existing_session():
    e,sessions,tid=setup_session()
    patch_workload('session_one',{'expected_gpu_uuid':UUID})
    n=db.query("SELECT snapshot FROM nodes WHERE id='lab-1'",one=True)
    s=db.loads(n['snapshot']);s['gpu_uuid']='GPU-other'
    db.execute("UPDATE nodes SET snapshot=? WHERE id='lab-1'",(db.dumps(s),))
    await sessions.reconcile_one(get_session('session_one'))
    assert get_session('session_one')['status']=='STOPPING'
    await e.close()

@pytest.mark.asyncio
async def test_ready_rejects_cpu_fallback_then_persists_cuda_proof():
    import httpx
    from app.main import app
    from test_session_execution import sh
    e,sessions,tid=setup_session()
    patch_workload('session_one',dict(inference_backend='cuda',expected_gpu_uuid=UUID))
    db.execute("UPDATE execution_sessions SET ready_at=NULL,model_load_count=0,status='STARTING' WHERE id='session_one'")
    evidence=proof()|{'model_sha256':'modelhash'}
    body={'model_sha256':'modelhash','model_load_seconds':1.2,'inference':evidence|{'provider':'CPUExecutionProvider'}}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        assert (await c.post('/internal/sessions/session_one/ready',headers=sh(),json=body)).status_code==422
        assert get_session('session_one')['model_load_count']==0
        body['inference']=evidence
        assert (await c.post('/internal/sessions/session_one/ready',headers=sh(),json=body)).status_code==200
        assert (await c.post('/internal/sessions/session_one/ready',headers=sh(),json=body)).status_code==200
    s=get_session('session_one')
    assert s['model_load_count']==1 and db.loads(s['workload'])['inference']['gpu']['uuid']==UUID
    await e.close()

@pytest.mark.asyncio
async def test_duplicate_physical_gpu_registration_disables_both_nodes(monkeypatch):
    from app.container_execution import ContainerExecution
    from app.engine import Engine
    monkeypatch.setenv('CHANGAN_VIDEO_IMAGE',IMAGE)
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE',IMAGE)
    e=Engine();ce=ContainerExecution(e)
    class K:
        cluster='test'
        async def nodes(self):
            return [dict(id=f'n{i}',name=f'node{i}',uid=f'uid{i}',architecture='amd64',healthy=True,memory=4096,active=0,capacity=1,runtime='containerd',kubelet='test',physical_host='same',kind='logical',**node()) for i in range(2)]
    ce.kube=K();await ce.refresh()
    for n in db.query("SELECT snapshot FROM nodes WHERE id IN ('n0','n1')"):
        s=db.loads(n['snapshot']);assert s['gpu_available']==0 and not s['gpu_runtime_ready'] and '重复登记' in s['gpu_registration_error']
    await e.close()

def test_gpu_worker_fails_before_loading_model_when_cuda_provider_unavailable(monkeypatch):
    from workers import inference
    monkeypatch.setattr(inference.ort,'get_available_providers',lambda:['CPUExecutionProvider'])
    if hasattr(inference.ort,'preload_dlls'):monkeypatch.setattr(inference.ort,'preload_dlls',lambda:None)
    with pytest.raises(RuntimeError,match='禁止静默回退'):
        inference.Detector('not-needed.onnx',backend='cuda')

def test_gpu_architecture_alias_is_normalized_in_frozen_scheduling_params(monkeypatch):
    monkeypatch.setenv('CHANGAN_VIDEO_CUDA_IMAGE',IMAGE)
    a=application({'inference_backend':'cuda','architecture':'x86_64'})
    p=scheduling_params({'architecture':'x86_64'},a)
    assert p['architecture']=='amd64'
    assert decide(candidate(),application_architectures=p['application_architectures'],architecture=p['architecture'])['target_node']=='gpu-1'


def test_gpu_accounting_includes_init_containers_and_persistent_sidecars():
    from types import SimpleNamespace as NS
    from app.kube import pod_gpu_requests
    def c(n,sidecar=False):return NS(resources=NS(requests={'nvidia.com/gpu':str(n)},limits={}),restart_policy='Always' if sidecar else None)
    p=NS(spec=NS(containers=[c(1)],init_containers=[c(2)]))
    assert pod_gpu_requests(p)==2
    p.spec.init_containers=[c(1,True),c(2)]
    assert pod_gpu_requests(p)==3
