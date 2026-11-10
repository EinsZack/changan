from datetime import datetime, timezone
from types import SimpleNamespace as NS
import copy
import pytest
import httpx
from fastapi import HTTPException
from app import db
from app.config import MODEL
from app.execution_timing import pod_lifecycle, validate_runtime, validate_segment, validate_startup
from app.kube import session_manifest
from app.session_execution import patch_workload, get_session
from test_session_execution import setup_session, sh

def runtime():
    return {'policy':'cpu','actual_backend':'cpu','providers':['CPUExecutionProvider'],
            'cuda_compute_events':0,'gpu':None,'fallback_reason':None,
            'initialization_stages':[{'name':'model_load','started_at':10.,'seconds':.2}],
            'timings':{'initialization_seconds':.5,'session_seconds':.2,'warmup_seconds':.2,'verification_seconds':.01}}

def segment():
    return {'inference_runtime':runtime(), 'frames':[{'timing':{'inference_offset_seconds':.3,'inference_seconds':.4,'preprocess_seconds':.1,'postprocess_seconds':.1}}],
        'segment_timing':{'version':1,'started_at':1.,'total_seconds':3.,'phases':[
            {'name':'download','offset_seconds':0.,'seconds':.5},{'name':'analyze','offset_seconds':.6,'seconds':2.},{'name':'upload','offset_seconds':2.7,'seconds':.2}]},
        'analysis_timing':{'decode_seconds':.3,'preprocess_seconds':.1,'inference_seconds':.4,'postprocess_seconds':.1,'preview_seconds':.1,'other_seconds':1.,'analysis_seconds':2.,'analysis_started_at':1.6}}

def startup():
    return {'entry_at':1.,'imports_seconds':.2,'claim_started_at':1.3,'claim_seconds':.1,
            'model_started_at':1.5,'model_ready_at':2.,'model_load_seconds':.5}

def test_auto_cpu_fallback_records_reason_and_strict_cuda_fails(monkeypatch):
    from workers.inference import Detector
    monkeypatch.setattr('workers.inference.ort.get_available_providers', lambda:['CPUExecutionProvider'])
    d=Detector(MODEL, policy='auto', instrument=True)
    assert d.runtime['actual_backend']=='cpu'
    assert 'CUDAExecutionProvider' in d.runtime['fallback_reason']
    validate_runtime(d.runtime, {'model_policy':'auto'})
    with pytest.raises(RuntimeError, match='CUDA initialization failed'):
        Detector(MODEL, policy='cuda', instrument=True)

def test_gpu_access_is_capability_of_same_session_not_a_new_node_kind():
    w={'job_name':'job','architecture':'amd64','kube_node':'original-agent','image':'same-image','gpu_access':True,'container_memory_floor_mb':2048}
    s={'id':'same-session','node_id':'lab-1','task_id':'t'}
    p={'attempt_timeout':180,'min_memory_mb':256}
    gpu=session_manifest(s,w,p,300)['spec']['template']['spec']
    cpu=session_manifest(s,w|{'gpu_access':False},p,300)['spec']['template']['spec']
    assert gpu['nodeSelector']==cpu['nodeSelector']
    assert gpu['runtimeClassName']=='nvidia'
    assert gpu['containers'][0]['image']==cpu['containers'][0]['image']
    assert gpu['containers'][0]['resources']['limits']['nvidia.com/gpu']=='1'
    assert gpu['containers'][0]['resources']['limits']['memory']=='2048Mi'
    assert cpu['containers'][0]['resources']['limits']['memory']=='2048Mi'
    assert 'runtimeClassName' not in cpu
    assert 'nvidia.com/gpu' not in cpu['containers'][0]['resources']['limits']

def test_cache_event_never_fabricates_pull_duration():
    at=datetime(2026,11,10,tzinfo=timezone.utc)
    pod=NS(metadata=NS(uid='p',name='pod',creation_timestamp=at),spec=NS(node_name='n'),
           status=NS(container_statuses=[],conditions=[]))
    event=NS(reason='Pulled',first_timestamp=at,message='Container image already present on machine',metadata=NS(creation_timestamp=at))
    life=pod_lifecycle(pod,[event])
    assert life['image_cached'] is True
    assert 'image_started_at' not in life
    assert 'scheduled_at' not in life
    assert pod_lifecycle(pod,[])=={'pod_uid':'p','pod_name':'pod','node_name':'n','pod_created_at':at.timestamp()}

@pytest.mark.parametrize('mutation',[
    lambda r:r['analysis_timing'].update(inference_seconds=float('nan')),
    lambda r:r['frames'][0]['timing'].update(inference_offset_seconds=3),
    lambda r:r['segment_timing']['phases'][2].update(offset_seconds=.1),
    lambda r:r['analysis_timing'].update(inference_seconds=.8),
    lambda r:r['inference_runtime'].update(actual_backend='cuda'),
])
def test_segment_rejects_invalid_or_inconsistent_evidence(mutation):
    r=segment(); validate_segment(r,runtime())
    mutation(r)
    with pytest.raises(HTTPException):validate_segment(r,runtime())

def test_cuda_provider_name_alone_is_not_cuda_execution_evidence():
    r=runtime()|{'policy':'auto','actual_backend':'cuda','providers':['CUDAExecutionProvider']}
    with pytest.raises(HTTPException):validate_runtime(r,{'model_policy':'auto','gpu_access':True})

@pytest.mark.asyncio
async def test_ready_evidence_is_frozen_and_copied_to_assignment():
    e,sessions,tid=setup_session()
    db.execute("UPDATE execution_sessions SET ready_at=NULL,status='STARTING',model_load_count=0")
    db.execute("UPDATE attempts SET execution_id=NULL WHERE id='a1'")
    patch_workload('session_one', {'timing_version':1,'model_policy':'cpu'})
    from app.main import app
    body={'model_sha256':'modelhash','model_load_seconds':.5,'startup_timing':startup(),'inference_runtime':runtime()}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        url='/internal/sessions/session_one/ready'
        assert (await c.post(url,headers=sh(),json=body)).status_code==200
        assert (await c.post(url,headers=sh(),json=body)).status_code==200
        changed=copy.deepcopy(body);changed['inference_runtime']['ort_version']='changed'
        assert (await c.post(url,headers=sh(),json=changed)).status_code==409
        assert (await c.post('/internal/sessions/session_one/next',headers=sh())).status_code==200
    assert get_session('session_one')['model_load_count']==1
    assert db.loads(db.query("SELECT workload FROM attempts WHERE id='a1'",one=True)['workload'])['inference_runtime']==runtime()
    await e.close()
