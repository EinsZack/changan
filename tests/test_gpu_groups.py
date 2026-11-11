import pytest
from app.container_execution import gpu_supplemental_groups
from app.kube import session_manifest

def test_gpu_groups_are_numeric_and_configured(monkeypatch):
    monkeypatch.setenv('CHANGAN_GPU_SUPPLEMENTAL_GROUPS','993,44,44')
    assert gpu_supplemental_groups()==[44,993]
    for value in ['root','0','-1','44,','2147483648']:
        monkeypatch.setenv('CHANGAN_GPU_SUPPLEMENTAL_GROUPS',value)
        with pytest.raises(ValueError):gpu_supplemental_groups()
    monkeypatch.delenv('CHANGAN_GPU_SUPPLEMENTAL_GROUPS')
    assert gpu_supplemental_groups()==[]

def test_groups_apply_only_to_gpu_workload():
    session={'id':'session_a','node_id':'thor1','task_id':'task_a'}
    workload={'job_name':'video-a','image':'image@sha256:abc','architecture':'arm64','kube_node':'thor1','gpu_access':True,'gpu_supplemental_groups':[44,993]}
    params={'attempt_timeout':180,'min_memory_mb':2048}
    spec=session_manifest(session,workload,params,600)['spec']['template']['spec']
    assert spec['securityContext']['supplementalGroups']==[44,993]
    assert spec['securityContext']['runAsUser']==10001
    assert spec['containers'][0]['securityContext']['allowPrivilegeEscalation'] is False
    workload['gpu_access']=False
    spec=session_manifest(session,workload,params,600)['spec']['template']['spec']
    assert 'supplementalGroups' not in spec['securityContext']
