from pathlib import Path
import pytest
from scripts.upgrade_lab_nodes import validate_mapping

def test_allows_shared_gpu_across_simulated_nodes():
    mapping={f'lab-{i}':'GPU-shared' for i in range(1,5)}
    assert validate_mapping(mapping,['GPU-shared'])==mapping

def test_rejects_unknown_device_and_incomplete_node_set():
    mapping={f'lab-{i}':f'GPU-{i}' for i in range(1,5)}
    with pytest.raises(ValueError,match='Unknown'):validate_mapping(mapping,['GPU-1','GPU-2','GPU-3'])
    with pytest.raises(ValueError,match='exactly'):validate_mapping({'lab-1':'GPU-1'},['GPU-1'])

def test_accepts_four_distinct_node_devices():
    mapping={f'lab-{i}':f'GPU-{i}' for i in range(1,5)}
    assert validate_mapping(mapping,list(mapping.values()))==mapping

def test_supports_all_or_multiple_visible_gpus():
    mapping={"lab-1":"all","lab-2":"GPU-1,GPU-2","lab-3":"GPU-1","lab-4":"GPU-1"}
    assert validate_mapping(mapping,["GPU-1","GPU-2"])==mapping


def test_node_replacement_keeps_k3d_bootstrap_and_dynamic_network():
    from scripts.upgrade_lab_nodes import creation
    original={
        'Config':{'Hostname':'lab-host','Env':['PATH=/old','K3S_URL=https://cluster:6443'],'Labels':{'k3d.role':'agent'},'Entrypoint':['/bin/k3d-entrypoint.sh'],'Cmd':['agent']},
        'HostConfig':{'Privileged':True,'NanoCpus':3000000000,'Memory':8589934592},
        'Mounts':[{'Type':'volume','Name':'original-data','Destination':'/var/lib/rancher/k3s','Mode':'rw','RW':True}],
        'NetworkSettings':{'Networks':{'project-net':{'IPAddress':'172.29.1.6','Aliases':['lab-host']}}},
    }
    body=creation(original,'new-image','GPU-shared','/identity','/registry',[Path('/bootstrap/k3d-entrypoint.sh')])
    assert body['Entrypoint']==['/bin/k3d-entrypoint.sh']
    assert 'original-data:/var/lib/rancher/k3s:rw' in body['HostConfig']['Binds']
    assert '/bootstrap/k3d-entrypoint.sh:/bin/k3d-entrypoint.sh:ro' in body['HostConfig']['Binds']
    assert 'IPAMConfig' not in body['NetworkingConfig']['EndpointsConfig']['project-net']
    assert body['HostConfig']['NanoCpus']==3000000000
    assert not any(x.startswith('PATH=') for x in body['Env'])
