import time
from app.scheduler import choose
from app.config import MODEL_ID

def node(id='n1',**changes):
    n={'id':id,'enabled':1,'observed':time.time(),'reserved':0,'snapshot':{'healthy':True,'node_id':id,'models':[MODEL_ID],'available_memory_mb':2048,'capacity':1,'active':0,'architecture':'x86_64','tags':['cpu'],'measured_fps':10}}
    n.update(changes);return n

def test_stale_busy_incompatible_are_excluded():
    stale=node('stale',observed=time.time()-30)
    busy=node('busy',reserved=1)
    incompatible=node('arm');incompatible['snapshot']['models']=[]
    d=choose([stale,busy,incompatible,node('good')],{'min_memory_mb':256,'sample_fps':2},4,10,{'method':'resource_aware'})
    assert d['target_node']=='good'
    assert all(r['reasons'] for r in d['candidates'][:3])
def test_empty_candidates_explain_reason():
    d=choose([node()],{'required_tags':['gpu']},4,10,{'method':'round_robin'})
    assert d['target_node'] is None and '缺少必要标签' in d['candidates'][0]['reasons']
def test_estimates_prefer_measured_speed_and_round_robin_rotates():
    a,b=node('a'),node('b');b['snapshot']['measured_fps']=20
    assert choose([a,b],{},4,100,{'method':'resource_aware'})['target_node']=='b'
    assert choose([a,b],{},4,100,{'method':'round_robin'},1)['target_node']=='b'
def test_resource_memory_arch_and_allowlist():
    d=choose([node()],{'architecture':'aarch64','node_ids':['n2'],'min_memory_mb':4096},4,1,{'method':'round_robin'})
    assert len(d['candidates'][0]['reasons'])==3

def test_location_is_a_hard_constraint_when_requested():
    d=choose([node()],{'location':'parking-A'},4,10,{'method':'round_robin'})
    assert d['target_node'] is None
    assert '节点位置不匹配' in d['candidates'][0]['reasons']
