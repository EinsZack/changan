import time
import pytest
from fastapi.testclient import TestClient
from app import db
from app.main import app
from app.config import MODEL_ID

@pytest.mark.parametrize('required,eligible',[(65537,True),(999999,True),(1048576,True),(1048577,False)])
def test_candidate_memory_uses_node_capacity_instead_of_64_gib_limit(required,eligible):
    snapshot={'node_id':'lab-1','healthy':True,'models':[MODEL_ID],'tags':['cpu'],
              'available_memory_mb':1048576,'capacity':1,'active':0}
    db.execute("UPDATE nodes SET observed=?,snapshot=?,enabled=1,error=NULL WHERE id='lab-1'",(time.time(),db.dumps(snapshot)))
    client=TestClient(app)
    response=client.post('/api/scheduling/candidates',json={'params':{'min_memory_mb':required}})
    assert response.status_code==200
    row=next(n for n in response.json()['candidates'] if n['node_id']=='lab-1')
    assert row['eligible'] is eligible
    assert ('可用内存不足' in row['reasons']) is (not eligible)

@pytest.mark.parametrize('required',[63,256.5])
def test_memory_still_requires_integer_at_least_64_mib(required):
    client=TestClient(app)
    assert client.post('/api/scheduling/candidates',json={'params':{'min_memory_mb':required}}).status_code==422
    assert client.post('/api/tasks',json={'min_memory_mb':required}).status_code==422

def test_task_and_strategy_preserve_large_memory_requirement():
    client=TestClient(app)
    task=client.post('/api/tasks',json={'min_memory_mb':999999})
    assert task.status_code==200
    assert task.json()['task']['params']['min_memory_mb']==999999
    strategy=client.post('/api/strategies',json={'name':'large memory regression','enabled':False,'params':{'min_memory_mb':999999}})
    assert strategy.status_code==200
    saved=next(s for s in client.get('/api/strategies').json() if s['id']==strategy.json()['id'])
    assert saved['params']['min_memory_mb']==999999
