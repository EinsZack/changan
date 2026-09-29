from fastapi.testclient import TestClient
from app.main import app
from app import db

def test_strategy_crud_and_service_trigger():
    client=TestClient(app)
    r=client.post('/api/strategies',json={'name':'test','kind':'service','params':{'name':'triggered'}})
    assert r.status_code==200,r.text
    id=r.json()['id']
    response=client.post(f'/api/strategies/{id}/trigger');assert response.status_code==200
    task=db.task_detail(response.json()['id']);assert task['strategy_id']==id and task['trigger_type']=='service'
    client.delete('/api/strategies/'+id)
    assert not client.get('/api/strategies').json()
def test_security_and_input_validation():
    client=TestClient(app)
    assert client.get('/internal/segments/unknown').status_code==401
    assert client.post('/api/tasks',json={'source':'../../etc/passwd'}).status_code==422
    assert client.post('/api/tasks',json={'segment_seconds':0}).status_code==422
    assert client.post('/api/tasks',json={},headers={'Origin':'https://foreign.example'}).status_code==403
def test_algorithm_registration_and_validation():
    client=TestClient(app)
    assert client.post('/api/configs/algorithm',json={'body':{'method':'shell'}}).status_code==422
    result=client.post('/api/configs/algorithm',json={'name':'v2','body':{'method':'resource_aware','weights':{'compute':2}}})
    assert result.status_code==200
    assert client.delete('/api/configs/algorithm/'+result.json()['id']).status_code==200

def test_api_key_browser_cookie_protects_media_and_exports(monkeypatch):
    import app.main as main
    monkeypatch.setattr(main,'API_KEY','unit-test-key')
    client=TestClient(app)
    assert client.get('/api/tasks').status_code==401
    assert client.post('/api/session',json={'api_key':'wrong'}).status_code==401
    assert client.post('/api/session',json={'api_key':'unit-test-key'}).status_code==200
    assert client.get('/api/tasks').status_code==200
    assert 'HttpOnly' in client.post('/api/session',json={'api_key':'unit-test-key'}).headers['set-cookie']
