import time
import pytest
from fastapi.testclient import TestClient
from app import db
from app.main import app
from app.schemas import CandidateInput
from app.incidents import observe_node,list_incidents,reconcile_observation
from test_state_machine import seeded,body
from app.config import MODEL_ID

def snapshot():
    return {'node_id':'lab-1','healthy':True,'models':[MODEL_ID],'tags':['cpu','onnx'],'architecture':'x86_64','available_memory_mb':4096,'capacity':1,'active':0}

def set_node():
    db.execute("UPDATE nodes SET observed=?,snapshot=?,enabled=1,error=NULL WHERE id='lab-1'",(time.time(),db.dumps(snapshot())))

def test_candidate_api_live_tags_and_memory():
    set_node()
    with TestClient(app) as client:
        result=client.post('/api/scheduling/candidates',json={'params':{'required_tags':['cpu'],'min_memory_mb':512}})
        assert result.status_code==200
        assert next(x for x in result.json()['candidates'] if x['node_id']=='lab-1')['eligible']
        result=client.post('/api/scheduling/candidates',json={'params':{'required_tags':['apu']}})
        row=next(x for x in result.json()['candidates'] if x['node_id']=='lab-1')
        assert not row['eligible'] and '缺少必要标签' in row['reasons']
        assert not db.query("SELECT id FROM tasks")

def test_candidate_rejects_bad_input():
    with TestClient(app) as client:
        assert client.post('/api/scheduling/candidates',json={'input_bytes':-1}).status_code==422
        assert client.post('/api/scheduling/candidates',json={'params':{'algorithm':'missing'}}).status_code==422

@pytest.mark.asyncio
async def test_loss_preserves_state_and_deduplicates():
    e,tid=seeded()
    node={'id':'lab-1','observed':time.time()-20}
    observe_node(node,False,'test transport failure')
    observe_node(node,False,'test transport failure')
    rows=list_incidents(tid)
    assert len(rows)==1 and rows[0]['affected_segments']==1
    assert rows[0]['affected'][0]['last_status']=='RUNNING'
    assert db.query("SELECT status FROM segments WHERE id='s1'",one=True)['status']=='UNKNOWN'
    assert len(db.query("SELECT id FROM events WHERE type='node_lost'"))==1
    observe_node(node,True)
    r=list_incidents(tid)[0]
    assert r['link_restored_at'] and r['task_reconciled_at'] is None
    await e.ingest(body(tid))
    assert list_incidents(tid)[0]['task_reconciled_at']
    assert db.query("SELECT status FROM segments WHERE id='s1'",one=True)['status']=='RUNNING'
    await e.close()

@pytest.mark.asyncio
async def test_loss_same_sequence_reconciliation_restores_segment():
    e,tid=seeded()
    db.execute("UPDATE attempts SET last_seq=2 WHERE id='a1'")
    observe_node({'id':'lab-1','observed':time.time()-20},False)
    observe_node({'id':'lab-1'},True)
    r=await e.ingest(body(tid))
    assert r['reason']=='reconciled_without_new_event'
    assert db.query("SELECT status FROM segments WHERE id='s1'",one=True)['status']=='RUNNING'
    assert list_incidents(tid)[0]['task_reconciled_at']
    await e.close()

@pytest.mark.asyncio
async def test_new_outage_creates_distinct_incident():
    e,tid=seeded()
    node={'id':'lab-1','observed':time.time()-20}
    observe_node(node,False);observe_node(node,True)
    await e.ingest(body(tid))
    observe_node(node,False)
    assert len(list_incidents(tid))==2
    await e.close()

@pytest.mark.asyncio
async def test_brief_failure_keeps_last_state():
    e,tid=seeded()
    observe_node({'id':'lab-1','observed':time.time()},False)
    assert list_incidents(tid)==[]
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='RUNNING'
    await e.close()

def test_video_source_path_validation():
    with TestClient(app) as client:
        assert client.get('/api/sources/not-allowed/video').status_code==404
        r=client.get('/api/sources/demo/video',headers={'Range':'bytes=0-511'})
        assert r.status_code==206 and len(r.content)==512

@pytest.mark.asyncio
async def test_segment_endpoint_requires_parent_match(tmp_path):
    e,tid=seeded()
    f=tmp_path/'test.mp4';f.write_bytes(b'video')
    db.execute("UPDATE segments SET path=? WHERE id='s1'",(str(f),))
    with TestClient(app) as client:
        assert client.get('/api/tasks/wrong/segments/s1/video').status_code==404
        assert client.get(f'/api/tasks/{tid}/segments/s1/video').content==b'video'
    await e.close()

@pytest.mark.asyncio
async def test_external_pool_fields_drive_candidate_filtering():
    import httpx
    from app.engine import Engine
    from app.scheduler import choose
    e=Engine()
    await e.client.aclose()
    now=time.time()
    def handler(req):
        if req.url.host=='pool':
            return httpx.Response(200,json=[{'node_id':f'lab-{i}','healthy':True,'observed_at':now,'tags':['cpu','onnx'],'available_memory_mb':4096,'capacity':1,'active':int(i==1),'snapshot_version':'test-v1'} for i in range(1,5)])
        i=req.url.port-8320
        return httpx.Response(200,json=snapshot()|{'node_id':f'lab-{i}'})
    e.client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    e.resource_provider.client=e.client;e.resource_provider.url='http://pool'
    await e.refresh()
    result=choose(e.nodes(),{'required_tags':['cpu']},4,100,{})
    first=next(c for c in result['candidates'] if c['node_id']=='lab-1')
    assert not first['eligible']
    assert '执行槽已占用或预留' in first['reasons']
    assert first['snapshot']['resource_source']=='external_pool'
    assert first['snapshot']['snapshot_version']=='test-v1'
    await e.close()

@pytest.mark.asyncio
async def test_pool_unavailable_does_not_use_worker_fallback():
    import httpx
    from app.engine import Engine
    from app.scheduler import choose
    set_node()
    e=Engine();await e.client.aclose()
    e.client=httpx.AsyncClient(transport=httpx.MockTransport(lambda req:httpx.Response(503)))
    e.resource_provider.client=e.client;e.resource_provider.url='http://pool'
    await e.refresh()
    result=choose(e.nodes(),{},4,100,{})
    assert result['target_node'] is None
    assert all('资源池快照不可用' in c['reasons'] for c in result['candidates'])
    await e.close()


@pytest.mark.asyncio
async def test_stale_receipt_does_not_complete_reconciliation():
    e,tid=seeded()
    await e.ingest(body(tid,seq=2))
    observe_node({'id':'lab-1','observed':time.time()-20},False)
    observe_node({'id':'lab-1'},True)
    assert not (await e.ingest(body(tid,seq=1)))['accepted']
    assert list_incidents(tid)[0]['task_reconciled_at'] is None
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='UNKNOWN'
    await e.ingest(body(tid,seq=2))
    assert list_incidents(tid)[0]['task_reconciled_at']
    await e.close()
