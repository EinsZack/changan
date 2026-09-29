import time,asyncio
import pytest
from app import db
from app.engine import Engine
from app.schemas import TaskInput
from app.config import MODEL_ID

def seeded():
    engine=Engine();id=engine.create(TaskInput(name='isolation'))
    db.execute("UPDATE tasks SET status='RUNNING',total=2 WHERE id=?",(id,))
    db.execute("INSERT INTO segments(id,task_id,ordinal,start,end,path,status,active_attempt) VALUES('s1',?,0,0,4,'unused','RUNNING','a1')",(id,))
    db.execute("INSERT INTO segments(id,task_id,ordinal,start,end,path,status) VALUES('s2',?,1,4,8,'unused','PENDING')",(id,))
    db.execute("INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision) VALUES('a1',?,'s1','lab-1','test-token','RUNNING',?,?,'{}')",(id,time.time(),time.time()))
    return engine,id

def body(id,**changes):
    b={'attempt_id':'a1','task_id':id,'segment_id':'s1','node_id':'lab-1','token':'test-token','status':'RUNNING','seq':2,'progress':.5,'id':'e1'};b.update(changes);return b
@pytest.mark.asyncio
async def test_event_dedup_and_out_of_order():
    e,id=seeded()
    assert (await e.ingest(body(id)))['accepted']
    assert not (await e.ingest(body(id,seq=1,progress=.1)))['accepted']
    assert not (await e.ingest(body(id)))['accepted']
    assert db.query("SELECT progress FROM attempts WHERE id='a1'",one=True)['progress']==.5
    await e.close()
@pytest.mark.asyncio
async def test_wrong_node_is_failure_not_success():
    e,id=seeded()
    assert not (await e.ingest(body(id,node_id='lab-2')))['accepted']
    assert db.query("SELECT status FROM attempts WHERE id='a1'",one=True)['status']=='FAILED'
    await e.close()
@pytest.mark.asyncio
async def test_old_attempt_cannot_overwrite_new_attempt():
    e,id=seeded();db.execute("UPDATE segments SET active_attempt='a2' WHERE id='s1'")
    assert not (await e.ingest(body(id)))['accepted']
    assert db.query("SELECT status FROM segments WHERE id='s1'",one=True)['status']=='RUNNING'
    await e.close()
@pytest.mark.asyncio
async def test_canceled_parent_rejects_late_result():
    e,id=seeded();e.fail_task(id,'user','CANCELED')
    assert not (await e.ingest(body(id)))['accepted']
    assert db.task_detail(id)['status']=='CANCELED'
    await e.close()
@pytest.mark.asyncio
async def test_incomplete_aggregation_never_succeeds():
    e,id=seeded();e.aggregate(db.query('SELECT * FROM tasks WHERE id=?',(id,),one=True))
    assert db.task_detail(id)['status']=='RUNNING'
    await e.close()
@pytest.mark.asyncio
async def test_retry_bound():
    e,id=seeded();db.execute("UPDATE segments SET retries=2 WHERE id='s1'")
    e.attempt_failure('a1','test failure')
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'",one=True)=={'status':'FAILED','retries':3}
    await e.close()
@pytest.mark.asyncio
async def test_idempotent_submit_does_not_duplicate():
    e=Engine();p=TaskInput()
    a=e.create(p,key='same');b=e.create(p,key='same');assert a==b
    with pytest.raises(ValueError):e.create(TaskInput(name='changed'),key='same')
    await e.close()

def test_concurrent_idempotent_submissions():
    from concurrent.futures import ThreadPoolExecutor
    e=Engine()
    with ThreadPoolExecutor(max_workers=8) as pool:
        ids=list(pool.map(lambda _:e.create(TaskInput(),key='concurrent'),range(16)))
    assert len(set(ids))==1
    assert db.query('SELECT count(*) n FROM tasks',one=True)['n']==1
    asyncio.run(e.close())

@pytest.mark.asyncio
async def test_failed_callback_preserves_execution_provenance():
    e,id=seeded()
    callback=body(id,status='FAILED',error='injected failure');callback['id']='execution-failed'
    assert (await e.ingest(callback))['accepted']
    attempt=db.query("SELECT execution_id,actual_node,last_seq,status FROM attempts WHERE id='a1'",one=True)
    assert attempt=={'execution_id':'execution-failed','actual_node':'lab-1','last_seq':2,'status':'FAILED'}
    assert db.query("SELECT status,retries FROM segments WHERE id='s1'",one=True)=={'status':'PENDING','retries':1}
    assert not (await e.ingest(callback))['accepted']
    assert db.query("SELECT retries FROM segments WHERE id='s1'",one=True)['retries']==1
    await e.close()
