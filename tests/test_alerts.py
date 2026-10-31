import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import pytest
from fastapi.testclient import TestClient
from app import db
from app.engine import Engine
from app.main import app
from app.schemas import TaskInput

@pytest.fixture
def alert_engine():
    engine = Engine()
    yield engine
    asyncio.run(engine.close())

@pytest.mark.asyncio
async def test_deadline_tick_creates_one_alert_and_preserves_progress(alert_engine):
    task_id = alert_engine.create(TaskInput(name='截止时间核对', deadline_seconds=10))
    db.execute("UPDATE tasks SET created=?,status='RUNNING',total=2 WHERE id=?",
               (time.time() - 20, task_id))
    for ordinal, state in enumerate(['SUCCEEDED', 'PENDING']):
        db.execute(
            '''INSERT INTO segments(id,task_id,ordinal,start,end,path,status)
               VALUES(?,?,?,?,?,?,?)''',
            (db.uid('seg'), task_id, ordinal, ordinal * 4, (ordinal + 1) * 4, 'unused', state))
    alert_engine.last_refresh = time.monotonic()
    await alert_engine.tick()
    alert_engine.fail_task(task_id, '重复超时处理', 'TIMED_OUT')
    await alert_engine.tick()
    db.init()
    rows = db.query('SELECT * FROM alerts')
    assert len(rows) == 1
    assert rows[0]['rule'] == 'task_timeout'
    assert rows[0]['completed'] == 1
    assert rows[0]['total'] == 2
    assert rows[0]['read_at'] is None
    assert rows[0]['acknowledged_at'] is None
    assert db.task_detail(task_id)['status'] == 'TIMED_OUT'
    assert len(db.query("SELECT id FROM events WHERE type='alert_created'")) == 1

def test_failure_creates_alert_but_success_and_user_cancel_do_not(alert_engine):
    failed = alert_engine.create(TaskInput(name='失败任务'))
    canceled = alert_engine.create(TaskInput(name='用户取消'))
    succeeded = alert_engine.create(TaskInput(name='成功任务'))
    alert_engine.fail_task(failed, '输入视频无法解码')
    alert_engine.fail_task(canceled, '用户取消', 'CANCELED')
    db.execute("UPDATE tasks SET status='SUCCEEDED' WHERE id=?", (succeeded,))
    alert_engine.fail_task(succeeded, '迟到的失败', 'FAILED')
    rows = db.query('SELECT * FROM alerts')
    assert len(rows) == 1
    assert rows[0]['task_id'] == failed
    assert rows[0]['rule'] == 'task_failed'
    assert rows[0]['message'] == '输入视频无法解码'

def test_retryable_attempt_failure_does_not_alert(alert_engine):
    task_id = alert_engine.create(TaskInput())
    db.execute("UPDATE tasks SET status='RUNNING',total=1 WHERE id=?", (task_id,))
    db.execute(
        '''INSERT INTO segments(id,task_id,ordinal,start,end,path,status,active_attempt)
           VALUES('retry-segment',?,0,0,4,'unused','RUNNING','retry-attempt')''', (task_id,))
    db.execute(
        '''INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision)
           VALUES('retry-attempt',?,'retry-segment','lab-1','test-token','RUNNING',?,?,'{}')''',
        (task_id, time.time(), time.time()))
    alert_engine.attempt_failure('retry-attempt', '暂时失败')
    assert not db.query('SELECT * FROM alerts')
    assert db.task_detail(task_id)['segments'][0]['status'] == 'PENDING'

def test_alert_filters_read_acknowledge_and_persistence(alert_engine):
    client = TestClient(app)
    task_ids = [alert_engine.create(TaskInput(name=f'告警任务{index}')) for index in range(3)]
    for task_id, state in zip(task_ids, ['TIMED_OUT', 'TIMED_OUT', 'FAILED']):
        alert_engine.fail_task(task_id, '测试终态', state)
    response = client.get('/api/alerts?limit=1').json()
    assert response['total'] == 3
    assert response['summary'] == {'total': 3, 'unread': 3, 'open': 3}
    assert len(response['items']) == 1
    first_id = response['items'][0]['id']
    assert client.get('/api/alerts?offset=1&limit=1').json()['items'][0]['id'] != first_id
    assert client.get('/api/alerts?rule=task_timeout').json()['total'] == 2
    selected = client.get('/api/alerts', params={'task_id': task_ids[0]}).json()['items'][0]
    alert_id = selected['id']
    read_at = client.post(f'/api/alerts/{alert_id}/read').json()['read_at']
    assert client.post(f'/api/alerts/{alert_id}/read').json()['read_at'] == read_at
    assert client.get('/api/alerts?unread=true').json()['total'] == 2
    acknowledged = client.post(f'/api/alerts/{alert_id}/acknowledge').json()
    assert acknowledged['read_at'] == read_at
    assert acknowledged['acknowledged_at'] is not None
    assert client.post(f'/api/alerts/{alert_id}/acknowledge').json() == acknowledged
    db.init()
    assert client.get('/api/alerts?status=acknowledged').json()['items'][0] == acknowledged
    assert client.get('/api/alerts?status=open').json()['total'] == 2
    assert db.task_detail(task_ids[0])['status'] == 'TIMED_OUT'
    assert len(db.query("SELECT id FROM events WHERE type='alert_acknowledged'")) == 1
    assert len(client.get('/api/alerts/rules').json()) == 2

def test_acknowledging_unread_alert_also_marks_it_read(alert_engine):
    task_id = alert_engine.create(TaskInput())
    alert_engine.fail_task(task_id, '执行失败')
    client = TestClient(app)
    alert_id = client.get('/api/alerts').json()['items'][0]['id']
    row = client.post(f'/api/alerts/{alert_id}/acknowledge').json()
    assert row['read_at'] == row['acknowledged_at']
    assert client.get('/api/alerts').json()['summary'] == {'total': 1, 'unread': 0, 'open': 0}

def test_terminal_status_and_alert_are_committed_atomically(alert_engine, monkeypatch):
    import app.engine as engine_module
    task_id = alert_engine.create(TaskInput())
    def unavailable(*args):
        raise RuntimeError('模拟告警写入失败')
    monkeypatch.setattr(engine_module, 'record_terminal', unavailable)
    with pytest.raises(RuntimeError):
        alert_engine.fail_task(task_id, '测试失败')
    assert db.task_detail(task_id)['status'] == 'QUEUED'
    assert not db.query('SELECT * FROM alerts')
    assert not db.query("SELECT * FROM events WHERE type='task_terminal'")

def test_concurrent_terminal_updates_do_not_duplicate_alerts(alert_engine):
    task_id = alert_engine.create(TaskInput())
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda index: alert_engine.fail_task(task_id, '执行失败'), range(8)))
    assert len(db.query('SELECT * FROM alerts')) == 1
    assert len(db.query("SELECT * FROM events WHERE type='alert_created'")) == 1

def test_alert_endpoints_enforce_auth_and_validate_filters(monkeypatch):
    import app.main as main
    monkeypatch.setattr(main, 'API_KEY', 'alert-test-key')
    client = TestClient(app)
    assert client.get('/api/alerts').status_code == 401
    assert client.post('/api/alerts/missing/read').status_code == 401
    assert client.post('/api/session', json={'api_key': 'alert-test-key'}).status_code == 200
    for query in ['status=unknown', 'rule=unknown', 'limit=0', 'offset=-1']:
        assert client.get('/api/alerts?' + query).status_code == 422
    assert client.post('/api/alerts/missing/read').status_code == 404
    assert client.post('/api/alerts/missing/acknowledge').status_code == 404


def test_concurrent_acknowledgements_record_one_event(alert_engine):
    from app.alerts import acknowledge
    task_id = alert_engine.create(TaskInput())
    alert_engine.fail_task(task_id, '执行失败')
    alert_id = db.query('SELECT id FROM alerts', one=True)['id']
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(lambda index: acknowledge(alert_id), range(8)))
    assert len({row['acknowledged_at'] for row in responses}) == 1
    assert len(db.query("SELECT id FROM events WHERE type='alert_acknowledged'")) == 1
