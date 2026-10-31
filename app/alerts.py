import time
from typing import Literal
from fastapi import APIRouter, HTTPException, Query
from . import db

router = APIRouter(prefix='/api/alerts', tags=['站内告警'])
RULES = [
    {'id': 'task_timeout', 'name': '任务超期', 'severity': 'warning',
     'condition': '任务超过提交时设置的截止时限，进入超时状态。',
     'action': '查看任务进度和调度记录，调整条件后重新提交。'},
    {'id': 'task_failed', 'name': '任务执行失败', 'severity': 'error',
     'condition': '任务准备、执行或聚合最终失败，无法形成完整结果。',
     'action': '查看失败原因和执行事件，处理问题后重新提交。'},
]

def record_terminal(connection, task_id, state, reason, occurred_at):
    if state not in {'TIMED_OUT', 'FAILED'}:
        return
    task = connection.execute('SELECT * FROM tasks WHERE id=?', (task_id,)).fetchone()
    if task is None:
        return
    rule = RULES[0] if state == 'TIMED_OUT' else RULES[1]
    completed = connection.execute(
        "SELECT count(*) FROM segments WHERE task_id=? AND status='SUCCEEDED'", (task_id,)
    ).fetchone()[0]
    params = db.loads(task['params'], {})
    alert_id = db.uid('alert')
    changed = connection.execute(
        '''INSERT OR IGNORE INTO alerts
           (id,task_id,rule,severity,title,message,created,deadline_at,completed,total)
           VALUES(?,?,?,?,?,?,?,?,?,?)''',
        (alert_id, task_id, rule['id'], rule['severity'], rule['name'],
         str(reason)[:1000], occurred_at, task['created'] + params['deadline_seconds'],
         completed, task['total'])
    ).rowcount
    if changed:
        db.event(task_id, 'alert_created',
                 {'alert_id': alert_id, 'rule': rule['id'], 'severity': rule['severity'],
                  'message': rule['name']}, conn=connection)

def alert_detail(alert_id):
    return db.query(
        '''SELECT alerts.*,tasks.name AS task_name,tasks.status AS task_status
           FROM alerts JOIN tasks ON tasks.id=alerts.task_id WHERE alerts.id=?''',
        (alert_id,), one=True)

@router.get('/rules')
def rules():
    return RULES

@router.get('')
def list_alerts(
    status: Literal['open', 'acknowledged'] | None = None,
    unread: bool | None = None,
    rule: Literal['task_timeout', 'task_failed'] | None = None,
    task_id: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    clauses = []
    values = []
    if status is not None:
        clauses.append('alerts.acknowledged_at IS ' + ('NULL' if status == 'open' else 'NOT NULL'))
    if unread is not None:
        clauses.append('alerts.read_at IS ' + ('NULL' if unread else 'NOT NULL'))
    if rule is not None:
        clauses.append('alerts.rule=?')
        values.append(rule)
    if task_id is not None:
        clauses.append('alerts.task_id=?')
        values.append(task_id)
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    with db.connect() as connection:
        connection.execute('BEGIN')
        summary = dict(connection.execute(
            '''SELECT count(*) AS total,
               coalesce(sum(read_at IS NULL),0) AS unread,
               coalesce(sum(acknowledged_at IS NULL),0) AS open
               FROM alerts'''
        ).fetchone())
        total = connection.execute('SELECT count(*) FROM alerts' + where, values).fetchone()[0]
        items = [dict(row) for row in connection.execute(
            '''SELECT alerts.*,tasks.name AS task_name,tasks.status AS task_status
               FROM alerts JOIN tasks ON tasks.id=alerts.task_id''' + where +
            ' ORDER BY alerts.created DESC,alerts.id DESC LIMIT ? OFFSET ?',
            values + [limit, offset])]
    return {'items': items, 'total': total, 'summary': summary}

@router.post('/{alert_id}/read')
def mark_read(alert_id: str):
    if not db.execute('UPDATE alerts SET read_at=coalesce(read_at,?) WHERE id=?',
                      (time.time(), alert_id)):
        raise HTTPException(404, '告警不存在')
    return alert_detail(alert_id)

@router.post('/{alert_id}/acknowledge')
def acknowledge(alert_id: str):
    now = time.time()
    with db.connect() as connection:
        connection.execute('BEGIN IMMEDIATE')
        row = connection.execute('SELECT * FROM alerts WHERE id=?', (alert_id,)).fetchone()
        if row is None:
            raise HTTPException(404, '告警不存在')
        if row['acknowledged_at'] is None:
            connection.execute(
                'UPDATE alerts SET read_at=coalesce(read_at,?),acknowledged_at=? WHERE id=?',
                (now, now, alert_id))
            db.event(row['task_id'], 'alert_acknowledged',
                     {'alert_id': alert_id, 'message': '站内告警已确认'}, conn=connection)
    return alert_detail(alert_id)
