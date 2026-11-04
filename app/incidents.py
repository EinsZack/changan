"""Persist link observations separately from task state reconciliation."""
import time
from . import db
from .config import TERMINAL

def observe_node(node, reachable, reason="执行器健康请求失败"):
    now=time.time()
    with db.connect() as c:
        c.execute("BEGIN IMMEDIATE")
        incident=c.execute("SELECT * FROM node_incidents WHERE node_id=? AND link_restored_at IS NULL",(node['id'],)).fetchone()
        if reachable:
            if incident:
                c.execute("UPDATE node_incidents SET link_restored_at=? WHERE id=?",(now,incident['id']))
                for a in db.loads(incident['affected'],[]):
                    db.event(a['task_id'],'node_reconnected',{'incident_id':incident['id'],'node_id':node['id'],'stage':'execute','state':'UNKNOWN','message':'节点通信恢复，等待核对在途任务','link_restored_at':now},a['attempt_id'],c)
            return
        if node.get('observed') and now-node['observed']<8:
            return
        attempts=c.execute("""SELECT a.id attempt_id,a.task_id,a.segment_id,a.status last_status,
                   a.progress,a.updated last_updated,s.ordinal FROM attempts a
                   JOIN segments s ON s.id=a.segment_id JOIN tasks t ON t.id=a.task_id
                   WHERE a.node_id=? AND a.status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')
                   AND t.status NOT IN ('SUCCEEDED','FAILED','CANCELED','TIMED_OUT')
                   AND s.active_attempt=a.id""",(node['id'],)).fetchall()
        affected=db.loads(incident['affected'],[]) if incident else []
        known={a['attempt_id'] for a in affected}
        added=[dict(a) for a in attempts if a['attempt_id'] not in known]
        for a in added:
            if a['last_status']=='UNKNOWN':
                last=c.execute("SELECT at,payload FROM events WHERE attempt_id=? AND type='execution' ORDER BY id DESC LIMIT 1",(a['attempt_id'],)).fetchone()
                if last:
                    payload=db.loads(last['payload'],{})
                    a['last_status']=payload.get('state',a['last_status'])
                    a['progress']=payload.get('progress',a['progress'])
                    a['last_updated']=last['at']
        if not incident and not node.get('observed') and not added:
            return
        iid=incident['id'] if incident else db.uid('loss')
        if not incident:
            c.execute("INSERT INTO node_incidents(id,node_id,detected_at,last_seen_at,reason,affected) VALUES(?,?,?,?,?,?)",(iid,node['id'],now,node.get('observed'),reason,db.dumps(added)))
        elif added:
            c.execute("UPDATE node_incidents SET affected=? WHERE id=?",(db.dumps(affected+added),iid))
        for a in added:
            db.event(a['task_id'],'node_lost',{'incident_id':iid,'node_id':node['id'],'stage':'execute','state':'UNKNOWN','message':'节点失联，已标记受影响的在途片段','last_status':a['last_status'],'last_progress':a['progress'],'last_updated':a['last_updated'],'segment_id':a['segment_id'],'last_seen_at':node.get('observed')},a['attempt_id'],c)
            c.execute("UPDATE attempts SET status='UNKNOWN' WHERE id=?",(a['attempt_id'],))
            c.execute("UPDATE segments SET status='UNKNOWN' WHERE active_attempt=?",(a['attempt_id'],))

def reconcile_observation(attempt_id):
    """Only an observed worker response closes task reconciliation, not link recovery."""
    with db.connect() as c:
        for incident in c.execute("SELECT * FROM node_incidents WHERE link_restored_at IS NOT NULL AND task_reconciled_at IS NULL").fetchall():
            affected=db.loads(incident['affected'],[])
            if not any(a['attempt_id']==attempt_id for a in affected): continue
            for a in affected:
                if a['attempt_id']==attempt_id: a['reconciled_at']=time.time()
            done=all(a.get('reconciled_at') for a in affected)
            c.execute("UPDATE node_incidents SET affected=?,task_reconciled_at=? WHERE id=?",(db.dumps(affected),time.time() if done else None,incident['id']))

def list_incidents(task_id=None):
    rows=db.query("SELECT * FROM node_incidents ORDER BY detected_at DESC LIMIT 100")
    result=[]
    for r in rows:
        r['affected']=db.loads(r['affected'],[])
        if task_id and not any(a['task_id']==task_id for a in r['affected']): continue
        for a in r['affected']:
            current=db.query("SELECT status,actual_node,progress FROM attempts WHERE id=?",(a['attempt_id'],),one=True)
            a['current']=current
        r['affected_tasks']=len({a['task_id'] for a in r['affected']})
        r['affected_segments']=len(r['affected'])
        result.append(r)
    return result
