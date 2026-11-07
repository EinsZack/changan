import sqlite3, json, time, uuid, os
from contextlib import contextmanager
from .config import DATA
DB = DATA / 'control.sqlite'
def uid(prefix): return prefix + '_' + uuid.uuid4().hex[:16]
def dumps(value): return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
def loads(value, default=None):
    return json.loads(value) if value else default
@contextmanager
def connect():
    conn = sqlite3.connect(DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA busy_timeout=30000')
    try:
        yield conn
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally: conn.close()
def query(sql, args=(), one=False):
    with connect() as c:
        r = c.execute(sql, args)
        return (dict(x) if (x := r.fetchone()) else None) if one else [dict(x) for x in r.fetchall()]
def execute(sql, args=()):
    with connect() as c: return c.execute(sql, args).rowcount
def init():
    with connect() as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.executescript('''
CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,name TEXT NOT NULL,source TEXT NOT NULL,status TEXT NOT NULL,created REAL NOT NULL,updated REAL NOT NULL,started REAL,finished REAL,params TEXT NOT NULL,metadata TEXT,total INTEGER DEFAULT 0,completed INTEGER DEFAULT 0,error TEXT,trigger_type TEXT DEFAULT 'service',strategy_id TEXT,idempotency_key TEXT UNIQUE,result TEXT);
CREATE TABLE IF NOT EXISTS segments(id TEXT PRIMARY KEY,task_id TEXT NOT NULL REFERENCES tasks(id),ordinal INTEGER NOT NULL,start REAL NOT NULL,end REAL NOT NULL,path TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'PENDING',active_attempt TEXT,retries INTEGER DEFAULT 0,result TEXT,UNIQUE(task_id,ordinal));
CREATE TABLE IF NOT EXISTS attempts(id TEXT PRIMARY KEY,task_id TEXT NOT NULL,segment_id TEXT NOT NULL REFERENCES segments(id),node_id TEXT NOT NULL,token TEXT NOT NULL,status TEXT NOT NULL,created REAL NOT NULL,started REAL,finished REAL,updated REAL NOT NULL,last_seq INTEGER DEFAULT 0,progress REAL DEFAULT 0,execution_id TEXT,actual_node TEXT,error TEXT,result TEXT,decision TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,attempt_id TEXT,type TEXT NOT NULL,at REAL NOT NULL,payload TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS events_task ON events(task_id,id);
CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY,endpoint TEXT NOT NULL,cluster_id TEXT NOT NULL,kind TEXT NOT NULL,enabled INTEGER DEFAULT 1,observed REAL,snapshot TEXT,error TEXT);
CREATE TABLE IF NOT EXISTS node_incidents(id TEXT PRIMARY KEY,node_id TEXT NOT NULL,detected_at REAL NOT NULL,last_seen_at REAL,reason TEXT NOT NULL,affected TEXT NOT NULL,link_restored_at REAL,task_reconciled_at REAL);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_node_incident ON node_incidents(node_id) WHERE link_restored_at IS NULL;
CREATE TABLE IF NOT EXISTS alerts(id TEXT PRIMARY KEY,task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),rule TEXT NOT NULL,severity TEXT NOT NULL,title TEXT NOT NULL,message TEXT NOT NULL,created REAL NOT NULL,deadline_at REAL NOT NULL,completed INTEGER NOT NULL,total INTEGER NOT NULL,read_at REAL,acknowledged_at REAL);
CREATE INDEX IF NOT EXISTS alerts_status_created ON alerts(acknowledged_at,created DESC);
CREATE TABLE IF NOT EXISTS strategies(id TEXT PRIMARY KEY,name TEXT NOT NULL,kind TEXT NOT NULL,interval_seconds INTEGER,enabled INTEGER DEFAULT 1,params TEXT NOT NULL,next_run REAL,last_run REAL,created REAL NOT NULL);
CREATE TABLE IF NOT EXISTS configs(id TEXT PRIMARY KEY,category TEXT NOT NULL,name TEXT NOT NULL,body TEXT NOT NULL,created REAL NOT NULL,updated REAL NOT NULL);
''')
        columns={r[1] for r in c.execute('PRAGMA table_info(attempts)')}
        for name,decl in [('backend',"TEXT NOT NULL DEFAULT 'process'"),('workload','TEXT'),('cleanup_state',"TEXT NOT NULL DEFAULT 'DONE'"),('session_id','TEXT')]:
            if name not in columns:c.execute('ALTER TABLE attempts ADD COLUMN '+name+' '+decl)
        c.executescript('''
CREATE TABLE IF NOT EXISTS execution_sessions(
 id TEXT PRIMARY KEY, task_id TEXT NOT NULL REFERENCES tasks(id), node_id TEXT NOT NULL,
 token TEXT NOT NULL, status TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
 heartbeat_at REAL, ready_at REAL, finished REAL, active_attempt TEXT, workload TEXT NOT NULL,
 cleanup_state TEXT NOT NULL DEFAULT 'PENDING', completed INTEGER NOT NULL DEFAULT 0,
 model_load_count INTEGER NOT NULL DEFAULT 0, model_load_seconds REAL,
 reservation_id TEXT, reservation_released INTEGER NOT NULL DEFAULT 0, error TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_live_task_node_session ON execution_sessions(task_id,node_id) WHERE cleanup_state!='DONE';
CREATE INDEX IF NOT EXISTS attempts_session ON attempts(session_id);
''')
        for i in range(4 if os.getenv('CHANGAN_BACKEND','k3s')=='process' else 0):
            c.execute('INSERT OR IGNORE INTO nodes(id,endpoint,cluster_id,kind) VALUES(?,?,?,?)', (f'lab-{i+1}',f'http://127.0.0.1:{8321+i}','server-lab','server_process'))
        defaults=[
          ('round_robin','algorithm','可行节点轮询',{'version':1,'method':'round_robin','weights':{}}),
          ('resource_aware','algorithm','资源与网络感知',{'version':1,'method':'resource_aware','weights':{'compute':1,'transfer':1,'queue':1}}),
          ('video-analysis','workflow','协同视频分析',{'version':1,'enabled':True,'tags':['视频','中期'],'nodes':['validate','split','snapshot','schedule','execute','aggregate']}),
          ('capability-demo','workflow','五类能力验证',{'version':1,'enabled':True,'tags':['连接器'],'nodes':['api','redis','script','algorithm','platform']}),
          ('none','auth','无鉴权',{'type':'none'}),
          ('normalize-python','script','Python参数整理',{'language':'python','builtin':'normalize'}),
          ('probe-shell','script','Shell执行探测',{'language':'shell','builtin':'probe'})]
        for id,cat,name,body in defaults:
            c.execute('INSERT OR IGNORE INTO configs VALUES(?,?,?,?,?,?)',(id,cat,name,dumps(body),time.time(),time.time()))
def event(task_id, type, payload, attempt_id=None, conn=None):
    sql='INSERT INTO events(task_id,attempt_id,type,at,payload) VALUES(?,?,?,?,?)'
    args=(task_id,attempt_id,type,time.time(),dumps(payload))
    if conn is not None: conn.execute(sql,args)
    else: execute(sql,args)
def task_detail(id):
    task=query('SELECT * FROM tasks WHERE id=?',(id,),one=True)
    if not task: return None
    for k in ['params','metadata','result']: task[k]=loads(task[k])
    task['segments']=query('SELECT * FROM segments WHERE task_id=? ORDER BY ordinal',(id,))
    for s in task['segments']: s['result']=loads(s['result'])
    task['attempts']=query('SELECT id,task_id,segment_id,node_id,status,created,started,finished,updated,last_seq,progress,execution_id,actual_node,error,decision,backend,workload,cleanup_state,session_id FROM attempts WHERE task_id=? ORDER BY created',(id,))
    for a in task['attempts']:
        a['decision']=loads(a['decision']);a['workload']=loads(a['workload'],{})
    task['sessions']=query('SELECT id,task_id,node_id,status,created,updated,heartbeat_at,ready_at,finished,active_attempt,workload,cleanup_state,completed,model_load_count,model_load_seconds,error FROM execution_sessions WHERE task_id=? ORDER BY created',(id,))
    for session in task['sessions']:session['workload']=loads(session['workload'],{})
    return task
