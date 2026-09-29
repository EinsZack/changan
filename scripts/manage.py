#!/usr/bin/env python
"""Small process supervisor for the midterm lab deployment; PID identities are verified."""
import os, sys, json, time, signal, subprocess, socket
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from app.config import DATA,TOKEN
import psutil
PIDS=DATA/'processes.json'
PY=sys.executable

def alive(record):
    try:
        p=psutil.Process(record['pid'])
        return abs(p.create_time()-record['created'])<.01 and p.status()!=psutil.STATUS_ZOMBIE
    except psutil.Error:return False

def start():
    records=json.loads(PIDS.read_text()) if PIDS.exists() else {}
    logdir=ROOT/'logs';logdir.mkdir(exist_ok=True)
    env=os.environ.copy();env['CHANGAN_INTERNAL_TOKEN']=TOKEN;env['PYTHONUNBUFFERED']='1'
    services={}
    redis=ROOT/'vendor/bin/redis-server'
    if not redis.exists(): redis=Path(sys.prefix)/'bin/redis-server'
    if redis.exists():
        redisdir=DATA/'redis';redisdir.mkdir(exist_ok=True)
        services['redis']=([str(redis),'--bind','127.0.0.1','--port','8390','--save','60','1','--dir',str(redisdir)],{})
    for i in range(4):services[f'worker-{i+1}']=([PY,'-m','uvicorn','workers.service:app','--host','127.0.0.1','--port',str(8321+i),'--no-access-log'],{'NODE_ID':f'lab-{i+1}'})
    api_host=os.getenv('CHANGAN_HOST','127.0.0.1');api_port=os.getenv('CHANGAN_PORT','8310')
    services['api']=([PY,'-m','uvicorn','app.main:app','--host',api_host,'--port',api_port,'--no-access-log'],{})
    for name,(cmd,overrides) in services.items():
        if name in records and alive(records[name]):continue
        with (logdir/f'{name}.log').open('ab') as log:
            process=subprocess.Popen(cmd,cwd=ROOT,env=env|overrides,stdout=log,stderr=log,start_new_session=True)
        records[name]={'pid':process.pid,'created':psutil.Process(process.pid).create_time(),'command':cmd}
        PIDS.write_text(json.dumps(records,indent=2));time.sleep(.2)
    print(json.dumps({'services':{n:alive(r) for n,r in records.items()},'url':f'http://{api_host}:{api_port}'},ensure_ascii=False))
def stop(names=None):
    records=json.loads(PIDS.read_text()) if PIDS.exists() else {}
    for name,r in reversed(list(records.items())):
        if names and name not in names:continue
        if alive(r):
            process=psutil.Process(r['pid']);os.killpg(r['pid'],signal.SIGTERM)
            try:process.wait(15)
            except psutil.TimeoutExpired:
                if alive(r):os.killpg(r['pid'],signal.SIGKILL)
        records.pop(name,None)
    PIDS.write_text(json.dumps(records,indent=2))
def main():
    action=sys.argv[1] if len(sys.argv)>1 else 'status'
    if action=='start':start()
    elif action=='stop':stop(sys.argv[2:] or None)
    elif action=='restart':stop(sys.argv[2:] or None);start()
    elif action=='status':
        records=json.loads(PIDS.read_text()) if PIDS.exists() else {}
        print(json.dumps({n:{'pid':r['pid'],'alive':alive(r)} for n,r in records.items()},indent=2))
    else:raise SystemExit('usage: manage.py start|stop|restart|status [api|worker-1|...]')
if __name__=='__main__':main()
