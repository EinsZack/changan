import os
os.environ['CHANGAN_BACKEND']='process'
"""Isolated live four-worker verification environment, with process cleanup."""
import os,sys,json,time,subprocess,signal
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
folder=ROOT/'tmp/weekly-20261104'
folder.mkdir(parents=True,exist_ok=True)
data=folder/'data'
data.mkdir(exist_ok=True)
os.environ.update(CHANGAN_DATA_DIR=str(data),CHANGAN_INTERNAL_TOKEN='weekly-local-test',CHANGAN_API_KEY='',CHANGAN_RESOURCE_PROVIDER_URL='',CHANGAN_PUBLIC_BASE='http://127.0.0.1:18410')
from app import db
db.init()
for i in range(4):
    db.execute("UPDATE nodes SET endpoint=?,enabled=1 WHERE id=?",(f'http://127.0.0.1:{18421+i}',f'lab-{i+1}'))
processes={}
def stop(*_):
    for p in processes.values():
        if p.poll() is None:
            try:os.kill(p.pid,signal.SIGCONT);p.terminate()
            except ProcessLookupError:pass
    for p in processes.values():
        try:p.wait(timeout=10)
        except subprocess.TimeoutExpired:p.kill()
    sys.exit(0)
signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
for i in range(4):
    log=(folder/f'worker-{i+1}.log').open('ab')
    processes[f'worker-{i+1}']=subprocess.Popen([sys.executable,'-m','uvicorn','workers.service:app','--host','127.0.0.1','--port',str(18421+i),'--no-access-log'],cwd=ROOT,env=os.environ|{'NODE_ID':f'lab-{i+1}'},stdout=log,stderr=log)
log=(folder/'api.log').open('ab')
processes['api']=subprocess.Popen([sys.executable,'-m','uvicorn','app.main:app','--host','127.0.0.1','--port','18410','--no-access-log'],cwd=ROOT,env=os.environ,stdout=log,stderr=log)
(folder/'pids.json').write_text(json.dumps({k:p.pid for k,p in processes.items()}))
print('isolated live environment http://127.0.0.1:18410',flush=True)
while True:
    if any(p.poll() is not None for p in processes.values()):stop()
    time.sleep(1)
