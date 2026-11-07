#!/usr/bin/env python
"""Manage this project's isolated Docker daemon and k3d development cluster."""
import os,sys,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
DATA=ROOT/'data/k3s'
BIN=ROOT/'vendor/bin'
env=os.environ.copy()
env['PATH']=str(ROOT/'vendor/docker')+':'+str(BIN)+':'+env['PATH']
env['DOCKER_HOST']='unix://'+str(DATA/'docker.sock')
def run(*args,**kw):return subprocess.run(args,cwd=ROOT,env=env,check=True,**kw)
def docker_ready():
    return subprocess.run(['docker','info'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
def start_daemon():
    if docker_ready():return
    DATA.mkdir(parents=True,exist_ok=True);(ROOT/'logs').mkdir(exist_ok=True)
    # Proxy environment can be inherited from the user's approved magic on session.
    with (ROOT/'logs/docker.log').open('ab') as log:
        subprocess.Popen(['dockerd','--host='+env['DOCKER_HOST'],'--data-root='+str(DATA/'docker'),
          '--exec-root='+str(DATA/'docker-exec'),'--pidfile='+str(DATA/'docker.pid'),
          '--default-address-pool=base=172.29.0.0/16,size=24'],env=env,stdout=log,stderr=log,start_new_session=True)
    for _ in range(40):
        if docker_ready():return
        time.sleep(1)
    raise SystemExit('Docker 启动失败，请检查 logs/docker.log')
action=sys.argv[1] if len(sys.argv)>1 else 'status'
if action=='start':
    start_daemon();run('k3d','cluster','start','changan-dev')
elif action=='create':
    start_daemon()
    run('k3d','cluster','create','changan-dev','--image','rancher/k3s:v1.35.5-k3s1','--agents','4',
      '--agents-memory','8g','--servers-memory','8g','--api-port','127.0.0.1:6445',
      '--registry-create','changan-registry:127.0.0.1:5500','--kubeconfig-update-default=false',
      '--kubeconfig-switch-context=false','--k3s-arg','--disable=traefik,servicelb,metrics-server@server:0','--timeout','240s')
elif action=='status':
    run('k3d','cluster','list');run('docker','ps','--format','table {{.Names}}\t{{.Status}}')
elif action=='kubectl':
    run('docker','exec','k3d-changan-dev-server-0','kubectl',*sys.argv[2:])
else:raise SystemExit('usage: cluster.py start|create|status|kubectl ...')
