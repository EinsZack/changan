#!/usr/bin/env python
"""Start native macOS services using the dedicated environment and user session."""
import argparse, os, plistlib, shutil, subprocess, time
from pathlib import Path

def install(root, prefix, gateway):
    root=Path(root).resolve();prefix=Path(prefix).resolve()
    for needed in [prefix/'bin/python',prefix/'bin/redis-server',prefix/'bin/ffmpeg',root/'current/.env',root/'data/control.sqlite']:
        if not needed.exists():raise ValueError('Missing native deployment dependency: '+str(needed))
    (root/'logs').mkdir(exist_ok=True);(root/'data/redis').mkdir(exist_ok=True)
    agents=Path.home()/'Library/LaunchAgents';agents.mkdir(exist_ok=True)
    environment={'PATH':str(prefix/'bin')+':/usr/bin:/bin:/usr/sbin:/sbin','PYTHONUNBUFFERED':'1',
                 'PYTHONDONTWRITEBYTECODE':'1','IMAGEIO_FFMPEG_EXE':str(prefix/'bin/ffmpeg'),
                 'NO_PROXY':'localhost,127.0.0.1,'+gateway+',192.168.50.151'}
    # On the tested Mac, launchd-started Python cannot reach the LAN (Errno 65), while
    # the same interpreter started from the authorized user session can. Keep API and
    # gateway in that user session; manage.py verifies PID identity and starts each once.
    archive=root/'config/disabled-launch-agents';archive.mkdir(exist_ok=True)
    for name in ['api','gateway']:
        label='local.changan.'+name
        subprocess.run(['launchctl','bootout','gui/'+str(os.getuid())+'/'+label],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        path=agents/(label+'.plist')
        if path.exists():shutil.move(str(path),str(archive/path.name))
    commands={
        'redis':[str(prefix/'bin/redis-server'),'--bind','127.0.0.1','--port','8390','--save','60','1','--dir',str(root/'data/redis')],
        'awake':['/usr/bin/caffeinate','-i']}
    for name,command in commands.items():
        label='local.changan.'+name;path=agents/(label+'.plist')
        config={'Label':label,'ProgramArguments':command,'WorkingDirectory':str(root/'current'),
                'EnvironmentVariables':environment,'RunAtLoad':True,'KeepAlive':True,
                'StandardOutPath':str(root/'logs'/(name+'.log')),'StandardErrorPath':str(root/'logs'/(name+'-stderr.log'))}
        domain='gui/'+str(os.getuid())
        if path.exists() and plistlib.loads(path.read_bytes())==config and subprocess.run(
                ['launchctl','print',domain+'/'+label],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
            print('Already running '+label)
            continue
        subprocess.run(['launchctl','bootout',domain+'/'+label],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        path.write_bytes(plistlib.dumps(config))
        # launchd teardown can outlive bootout; immediate bootstrap may return EIO.
        for delay in [.2,.4,.8,1.6,2]:
            result=subprocess.run(['launchctl','bootstrap',domain,str(path)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if result.returncode==0:break
            time.sleep(delay)
        else:raise RuntimeError('Unable to bootstrap '+label+'; launchctl exit '+str(result.returncode))
        print('Started '+label)
    subprocess.run([str(prefix/'bin/python'),'scripts/manage.py','start'],cwd=root/'current',
                   env=os.environ|environment|{'CHANGAN_SKIP_REDIS':'1'},check=True)
    print('API and gateway started in the native user session')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',required=True);p.add_argument('--env-prefix',required=True);p.add_argument('--gateway-host',required=True)
    a=p.parse_args();install(a.root,a.env_prefix,a.gateway_host)
