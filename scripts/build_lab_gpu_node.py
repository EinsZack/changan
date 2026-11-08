#!/usr/bin/env python
"""Build the nested lab agent from checked packages and this host's driver files.

Does not install packages on the host, replace a running node, or configure THOR.
The cgroup-enabled NVIDIA runtime is required for nonprivileged business Pods.
"""
import hashlib,json,os,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
context=ROOT/'tmp/gpu-build/node-context';context.mkdir(parents=True,exist_ok=True)
for folder in ['toolkit','debian-libs','driver']:
    if (context/folder).exists():shutil.rmtree(context/folder)
debs=ROOT/'tmp/gpu-build/debs';debs.mkdir(parents=True,exist_ok=True)
for row in json.loads((ROOT/'deploy/lab-gpu-packages.lock.json').read_text()):
    file=debs/row['url'].split('/')[-1]
    if not file.exists():subprocess.run(['curl','-fLsS','--retry','2','--max-time','120',row['url'],'-o',str(file)],check=True)
    if hashlib.sha256(file.read_bytes()).hexdigest()!=row['SHA256']:raise ValueError('Package checksum: '+file.name)
    subprocess.run(['dpkg-deb','-x',str(file),str(context/('debian-libs' if row['Package'] in ('libcap2','libseccomp2') else 'toolkit'))],check=True)
source=Path('/usr/lib/x86_64-linux-gnu');target=context/'driver/usr/lib/x86_64-linux-gnu';target.mkdir(parents=True,exist_ok=True)
for pattern in ['libcap.so*','libseccomp.so*']:
    for p in (context/'debian-libs').rglob(pattern):shutil.copy2(p,target/p.name,follow_symlinks=False)
for pattern in ['libcuda*.so*','libnvidia*.so*','libnvcuvid.so*']:
    for p in source.glob(pattern):shutil.copy2(p,target/p.name,follow_symlinks=False)
binpath=context/'driver/usr/bin';binpath.mkdir(parents=True,exist_ok=True)
shutil.copy2(shutil.which('nvidia-smi'),binpath/'nvidia-smi')
shutil.copy2(ROOT/'deploy/Dockerfile.k3s-gpu',context/'Dockerfile')
version=subprocess.check_output(['nvidia-smi','--query-gpu=driver_version','--format=csv,noheader'],text=True).splitlines()[0]
if version!='590.48.01':raise SystemExit('Revalidate a changed driver before building this lab image: '+version)
env=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
subprocess.run([str(ROOT/'vendor/docker/docker'),'build','-t','changan/k3s-gpu:1.35.5-driver590',str(context)],env=env,check=True)
print('Built lab image; replace only an idle node and preserve its named volumes.')
