#!/usr/bin/env python
"""Build general lab-node and business images without changing running services."""
import argparse, hashlib, json, os, shutil, subprocess
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
DOCKER = str(ROOT / 'vendor/docker/docker')
ENV = os.environ | {'DOCKER_HOST': 'unix://' + str(ROOT / 'data/k3s/docker.sock')}
CACHE = ROOT / 'tmp/gpu-build'
OUT = ROOT / 'data/runtime/unified'

def digest(path):
    return hashlib.file_digest(Path(path).open('rb'), 'sha256').hexdigest()

def publish(context, name):
    tag = '127.0.0.1:5500/changan/' + name
    subprocess.run([DOCKER, 'build', '--network', 'host', '-t', tag, str(context)], env=ENV, check=True)
    subprocess.run([DOCKER, 'push', tag], env=ENV, check=True)
    info = json.loads(subprocess.check_output([DOCKER, 'inspect', tag], env=ENV))[0]
    immutable = next(r for r in info['RepoDigests'] if r.startswith(tag.split(':')[0] + ':5500/'))
    return {'id': info['Id'], 'local_ref': immutable,
            'cluster_ref': 'changan-registry:5000/' + immutable.split('/', 1)[1]}

def node():
    context = ROOT / 'tmp/unified-runtime/node';context.mkdir(parents=True, exist_ok=True)
    for name in ['toolkit', 'driver', 'debian-libs']:
        if (context/name).exists():shutil.rmtree(context/name)
    packages = json.loads((ROOT/'deploy/node-runtime-packages.lock.json').read_text())
    (CACHE/'debs').mkdir(parents=True, exist_ok=True)
    for row in packages:
        path = CACHE/'debs'/row['url'].split('/')[-1]
        if not path.exists():
            subprocess.run(['curl', '-fLsS', '--retry', '2', '--max-time', '120', row['url'], '-o', str(path)], check=True)
        if digest(path) != row['SHA256']:raise ValueError('Package hash mismatch: '+path.name)
        destination = context/('debian-libs' if row['Package'] in ('libcap2','libseccomp2') else 'toolkit')
        subprocess.run(['dpkg-deb','-x',str(path),str(destination)],check=True)
    versions = set(subprocess.check_output(['nvidia-smi','--query-gpu=driver_version','--format=csv,noheader'],text=True).splitlines())
    if versions != {'590.48.01'}:raise ValueError('Host driver changed; revalidate before building')
    target=context/'driver/usr/lib/x86_64-linux-gnu';target.mkdir(parents=True)
    for pattern in ['libcap.so*','libseccomp.so*']:
        for path in (context/'debian-libs').rglob(pattern):shutil.copy2(path,target/path.name,follow_symlinks=False)
    for pattern in ['libcuda*.so*','libnvidia*.so*','libnvcuvid.so*']:
        for path in Path('/usr/lib/x86_64-linux-gnu').glob(pattern):shutil.copy2(path,target/path.name,follow_symlinks=False)
    bindir=context/'driver/usr/bin';bindir.mkdir(parents=True)
    shutil.copy2(shutil.which('nvidia-smi'),bindir/'nvidia-smi')
    provenance={'driver':'590.48.01','packages':packages,'driver_files':{str(p.relative_to(context/'driver')):('symlink:'+os.readlink(p) if p.is_symlink() else digest(p)) for p in (context/'driver').rglob('*') if p.is_file() or p.is_symlink()}}
    (context/'runtime-provenance.json').write_text(json.dumps(provenance,sort_keys=True,indent=2)+'\n')
    shutil.copy2(ROOT/'deploy/Dockerfile.k3s-node',context/'Dockerfile')
    return publish(context,'k3s-node:1.35.5-runtime1') | {'provenance':provenance}

def business():
    context=ROOT/'tmp/unified-runtime/business';context.mkdir(parents=True,exist_ok=True)
    if (context/'workers').exists():shutil.rmtree(context/'workers')
    shutil.copytree(ROOT/'workers',context/'workers',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    shutil.copy2(ROOT/'assets/models/yolox_tiny.onnx',context/'model.onnx')
    shutil.copy2(ROOT/'scripts/cpu_gpu_probe.py',context/'cpu_gpu_probe.py')
    shutil.copy2(ROOT/'deploy/requirements-unified-runtime.txt',context/'requirements.txt')
    shutil.copy2(ROOT/'deploy/Dockerfile.video-unified',context/'Dockerfile')
    wheels=context/'wheels';wheels.mkdir(exist_ok=True)
    locked=json.loads((ROOT/'deploy/unified-runtime-wheels.lock.json').read_text())
    names={r['file'] for r in locked}
    for path in wheels.iterdir():
        if path.name not in names:path.unlink()
    for row in locked:
        source=CACHE/'wheels'/row['file']
        if not source.exists() or digest(source)!=row['sha256']:raise ValueError('Download locked wheel first: '+row['file'])
        dest=wheels/source.name
        if not dest.exists() or digest(dest)!=row['sha256']:shutil.copy2(source,dest)
    return publish(context,'video-unified:0.5.1-runtime1') | {'model_sha256':digest(context/'model.onnx'),'wheel_count':len(locked)}

def main():
    p=argparse.ArgumentParser();p.add_argument('--target',choices=['node','business','all'],default='all');args=p.parse_args()
    OUT.mkdir(parents=True,exist_ok=True);path=OUT/'images.json';result=json.loads(path.read_text()) if path.exists() else {}
    for name,fn in [('node',node),('business',business)]:
        if args.target in (name,'all'):
            result[name]=fn();path.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:{s:v[s] for s in ['id','local_ref','cluster_ref']} for k,v in result.items()},indent=2))
if __name__=='__main__':main()
