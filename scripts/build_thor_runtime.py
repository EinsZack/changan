#!/usr/bin/env python
"""Rebuild the pinned Thor business image on the x86_64 development server."""
import argparse, hashlib, json, os, shutil, subprocess, urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent

def sha(path):
    with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--qemu',type=Path,default=ROOT/'tmp/thor-runtime/qemu/usr/bin/qemu-aarch64-static')
    parser.add_argument('--tag',default='changan/video-thor:0.5.2-runtime3')
    parser.add_argument('--export',type=Path)
    args=parser.parse_args()
    lock=json.loads((ROOT/'deploy/thor-build-tools.lock.json').read_text())
    if not args.qemu.is_file() or sha(args.qemu)!=lock['qemu_binary_sha256']:
        raise SystemExit('Provide the pinned qemu-aarch64-static binary with --qemu')
    context=ROOT/'tmp/thor-runtime2/business';context.mkdir(parents=True,exist_ok=True)
    wheels=context/'wheels';wheels.mkdir(exist_ok=True)
    rows=json.loads((ROOT/'deploy/thor-runtime-wheels.lock.json').read_text())
    allowed={a['file'] for a in rows}
    for path in wheels.iterdir():
        if path.name not in allowed:path.unlink()
    for row in rows:
        path=wheels/row['file']
        if not path.exists() or sha(path)!=row['sha256']:
            urllib.request.urlretrieve(row['url'],path)
        if sha(path)!=row['sha256']:raise ValueError('Wheel checksum mismatch: '+path.name)
    for src,dst in [('deploy/Dockerfile.thor-runtime','Dockerfile'),
                    ('deploy/requirements-thor-runtime.lock.txt','requirements.txt'),
                    ('assets/models/yolox_tiny.onnx','model.onnx'),
                    ('scripts/cpu_gpu_probe.py','cpu_gpu_probe.py')]:
        shutil.copy2(ROOT/src,context/dst)
    shutil.copy2(args.qemu,context/'qemu-aarch64-static')
    if (context/'workers').exists():shutil.rmtree(context/'workers')
    shutil.copytree(ROOT/'workers',context/'workers',ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
    docker=str(ROOT/'vendor/docker/docker')
    env=os.environ|{'DOCKER_HOST':'unix://'+str(ROOT/'data/k3s/docker.sock')}
    subprocess.run([docker,'build','--platform','linux/arm64','-t',args.tag,str(context)],env=env,check=True)
    info=json.loads(subprocess.check_output([docker,'inspect',args.tag],env=env))[0]
    if info['Architecture']!='arm64' or info['Os']!='linux':raise ValueError('Wrong output image architecture')
    if args.export:
        args.export.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run([docker,'save','-o',str(args.export),args.tag],env=env,check=True)
    print(json.dumps({'image':args.tag,'id':info['Id'],'architecture':info['Architecture'],
                      'model_sha256':sha(context/'model.onnx'),'wheel_count':len(rows)},indent=2))

if __name__=='__main__':main()
