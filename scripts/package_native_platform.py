#!/usr/bin/env python
"""Package a deployable native control plane; development stays in the server repository."""
import argparse, hashlib, io, json, subprocess, tarfile, time
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent

def package(output):
    files = []
    for directory in ['app', 'workers', 'web/dist']:
        files.extend(p for p in (ROOT/directory).rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc')
    files += [ROOT/p for p in ['scripts/manage.py', 'scripts/kubeconfig.py', 'scripts/migrate_platform_data.py', 'scripts/install_native_services.py', 'scripts/publish_oci_archive.py', 'scripts/validate_thor_native.py',
                              'requirements.txt', 'assets/models/yolox_tiny.onnx', 'assets/samples/demo.mp4']]
    blobs = {str(p.relative_to(ROOT)): p.read_bytes() for p in sorted(files)}
    checks = {name: hashlib.sha256(value).hexdigest() for name, value in blobs.items()}
    metadata = {'source_commit': subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
                'working_tree_dirty': bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True).strip()),
                'scope': 'native macOS management services; Linux ARM64 task execution stays on Thor',
                'files': checks}
    blobs['DEPLOYMENT.json'] = (json.dumps(metadata,ensure_ascii=False,indent=2)+'\n').encode()
    output = Path(output); output.parent.mkdir(parents=True,exist_ok=True)
    with tarfile.open(output,'w:gz') as archive:
        for name,value in blobs.items():
            item=tarfile.TarInfo(name);item.size=len(value);item.mode=0o644
            # StaticFiles derives validators from mtime and size. Epoch timestamps
            # made equal-sized HTML releases reuse a stale browser cache entry.
            item.mtime=int((ROOT/name).stat().st_mtime) if (ROOT/name).exists() else int(time.time())
            archive.addfile(item,io.BytesIO(value))
    print(json.dumps({'path':str(output),'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),
                      'source_commit':metadata['source_commit'],'files':len(blobs)},ensure_ascii=False))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('output');args=parser.parse_args();package(args.output)
