"""Create a source + evidence release, excluding runtime data and credentials."""
from pathlib import Path
import hashlib, io, json, os, shutil, subprocess, tarfile
from datetime import datetime, timezone

ROOT=Path(__file__).resolve().parent.parent
VERSION='0.1.0'
PREFIX='changan-orchestrator-midterm-'+VERSION
os.chdir(ROOT)
if subprocess.check_output(['git','status','--porcelain'],text=True).strip():
    raise SystemExit('请先提交待发布的源码与文档，交付包需关联明确版本。')
files=[ROOT/p for p in subprocess.check_output(['git','ls-files','-z']).decode().split('\0') if p]
for extra in ['web/dist','docs/screenshots','docs/recordings']:
    files.extend(p for p in (ROOT/extra).rglob('*') if p.is_file())
files=sorted(set(files))
secret=(ROOT/'data/.internal-token').read_bytes().strip() if (ROOT/'data/.internal-token').exists() else b''
blocked={'data','node_modules','vendor','.git','__pycache__','releases','test-results','playwright-report'}
blobs={}
for p in files:
    rel=p.relative_to(ROOT)
    if p.is_symlink() or any(part in blocked for part in rel.parts) or p.name=='.env' or p.suffix in {'.pyc','.pem','.key'}:
        raise SystemExit('不允许打包：'+str(rel))
    content=p.read_bytes()
    if secret and secret in content:
        raise SystemExit('发布文件中发现内部令牌，已停止：'+str(rel))
    blobs[rel.as_posix()]=content
manifest={'version':VERSION,'created_utc':datetime.now(timezone.utc).isoformat(),'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'scope':'server-process midterm software prototype; vehicle integration pending','binary_assets':'Model, sample video and Redis binaries excluded; bootstrap downloads verified assets.'}
blobs['RELEASE.json']=json.dumps(manifest,ensure_ascii=False,indent=2).encode()
blobs['CONTENTS.sha256']=''.join(hashlib.sha256(content).hexdigest()+'  '+name+'\n' for name,content in sorted(blobs.items())).encode()
out=ROOT/'releases';out.mkdir(exist_ok=True)
archive=out/(PREFIX+'.tar.gz')
with tarfile.open(archive,'w:gz') as t:
    for name,content in sorted(blobs.items()):
        info=tarfile.TarInfo(PREFIX+'/'+name);info.size=len(content);info.mode=0o755 if name.endswith('.sh') else 0o644
        t.addfile(info,io.BytesIO(content))
with tarfile.open(archive) as t:
    stored={m.name[len(PREFIX)+1:]:t.extractfile(m).read() for m in t.getmembers() if m.isfile()}
    for line in stored['CONTENTS.sha256'].decode().splitlines():
        expected,name=line.split('  ',1)
        assert hashlib.sha256(stored[name]).hexdigest()==expected,name
for name in ['交付物说明.md','使用说明.md']:
    shutil.copy2(ROOT/'docs'/name,out/name)
(out/'RELEASE.json').write_bytes(blobs['RELEASE.json'])
outputs=[archive,out/'交付物说明.md',out/'使用说明.md',out/'RELEASE.json']
(out/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+p.name+'\n' for p in outputs))
print(json.dumps({'archive':str(archive),'bytes':archive.stat().st_size,'files':len(blobs),'sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'source_commit':manifest['source_commit'],'content_verified':True},ensure_ascii=False))
