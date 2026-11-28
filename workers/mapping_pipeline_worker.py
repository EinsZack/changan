"""Thor-only mapping DAG worker. Safe, checksummed, explicit stage transfers."""
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import time
import zipfile
import requests
from photo_bundle import photos

ROOT=Path('/work/project')

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()

def extract(archive,root,limit):
    """No links/devices/traversal, bounded expansion and unique entries per archive.
    Different dependencies intentionally overlay aligned reconstruction/config files.
    """
    total=0;seen=set()
    with tarfile.open(archive,'r:gz') as t:
        for m in t:
            p=PurePosixPath(m.name)
            if p.is_absolute() or '..' in p.parts or not p.parts or p.parts[0]!='project' or '\\' in m.name or m.name in seen or not (m.isdir() or m.isfile()):raise ValueError('中间归档包含非法路径、链接或重复成员')
            seen.add(m.name);total+=m.size
            if total>limit or len(seen)>100000:raise ValueError('中间归档超过展开配额')
            dest=root.joinpath(*p.parts)
            if m.isdir():dest.mkdir(parents=True,exist_ok=True)
            else:
                dest.parent.mkdir(parents=True,exist_ok=True)
                with t.extractfile(m) as src,dest.open('wb') as out:shutil.copyfileobj(src,out)
    return total

# Pipeline v2 (2026-11-28): align is fused into every toolchain unit, so one K3s Job
# barrier disappears. Default ODX mesh budget is per *mesh*, not per area; v2 gives
# each submodel a share proportional to its photos so face density matches a single run.
DEFAULT_MESH_SIZE=200000
MIN_MESH_SIZE=20000

def mesh_budget(group_images,total_images,default=DEFAULT_MESH_SIZE):
    if total_images<=0 or group_images<=0:raise ValueError('无效的子模型照片数')
    return max(MIN_MESH_SIZE,min(default,-(-default*group_images//total_images)))

def alignment_digest(root,plan):
    """Stable digest of the global alignment every toolchain unit computed independently."""
    h=hashlib.sha256()
    for g in sorted(x['id'] for x in plan['submodels']):
        h.update(g.encode()+b'\0');h.update((root/'submodels'/g/'opensfm/reconstruction.json').read_bytes());h.update(b'\0')
    return h.hexdigest()

class ArtifactCache:
    """Node-local, content-addressed cache of stage archives (hostPath, UID 10001).

    Keyed by SHA256 and re-verified on every read, so a stale, truncated or foreign file
    is never trusted; on any doubt the worker downloads from the control plane as before.
    Only artifacts that already flow between units of a task are stored; the original
    photo ZIP is not cached so single-node runs stay a like-for-like baseline.
    """
    def __init__(self,root,max_bytes):
        self.root=Path(root) if root else None;self.max_bytes=max_bytes;self.hits=0;self.hit_bytes=0
        if self.root:
            try:
                for d in ('sha256','tmp'):(self.root/d).mkdir(parents=True,exist_ok=True)
                probe=self.root/'tmp'/('probe-%s'%os.getpid());probe.write_bytes(b'');probe.unlink()
            except OSError:self.root=None
    def path(self,sha):return self.root/'sha256'/sha
    def get(self,sha,size):
        if not self.root or not re.fullmatch(r'[0-9a-f]{64}',sha or ''):return None
        f=self.path(sha)
        try:
            if f.stat().st_size!=size or digest(f)!=sha:return None
            os.utime(f);self.hits+=1;self.hit_bytes+=size;return f
        except OSError:return None
    def tmp(self,name):
        return self.root/'tmp'/('%s-%s-%s'%(os.getpid(),time.monotonic_ns(),name))
    def put(self,file,sha):
        """Atomically publish a verified file; returns the cached path (or the original)."""
        if not self.root:return file
        dest=self.path(sha)
        try:
            os.replace(file,dest);return dest
        except OSError:return file
    def prune(self):
        if not self.root:return
        now=time.time()
        for f in (self.root/'tmp').iterdir():
            try:
                if now-f.stat().st_mtime>6*3600:f.unlink()
            except OSError:pass
        files=[]
        for f in (self.root/'sha256').iterdir():
            try:st=f.stat();files.append((st.st_mtime,st.st_size,f))
            except OSError:pass
        used=sum(x[1] for x in files)
        for mtime,size,f in sorted(files):
            # Recently used entries may belong to a unit still extracting them.
            if used<=self.max_bytes or now-mtime<1800:break
            try:f.unlink();used-=size
            except OSError:pass

def pack(destination,paths,compresslevel=1):
    """Materialize ODX internal symlinks; never send links to the control plane.
    Seed archives are almost entirely JPEG; level 0 keeps the gzip container but skips
    deflate work that cannot shrink them (measured: 2.5 s Brighton / 14.7 s Aukerman packing)."""
    seen=set()
    with tarfile.open(destination,'w:gz',compresslevel=compresslevel,dereference=True) as t:
        for path in paths:
            members=[path] if path.is_file() else [Path(d)/n for d,_,names in os.walk(path,followlinks=True) for n in sorted(names)]
            for f in members:
                if not f.is_file():continue
                if not f.resolve().is_relative_to(ROOT):raise ValueError('ODX中间文件链接离开工作区')
                name=str(f.relative_to(ROOT.parent))
                if name in seen:continue
                seen.add(name);t.add(f,arcname=name,recursive=False)

def final_products(root,plan,preset):
    from osgeo import gdal
    engine=json.loads((root/'log.json').read_text())
    if not engine.get('success') or engine.get('version')!='3.8.6':raise ValueError('融合引擎未成功退出或版本不符')
    ortho=root/'odm_orthophoto/odm_orthophoto.tif';shot=root/'odm_report/shots.geojson'
    features=json.loads(shot.read_text())['features']
    # Upstream merge_geojson_shots deduplicates by filename/id.
    expected={}
    for g in plan['submodels']:
        sub=json.loads((root/'submodels'/g['id']/'odm_report/shots.geojson').read_text())['features']
        names={f['properties']['filename'] for f in sub}
        if not names or not names<=set(g['images']):raise ValueError('子模型配准照片来源无效')
        expected.update({f['properties']['filename']:f for f in sub})
    names={f['properties']['filename'] for f in features}
    if names!=set(expected):raise ValueError('融合相机清单未包含所有子模型成果')
    # Pinned upstream fails to update its dedup set after the first model.
    # Normalize the public merged camera list; do not count duplicates as coverage.
    merged=json.loads(shot.read_text());merged['features']=list(expected.values())
    shot.write_text(json.dumps(merged));registered=len(expected)
    ds=gdal.Open(str(ortho));srs=ds.GetSpatialRef() if ds else None
    if not srs or ds.RasterXSize<=0 or ds.RasterYSize<=0:raise ValueError('融合正射图缺少空间参考')
    gt=ds.GetGeoTransform();w,h=ds.RasterXSize,ds.RasterYSize
    preview=root/'preview.jpg'
    thumb=gdal.Translate(str(preview),ds,format='JPEG',width=min(1024,w),height=0,bandList=list(range(1,min(3,ds.RasterCount)+1)))
    if thumb is None:raise ValueError('预览生成失败')
    thumb=None
    quality={'images':len(plan['images']),'registered':registered,'width':w,'height':h,'crs':srs.GetName(),'epsg':srs.GetAuthorityCode(None),'corners':[gdal.ApplyGeoTransform(gt,x,y) for x,y in ((0,0),(0,h),(w,0),(w,h))],'pixel_size':[gt[1],abs(gt[5])],'submodels':[g['id'] for g in plan['submodels']]}
    files={'orthophoto.tif':ortho,'preview.jpg':preview,'engine.json':root/'log.json','shots.geojson':shot}
    if preset=='standard':
        files.update({'dsm.tif':root/'odm_dem/dsm.tif','cloud.laz':root/'odm_georeferencing/odm_georeferenced_model.laz'})
        dem=gdal.Open(str(files['dsm.tif']))
        if not dem or not dem.GetSpatialRef():raise ValueError('融合DSM无效')
        with files['cloud.laz'].open('rb') as f:header=f.read(375)
        points=int.from_bytes(header[107:111],'little') or (int.from_bytes(header[247:255],'little') if header[25]>=4 else 0)
        if header[:4]!=b'LASF' or points<=0:raise ValueError('融合点云无效')
        quality['points']=points
    report=root/'collaboration.json'
    report.write_text(json.dumps({'kind':'mapping-collaboration-report','engine':'ODX 3.8.6','plan':plan,'quality':quality,'note':'子模型全量融合检查与配准覆盖；不等同于GCP测量精度验收'},ensure_ascii=False,indent=2))
    files['collaboration.json']=report
    return files,quality

def main(p):
    base=p['base_url'];http=requests.Session();http.trust_env=False
    http.headers.update({'X-Attempt-Token':p['token'],'X-Pod-UID':os.environ['POD_UID']})
    seq=0;started=time.monotonic();params=p['params'];phase=p['phase'];group=p['submodel'];version=int(p.get('pipeline_version',1))
    fused=phase=='toolchain' and version>=2;alignment=None
    metrics={'download_bytes':0,'download_seconds':0,'compute_seconds':0,'pack_seconds':0,'upload_bytes':0,'upload_seconds':0}
    def post(endpoint,body):
        for retry in range(4):
            try:
                r=http.post(base+endpoint,json=body,timeout=(5,120));r.raise_for_status();return
            except requests.RequestException:
                if retry==3:raise
                time.sleep(2**retry)
    def event(status,**kwargs):
        nonlocal seq
        seq+=1;post('/events',dict(status=status,seq=seq,**kwargs))
    cache=ArtifactCache(os.environ.get('CHANGAN_MAPPING_CACHE'),int(os.environ.get('CHANGAN_MAPPING_CACHE_MAX_BYTES',str(20*1024**3))))
    def fetch(url,expected,size,fallback):
        """Return a verified local archive for a declared dependency, preferring the node cache."""
        hit=cache.get(expected,size)
        if hit:return hit,False
        if cache.root:
            dest=cache.tmp('dep.tar.gz');download(url,dest,expected,size);return cache.put(dest,expected),False
        download(url,fallback,expected,size);return fallback,True
    def download(url,dest,expected,expected_size=None):
        t=time.monotonic();size=0;h=hashlib.sha256()
        with http.get(url,stream=True,timeout=(8,180)) as r,dest.open('wb') as f:
            r.raise_for_status()
            for b in r.iter_content(1048576):
                size+=len(b)
                if size>4*1024**3:raise ValueError('下载超过配额')
                h.update(b);f.write(b)
        metrics['download_seconds']+=time.monotonic()-t;metrics['download_bytes']+=size
        if h.hexdigest()!=expected or (expected_size is not None and size!=expected_size):raise ValueError('阶段输入摘要/大小不符')
    post('/claim',{})
    ROOT.mkdir(parents=True,exist_ok=True)
    os.environ.update(HOME='/work',TMPDIR='/tmp',no_ansiesc='1',OMP_NUM_THREADS=str(params['concurrency']),OPENBLAS_NUM_THREADS='1')
    try:
        event('RUNNING',phase='download')
        if phase=='prepare':
            archive=ROOT/'input.zip';download(p['input_url'],archive,p['input_sha256'])
            imgs=ROOT/'images';imgs.mkdir()
            with zipfile.ZipFile(archive) as z:
                for i in photos(z):
                    with z.open(i) as src,(imgs/Path(i.filename).name).open('xb') as dst:shutil.copyfileobj(src,dst)
            archive.unlink()
        else:
            total=0
            for index,entry in enumerate(p['inputs']):
                dest,temporary=fetch(base+'/dependency/'+str(index),entry['sha256'],entry['bytes'],ROOT.parent/('input-%s.tar.gz'%index))
                total+=extract(dest,ROOT.parent,params['workspace_gb']*1024**3-total)
                if temporary:dest.unlink()
            metrics['cache_hits']=cache.hits;metrics['cache_hit_bytes']=cache.hit_bytes
        if phase=='merge':
            merge_plan=json.loads((ROOT/'plan.json').read_text())
            if {d.name for d in (ROOT/'submodels').iterdir() if d.is_dir()}!={g['id'] for g in merge_plan['submodels']}:raise ValueError('融合输入子模型集合不一致')
            required=['odm_orthophoto/odm_orthophoto_cut.tif','odm_orthophoto/odm_orthophoto_feathered.tif','odm_report/shots.geojson']
            if params['preset']=='standard':required+=['odm_dem/dsm.tif','odm_georeferencing/odm_georeferenced_model.laz']
            for g in merge_plan['submodels']:
                if any(not (ROOT/'submodels'/g['id']/n).is_file() for n in required):raise ValueError('缺失子模型成果，拒绝部分融合')
        project=ROOT/'submodels'/group if group else ROOT
        if fused:
            # Same inputs as the former align unit (parent + every sparse state), plus this
            # submodel's seed. Every toolchain recomputes the identical global alignment; the
            # platform refuses to merge unless all digests agree.
            plan=json.loads((ROOT/'plan.json').read_text())
            if group not in {g['id'] for g in plan['submodels']}:raise ValueError('子模型不在协同计划中')
            event('RUNNING',phase='align',transition='Running');t=time.monotonic()
            env=dict(os.environ,CHANGAN_MAPPING_PHASE='align')
            done=subprocess.run(['python3','-u','/opt/changan/mapping_odx_stage.py'],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,errors='replace')
            (ROOT/'align-worker.log').write_text(done.stdout)
            if done.returncode:raise RuntimeError('统一对齐失败: '+done.stdout[-1200:])
            alignment=alignment_digest(ROOT,plan);metrics['align_seconds']=time.monotonic()-t
            event('RUNNING',phase='align',transition='Finished')
        if phase=='reconstruct':(project/'opensfm/split_merge_stop_at_reconstruction.txt').touch()
        if phase=='toolchain':
            (project/'opensfm/split_merge_stop_at_reconstruction.txt').unlink(missing_ok=True)
            # Matches/features were consumed before alignment. Explicit upstream seed markers
            # prevent recomputation; aligned tracks/reconstruction are authoritative.
            for d in ('features','matches','exif'):
                (project/'opensfm'/d).mkdir(exist_ok=True)
                (project/'opensfm'/d/'empty').touch()
        argv=['python3','-u','/opt/changan/mapping_odx_stage.py','--project-path',str(project.parent),project.name,'--no-gpu','--max-concurrency',str(params['concurrency']),'--feature-quality','medium','--skip-3dmodel','--orthophoto-resolution',str(params['resolution_cm'])]
        if params['preset']=='standard':argv+=['--pc-quality','low','--dsm','--dem-resolution',str(params['resolution_cm'])]
        else:argv+=['--fast-orthophoto']
        if phase=='prepare':argv+=['--split',str(params['submodel_size']),'--split-overlap',str(params['split_overlap_m'])]
        if phase in ('reconstruct','toolchain'):argv+=['--orthophoto-cutline','--dem-euclidean-map','--skip-report','--crop','0.015625']
        if fused:
            images=next(g['images'] for g in plan['submodels'] if g['id']==group)
            mesh=mesh_budget(len(images),len(plan['images']));argv+=['--mesh-size',str(mesh)];metrics['mesh_size']=mesh
        os.environ['CHANGAN_MAPPING_PHASE']=phase
        event('RUNNING',phase=phase,transition='Running');compute=time.monotonic();log=ROOT/'stage-worker.log'
        with log.open('w') as out,subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,errors='replace') as proc:
            for line in proc.stdout:
                out.write(line);print(line.rstrip(),flush=True)
                match=re.search(r'\b(Running|Finished) ([a-z0-9_]+) stage\b',line)
                if match:event('RUNNING',phase=match[2],transition=match[1])
            code=proc.wait()
        metrics['compute_seconds']=time.monotonic()-compute
        if code:raise RuntimeError('ODX阶段退出码%s: %s'%(code,log.read_bytes()[-1200:].decode(errors='replace')))
        event('RUNNING',phase=phase,transition='Finished')
        packing=time.monotonic();out=cache.tmp('out') if cache.root else ROOT.parent/'out';out.mkdir();files={};quality=None;plan=None
        if phase=='prepare':
            plan=json.loads((ROOT/'plan.json').read_text())
            parent=out/'parent.tar.gz';pack(parent,[x for x in ROOT.iterdir() if x.name not in ('images','submodels','stage-worker.log')]);files[parent.name]=parent
            for g in plan['submodels']:
                file=out/('seed-'+g['id']+'.tar.gz');pack(file,[ROOT/'submodels'/g['id']],0 if version>=2 else 1);files[file.name]=file
        elif phase=='reconstruct':
            recon=json.loads((project/'opensfm/reconstruction.json').read_text())
            if not recon or not any(r.get('shots') for r in recon):raise ValueError('子模型没有有效稀疏重建')
            file=out/'state.tar.gz';pack(file,[project/'opensfm'/n for n in ('image_list.txt','config.yaml','camera_models.json','reference_lla.json','reconstruction.json','tracks.csv')]);files[file.name]=file
        elif phase=='align':
            plan=json.loads((ROOT/'plan.json').read_text())
            for g in plan['submodels']:
                sp=ROOT/'submodels'/g['id']/'opensfm';file=out/('aligned-'+g['id']+'.tar.gz')
                pack(file,[sp/n for n in ('image_list.txt','config.yaml','camera_models.json','reference_lla.json','reconstruction.json','tracks.csv')]);files[file.name]=file
        elif phase=='toolchain':
            required=['odm_orthophoto/odm_orthophoto_cut.tif','odm_orthophoto/odm_orthophoto_feathered.tif','odm_orthophoto/cutline.gpkg','odm_report/shots.geojson']
            if params['preset']=='standard':required+=['odm_dem/dsm.tif','odm_georeferencing/odm_georeferenced_model.laz']
            if any(not (project/n).is_file() or not (project/n).stat().st_size for n in required):raise ValueError('子模型地理成果不完整')
            file=out/'state.tar.gz';pack(file,[project/n for n in ('odm_orthophoto','odm_dem','odm_report','odm_georeferencing','cameras.json')]);files[file.name]=file
        else:
            plan=json.loads((ROOT/'plan.json').read_text())
            expected={g['id'] for g in plan['submodels']}
            actual={d.name for d in (ROOT/'submodels').iterdir() if d.is_dir()}
            if actual!=expected:raise ValueError('融合前子模型清单不一致')
            files,quality=final_products(ROOT,plan,params['preset'])
        metrics['pack_seconds']=time.monotonic()-packing
        event('UPLOADING',phase='upload');manifest=[]
        for name,file in files.items():
            sha=digest(file);t=time.monotonic()
            # Stage archives were packed inside the cache volume: publishing is a rename,
            # so downstream units placed on this node need not download them again.
            if cache.root and phase!='merge':file=cache.put(file,sha)
            for retry in range(3):
                try:
                    with file.open('rb') as stream:r=http.put(base+'/artifacts/'+name,data=stream,headers={'X-Content-SHA256':sha},timeout=(8,240));r.raise_for_status()
                    break
                except requests.RequestException:
                    if retry==2:raise
                    time.sleep(2**retry)
            metrics['upload_seconds']+=time.monotonic()-t;metrics['upload_bytes']+=file.stat().st_size
            manifest.append({'name':name,'sha256':sha,'bytes':file.stat().st_size})
        result={'application_id':'aerial-mapping-odx','engine_version':'3.8.6','input_sha256':p['input_sha256'],'attempt_id':p['attempt_id'],'task_id':p['task_id'],'work_unit_id':p['segment_id'],'pod_uid':os.environ['POD_UID'],'actual_node':p['target_node'],'phase':phase,'submodel':group,'inputs':p['inputs'],'device':'cpu','artifacts':manifest,'metrics':metrics,'elapsed_seconds':time.monotonic()-started}
        if phase=='prepare':result['plan']=plan
        if version>=2:result['pipeline_version']=2
        if alignment:result['alignment_sha256']=alignment
        if quality:result['quality']=quality
        if cache.root:shutil.rmtree(out,ignore_errors=True);cache.prune()
        event('FINALIZING',phase=phase,result=result)
    except Exception as e:
        event('FAILED',phase=phase,error=str(e)[-1600:]);raise
