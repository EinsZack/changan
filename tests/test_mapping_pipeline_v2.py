"""Pipeline v2 (align fused into toolchain) contract tests. Fake bytes are NOT reconstruction evidence."""
import json,time
import pytest
from app import db,mapping,mapping_pipeline as pipe
from test_mapping_pipeline import pipeline as v1_pipeline,attempt,finish,units,plan  # noqa: F401
from test_container_execution import seed_container,FakeKube,job,pod
from app.container_execution import workload

@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.setenv('CHANGAN_MAPPING_PIPELINE_VERSION','2')
    monkeypatch.setenv('CHANGAN_MAPPING_IMAGE','r/base@sha256:'+'1'*64)
    monkeypatch.setenv('CHANGAN_MAPPING_COLLAB_IMAGE','r/collab@sha256:'+'2'*64)
    monkeypatch.setenv('CHANGAN_MAPPING_COLLAB_V2_IMAGE','r/collab@sha256:'+'3'*64)
    import io,zipfile
    from types import SimpleNamespace as NS
    from fastapi.testclient import TestClient
    from app.main import app
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w') as z:
        for i in range(8):z.writestr('images/p%s.JPG'%i,b'\xff\xd8\xff'+bytes([i]))
    cli=TestClient(app);d=cli.post('/api/datasets',files={'file':('flight.zip',raw.getvalue())}).json()
    params=mapping.MappingInput(workflow_id=mapping.ID,source=d['id'],execution_mode='collaborative',submodel_size=4,split_overlap_m=25,preset='preview')
    return mapping.create(NS(backend='k3s'),params),params,cli

def v2_attempt(tid,unit,node='thor2',digest='a'*64):
    a,w,r=attempt(tid,unit,node);r['pipeline_version']=2
    if pipe.get(unit['id'])['phase']=='toolchain':r['alignment_sha256']=digest
    return a,w,r

def run_until_merge(tid,digests=('a'*64,'a'*64)):
    a,w,r=v2_attempt(tid,units(tid,'prepare')[0]);finish(a,r)
    for node,u in zip(('thor2','thor3'),units(tid,'reconstruct')):a,w,r=v2_attempt(tid,u,node);finish(a,r)
    for node,u,d in zip(('thor2','thor3'),units(tid,'toolchain'),digests):a,w,r=v2_attempt(tid,u,node,d);finish(a,r)
    return units(tid,'merge')[0]

def test_invalid_pipeline_version_is_rejected(monkeypatch):
    monkeypatch.setenv('CHANGAN_MAPPING_PIPELINE_VERSION','3')
    with pytest.raises(ValueError):mapping.pipeline_version()

def test_v2_graph_has_no_align_unit_and_fans_in_all_sparse_states(pipeline):
    tid,_,_=pipeline;t=db.task_detail(tid);assert t['metadata']['application']['pipeline_version']==2
    a,w,r=v2_attempt(tid,units(tid,'prepare')[0]);finish(a,r)
    t=db.task_detail(tid);assert t['total']==6 and len(t['work_units'])==6 and not units(tid,'align')
    sparse=units(tid,'reconstruct');dense=units(tid,'toolchain')
    for u in dense:
        spec=pipe.get(u['id'])
        assert set(spec['dependencies'])=={s['id'] for s in sparse}
        names=[(x['unit_id'],x['name']) for x in spec['sources']]
        assert names[0][1]=='parent.tar.gz' and names[1]==(units(tid,'prepare')[0]['id'],'seed-'+spec['submodel']+'.tar.gz')
        assert {n for _,n in names[2:]}=={'state.tar.gz'} and len(names)==2+len(sparse)
    a,w,r=v2_attempt(tid,sparse[0]);finish(a,r);assert not any(pipe.ready(u) for u in dense)
    a,w,r=v2_attempt(tid,sparse[1],'thor3');finish(a,r);assert all(pipe.ready(u) for u in dense)

def test_v2_publishes_only_with_identical_alignment(pipeline):
    tid,_,_=pipeline;merge=run_until_merge(tid)
    assert pipe.ready(merge);a,w,r=v2_attempt(tid,merge);finish(a,r)
    t=db.task_detail(tid);assert t['status']=='SUCCEEDED' and t['completed']==t['total']==6

def test_v2_alignment_disagreement_blocks_merge(pipeline):
    tid,_,_=pipeline;merge=run_until_merge(tid,('a'*64,'b'*64))
    a,w,r=v2_attempt(tid,merge)
    with pytest.raises(ValueError,match='统一对齐'):pipe.validate_result(a,w,r)

@pytest.mark.parametrize('mutation',['legacy_image','no_digest','bad_digest'])
def test_v2_rejects_legacy_or_unaligned_toolchain(pipeline,mutation):
    tid,_,_=pipeline;a,w,r=v2_attempt(tid,units(tid,'prepare')[0]);finish(a,r)
    for node,u in zip(('thor2','thor3'),units(tid,'reconstruct')):a,w,r=v2_attempt(tid,u,node);finish(a,r)
    a,w,r=v2_attempt(tid,units(tid,'toolchain')[0])
    if mutation=='legacy_image':r.pop('pipeline_version')
    if mutation=='no_digest':r.pop('alignment_sha256')
    if mutation=='bad_digest':r['alignment_sha256']='xyz'
    with pytest.raises(ValueError):pipe.validate_result(a,w,r)

def test_v1_tasks_keep_align_barrier(v1_pipeline):
    tid,_,_=v1_pipeline;a,w,r=attempt(tid,units(tid,'prepare')[0]);finish(a,r)
    assert db.task_detail(tid)['total']==7 and len(units(tid,'align'))==1

def test_payload_carries_pipeline_version(pipeline,monkeypatch):
    import asyncio
    tid,_,_=pipeline;u=units(tid,'prepare')[0];aid=db.uid('try');now=time.time()
    db.execute("INSERT INTO nodes(id,endpoint,cluster_id,kind,snapshot) VALUES('thor2','k8s://t','c','k3s',?)",(db.dumps({'kube_node':'thor2'}),))
    db.execute("INSERT INTO attempts(id,task_id,segment_id,node_id,token,status,created,updated,decision) VALUES(?,?,?,?,?,?,?,?,?)",(aid,tid,u['id'],'thor2','t','DISPATCHING',now,now,'{}'))
    sent={}
    class Client:
        cluster='c';namespace='n'
        async def submit(self,manifest,payload):sent.update(payload);from types import SimpleNamespace as NS;return NS(metadata=NS(uid='job'))
    class Ctl:
        engine=type('E',(),{'base':'http://x'})()
        def client(self):return Client()
    task=db.query('SELECT * FROM tasks WHERE id=?',(tid,),one=True)
    asyncio.run(mapping.submit(Ctl(),task,u,aid,{'input_sha256':'0'*64,'token':'t'}))
    assert sent['pipeline_version']==2 and sent['phase']=='prepare'

def test_worker_mesh_budget_and_alignment_digest(tmp_path,monkeypatch):
    monkeypatch.syspath_prepend('workers');import mapping_pipeline_worker as w
    assert w.mesh_budget(18,18)==200000 and w.mesh_budget(9,18)==100000 and w.mesh_budget(10,18)==111112
    assert w.mesh_budget(1,77)==w.MIN_MESH_SIZE and w.mesh_budget(80,77)==200000
    with pytest.raises(ValueError):w.mesh_budget(0,18)
    p={'submodels':[{'id':'submodel_0001'},{'id':'submodel_0000'}]}
    for g,body in (('submodel_0000',b'[1]'),('submodel_0001',b'[2]')):
        f=tmp_path/'submodels'/g/'opensfm'/'reconstruction.json';f.parent.mkdir(parents=True);f.write_bytes(body)
    d=w.alignment_digest(tmp_path,p);assert len(d)==64 and d==w.alignment_digest(tmp_path,{'submodels':list(reversed(p['submodels']))})
    (tmp_path/'submodels/submodel_0001/opensfm/reconstruction.json').write_bytes(b'[3]');assert w.alignment_digest(tmp_path,p)!=d

@pytest.mark.asyncio
async def test_hot_reconcile_confirms_sealed_batch_result_without_full_refresh():
    e,c,tid=seed_container()
    db.execute("UPDATE attempts SET status='FINALIZING',workload=json_set(workload,'$.execution_mode','batch_job') WHERE id='a1'")
    c.kube=FakeKube(job(),[pod('Succeeded')])
    refreshed=[]
    async def refresh():refreshed.append(1)
    c.refresh=refresh;e.last_refresh=time.monotonic()  # inside the 2 s full-refresh window
    await e.maintain()
    assert not refreshed and c.kube.deleted==['video-a1']   # observed and moved to cleanup at 0.5 s cadence
    a=db.query("SELECT * FROM attempts WHERE id='a1'",one=True);assert workload(a)['cleanup_requested'] and a['cleanup_state']=='PENDING'
    # Resources stay reserved until the Job and Pods are actually gone.
    c.kube.job_value=None;c.kube.pod_values=[]
    db.execute("UPDATE nodes SET observed=?,snapshot=?,error=NULL WHERE id='lab-1'",(time.time(),db.dumps({'healthy':True})))
    await e.maintain()
    assert db.query("SELECT cleanup_state FROM attempts WHERE id='a1'",one=True)['cleanup_state']=='DONE' and not refreshed
    await e.close()

@pytest.mark.asyncio
async def test_hot_reconcile_ignores_running_and_session_attempts():
    e,c,tid=seed_container()
    db.execute("UPDATE attempts SET workload=json_set(workload,'$.execution_mode','batch_job') WHERE id='a1'")
    calls=[]
    async def spy(a):calls.append(a['id'])
    c.reconcile_one=spy
    assert await c.reconcile_hot()==0 and not calls   # RUNNING: left to the normal 2 s pass
    db.execute("UPDATE attempts SET status='FINALIZING' WHERE id='a1'")
    assert await c.reconcile_hot()==1 and calls==['a1']
    await e.close()

def test_artifact_cache_snapshot_and_manifest(pipeline,monkeypatch):
    monkeypatch.setenv('CHANGAN_MAPPING_CACHE_HOSTPATH','/var/lib/rancher/k3s/changan-cache/mapping')
    assert mapping.artifact_cache()=={'host_path':'/var/lib/rancher/k3s/changan-cache/mapping','max_bytes':20*1024**3}
    a={'id':'try_x','node_id':'thor2'};p={'attempt_timeout':600,'min_memory_mb':16384,'concurrency':4,'workspace_gb':32}
    w={'job_name':'mapping-x','image':'r@sha256:'+'2'*64,'architecture':'arm64','kube_node':'thor2','phase':'toolchain','artifact_cache':mapping.artifact_cache()}
    spec=mapping.manifest(a,w,p)['spec']['template']['spec'];c=spec['containers'][0]
    assert {'name':'mapping-cache','hostPath':{'path':'/var/lib/rancher/k3s/changan-cache/mapping','type':'Directory'}} in spec['volumes']
    assert {'name':'mapping-cache','mountPath':'/cache/mapping'} in c['volumeMounts'] and c['securityContext']['readOnlyRootFilesystem']
    assert {'name':'CHANGAN_MAPPING_CACHE','value':'/cache/mapping'} in c['env']
    # Single-node mapping and unset configuration keep the original Pod shape.
    assert not any(v['name']=='mapping-cache' for v in mapping.manifest(a,dict(w,phase=None),p)['spec']['template']['spec']['volumes'])
    for bad in ('relative/path','/a/../b'):
        monkeypatch.setenv('CHANGAN_MAPPING_CACHE_HOSTPATH',bad)
        with pytest.raises(ValueError):mapping.artifact_cache()
    monkeypatch.delenv('CHANGAN_MAPPING_CACHE_HOSTPATH');assert mapping.artifact_cache() is None

def test_worker_artifact_cache_verifies_and_prunes(tmp_path,monkeypatch):
    monkeypatch.syspath_prepend('workers');import mapping_pipeline_worker as w,hashlib,os
    c=w.ArtifactCache(tmp_path/'c',10);body=b'x'*8;sha=hashlib.sha256(body).hexdigest()
    assert c.get(sha,8) is None
    f=c.tmp('a');f.write_bytes(body);assert c.put(f,sha)==c.path(sha) and c.get(sha,8)==c.path(sha) and c.hits==1
    assert c.get(sha,9) is None                         # size mismatch
    c.path(sha).write_bytes(b'y'*8);assert c.get(sha,8) is None   # tampered content never trusted
    assert c.get('../etc',8) is None
    c.path(sha).unlink();old=[]
    for i in range(3):
        b=bytes([i])*8;h=hashlib.sha256(b).hexdigest();g=c.tmp('b');g.write_bytes(b);c.put(g,h);old.append(c.path(h))
        os.utime(c.path(h),(1,1+i))                      # older than the 30 min protection window
    c.prune();assert sum(p.exists() for p in old)<=1 and old[-1].exists()
    assert w.ArtifactCache(None,1).root is None and w.ArtifactCache(tmp_path/'missing'/'deny',1).root is not None

def test_scheduler_prefers_node_holding_unit_inputs():
    from app.scheduler import choose
    snap=lambda n:{'node_id':n,'healthy':True,'architecture':'arm64','cpu_cores':8,'ephemeral_storage_gb':64,'available_memory_mb':60000,'memory_budget_mb':64000,'capacity':2,'active':0}
    nodes=[{'id':n,'enabled':1,'observed':time.time(),'snapshot':snap(n),'reserved':0,'cached_input_bytes':b} for n,b in (('thor1',0),('thor2',300*1024**2),('thor3',0))]
    params={'application_id':'aerial-mapping-odx','concurrency':4,'workspace_gb':32,'min_memory_mb':16384}
    for alg in ({'method':'round_robin'},{'method':'resource_aware'}):
        for cursor in range(3):
            d=choose([dict(n) for n in nodes],params,0,1,alg,cursor);assert d['target_node']=='thor2' and d['locality']['nodes']==['thor2']
    nodes[1]['session_blocked']='busy'                  # locality never overrides eligibility
    d=choose(nodes,params,0,1,{'method':'round_robin'},0);assert d['target_node'] in ('thor1','thor3') and d['locality'] is None
    nodes[1].pop('session_blocked');nodes[1]['cached_input_bytes']=1024   # tiny archives are not worth pinning
    assert choose(nodes,params,0,1,{'method':'round_robin'},0)['locality'] is None

def test_cached_input_bytes_follow_producers_and_downloaders(pipeline,monkeypatch):
    tid,_,_=pipeline
    meta=db.task_detail(tid)['metadata'];meta['application']['artifact_cache']={'host_path':'/c','max_bytes':1}
    db.execute('UPDATE tasks SET metadata=? WHERE id=?',(db.dumps(meta),tid))
    a,w,r=v2_attempt(tid,units(tid,'prepare')[0],'thor1');finish(a,r)
    task=db.query('SELECT * FROM tasks WHERE id=?',(tid,),one=True);u,other=units(tid,'reconstruct')
    assert set(pipe.cached_input_bytes(task,u))=={'thor1'}
    a,w,r=v2_attempt(tid,u,'thor2');finish(a,r);a,w,r=v2_attempt(tid,other,'thor3');finish(a,r)
    dense=next(x for x in units(tid,'toolchain') if pipe.get(x['id'])['submodel']==pipe.get(u['id'])['submodel'])
    held=pipe.cached_input_bytes(task,dense)
    seed=next(e for e in pipe.inputs(dense) if e['name'].startswith('seed-'))
    assert held['thor1']>=seed['bytes'] and held['thor2']>=seed['bytes'] and held['thor3']<held['thor2']   # thor2 downloaded seed-g
    meta['application'].pop('artifact_cache');db.execute('UPDATE tasks SET metadata=? WHERE id=?',(db.dumps(meta),tid))
    assert pipe.cached_input_bytes(db.query('SELECT * FROM tasks WHERE id=?',(tid,),one=True),dense)=={}


def test_per_task_pipeline_version_and_image(monkeypatch):
    import io,zipfile
    from types import SimpleNamespace as NS
    from fastapi.testclient import TestClient
    from app.main import app
    monkeypatch.setenv('CHANGAN_MAPPING_IMAGE','r/base@sha256:'+'1'*64);monkeypatch.setenv('CHANGAN_MAPPING_COLLAB_IMAGE','r/collab@sha256:'+'2'*64)
    monkeypatch.delenv('CHANGAN_MAPPING_PIPELINE_VERSION',raising=False);monkeypatch.delenv('CHANGAN_MAPPING_COLLAB_V2_IMAGE',raising=False)
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w') as z:
        for i in range(8):z.writestr('images/p%s.JPG'%i,b'\xff\xd8\xff'+bytes([i]))
    d=TestClient(app).post('/api/datasets',files={'file':('f.zip',raw.getvalue())}).json()
    mk=lambda **k:mapping.MappingInput(workflow_id=mapping.ID,source=d['id'],execution_mode='collaborative',submodel_size=4,preset='preview',**k)
    t1=mapping.create(NS(backend='k3s'),mk());app1=db.task_detail(t1)['metadata']['application']
    assert app1['pipeline_version']==1 and app1['image'].endswith('2'*64)
    with pytest.raises(ValueError,match='V2'):mapping.create(NS(backend='k3s'),mk(pipeline_version=2))
    monkeypatch.setenv('CHANGAN_MAPPING_COLLAB_V2_IMAGE','r/collab@sha256:'+'3'*64)
    app2=db.task_detail(mapping.create(NS(backend='k3s'),mk(pipeline_version=2)))['metadata']['application']
    assert app2['pipeline_version']==2 and app2['image'].endswith('3'*64)
