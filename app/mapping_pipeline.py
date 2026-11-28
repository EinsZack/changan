"""Mapping-only DAG. Persist dependencies and immutable attempt-scoped inputs.
This is intentionally not an arbitrary-code/generic workflow API.
"""
import hashlib
import json
import math
import re
import time
from pathlib import Path
from fastapi import HTTPException, Request
from fastapi.responses import FileResponse
from . import db
from .config import DATA, TERMINAL

PHASES = {"prepare", "reconstruct", "align", "toolchain", "merge"}
SUBMODEL = re.compile(r"submodel_\d{4}$")
ARCHIVE = re.compile(r"(?:parent|state|(?:seed|aligned)-submodel_\d{4})\.tar\.gz$")

def enabled(params):
    return params.get("workflow_id") == "aerial-mapping-odx" and params.get("execution_mode") == "collaborative"

def get(unit_id):
    row = db.query("SELECT spec FROM mapping_units WHERE unit_id=?", (unit_id,), one=True)
    return db.loads(row["spec"]) if row else None

def add(c, task_id, ordinal, path, phase, dependencies=(), submodel=None, sources=()):
    uid = db.uid("unit")
    spec = {"phase": phase, "submodel": submodel, "dependencies": list(dependencies), "sources": list(sources)}
    c.execute("INSERT INTO segments(id,task_id,ordinal,start,end,path,kind) VALUES(?,?,?,0,0,?,?)", (uid, task_id, ordinal, path, "mapping_"+phase))
    c.execute("INSERT INTO mapping_units(unit_id,spec) VALUES(?,?)", (uid, db.dumps(spec)))
    return uid

def ref(unit_id, name):
    return {"unit_id": unit_id, "name": name}

def version(meta):
    return int(meta.get("application",{}).get("pipeline_version",1))

def unit_count(groups, pipeline_version):
    return 2*groups+(2 if pipeline_version>=2 else 3)

def expand(c, a, result):
    """Called in prepare-completion transaction; duplicate callbacks cannot expand twice.
    v1: prepare -> N reconstruct -> align -> N toolchain -> merge (2N+3 Jobs, 5 barriers).
    v2: prepare -> N reconstruct -> N toolchain(each re-runs the deterministic global
        alignment from the same immutable inputs) -> merge (2N+2 Jobs, 4 barriers).
    """
    plan = result["plan"]
    ids = [g["id"] for g in plan["submodels"]]
    tid, prep = a["task_id"], a["segment_id"]
    path = c.execute("SELECT path FROM segments WHERE id=?", (prep,)).fetchone()[0]
    meta = db.loads(c.execute("SELECT metadata FROM tasks WHERE id=?", (tid,)).fetchone()[0])
    v = version(meta)
    sparse = []
    for g in ids:
        sparse.append(add(c,tid,1+len(sparse),path,"reconstruct",[prep],g,[ref(prep,"seed-"+g+".tar.gz")]))
    dense = []
    if v >= 2:
        states = [ref(s,"state.tar.gz") for s in sparse]
        for g in ids:
            dense.append(add(c,tid,len(ids)+1+len(dense),path,"toolchain",sparse,g,[ref(prep,"parent.tar.gz"),ref(prep,"seed-"+g+".tar.gz")]+states))
    else:
        align = add(c,tid,len(ids)+1,path,"align",sparse, sources=[ref(prep,"parent.tar.gz")]+[ref(s,"state.tar.gz") for s in sparse])
        for g in ids:
            dense.append(add(c,tid,len(ids)+2+len(dense),path,"toolchain",[align],g,[ref(prep,"seed-"+g+".tar.gz"),ref(align,"aligned-"+g+".tar.gz")]))
    total = unit_count(len(ids), v)
    add(c,tid,total-1,path,"merge",dense,sources=[ref(prep,"parent.tar.gz")]+[ref(s,"state.tar.gz") for s in dense])
    meta["mapping_plan"] = plan
    c.execute("UPDATE tasks SET total=?,metadata=? WHERE id=?", (total,db.dumps(meta),tid))
    db.event(tid,"mapping_plan",{"stage":"prepare","state":"SUCCEEDED","submodels":plan["submodels"],"overlap_edges":plan["overlap_edges"],"work_units":total,"pipeline_version":v},a["id"],c)

HEX64 = re.compile(r"[0-9a-f]{64}")

def alignment_consensus(task_id):
    """v2: every toolchain recomputed the global alignment; all must be bit-identical."""
    rows = db.query("SELECT result FROM segments WHERE task_id=? AND kind='mapping_toolchain'",(task_id,))
    digests = {db.loads(r["result"],{}).get("alignment_sha256") for r in rows}
    if len(digests)!=1 or not all(isinstance(d,str) and HEX64.fullmatch(d) for d in digests):
        raise ValueError("各子模型独立计算的统一对齐结果不一致，拒绝融合")
    return digests.pop()

def ready(unit):
    spec = get(unit["id"])
    if not spec: return True
    # Retry never overlaps a still-live previous Pod, including cancellation cleanup.
    if db.query("SELECT id FROM attempts WHERE segment_id=? AND cleanup_state!='DONE'",(unit["id"],),one=True): return False
    for uid in set(spec["dependencies"]+[r["unit_id"] for r in spec["sources"]]):
        row=db.query("SELECT s.status,s.active_attempt,a.status attempt_status FROM segments s LEFT JOIN attempts a ON a.id=s.active_attempt WHERE s.id=?",(uid,),one=True)
        if not row or row["status"]!="SUCCEEDED" or row["attempt_status"]!="SUCCEEDED": return False
    return True

def inputs(unit):
    spec=get(unit["id"])
    entries=[]
    for source in spec["sources"]:
        row=db.query("SELECT s.active_attempt,s.result FROM segments s WHERE s.id=? AND s.task_id=? AND s.status='SUCCEEDED'",(source["unit_id"],unit["task_id"]),one=True)
        if not row: raise ValueError("前置工作单元尚未成功")
        artifact=next((x for x in db.loads(row["result"])["artifacts"] if x["name"]==source["name"]),None)
        if not artifact: raise ValueError("缺少阶段输入产物")
        entries.append(dict(source,attempt_id=row["active_attempt"],sha256=artifact["sha256"],bytes=artifact["bytes"]))
    return entries

def input_hash(entries):
    return hashlib.sha256(json.dumps(entries,sort_keys=True,separators=(",",":")).encode()).hexdigest()

LOCALITY_MIN_BYTES = 32*1024**2

def cached_input_bytes(task, unit):
    """Bytes of this unit's declared inputs that a node-local cache probably holds, per node.
    Hint only: the worker re-verifies SHA256 and falls back to downloading."""
    if not db.loads(task["metadata"]).get("application",{}).get("artifact_cache"):return {}
    try:entries=inputs(unit)
    except ValueError:return {}
    rows=db.query("SELECT node_id,status,result,workload FROM attempts WHERE task_id=? AND status='SUCCEEDED'",(task["id"],))
    holders={}
    for r in rows:
        held={x["sha256"] for x in db.loads(r["result"],{}).get("artifacts",[]) if isinstance(x,dict)}
        held|={x["sha256"] for x in db.loads(r["workload"],{}).get("inputs",[])}
        for sha in held:holders.setdefault(sha,set()).add(r["node_id"])
    out={}
    for e in entries:
        for node in holders.get(e["sha256"],()):out[node]=out.get(node,0)+e["bytes"]
    return out

def scheduling_nodes(nodes, task, unit=None):
    p=db.loads(task["params"])
    active=db.query("SELECT DISTINCT node_id FROM attempts WHERE task_id=? AND cleanup_state!='DONE'",(task["id"],))
    busy={a["node_id"] for a in active}
    # Fence the last remaining sparse unit onto another real node if all other
    # sparse units ran on just one. Even when a board goes offline, a request for
    # collaborative mapping must never silently become a single-board success.
    excluded=set()
    if unit and unit['kind']=='mapping_reconstruct':
        peers=db.query("SELECT s.status,a.node_id FROM segments s LEFT JOIN attempts a ON a.id=s.active_attempt WHERE s.task_id=? AND s.kind='mapping_reconstruct' AND s.id!=?",(task['id'],unit['id']))
        used={r['node_id'] for r in peers if r['node_id'] and r['status'] not in ('PENDING','FAILED','CANCELED','TIMED_OUT')}
        if len(used)==1 and len(peers)>0 and all(r['node_id'] and r['status']!='PENDING' for r in peers):excluded=used
    cached=cached_input_bytes(task,unit) if unit else {}
    for n in nodes:
        n['cached_input_bytes']=cached.get(n['id'],0)
        if n['id'] in excluded:n['session_blocked']='同图协同至少两个不同节点参与稀疏重建，等待另一合格节点'
        elif n["id"] in busy:n["session_blocked"]="同一建图任务每节点只运行一个阶段单元"
        elif len(busy)>=p["max_nodes"]:n["session_blocked"]="已达到本任务协同节点上限"
    return nodes

def allowed_artifact(a,name):
    spec=get(a["segment_id"])
    if not spec:return False
    phase=spec["phase"]
    if phase=="prepare":return name=="parent.tar.gz" or bool(re.fullmatch(r"seed-submodel_\d{4}\.tar\.gz",name))
    if phase=="align":return bool(re.fullmatch(r"aligned-submodel_\d{4}\.tar\.gz",name))
    if phase in ("reconstruct","toolchain"):return name=="state.tar.gz"
    return name=="collaboration.json"

def check_plan(plan,meta,params):
    if not isinstance(plan,dict) or plan.get("version")!=1:raise ValueError("无效协同计划")
    groups=plan.get("submodels",[])
    if not isinstance(groups,list) or not 2<=len(groups)<=8:raise ValueError("子模型数必须2～8")
    source=db.query("SELECT manifest FROM datasets WHERE id=?",(meta["input"]["id"],),one=True)
    images={Path(i["path"]).name for i in db.loads(source["manifest"])}
    ids=[];sets=[]
    for g in groups:
        if not isinstance(g,dict) or not isinstance(g.get("id"),str) or not SUBMODEL.fullmatch(g["id"]):raise ValueError("无效子模型标识")
        names=g.get("images")
        if not isinstance(names,list) or not all(isinstance(n,str) for n in names) or len(names)!=len(set(names)) or len(names)<3 or not set(names)<=images:raise ValueError("子模型照片清单无效")
        ids.append(g["id"]);sets.append(set(names))
    if len(set(ids))!=len(ids) or set().union(*sets)!=images or any(a==b for i,a in enumerate(sets) for b in sets[i+1:]):raise ValueError("子模型重复或照片覆盖不完整")
    reached={0}
    for _ in sets:
        for i,a in enumerate(sets):
            if any(len(a&sets[j])>=3 for j in reached):reached.add(i)
    if len(reached)!=len(sets):raise ValueError("重叠照片图不连通")
    if set(plan.get("images",[]))!=images or plan.get("split_size")!=params["submodel_size"] or plan.get("overlap_m")!=params["split_overlap_m"]:raise ValueError("分组参数或输入清单不一致")
    # Do not trust worker-provided visualization edges; derive them from actual sets.
    plan["overlap_edges"]=[{"source":ids[i],"target":ids[j],"images":sorted(sets[i]&sets[j])} for i in range(len(ids)) for j in range(i+1,len(ids)) if len(sets[i]&sets[j])>=3]
    return plan

def validate_result(a,w,r,verify_content=False):
    from .mapping import ID,sha,required
    spec=get(a["segment_id"])
    if not isinstance(r,dict):raise ValueError("缺少阶段结果")
    expected={"application_id":ID,"engine_version":"3.8.6","attempt_id":a["id"],"task_id":a["task_id"],"work_unit_id":a["segment_id"],"actual_node":a["node_id"],"pod_uid":w["pod_uid"],"input_sha256":w["input_sha256"],"device":"cpu","phase":spec["phase"],"submodel":spec["submodel"]}
    if any(r.get(k)!=v for k,v in expected.items()) or r.get("inputs")!=w.get("inputs",[]):raise ValueError("阶段结果来源/依赖版本不一致")
    task=db.query("SELECT params,metadata FROM tasks WHERE id=?",(a["task_id"],),one=True);params=db.loads(task["params"]);meta=db.loads(task["metadata"])
    if version(meta)>=2:
        # An image whose worker predates v2 would run toolchain on unaligned sparse models.
        if r.get("pipeline_version")!=2:raise ValueError("协同镜像未声明v2阶段协议，拒绝采用")
        if spec["phase"]=="toolchain" and not (isinstance(r.get("alignment_sha256"),str) and HEX64.fullmatch(r["alignment_sha256"])):raise ValueError("地理成果单元缺少统一对齐摘要")
    # Re-evaluate lineage: a changed/failed upstream cannot publish stale descendants.
    if inputs(dict(id=a["segment_id"],task_id=a["task_id"]))!=w.get("inputs",[]):raise ValueError("前置产物版本已失效")
    phase=spec["phase"]
    if phase=="prepare":
        r["plan"]=check_plan(r.get("plan"),meta,params)
        names={"parent.tar.gz"}|{"seed-"+g["id"]+".tar.gz" for g in r["plan"]["submodels"]}
    elif phase=="align":names={"aligned-"+g["id"]+".tar.gz" for g in meta["mapping_plan"]["submodels"]}
    elif phase=="merge":
        names=(required(params["preset"])-{"report.pdf"})|{"collaboration.json"}
        q=r.get("quality",{})
        if q.get("images")!=meta["input"]["image_count"] or type(q.get("registered")) is not int or not math.ceil(q["images"]*params["min_registered_ratio"])<=q["registered"]<=q["images"]:raise ValueError("融合照片配准率不达标")
        if set(q.get("submodels",[]))!={g["id"] for g in meta["mapping_plan"]["submodels"]}:raise ValueError("融合子模型不完整")
        if any(type(q.get(k)) is not int or q[k]<=0 for k in ("width","height")) or not q.get("crs"):raise ValueError("无效空间成果")
        if params["preset"]=="standard" and (type(q.get("points")) is not int or q["points"]<=0):raise ValueError("无有效融合点云")
        if version(meta)>=2:alignment_consensus(a["task_id"])
    else:names={"state.tar.gz"}
    artifacts=r.get("artifacts",[])
    if not isinstance(artifacts,list) or any(not isinstance(m,dict) for m in artifacts) or len(artifacts)!=len(names) or {m.get("name") for m in artifacts}!=names:raise ValueError("阶段产物不完整")
    for m in artifacts:
        stored=db.query("SELECT bytes,sha256 FROM artifacts WHERE attempt_id=? AND name=?",(a["id"],m["name"]),one=True)
        file=DATA/"tasks"/a["task_id"]/a["id"]/m["name"]
        if not stored or type(m.get("bytes")) is not int or m["bytes"]<=0 or stored!={k:m.get(k) for k in ("bytes","sha256")} or not file.is_file() or file.stat().st_size!=m["bytes"]:raise ValueError("阶段产物未落盘或摘要不符")
        if verify_content and sha(file)!=m["sha256"]:raise ValueError("阶段产物内容校验失败")
        if phase=="merge":m["url"]="/api/tasks/"+a["task_id"]+"/artifacts/"+a["id"]+"/"+m["name"]
    if not isinstance(r.get("metrics"),dict) or any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in r["metrics"].values()):raise ValueError("阶段计量无效")
    return r|{"image":w["image"],"job_uid":w["job_uid"],"pod_uid":w["pod_uid"],"kube_node":w["node_name"],"execution_backend":"k3s","execution_mode":"collaborative"}

def complete(a,r,c):
    phase=get(a["segment_id"])["phase"]
    if phase=="prepare":expand(c,a,r)
    count=c.execute("SELECT count(*) FROM segments WHERE task_id=? AND status='SUCCEEDED'",(a["task_id"],)).fetchone()[0]
    c.execute("UPDATE tasks SET completed=?,updated=? WHERE id=?",(count,time.time(),a["task_id"]))
    if phase=="merge":
        total=c.execute("SELECT total FROM tasks WHERE id=?",(a["task_id"],)).fetchone()[0]
        if count!=total:raise ValueError("融合发布时仍有未成功的必要阶段")
        node_rows=c.execute("SELECT DISTINCT a.actual_node FROM attempts a JOIN segments s ON s.active_attempt=a.id WHERE s.task_id=? AND s.kind IN ('mapping_reconstruct','mapping_toolchain')",(a["task_id"],)).fetchall()
        if len({x[0] for x in node_rows if x[0]})<2:raise ValueError('缺少两个实际计算节点，不发布为多节点协同成功')
        r["collaboration"]={"compute_nodes":[x[0] for x in node_rows],"work_units":total,"submodels":r["quality"]["submodels"]}
        c.execute("UPDATE tasks SET status='SUCCEEDED',finished=?,updated=?,result=? WHERE id=?",(time.time(),time.time(),db.dumps(r),a["task_id"]))
    db.event(a["task_id"],"mapping_unit_complete",{"stage":phase,"state":"SUCCEEDED","unit_id":a["segment_id"],"submodel":r.get("submodel"),"node_id":a["node_id"],"metrics":r["metrics"]},a["id"],c)

async def dependency_input(aid:str,index:int,request:Request):
    from .container_execution import authenticate
    a,w=authenticate(aid,request)
    entries=w.get("inputs",[])
    if not 0<=index<len(entries):raise HTTPException(404)
    entry=entries[index]
    unit={"id":a["segment_id"],"task_id":a["task_id"]}
    if inputs(unit)!=entries:raise HTTPException(409,"前置版本失效")
    path=DATA/"tasks"/a["task_id"]/entry["attempt_id"]/entry["name"]
    if not path.is_file() or path.stat().st_size!=entry["bytes"]:raise HTTPException(409,"中间产物不可用")
    return FileResponse(path,media_type="application/gzip")
