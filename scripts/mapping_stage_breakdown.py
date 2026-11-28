"""从平台SQLite（只读副本）事件拆解建图任务各阶段耗时：python mapping_stage_breakdown.py control.sqlite task_... > out.json"""
import sqlite3,json,sys,collections
DB=sys.argv[1]; TASKS=sys.argv[2:]
c=sqlite3.connect(DB);c.row_factory=sqlite3.Row
ODX=['dataset','split','merge','opensfm','openmvs','odm_filterpoints','odm_meshing','mvs_texturing','odm_georeferencing','odm_dem','odm_orthophoto','odm_report','odm_postprocess']
def analyze(tid):
    t=c.execute("select * from tasks where id=?",(tid,)).fetchone()
    p=json.loads(t['params']);t0=t['created']
    segs={r['id']:r for r in c.execute("select * from segments where task_id=?",(tid,))}
    atts={r['id']:r for r in c.execute("select * from attempts where task_id=?",(tid,))}
    units=collections.OrderedDict()
    for a in sorted(atts.values(),key=lambda a:a['created']):
        s=segs[a['segment_id']];spec=c.execute("select spec from mapping_units where unit_id=?",(s['id'],)).fetchone()
        spec=json.loads(spec[0]) if spec else {'phase':'single','submodel':None}
        res=json.loads(a['result']) if a['result'] else {}
        units[a['id']]=dict(node=a['node_id'],phase=spec['phase'],submodel=spec.get('submodel'),status=a['status'],created=a['created']-t0,metrics=res.get('metrics',{}),odx={},marks={})
    for e in c.execute("select * from events where task_id=? order by id",(tid,)):
        u=units.get(e['attempt_id']);
        if not u:continue
        pl=json.loads(e['payload']);at=e['at']-t0;m=u['marks']
        if e['type']=='dispatch_ack':m.setdefault('ack',at)
        elif e['type']=='mapping_stage':
            st,state=pl.get('stage'),pl.get('state')
            if st=='download':m.setdefault('download',at)
            elif st=='upload':m['upload']=at
            elif state=='FINALIZING':m['final']=at
            elif st in ODX:
                d=u['odx'].setdefault(st,[None,None])
                if state=='RUNNING' and d[0] is None:d[0]=at
                if state=='SUCCEEDED':d[1]=at
            elif st==u['phase'] and state=='RUNNING':m.setdefault('compute',at)
            elif st==u['phase'] and state=='SUCCEEDED':m['compute_end']=at
        elif e['type']=='mapping_unit_complete' or (e['type']=='stage' and pl.get('stage')=='complete' and pl.get('state')=='SUCCEEDED'):m['complete']=at
        elif e['type']=='container_cleaned':m['cleaned']=at
    for u in units.values():
        m=u['marks']
        if u['phase']=='single':
            m['compute']=u['odx'].get('dataset',[m.get('download')])[0]
            m['compute_end']=max((v[1] for v in u['odx'].values() if v[1]),default=None)
        u['odx']={k:round(v[1]-v[0],2) for k,v in u['odx'].items() if v[0] is not None and v[1] is not None}
        def d(a,b):return round(m[b]-m[a],2) if a in m and b in m and m[a] is not None and m[b] is not None else None
        u['seg']=dict(submit_to_ack=round(m['ack']-u['created'],2) if 'ack' in m else None,download_extract=d('download','compute'),compute=d('compute','compute_end'),pack=d('compute_end','upload'),upload=d('upload','final'),confirm=d('final','complete'),cleanup=d('complete','cleaned'))
    el=(t['finished'] or t['updated'])-t0
    return dict(task=tid,name=t['name'],status=t['status'],mode=p.get('execution_mode') or 'single',preset=p['preset'],source=p['source'],submodel_size=p.get('submodel_size'),overlap=p.get('split_overlap_m'),max_nodes=p.get('max_nodes'),concurrency=p.get('concurrency'),elapsed=round(el,2),plan=[len(g['images']) for g in json.loads(t['metadata']).get('mapping_plan',{}).get('submodels',[])],units=list(units.values()))
out=[analyze(t) for t in TASKS]
json.dump(out,sys.stdout,ensure_ascii=False,indent=1)
