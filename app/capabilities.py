import asyncio, time, json, os, sys, tempfile
from pathlib import Path
import httpx, redis.asyncio as redis
from . import db
from .config import REDIS_URL, DATA
from .scheduler import choose
async def run_capabilities(engine):
    id=db.uid('cap'); outputs=[]
    async def step(kind, fn):
        at=time.time()
        try:
            value=await fn(); record={'kind':kind,'status':'SUCCEEDED','seconds':time.time()-at,'output':value}
        except Exception as e:
            record={'kind':kind,'status':'FAILED','seconds':time.time()-at,'error':str(e)[:300]}
        outputs.append(record); return record
    async def api():
        async with httpx.AsyncClient(trust_env=False) as c:
            r=await c.get(engine.base+'/health'); r.raise_for_status(); return r.json()
    async def cache():
        c=redis.from_url(REDIS_URL,decode_responses=True,socket_connect_timeout=3,socket_timeout=3)
        try:
            await c.set('changan:capability:'+id,id,ex=600)
            result=await c.get('changan:capability:'+id)
            if result!=id: raise ValueError('Redis读写不一致')
            return {'key':'changan:capability:'+id,'value':result,'mode':'standalone','actual_io':True}
        finally: await c.aclose()
    async def script():
        results=[]
        for args in [[sys.executable,'-I','-c','import json; print(json.dumps({"normalized": True, "value": 4 * 2}))'],['/bin/sh','-c','printf "shell-executed\\n"']]:
            p=await asyncio.create_subprocess_exec(*args,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
            out,err=await asyncio.wait_for(p.communicate(),5)
            if p.returncode: raise ValueError('脚本执行失败')
            results.append({'runtime':Path(args[0]).name,'exit_code':p.returncode,'stdout':out.decode().strip()})
        return results
    async def algorithm():
        params={'sample_fps':2,'min_memory_mb':64}
        return choose(engine.nodes(),params,4,100000,{'method':'resource_aware'})
    async def platform():
        nodes=engine.nodes()
        if not nodes: raise ValueError('无执行器')
        r=await engine.client.get(nodes[0]['endpoint']+'/health');r.raise_for_status();return r.json()
    for kind,fn in [('api',api),('redis',cache),('script',script),('algorithm',algorithm),('platform',platform)]: await step(kind,fn)
    report={'id':id,'workflow_id':'capability-demo','created':time.time(),'status':'SUCCEEDED' if all(o['status']=='SUCCEEDED' for o in outputs) else 'FAILED','nodes':outputs,'scope':'五类固定模板节点实际调用验证；不是任意脚本或拖拽编排平台'}
    folder=DATA/'capabilities';folder.mkdir(exist_ok=True);(folder/f'{id}.json').write_text(db.dumps(report))
    return report

def redact(value):
    if isinstance(value,dict): return {k:('••••••' if any(word in k.lower() for word in ['secret','password','token','authorization']) else redact(v)) for k,v in value.items()}
    if isinstance(value,list): return [redact(v) for v in value]
    return value
async def connector_call(config, params=None):
    body=db.loads(config['body']); headers={}; auth_id=body.get('auth_id','none')
    auth=db.query("SELECT body FROM configs WHERE id=? AND category='auth'",(auth_id,),one=True)
    if not auth: raise ValueError('认证配置不存在')
    auth=db.loads(auth['body'])
    if auth.get('type')=='env_bearer':
        key=auth.get('env_name','')
        if not key.startswith('CHANGAN_CONNECTOR_'): raise ValueError('凭据变量必须使用 CHANGAN_CONNECTOR_ 前缀')
        token=os.getenv(key)
        if not token: raise ValueError('认证凭据环境变量未设置')
        headers['Authorization']='Bearer '+token
    elif auth.get('type')!='none': raise ValueError('不支持的认证插件')
    # Credentials are resolved afresh on each call; no secret is persisted or returned.
    method=body.get('method','GET'); url=body['url']
    if not url.startswith(('http://','https://')): raise ValueError('仅支持HTTP接口')
    data=params or body.get('params',{})
    if body.get('input_map'):
        data={target:data[source] for target,source in body['input_map'].items() if source in data}
    async with httpx.AsyncClient(timeout=10,trust_env=False,follow_redirects=False) as client:
        r=await client.request(method,url,headers=headers,params=data if method=='GET' else None,json=data if method=='POST' else None)
        r.raise_for_status()
        if len(r.content)>1000000: raise ValueError('接口返回超过1MB限制')
        output=r.json()
        if body.get('output_map'):
            output={target:output[source] for target,source in body['output_map'].items() if source in output}
        return {'status_code':r.status_code,'output':output,'auth_type':auth.get('type'),'credential_refresh':'每次调用从环境重新解析'}
