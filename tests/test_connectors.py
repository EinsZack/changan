import pytest,httpx,os
from app import db
from app.capabilities import connector_call
from app.resource_provider import ResourceProvider
@pytest.mark.asyncio
async def test_auth_plugin_refresh_and_mapping(monkeypatch):
    import time
    db.execute('INSERT OR REPLACE INTO configs VALUES(?,?,?,?,?,?)',('plugin-test','auth','bearer',db.dumps({'type':'env_bearer','env_name':'CHANGAN_CONNECTOR_TEST'}),time.time(),time.time()))
    observed=[]
    def handler(r):
        observed.append(r.headers['authorization']);return httpx.Response(200,json={'value':5})
    real=httpx.AsyncClient
    monkeypatch.setattr('app.capabilities.httpx.AsyncClient',lambda **kwargs:real(transport=httpx.MockTransport(handler)))
    config={'body':db.dumps({'url':'http://contract.test/api','method':'POST','auth_id':'plugin-test','input_map':{'remote':'local'},'output_map':{'result':'value'}})}
    monkeypatch.setenv('CHANGAN_CONNECTOR_TEST','first');assert (await connector_call(config,{'local':2}))['output']=={'result':5}
    monkeypatch.setenv('CHANGAN_CONNECTOR_TEST','second');await connector_call(config,{'local':3})
    assert observed==['Bearer first','Bearer second']
@pytest.mark.asyncio
async def test_resource_pool_contract(monkeypatch):
    monkeypatch.setenv('CHANGAN_RESOURCE_PROVIDER_URL','http://pool.test')
    calls=[]
    def handler(r):
        calls.append((r.method,r.url.path))
        if r.url.path=='/nodes':return httpx.Response(200,json=[{'node_id':'n','healthy':True,'observed_at':1}])
        if r.method=='POST':return httpx.Response(200,json={'reservation_id':'lease-1'})
        return httpx.Response(204)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as c:
        p=ResourceProvider(c);assert p.enabled
        assert 'n' in await p.snapshots()
        assert await p.reserve('n','t','a',{'slots':1},1,30)=='lease-1'
        await p.release('lease-1')
    assert calls==[('GET','/nodes'),('POST','/reservations'),('DELETE','/reservations/lease-1')]
