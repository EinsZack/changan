"""Optional resource-pool integration contract. No real pool is claimed until integrated."""
import os,time
import httpx
class ResourceProvider:
    def __init__(self,client):
        self.url=os.getenv('CHANGAN_RESOURCE_PROVIDER_URL','').rstrip('/')
        self.client=client
        token=os.getenv('CHANGAN_RESOURCE_PROVIDER_TOKEN','')
        self.headers={'Authorization':'Bearer '+token} if token else {}
    @property
    def enabled(self):return bool(self.url)
    async def snapshots(self):
        r=await self.client.get(self.url+'/nodes',headers=self.headers);r.raise_for_status()
        rows=r.json()
        if not isinstance(rows,list):raise ValueError('资源池快照必须为数组')
        return {n['node_id']:n for n in rows}
    async def reserve(self,node_id,task_id,attempt_id,requirements,snapshot_version,lease_seconds):
        r=await self.client.post(self.url+'/reservations',headers=self.headers,json={'node_id':node_id,'task_id':task_id,'attempt_id':attempt_id,'idempotency_key':attempt_id,'requirements':requirements,'snapshot_version':snapshot_version,'lease_seconds':lease_seconds})
        r.raise_for_status();return r.json()['reservation_id']
    async def release(self,id):
        r=await self.client.delete(self.url+'/reservations/'+id,headers=self.headers)
        if r.status_code!=404:r.raise_for_status()
