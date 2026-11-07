"""One Pod executes one attempt; no resident worker HTTP service or cluster credentials."""
import hashlib,json,os,signal,time
from pathlib import Path
import httpx
from .inference import Detector,analyze
MODEL_ID='yolox-tiny-coco-416-v1'
stopping=False
def terminate(*_):
    global stopping
    stopping=True
signal.signal(signal.SIGTERM,terminate)
def main():
    p=json.loads(Path('/run/task/payload.json').read_text())
    uid=os.environ['POD_UID'];seq=0
    client=httpx.Client(timeout=30,trust_env=False,headers={'X-Attempt-Token':p['token'],'X-Pod-UID':uid})
    def request(method,url,**kwargs):
        last=None
        for i in range(5):
            if stopping:raise InterruptedError('容器正在停止')
            try:
                r=client.request(method,url,**kwargs);r.raise_for_status();return r
            except httpx.HTTPStatusError as e:
                if e.response.status_code<500:raise
                last=e
            except httpx.TransportError as e:last=e
            time.sleep(min(2**i,5))
        raise last
    def event(state,progress=0,**kwargs):
        nonlocal seq
        seq+=1
        request('POST',p['callback_url'],json={'seq':seq,'status':state,'progress':progress,**kwargs})
    request('POST',p['base_url']+'/claim')
    try:
        event('MODEL_LOADING')
        model=Path('/opt/model/yolox_tiny.onnx')
        if hashlib.sha256(model.read_bytes()).hexdigest()!=p['model_sha256']:raise ValueError('模型摘要不一致')
        detector=Detector(model)
        out=Path('/work');started=time.monotonic()
        r=request('GET',p['input_url']);video=out/'input.mp4';video.write_bytes(r.content)
        download=time.monotonic()-started
        if hashlib.sha256(r.content).hexdigest()!=p['input_sha256']:raise ValueError('输入摘要不一致')
        event('RUNNING');last=0
        def progress(value,frames=None):
            nonlocal last
            if time.monotonic()-last>1:
                event('RUNNING',value);last=time.monotonic()
        result=analyze(detector,video,out,p['params'],progress,lambda:stopping)
        event('UPLOADING',.99)
        artifacts=[]
        for name in result['previews']:
            content=(out/name).read_bytes()
            artifacts.append(request('PUT',p['base_url']+'/artifacts/'+name,content=content,headers={'X-Content-SHA256':hashlib.sha256(content).hexdigest()}).json())
        result.update({'task_id':p['task_id'],'segment_id':p['segment_id'],'attempt_id':p['attempt_id'],
          'actual_node':p['target_node'],'pod_uid':uid,'model_id':MODEL_ID,'artifacts':artifacts,
          'input_sha256':p['input_sha256'],'input_bytes':len(r.content),'download_seconds':download,
          'observed_transfer_mbps':len(r.content)*8/max(download,.000001)/1e6})
        event('FINALIZING',1,result=result)
        print(json.dumps({'attempt_id':p['attempt_id'],'sampled_frames':result['sampled_frames'],'status':'result_persisted'}),flush=True)
    except BaseException as e:
        try:event('FAILED',error=type(e).__name__+': '+str(e)[:500])
        except Exception:pass
        raise
    finally:client.close()
if __name__=='__main__':main()
