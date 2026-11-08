"""Executed only inside the benchmark Pod; compare two backends in the same image."""
import base64,json,time,statistics
from pathlib import Path
import cv2,numpy as np
from workers.inference import Detector
payload=json.loads(Path('/bench/inputs.json').read_text())
frames=[cv2.imdecode(np.frombuffer(base64.b64decode(v),np.uint8),cv2.IMREAD_COLOR) for v in payload['frames']]
report={'input_sha256':payload['input_sha256'],'frame_indices':payload['indices'],'frames_per_round':len(frames),'rounds':3,'backends':{}}
outputs={}
for backend in ['cpu','cuda']:
    began=time.perf_counter();d=Detector('/opt/model/yolox_tiny.onnx',backend,2048,payload['gpu_uuid'] if backend=='cuda' else None)
    loaded=time.perf_counter()-began;samples=[];run_times=[]
    for _ in range(3):
        previous=d.inference_seconds;began=time.perf_counter()
        values=[d.detect(frame,.35) for frame in frames]
        samples.append(time.perf_counter()-began);run_times.append(d.inference_seconds-previous)
    outputs[backend]=values
    report['backends'][backend]={'model_load_and_warmup_seconds':loaded,'detect_seconds':samples,'session_run_seconds':run_times,'median_session_run_ms_per_frame':1000*statistics.median(run_times)/len(frames),'median_detect_ms_per_frame':1000*statistics.median(samples)/len(frames),'runtime':d.execution}
    del d

def iou(a,b):
    x1,y1,x2,y2=a;u1,v1,u2,v2=b
    inter=max(0,min(x2,u2)-max(x1,u1))*max(0,min(y2,v2)-max(y1,v1))
    return inter/max((x2-x1)*(y2-y1)+(u2-u1)*(v2-v1)-inter,1e-9)
errors=[];ious=[];confidence=[]
for index,(cpu,gpu) in enumerate(zip(outputs['cpu'],outputs['cuda'])):
    if len(cpu)!=len(gpu):errors.append({'frame':index,'cpu_count':len(cpu),'cuda_count':len(gpu)})
    remaining=list(gpu)
    for detection in cpu:
        candidates=[v for v in remaining if v['label']==detection['label']]
        if not candidates:errors.append({'frame':index,'missing':detection['label']});continue
        best=max(candidates,key=lambda v:iou(v['box'],detection['box']));remaining.remove(best)
        ious.append(iou(best['box'],detection['box']));confidence.append(abs(best['confidence']-detection['confidence']))
report['numerical_comparison']={'passed':not errors and min(ious,default=1)>=.99 and max(confidence,default=0)<=.001,'minimum_box_iou':min(ious,default=1),'maximum_confidence_difference':max(confidence,default=0),'errors':errors,'tolerance':{'box_iou_min':.99,'confidence_absolute_max':.001},'cpu_detections':sum(map(len,outputs['cpu'])),'cuda_detections':sum(map(len,outputs['cuda']))}
report['detected_frames']=outputs
print(json.dumps(report),flush=True)
