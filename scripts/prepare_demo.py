from pathlib import Path
import sys,json,hashlib
ROOT=Path(__file__).resolve().parent.parent;sys.path.insert(0,str(ROOT))
from app.video import transcode
raw=ROOT/'assets/samples/vtest.avi';out=ROOT/'assets/samples/demo.mp4'
meta=transcode(raw,out,0,24)
manifest={'video':{'path':str(out.relative_to(ROOT)),'source':'https://github.com/opencv/opencv/blob/master/samples/data/vtest.avi','note':'OpenCV仓库测试视频的前24秒，测试用途；商用或对外交付素材使用权需由项目确认','metadata':meta},'model':{'path':'assets/models/yolox_tiny.onnx','source':'https://github.com/Megvii-BaseDetection/YOLOX/releases/download/0.1.1rc0/yolox_tiny.onnx','license':'Apache-2.0','sha256':hashlib.sha256((ROOT/'assets/models/yolox_tiny.onnx').read_bytes()).hexdigest()}}
(ROOT/'assets/manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
print(json.dumps(meta,ensure_ascii=False))
