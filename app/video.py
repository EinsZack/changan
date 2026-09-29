import cv2, subprocess, hashlib, math
from pathlib import Path
import imageio_ffmpeg

def metadata(path):
    cap=cv2.VideoCapture(str(path))
    try:
        if not cap.isOpened(): raise ValueError('无法读取视频')
        fps=cap.get(cv2.CAP_PROP_FPS); frames=int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if fps<=0 or frames<=0: raise ValueError('视频时长或帧率无效')
        return {'fps':fps,'frames':frames,'duration':frames/fps,'width':int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),'height':int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),'bytes':Path(path).stat().st_size,'sha256':hashlib.sha256(Path(path).read_bytes()).hexdigest()}
    finally: cap.release()
def transcode(source,target,start=0,duration=None):
    cmd=[imageio_ffmpeg.get_ffmpeg_exe(),'-nostdin','-y','-loglevel','error','-ss',str(start),'-i',str(source)]
    if duration is not None: cmd+=['-t',str(duration)]
    cmd+=['-an','-c:v','libx264','-preset','ultrafast','-crf','24','-pix_fmt','yuv420p','-threads','2','-movflags','+faststart',str(target)]
    p=subprocess.run(cmd,capture_output=True,timeout=180)
    if p.returncode: raise ValueError('视频转码失败：'+p.stderr.decode(errors='replace')[-400:])
    return metadata(target)
def split(source,folder,seconds):
    info=metadata(source)
    if info['duration']>600: raise ValueError('中期原型限制单视频不超过10分钟')
    if info['width']*info['height']>3840*2160: raise ValueError('视频分辨率超过4K上限')
    result=[]
    for ordinal in range(math.ceil(info['duration']/seconds)):
        start=ordinal*seconds; end=min((ordinal+1)*seconds,info['duration'])
        if end-start<.01: continue
        target=folder/f'segment-{ordinal:04d}.mp4'
        segment=transcode(source,target,start,end-start)
        result.append({'ordinal':ordinal,'start':start,'end':end,'path':str(target),'metadata':segment})
    return info,result
