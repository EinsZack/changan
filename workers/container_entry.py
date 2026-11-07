"""One task/node session loads its model once and processes successive assignments."""
import hashlib
import json
import os
import shutil
import signal
import threading
import time
from pathlib import Path
import httpx
from .inference import Detector, analyze

MODEL_ID = 'yolox-tiny-coco-416-v1'


class SessionStopped(Exception):
    pass


def run_session(config):
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    uid = os.environ['POD_UID']
    sid = config['session_id']
    session_url = config['base_url'] + '/internal/sessions/' + sid
    headers = {'X-Session-Token': config['session_token'], 'X-Pod-UID': uid}
    client = httpx.Client(timeout=10, trust_env=False)

    def request(method, url, **kwargs):
        last = None
        for i in range(8):
            if stop.is_set():
                raise SessionStopped('会话已停止')
            try:
                r = client.request(method, url, **kwargs)
                if r.status_code in (401, 409):
                    stop.set()
                    raise SessionStopped('执行权已撤销')
                r.raise_for_status()
                return r
            except httpx.HTTPStatusError as e:
                if e.response.status_code < 500:
                    raise
                last = e
            except httpx.TransportError as e:
                last = e
            stop.wait(min(2**i * .25, 2))
        raise last

    def keep_alive():
        # Independent of decoding/inference, including initial model loading.
        last_ok = time.monotonic()
        with httpx.Client(timeout=3, trust_env=False, headers=headers) as heart:
            while not stop.wait(config.get('heartbeat_seconds', 2)):
                try:
                    r = heart.post(session_url + '/heartbeat')
                    if r.status_code in (401, 409):
                        stop.set()
                        return
                    r.raise_for_status()
                    last_ok = time.monotonic()
                except httpx.HTTPError:
                    if time.monotonic() - last_ok > config.get('lease_seconds', 20):
                        stop.set()
                        return

    heartbeat_thread = None
    try:
        request('POST', session_url + '/claim', headers=headers)
        heartbeat_thread = threading.Thread(target=keep_alive, daemon=True)
        heartbeat_thread.start()
        began = time.monotonic()
        model = Path('/opt/model/yolox_tiny.onnx')
        if hashlib.sha256(model.read_bytes()).hexdigest() != config['model_sha256']:
            raise ValueError('模型摘要不一致')
        detector = Detector(model)
        load_seconds = time.monotonic() - began
        request('POST', session_url + '/ready', headers=headers,
                json={'model_sha256': detector.sha256, 'model_load_seconds': load_seconds})
        print(json.dumps({'session_id': sid, 'pod_uid': uid, 'event': 'model_loaded', 'model_load_seconds': load_seconds}), flush=True)
        while not stop.is_set():
            response = request('POST', session_url + '/next', headers=headers).json()
            if response['action'] == 'wait':
                stop.wait(response.get('retry_after', .25))
                continue
            p = response['payload']
            out = Path('/work') / p['attempt_id']
            out.mkdir(parents=True, exist_ok=True)
            attempt_headers = {'X-Attempt-Token': p['token'], 'X-Pod-UID': uid}
            seq = 0

            def event(state, progress=0, **kwargs):
                nonlocal seq
                seq += 1
                return request('POST', p['callback_url'], headers=attempt_headers,
                               json={'seq': seq, 'status': state, 'progress': progress, **kwargs})

            try:
                event('RUNNING')
                began = time.monotonic()
                r = request('GET', p['input_url'], headers=attempt_headers)
                video = out / 'input.mp4'
                video.write_bytes(r.content)
                download = time.monotonic() - began
                if hashlib.sha256(r.content).hexdigest() != p['input_sha256']:
                    raise ValueError('输入摘要不一致')
                last_progress = 0

                def progress(value, frames=None):
                    nonlocal last_progress
                    if time.monotonic() - last_progress > 1:
                        event('RUNNING', value)
                        last_progress = time.monotonic()

                result = analyze(detector, video, out, p['params'], progress, stop.is_set)
                event('UPLOADING', .99)
                artifacts = []
                for name in result['previews']:
                    content = (out / name).read_bytes()
                    h = attempt_headers | {'X-Content-SHA256': hashlib.sha256(content).hexdigest()}
                    artifacts.append(request('PUT', p['base_url'] + '/artifacts/' + name, content=content, headers=h).json())
                result.update({'task_id': p['task_id'], 'segment_id': p['segment_id'], 'attempt_id': p['attempt_id'],
                               'session_id': sid, 'actual_node': p['target_node'], 'pod_uid': uid, 'model_id': MODEL_ID,
                               'artifacts': artifacts, 'input_sha256': p['input_sha256'], 'input_bytes': len(r.content),
                               'download_seconds': download, 'observed_transfer_mbps': len(r.content) * 8 / max(download, .000001) / 1e6})
                event('FINALIZING', 1, result=result)
                print(json.dumps({'session_id': sid, 'attempt_id': p['attempt_id'], 'pod_uid': uid, 'event': 'segment_completed', 'sampled_frames': result['sampled_frames']}), flush=True)
            except (SessionStopped, InterruptedError):
                raise SessionStopped('执行会话停止')
            except Exception as error:
                # Input/inference failures are segment failures. A healthy model/session stays reusable.
                event('FAILED', error=type(error).__name__ + ': ' + str(error)[:500])
            finally:
                # Segments never accumulate in emptyDir during a long session.
                shutil.rmtree(out, ignore_errors=True)
    except SessionStopped:
        pass
    finally:
        stop.set()
        if heartbeat_thread:
            heartbeat_thread.join(timeout=4)
        client.close()


def main():
    config = json.loads(Path('/run/task/payload.json').read_text())
    if config.get('execution_mode') != 'task_session':
        from .attempt_entry import main as legacy_main
        return legacy_main()
    run_session(config)


if __name__ == '__main__':
    main()
