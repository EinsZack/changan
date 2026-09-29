from pathlib import Path
import os
ROOT = Path(__file__).resolve().parent.parent
# Read simple KEY=value configuration without executing shell content.
if (ROOT/'.env').exists():
    for line in (ROOT/'.env').read_text().splitlines():
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line:continue
        key,value=line.split('=',1)
        if key.strip().startswith(('CHANGAN_','NODE_','WORKER_')):os.environ.setdefault(key.strip(),value.strip().strip('\"').strip("'"))
DATA = Path(os.getenv('CHANGAN_DATA_DIR', str(ROOT / 'data'))).resolve()
DATA.mkdir(parents=True, exist_ok=True)
TOKEN = os.environ.get('CHANGAN_INTERNAL_TOKEN', '')
if not TOKEN:
    tokenfile = DATA / '.internal-token'
    if not tokenfile.exists():
        import secrets
        fd = os.open(tokenfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as f: f.write(secrets.token_urlsafe(32))
    TOKEN = tokenfile.read_text().strip()
API_KEY = os.getenv('CHANGAN_API_KEY', '')
REDIS_URL = os.getenv('CHANGAN_REDIS_URL', 'redis://127.0.0.1:8390/0')
MODEL = ROOT / 'assets/models/yolox_tiny.onnx'
MODEL_ID = 'yolox-tiny-coco-416-v1'
TERMINAL = {'SUCCEEDED', 'FAILED', 'CANCELED', 'TIMED_OUT'}
