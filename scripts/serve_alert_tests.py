import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

def main():
    temp_root = '/dev/shm' if Path('/dev/shm').is_dir() else None
    with tempfile.TemporaryDirectory(prefix='changan-alert-e2e-', dir=temp_root) as directory:
        os.environ['CHANGAN_DATA_DIR'] = directory
        os.environ['CHANGAN_INTERNAL_TOKEN'] = 'isolated-browser-test'
        os.environ['CHANGAN_API_KEY'] = ''
        os.environ['CHANGAN_RESOURCE_PROVIDER_URL'] = ''
        os.environ['CHANGAN_PUBLIC_BASE'] = 'http://127.0.0.1:18310'
        os.environ['CHANGAN_WEB_DIR'] = str(ROOT / 'tmp/alerts-web-dist')
        from app import db
        from app.config import DATA
        db.init()
        db.execute("UPDATE nodes SET enabled=0,endpoint='http://127.0.0.1:9'")
        uploads = DATA / 'uploads'
        uploads.mkdir()
        (uploads / 'upload_alerttest.mp4').write_bytes(b'invalid video for failure validation')
        import uvicorn
        uvicorn.run('app.main:app', host='127.0.0.1', port=18310, log_level='warning')

if __name__ == '__main__':
    main()
