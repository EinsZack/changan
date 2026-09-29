from pathlib import Path
import hashlib,json
root=Path(__file__).resolve().parent.parent
for relative,expected in json.loads((root/'assets/checksums.json').read_text()).items():
    path=root/relative
    if not path.exists():
        if relative.startswith('vendor/') and (root/'vendor/bin/redis-server').exists():continue
        raise SystemExit('缺少资产：'+relative)
    actual=hashlib.sha256(path.read_bytes()).hexdigest()
    if actual!=expected:raise SystemExit('资产校验值不一致，请核对来源版本：'+relative)
    print('SHA256 verified:',relative)
