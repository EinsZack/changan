#!/usr/bin/env python
"""Lab host NVML telemetry for explicitly labelled shared-server GPU nodes.

Real boards should publish the same fields with a board-local NVML/tegrastats
adapter or the resource-pool service; this host monitor must not claim their GPUs.
"""
import csv,json,subprocess,time,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent.parent
PYTHON=sys.executable


def kube(*args):
    return subprocess.check_output([PYTHON,str(ROOT/'scripts/cluster.py'),'kubectl',*args],text=True,timeout=15)


def sample():
    output=subprocess.check_output(['nvidia-smi','--query-gpu=uuid,name,memory.total,memory.free,memory.used,utilization.gpu','--format=csv,noheader,nounits'],text=True,timeout=5)
    rows={r[0].strip():[x.strip() for x in r] for r in csv.reader(output.splitlines())}
    nodes=json.loads(kube('get','nodes','-l','changan.io/gpu-telemetry=server','-o','json'))['items']
    for node in nodes:
        uuid=node['metadata']['labels'].get('changan.io/gpu-uuid')
        if uuid not in rows:continue
        row=rows[uuid]
        telemetry={'uuid':uuid,'name':row[1],'memory_total_mb':int(row[2]),'memory_free_mb':int(row[3]),'memory_used_mb':int(row[4]),'utilization_percent':int(row[5]),'observed_at':time.time(),'source':'lab host nvidia-smi; shared GPU device memory, not per-container memory'}
        kube('annotate','node',node['metadata']['name'],'changan.io/gpu-telemetry='+json.dumps(telemetry,separators=(',',':')),'--overwrite')


if __name__=='__main__':
    while True:
        try:sample()
        except Exception as e:print('GPU telemetry unavailable:',type(e).__name__,flush=True)
        if '--once' in sys.argv:break
        time.sleep(5)
