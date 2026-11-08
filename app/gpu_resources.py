"""GPU capacity, telemetry freshness and measured inference profiles."""
import json
import time
import math
import re
from . import db
from .network_metrics import positive

TELEMETRY_TTL = 20


def gpu_snapshot(node, reserved):
    annotations = node.get('annotations', {})
    labels = node.get('labels', {})
    try:
        raw = json.loads(annotations.get('changan.io/gpu-telemetry', '{}'))
    except (ValueError, TypeError):
        raw = {}
    if not isinstance(raw,dict):raw={}
    at = positive(raw.get('observed_at'))
    uuid = labels.get('changan.io/gpu-uuid')
    identity = bool(isinstance(uuid,str) and re.fullmatch(r'GPU-[a-fA-F0-9-]{36}',uuid))
    fields = [raw.get(k) for k in ('memory_total_mb','memory_free_mb','memory_used_mb','utilization_percent')]
    numeric = all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>=0 for v in fields)
    valid = bool(identity and at and 0 <= time.time() - at < TELEMETRY_TTL and raw.get('uuid') == uuid and numeric and fields[0]>0 and fields[1]<=fields[0] and fields[2]<=fields[0] and fields[3]<=100)
    allocatable = node.get('gpu_allocatable', 0)
    used = max(node.get('gpu_active', 0), reserved)
    runtime_ready = identity and labels.get('changan.io/cuda-ready') == 'true' and allocatable == 1
    return {'gpu_allocatable': allocatable, 'gpu_available': max(0, allocatable - used),
            'gpu_uuid': uuid, 'gpu_name': raw.get('name') if valid else labels.get('changan.io/gpu-model'),
            'gpu_memory_total_mb': raw.get('memory_total_mb') if valid else None,
            'gpu_memory_free_mb': raw.get('memory_free_mb') if valid else None,
            'gpu_memory_used_mb': raw.get('memory_used_mb') if valid else None,
            'gpu_utilization_percent': raw.get('utilization_percent') if valid else None,
            'gpu_observed_at': at, 'gpu_telemetry_fresh': valid,
            'gpu_runtime_ready': runtime_ready,
            'gpu_scope': labels.get('changan.io/gpu-scope', 'node'),
            'inference_backends': ['cpu'] + (['cuda'] if runtime_ready else [])}


def throughput_profiles(node, model_sha256):
    result = {}
    for row in db.query("SELECT result,workload FROM attempts WHERE node_id=? AND backend='k3s' AND status='SUCCEEDED' ORDER BY finished DESC LIMIT 32", (node['id'],)):
        r, w = db.loads(row['result'], {}), db.loads(row['workload'], {})
        backend = r.get('inference', {}).get('backend', 'cpu')
        fps = positive(r.get('measured_fps'))
        if backend not in result and fps and r.get('model_sha256') == model_sha256 and (not w.get('node_uid') or w['node_uid'] == node['uid']):
            result[backend] = {'fps': fps, 'image': w.get('image'), 'model_sha256': model_sha256}
    return result
